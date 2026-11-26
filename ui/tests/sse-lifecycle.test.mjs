// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  deferred,
  findClickHandler,
  harness,
  installFakeTimers,
  jsonResponse,
  nextTick,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as api from "../api.js";
import * as app from "../app.js";
import * as stateMod from "../state.js";

describe("SSE lifecycle, preemption, and DB epoch isolation", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("regression_sse_unpause_reconnect_resync_and_cleared_db_non_recreation", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const db2Root = "projects/p/databases/db2/documents";
    let databases = [db1Root];
    let db1Docs = ["alice"];
    /** @type {string[]} */
    const getDbCalls = [];
    let db1ListCalls = 0;

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse(databases);
      if (url === "/v1/projects/p/databases/db1") {
        getDbCalls.push("db1");
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db2") {
        getDbCalls.push("db2");
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["orders"] });
      }
      if (url.startsWith("/v1/projects/p/databases/db1/documents/users?")) {
        db1ListCalls++;
        return jsonResponse({
          documents: db1Docs.map((id) => ({
            name: `${db1Root}/users/${id}`,
            createTime: "2026-01-01T00:00:00Z",
            updateTime: "2026-01-01T00:00:00Z",
          })),
        });
      }
      throw new Error("unexpected url: " + url);
    };

    window.location.pathname = "/projects/p/databases/db1/documents/users";
    await app.init();
    const es = harness.lastEventSource;
    assert.ok(es, "EventSource must be connected on init");
    assert.equal(getDbCalls.length, 1);
    assert.equal(db1ListCalls, 1);

    // 1. Initial "open" during/after init() must NOT trigger an extra resync.
    es.emit("open");
    await app.flushSseQueue();
    assert.equal(getDbCalls.length, 1, "initial open must not trigger resync");
    assert.equal(db1ListCalls, 1, "initial open must not re-list collection");

    // 2. Transition "error" -> "live" must enqueue a full active-DB resync via sseQueue.
    db1Docs = ["alice", "bob"];
    es.emit("error");
    assert.equal(app.state.sseStatus, "error");
    es.emit("open");
    assert.equal(app.state.sseStatus, "live");
    await app.flushSseQueue();

    assert.equal(
      getDbCalls.length,
      2,
      "reconnect error -> live must refresh metadata"
    );
    assert.equal(
      db1ListCalls,
      2,
      "reconnect error -> live must refresh expanded column"
    );
    assert.deepEqual(
      app.state.collectionColumns
        .get(`${db1Root}/users`)
        ?.documents.map((d) => d.id),
      ["alice", "bob"]
    );

    // 3. Pausing must NOT trigger resync, and reconnect while paused must be ignored.
    const pauseBtn = findClickHandler(app.renderTopbar(), "Live");
    assert.ok(pauseBtn);
    await pauseBtn();
    assert.equal(app.state.ssePaused, true);

    db1Docs = ["alice", "bob", "carol"];
    es.emit("error");
    es.emit("open");
    await app.flushSseQueue();
    assert.equal(
      db1ListCalls,
      2,
      "paused state must suppress resync on reconnect"
    );

    // 4. Unpausing triggers full resync via sseQueue when DB still exists.
    const resumeBtn = findClickHandler(app.renderTopbar(), "Live Paused");
    assert.ok(resumeBtn);
    await resumeBtn();
    assert.equal(app.state.ssePaused, false);
    assert.deepEqual(
      app.state.collectionColumns
        .get(`${db1Root}/users`)
        ?.documents.map((d) => d.id),
      ["alice", "bob", "carol"]
    );

    // 5a. When active DB disappeared and another DB exists, switch cleanly without getDatabase(db1).
    const getDbCountBeforeSwitch = getDbCalls.length;
    databases = [db2Root];
    await app.handleSseBatch([{ type: "resync" }]);
    assert.equal(
      getDbCalls.slice(getDbCountBeforeSwitch).includes("db1"),
      false,
      "must never call getDatabase(db1) when db1 is absent after resync"
    );
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.deepEqual(app.state.rootCollections, ["orders"]);

    // 5b. When active DB disappeared on unpause and NO DBs remain, clear active state without recreating db2.
    const pauseBtn2 = findClickHandler(app.renderTopbar(), "Live");
    assert.ok(pauseBtn2);
    await pauseBtn2();
    databases = [];
    const getDbCountBeforeClear = getDbCalls.length;
    const resumeBtn2 = findClickHandler(app.renderTopbar(), "Live Paused");
    assert.ok(resumeBtn2);
    await resumeBtn2();
    assert.equal(
      getDbCalls.length,
      getDbCountBeforeClear,
      "must never call getDatabase(db2) when db2 is absent on unpause resync"
    );
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.equal(window.location.pathname, "/");
  });

  test("regression_sse_resync_error_and_inflight_select_database_guard", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const dbNewRoot = "projects/p/databases/db_new/documents";
    let failDbList = false;

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        if (failDbList) {
          return jsonResponse(
            {
              error: { code: 503, status: "UNAVAILABLE", message: "db list down" },
            },
            503
          );
        }
        return jsonResponse([db1Root]);
      }
      if (url === "/v1/projects/p/databases/db1") {
        if (failDbList) {
          throw new Error("must not call getDatabase when refreshDatabaseList failed");
        }
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        if (failDbList) {
          throw new Error(
            "must not call listCollectionIds when refreshDatabaseList failed"
          );
        }
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected url: " + url);
    };

    await app.selectDatabase("p", "db1", [], true);
    assert.equal(app.state.activeDatabaseId, "db1");
    assert.ok(app.state.activeDbMeta);
    assert.deepEqual(app.state.rootCollections, ["users"]);

    // 1. Transient /emulator/v1 error during resync notifies error toast and preserves active DB state.
    failDbList = true;
    app.state.toasts = [];
    await app.handleSseBatch([{ type: "resync" }]);
    assert.equal(app.state.activeProjectId, "p");
    assert.equal(app.state.activeDatabaseId, "db1");
    assert.ok(
      app.state.activeDbMeta,
      "activeDbMeta must not be cleared on refreshDatabaseList error"
    );
    assert.deepEqual(app.state.rootCollections, ["users"]);
    assert.equal(app.state.databases.length, 1);
    assert.ok(
      app.state.toasts.some(
        (t) => t.tone === "error" && t.text.includes("db list down")
      ),
      "expected error toast on refreshDatabaseList failure"
    );

    app.state.toasts = [];
    app.state.ssePaused = true;
    await app.toggleSsePaused();
    assert.equal(app.state.activeDatabaseId, "db1");
    assert.ok(
      app.state.toasts.some(
        (t) => t.tone === "error" && t.text.includes("db list down")
      ),
      "expected error toast when unpause resync fails"
    );

    // 2. In-flight selectDatabase for a newly created DB must NOT be mistaken for cleared on SSE resync.
    failDbList = false;
    const dbNewMetaDef = deferred();
    const dbNewColsDef = deferred();
    let dbNewGetCount = 0;
    let includeDbNewInList = false;

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        return jsonResponse(
          includeDbNewInList ? [db1Root, dbNewRoot] : [db1Root]
        );
      }
      if (url === "/v1/projects/p/databases/db_new") {
        dbNewGetCount++;
        return dbNewMetaDef.promise;
      }
      if (
        url === "/v1/projects/p/databases/db_new/documents:listCollectionIds"
      ) {
        return dbNewColsDef.promise;
      }
      throw new Error("unexpected url in inflight check: " + url);
    };

    app.state.toasts = [];
    const selectPromise = app.selectDatabase("p", "db_new", [], true);
    assert.equal(app.state.rootLoading, true);
    assert.equal(app.state.activeDbMeta, null);

    await app.handleSseBatch([{ type: "resync" }]);
    assert.equal(app.state.activeProjectId, "p");
    assert.equal(app.state.activeDatabaseId, "db_new");
    assert.equal(
      window.location.pathname,
      "/projects/p/databases/db_new/documents"
    );
    assert.equal(
      dbNewGetCount,
      1,
      "handleSseBatch must not duplicate getDatabase while rootLoading is true"
    );
    assert.equal(
      app.state.toasts.some((t) => t.text.includes("was cleared")),
      false,
      "in-flight selectDatabase must never emit cleared-DB toast"
    );

    includeDbNewInList = true;
    dbNewMetaDef.resolve(
      jsonResponse({
        name: "projects/p/databases/db_new",
        uid: "u_new",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "ENTERPRISE",
        concurrencyMode: "OPTIMISTIC",
      })
    );
    dbNewColsDef.resolve(jsonResponse({ collectionIds: ["items"] }));
    await selectPromise;

    assert.equal(app.state.rootLoading, false);
    assert.equal(app.state.activeDatabaseId, "db_new");
    assert.equal(
      /** @type {import('../api.js').DatabaseMetadata | null} */ (
        app.state.activeDbMeta
      )?.name,
      "projects/p/databases/db_new"
    );
    assert.deepEqual(app.state.rootCollections, ["items"]);

    // 3. Even when rootLoading is false, null activeDbMeta is not treated as cleared.
    app.state.activeDbMeta = null;
    app.state.toasts = [];
    includeDbNewInList = false;
    await app.handleSseBatch([{ type: "resync" }]);
    assert.equal(app.state.activeDatabaseId, "db_new");
    assert.equal(
      app.state.toasts.some((t) => t.text.includes("was cleared")),
      false
    );
  });

  test("slice2_sse_debounce_fifo_flush_and_sync_preemption", async () => {
    const rootPath = "projects/p/databases/db1/documents";

    // 1. connectEventStream: immediate FIFO flush of database_cleared & resync, plus MAX_WAIT_MS = 1000.
    const timers = installFakeTimers();
    try {
      /** @type {Array<import('../api.js').EmulatorSseEvent[]>} */
      const batches = [];
      const esHandle = api.connectEventStream(
        (b) => batches.push(b),
        () => {}
      );
      const es = harness.lastEventSource;
      assert.ok(es);

      es.emit("change", {
        data: JSON.stringify({
          type: "documents_changed",
          database: rootPath,
          paths: ["a"],
        }),
      });
      assert.equal(batches.length, 0);
      assert.equal(
        timers.scheduled.size,
        2,
        "debounce (150ms) and maxWait (1000ms) timers must both be armed"
      );
      es.emit("change", {
        data: JSON.stringify({ type: "database_cleared", database: rootPath }),
      });
      assert.equal(batches.length, 1);
      assert.deepEqual(
        batches[0].map((e) => e.type),
        ["documents_changed", "database_cleared"]
      );
      assert.equal(
        timers.scheduled.size,
        0,
        "all timers cleared on urgent flush"
      );

      es.emit("change", {
        data: JSON.stringify({
          type: "documents_changed",
          database: rootPath,
          paths: ["b"],
        }),
      });
      es.emit("resync", { data: JSON.stringify({ type: "resync" }) });
      assert.equal(batches.length, 2);
      assert.deepEqual(
        batches[1].map((e) => e.type),
        ["documents_changed", "resync"]
      );
      assert.equal(timers.scheduled.size, 0);

      es.emit("change", {
        data: JSON.stringify({
          type: "documents_changed",
          database: rootPath,
          paths: ["c1"],
        }),
      });
      const maxEntry = Array.from(timers.scheduled.entries()).find(
        ([, t]) => t.ms === 1000
      );
      assert.ok(maxEntry, "expected 1000ms maxWait timer");
      es.emit("change", {
        data: JSON.stringify({
          type: "documents_changed",
          database: rootPath,
          paths: ["c2"],
        }),
      });
      assert.equal(
        timers.scheduled.has(maxEntry[0]),
        true,
        "maxWait timer must NOT be reset on subsequent events"
      );
      await timers.advanceTimers(1000);
      assert.equal(batches.length, 3);
      assert.deepEqual(
        batches[2].map((e) => e.paths?.[0]),
        ["c1", "c2"]
      );
      assert.equal(timers.scheduled.size, 0);
      esHandle.close();
    } finally {
      timers.restore();
    }

    // 2. applySyncSseBatch preempts in-flight applyAsyncSseRefresh & active Explorer reads via getDbSignal().
    /** @type {string[]} */
    const fetched = [];
    /** @type {string[]} */
    const abortedUrls = [];
    const slowDb1Meta = deferred();
    const slowDb1Cols = deferred();

    harness.fetchImpl = async (url, init) => {
      fetched.push(url);
      if (init.signal && init.signal instanceof AbortSignal) {
        init.signal.addEventListener("abort", () => abortedUrls.push(url));
      }
      if (url === "/emulator/v1") {
        return jsonResponse([rootPath, "projects/p/databases/db2/documents"]);
      }
      if (url === "/v1/projects/p/databases/db1") return slowDb1Meta.promise;
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return slowDb1Cols.promise;
      }
      if (url === "/v1/projects/p/databases/db2") {
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: [] });
      }
      throw new Error("unexpected url in 2A: " + url);
    };

    const pDb1 = app.selectDatabase("p", "db1", [], true);
    const pDb2 = app.selectDatabase("p", "db2", [], true);
    assert.ok(abortedUrls.includes("/v1/projects/p/databases/db1"));
    assert.ok(
      abortedUrls.includes("/v1/projects/p/databases/db1/documents:listCollectionIds")
    );
    const abortErrA = new Error("Aborted");
    abortErrA.name = "AbortError";
    slowDb1Meta.reject(abortErrA);
    slowDb1Cols.reject(abortErrA);
    await pDb1;
    await pDb2;
    assert.equal(
      app.state.toasts.some((t) => t.tone === "error"),
      false
    );

    fetched.length = 0;
    abortedUrls.length = 0;
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "db1";
    app.state.activeDbMeta = {
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    app.state.segments = ["users", "alice"];
    app.state.rootCollections = ["users"];
    app.state.querySuggestions.fieldsByScope.set('["","users",false]', {
      items: [],
      aggItems: [],
      fieldHints: new Map(),
      scope: { parentSubpath: "", collectionId: "users", allDescendants: false },
      loading: false,
      stale: false,
      partial: false,
      sampledDocs: 1,
    });

    const inflightGetDoc = deferred();
    const inflightDocCols = deferred();
    /** @type {Array<{ url: string, signal: unknown }>} */
    const inflightSignals = [];
    harness.fetchImpl = async (url, init) => {
      fetched.push(url);
      if (init.signal) inflightSignals.push({ url, signal: init.signal });
      if (url === `/v1/${rootPath}:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url.startsWith(`/v1/${rootPath}/users?`)) {
        return jsonResponse({
          documents: [{ name: `${rootPath}/users/alice` }],
        });
      }
      if (url === `/v1/${rootPath}/users/alice`) return inflightGetDoc.promise;
      if (url === `/v1/${rootPath}/users/alice:listCollectionIds`) {
        return inflightDocCols.promise;
      }
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error("unexpected url in preemption test: " + url);
    };

    const plan1 = app.applySyncSseBatch([
      {
        type: "documents_changed",
        database: rootPath,
        paths: [`${rootPath}/users/alice`],
      },
    ]);
    assert.ok(plan1);
    assert.equal(
      app.state.querySuggestions.fieldsByScope.get('["","users",false]')?.stale,
      true
    );

    const pRefresh1 = app.applyAsyncSseRefresh(plan1);
    for (
      let tick = 0;
      tick < 5 && !inflightSignals.some((s) => s.url.endsWith("/users/alice"));
      tick++
    ) {
      await nextTick();
    }
    assert.ok(inflightSignals.some((s) => s.url.endsWith(":listCollectionIds")));
    assert.ok(inflightSignals.some((s) => s.url.includes("/users?")));
    assert.ok(inflightSignals.some((s) => s.url.endsWith("/users/alice")));

    const fetchedBeforeClear = fetched.length;
    const plan2 = app.applySyncSseBatch([
      {
        type: "database_cleared",
        database: rootPath,
      },
    ]);
    assert.ok(plan2);
    assert.equal(
      plan1.signal.aborted,
      true,
      "applySyncSseBatch must abort prior DB signal synchronously"
    );
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.equal(app.state.querySuggestions.fieldsByScope.size, 0);

    const abortErr = new Error("Aborted");
    abortErr.name = "AbortError";
    inflightGetDoc.reject(abortErr);
    inflightDocCols.reject(abortErr);
    await pRefresh1;
    await app.applyAsyncSseRefresh(plan2);

    const postClearV1Calls = fetched
      .slice(fetchedBeforeClear)
      .filter((u) => u.startsWith("/v1/"));
    assert.deepEqual(
      postClearV1Calls,
      [],
      "zero post-notification /v1 calls allowed against cleared DB"
    );
    assert.equal(
      app.state.toasts.some((t) => t.tone === "error"),
      false
    );

    app.state.ssePaused = true;
    assert.equal(app.applySyncSseBatch([{ type: "resync" }]), null);
    assert.equal(app.state.ssePaused, true);
    app.state.ssePaused = false;

    // 3. toggleSsePaused unpausing marks suggestions stale, and onOpenQueryCollection preserves caches.
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "db1";
    app.state.activeDbMeta = {
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    app.state.querySuggestions.fieldsByScope.set('["","orders",false]', {
      items: [],
      aggItems: [],
      fieldHints: new Map(),
      scope: { parentSubpath: "", collectionId: "orders", allDescendants: false },
      loading: false,
      stale: false,
      partial: false,
      sampledDocs: 2,
    });
    app.state.ssePaused = true;
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([rootPath]);
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse(app.state.activeDbMeta);
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["orders"] });
      }
      throw new Error("unexpected url on unpause: " + url);
    };
    await app.toggleSsePaused();
    assert.equal(
      app.state.querySuggestions.fieldsByScope.get('["","orders",false]')?.stale,
      true
    );

    app.state.view = "explorer";
    app.state.rootCollections = ["orders"];
    app.state.segments = ["orders"];
    app.state.collectionColumns = new Map([
      [
        `${rootPath}/orders`,
        {
          collectionPath: `${rootPath}/orders`,
          collectionId: "orders",
          parentPath: rootPath,
          documents: [],
          nextPageToken: "",
          filterText: "",
          loading: false,
        },
      ],
    ]);
    const queryBtn = findClickHandler(app.renderWorkspace(), "Query");
    assert.ok(queryBtn);
    queryBtn();
    assert.equal(app.state.view, "query");
    assert.equal(app.state.queryState.collectionId, "orders");
    assert.equal(
      app.state.querySuggestions.fieldsByScope.has('["","orders",false]'),
      true,
      "onOpenQueryCollection must preserve suggestion cache"
    );
  });

  test("thread_8: project-wide DB list refresh and empty-selection auto-select survive dbEpoch bump", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const db2Root = "projects/p/databases/db2/documents";

    // Part A: Active DB cleared while resync GET /emulator/v1 is in flight -> auto-selects newly created db2.
    const listDefA = deferred();
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return listDefA.promise;
      if (url === "/v1/projects/p/databases/db2") {
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["items"] });
      }
      throw new Error("unexpected url in thread_8 A: " + url);
    };

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "db1";
    app.state.activeDbMeta = {
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    const planA = app.applySyncSseBatch([{ type: "resync" }]);
    assert.ok(planA);
    const refreshA = app.applyAsyncSseRefresh(planA);

    // Clear active DB before GET /emulator/v1 settles, bumping dbEpoch (planA becomes stale).
    stateMod.bumpDbEpoch();
    stateMod.clearActiveDatabaseState(app.state);
    assert.equal(app.state.activeProjectId, null);

    listDefA.resolve(jsonResponse([db2Root]));
    await refreshA;

    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db2"]
    );
    assert.equal(app.state.activeProjectId, "p");
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.deepEqual(app.state.rootCollections, ["items"]);

    // Part B: User switches to db2 while db1's resync GET /emulator/v1 is in flight -> updates list without clearing db2.
    const listDefB = deferred();
    let listCallB = 0;
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        listCallB++;
        return listCallB === 1
          ? listDefB.promise
          : jsonResponse([db2Root]);
      }
      if (url === "/v1/projects/p/databases/db2") {
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["items"] });
      }
      throw new Error("unexpected url in thread_8 B: " + url);
    };

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "db1";
    app.state.activeDbMeta = {
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    app.state.toasts = [];
    const planB = app.applySyncSseBatch([{ type: "resync" }]);
    assert.ok(planB);
    const refreshB = app.applyAsyncSseRefresh(planB);

    // Switch to db2 (bumps dbEpoch), note selectDatabase calls refreshDatabaseList (#2) which bumps dbListSeq!
    // Use direct epoch bump + manual state switch to verify list update applies when dbListSeq wasn't superseded.
    stateMod.bumpDbEpoch();
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "db2";
    app.state.activeDbMeta = {
      name: "projects/p/databases/db2",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    listDefB.resolve(jsonResponse([db2Root, db1Root.replace("db1", "db3")]));
    await refreshB;

    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db2", "db3"]
    );
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.equal(
      app.state.toasts.some((t) => t.text.includes("was cleared")),
      false,
      "stale planB must not trigger cleared-DB cleanup for old db1"
    );
  });

  test("thread_8: state.ssePaused stops already-queued SSE batches", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const listDef1 = deferred();
    let listCalls = 0;
    /** @type {string[]} */
    const fetched = [];

    harness.fetchImpl = async (url) => {
      fetched.push(url);
      if (url === "/emulator/v1") {
        listCalls++;
        return listCalls === 1 ? listDef1.promise : jsonResponse([db1Root]);
      }
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected url in thread_8 pause test: " + url);
    };

    // Initialize so EventSource is wired into enqueueSseBatch.
    const initP = app.init();
    await nextTick();
    listDef1.resolve(jsonResponse([db1Root]));
    await initP;

    const es = harness.lastEventSource;
    assert.ok(es);
    fetched.length = 0;

    // Batch 1 starts and awaits GET /emulator/v1; Batch 2 is queued right behind it on sseQueue.
    const batch1ListDef = deferred();
    harness.fetchImpl = async (url) => {
      fetched.push(url);
      if (url === "/emulator/v1") return batch1ListDef.promise;
      throw new Error("queued batch must not fetch after ssePaused=true: " + url);
    };

    es.emit("resync", { data: JSON.stringify({ type: "resync" }) });
    await nextTick();
    assert.deepEqual(fetched, ["/emulator/v1"]);

    es.emit("resync", { data: JSON.stringify({ type: "resync" }) });

    // User pauses SSE while Batch 1 is awaiting /emulator/v1 and Batch 2 is already queued in sseQueue.
    app.state.ssePaused = true;
    batch1ListDef.resolve(jsonResponse([db1Root]));
    await app.flushSseQueue();

    // Batch 1 paused check after refreshDatabaseList stops before /v1 calls,
    // and Batch 2 paused check at start of applyAsyncSseRefresh stops before /emulator/v1!
    assert.deepEqual(fetched, ["/emulator/v1"]);

    // Also verify applyAsyncSseRefresh(plan) returns immediately if state.ssePaused is true before start.
    fetched.length = 0;
    app.state.ssePaused = false;
    const plan = app.applySyncSseBatch([
      {
        type: "documents_changed",
        database: db1Root,
        paths: [`${db1Root}/users/alice`],
      },
    ]);
    assert.ok(plan);
    app.state.ssePaused = true;
    await app.applyAsyncSseRefresh(plan);
    assert.deepEqual(fetched, []);
  });

  test("thread_9: resetDatabaseCaches clears rootLoading when selectDatabase is aborted", async () => {
    const metaDef = deferred();
    const colsDef = deferred();
    const sigCapture = { signal: /** @type {AbortSignal | null} */ (null) };

    harness.fetchImpl = async (url, init) => {
      if (url === "/v1/projects/p/databases/db1") {
        sigCapture.signal = /** @type {AbortSignal} */ (init.signal);
        return metaDef.promise;
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return colsDef.promise;
      }
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error("unexpected url: " + url);
    };

    // Part A: Direct resetDatabaseCaches() while selectDatabase is in flight.
    const pSelectA = app.selectDatabase("p", "db1", [], true);
    assert.equal(app.state.rootLoading, true);
    assert.ok(sigCapture.signal);
    assert.equal(sigCapture.signal.aborted, false);

    stateMod.bumpDbEpoch();
    stateMod.resetDatabaseCaches();
    assert.equal(sigCapture.signal.aborted, true);
    assert.equal(
      app.state.rootLoading,
      false,
      "resetDatabaseCaches must clear rootLoading immediately"
    );

    const abortErr = new Error("Aborted");
    abortErr.name = "AbortError";
    metaDef.reject(abortErr);
    colsDef.reject(abortErr);
    await pSelectA;
    assert.equal(app.state.rootLoading, false);

    // Part B: SSE database_cleared preemption while selectDatabase is in flight.
    const metaDefB = deferred();
    const colsDefB = deferred();
    harness.fetchImpl = async (url) => {
      if (url === "/v1/projects/p/databases/db2") return metaDefB.promise;
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return colsDefB.promise;
      }
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error("unexpected url in B: " + url);
    };

    const pSelectB = app.selectDatabase("p", "db2", [], true);
    assert.equal(app.state.rootLoading, true);

    await app.handleSseBatch([
      {
        type: "database_cleared",
        database: "projects/p/databases/db2/documents",
      },
    ]);
    assert.equal(
      app.state.rootLoading,
      false,
      "database_cleared SSE must clear rootLoading via clearActiveDatabaseState/resetDatabaseCaches"
    );

    metaDefB.reject(abortErr);
    colsDefB.reject(abortErr);
    await pSelectB;
    assert.equal(app.state.rootLoading, false);
  });

  test("f2_database_cleared_invalidates_inflight_refresh_database_list_for_active_db", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const db2Root = "projects/p/databases/db2/documents";

    // Part A: Active DB (only DB) is cleared while a prior SSE refreshDatabaseList() is in flight.
    // The stale pre-clear GET /emulator/v1 response must NOT repopulate state.databases or
    // auto-call selectDatabase(db1) (which would lazily resurrect the cleared DB).
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([db1Root]);
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected init url: " + url);
    };

    window.location.pathname = "/projects/p/databases/db1/documents";
    await app.init();
    const es = harness.lastEventSource;
    assert.ok(es);
    assert.equal(app.state.activeDatabaseId, "db1");
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"]
    );

    const preClearListDef = deferred();
    const postClearListDef = deferred();
    let listCalls = 0;
    /** @type {string[]} */
    const postClearV1Calls = [];

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        listCalls++;
        if (listCalls === 1) return preClearListDef.promise;
        if (listCalls === 2) return postClearListDef.promise;
        return jsonResponse([]);
      }
      if (url.startsWith("/v1/")) {
        postClearV1Calls.push(url);
      }
      throw new Error("must not issue /v1 fetch for cleared db1: " + url);
    };

    // Plan 1 starts via SSE resync and awaits GET /emulator/v1 (pre-clear).
    es.emit("resync", { data: JSON.stringify({ type: "resync" }) });
    await nextTick();
    assert.equal(listCalls, 1);

    // Plan 2 (database_cleared for active db1) arrives synchronously before Plan 1 settles.
    es.emit("change", {
      data: JSON.stringify({ type: "database_cleared", database: db1Root }),
    });
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      []
    );
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);

    // Stale pre-clear response resolves with [db1Root] while Plan 2's post-clear fetch is pending.
    preClearListDef.resolve(jsonResponse([db1Root]));
    for (let i = 0; i < 6 && listCalls < 2; i++) {
      await nextTick();
    }
    assert.equal(listCalls, 2, "queued post-clear refresh must run after plan1");
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      [],
      "stale pre-clear GET /emulator/v1 must not repopulate state.databases"
    );
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.deepEqual(
      postClearV1Calls,
      [],
      "stale pre-clear refresh must not call selectDatabase and resurrect cleared db1"
    );

    // Post-clear refresh resolves cleanly with empty list.
    postClearListDef.resolve(jsonResponse([]));
    await app.flushSseQueue();
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      []
    );
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.deepEqual(postClearV1Calls, []);

    // Part B: Active db1 is cleared while another db2 exists -> stale [db1, db2] must be ignored
    // (never selecting db1) while queued post-clear refresh [db2] applies and auto-selects db2.
    await resetHarness(app, stateMod);
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([db1Root, db2Root]);
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected init B url: " + url);
    };

    window.location.pathname = "/projects/p/databases/db1/documents";
    await app.init();
    const esB = harness.lastEventSource;
    assert.ok(esB);

    const preClearListB = deferred();
    const postClearListB = deferred();
    let listCallsB = 0;
    /** @type {string[]} */
    const v1CallsB = [];

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        listCallsB++;
        if (listCallsB === 1) return preClearListB.promise;
        if (listCallsB === 2) return postClearListB.promise;
        return jsonResponse([db2Root]);
      }
      if (url.startsWith("/v1/")) {
        v1CallsB.push(url);
      }
      if (url === "/v1/projects/p/databases/db2") {
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["orders"] });
      }
      throw new Error("unexpected B url: " + url);
    };

    esB.emit("resync", { data: JSON.stringify({ type: "resync" }) });
    await nextTick();
    assert.equal(listCallsB, 1);

    esB.emit("change", {
      data: JSON.stringify({ type: "database_cleared", database: db1Root }),
    });
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db2"]
    );
    assert.equal(app.state.activeDatabaseId, null);

    preClearListB.resolve(jsonResponse([db1Root, db2Root]));
    for (let i = 0; i < 6 && listCallsB < 2; i++) {
      await nextTick();
    }
    assert.equal(listCallsB, 2);
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db2"],
      "stale [db1, db2] must not overwrite post-clear state.databases"
    );
    assert.equal(
      v1CallsB.some((u) => u.includes("/databases/db1")),
      false,
      "must never request cleared db1"
    );

    postClearListB.resolve(jsonResponse([db2Root]));
    await app.flushSseQueue();
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.deepEqual(app.state.rootCollections, ["orders"]);
    assert.equal(
      v1CallsB.some((u) => u.includes("/databases/db1")),
      false
    );
  });

  test("f2_database_cleared_for_non_active_db_and_rapid_batches_invalidate_stale_list", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const db2Root = "projects/p/databases/db2/documents";
    const db3Root = "projects/p/databases/db3/documents";

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([db1Root, db2Root]);
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected init url: " + url);
    };

    window.location.pathname = "/projects/p/databases/db1/documents";
    await app.init();
    const es = harness.lastEventSource;
    assert.ok(es);
    assert.equal(app.state.activeDatabaseId, "db1");
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1", "db2"]
    );

    // 1. Non-active db2 is cleared while a prior GET /emulator/v1 is in flight.
    const preClearDef = deferred();
    const postClearDef = deferred();
    let listCalls = 0;
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        listCalls++;
        if (listCalls === 1) return preClearDef.promise;
        if (listCalls === 2) return postClearDef.promise;
        return jsonResponse([db1Root, db3Root]);
      }
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse(app.state.activeDbMeta);
      }
      if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      throw new Error("unexpected url: " + url);
    };

    es.emit("resync", { data: JSON.stringify({ type: "resync" }) });
    await nextTick();
    assert.equal(listCalls, 1);

    // Non-active db2 cleared: synchronously filtered out and dbListSeq bumped.
    es.emit("change", {
      data: JSON.stringify({ type: "database_cleared", database: db2Root }),
    });
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"]
    );
    assert.equal(app.state.activeDatabaseId, "db1");

    // Pre-clear response still includes db2 -> must be ignored without even transiently restoring db2.
    preClearDef.resolve(jsonResponse([db1Root, db2Root]));
    for (let i = 0; i < 8 && listCalls < 2; i++) {
      await nextTick();
    }
    assert.equal(listCalls, 2);
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"],
      "stale pre-clear response must not restore cleared non-active db2"
    );

    // Post-clear refresh applies cleanly (including newly appearing db3).
    postClearDef.resolve(jsonResponse([db1Root, db3Root]));
    await app.flushSseQueue();
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1", "db3"]
    );
    assert.equal(app.state.activeDatabaseId, "db1");

    // 2. database_created + database_cleared in the same batch AND rapid back-to-back clears
    // never drop the final legitimate post-clear refresh.
    const clear1ListDef = deferred();
    const clear2ListDef = deferred();
    let rapidListCalls = 0;
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        rapidListCalls++;
        if (rapidListCalls === 1) return clear1ListDef.promise;
        return clear2ListDef.promise;
      }
      throw new Error("unexpected rapid url: " + url);
    };

    const batchPlan1 = app.applySyncSseBatch([
      { type: "database_created", database: db2Root },
      { type: "database_cleared", database: db3Root },
    ]);
    assert.ok(batchPlan1);
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"]
    );

    const pBatch1 = app.applyAsyncSseRefresh(batchPlan1);
    await nextTick();
    assert.equal(rapidListCalls, 1);

    // Rapid second clear (db2) supersedes batchPlan1's in-flight refresh.
    const batchPlan2 = app.applySyncSseBatch([
      { type: "database_cleared", database: db2Root },
    ]);
    assert.ok(batchPlan2);

    // First refresh returns stale [db1, db2] -> ignored!
    clear1ListDef.resolve(jsonResponse([db1Root, db2Root]));
    await pBatch1;
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"]
    );

    // Second (final) refresh applies cleanly with [db1].
    const pBatch2 = app.applyAsyncSseRefresh(batchPlan2);
    clear2ListDef.resolve(jsonResponse([db1Root]));
    await pBatch2;
    assert.equal(rapidListCalls, 2);
    assert.deepEqual(
      app.state.databases.map((d) => d.databaseId),
      ["db1"]
    );
  });
});
