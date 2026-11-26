// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  DomElement,
  MockEventSource,
  deferred,
  findClickHandler,
  harness,
  installFakeIntervals,
  installFakeTimers,
  jsonResponse,
  nextTick,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as app from "../app.js";
import {
  createContentionState,
  renderContention,
  updateWaiterElapsedDom,
} from "../components/contention.js";
import { render } from "../lit-html.js";
import * as stateMod from "../state.js";

describe("Lock contention visualizer regressions", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("regression_contention_sse_lifecycle_and_routing", async () => {
    assert.deepEqual(
      app.parseBrowserRoute("/projects/p/databases/(default)/contention"),
      { projectId: "p", databaseId: "(default)", segments: [], view: "contention" }
    );
    assert.deepEqual(
      app.parseBrowserRoute("/projects/p/databases/(default)/contention/"),
      { projectId: "p", databaseId: "(default)", segments: [], view: "contention" }
    );
    assert.equal(
      app.parseBrowserRoute("/projects/../databases/(default)/contention"),
      null
    );
    assert.equal(
      app.parseBrowserRoute("/projects/p/databases/(default)/contention/extra"),
      null
    );
    assert.deepEqual(
      app.parseBrowserRoute(
        "/projects/p/databases/(default)/documents/users/alice"
      ),
      { projectId: "p", databaseId: "(default)", segments: ["users", "alice"] }
    );

    const rootPath = "projects/p/databases/(default)/documents";
    let contentionFetches = 0;
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") {
        return jsonResponse([rootPath]);
      }
      if (url === "/v1/projects/p/databases/(default)") {
        return jsonResponse({
          name: "projects/p/databases/(default)",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {
        return jsonResponse({ documents: [] });
      }
      if (url.startsWith("/emulator/v1/contention")) {
        contentionFetches++;
        return jsonResponse({
          databases: [
            {
              database: rootPath,
              summary: {
                woundsTotal: 0,
                timeoutsTotal: 0,
                upgradeAbortsTotal: 0,
                writeBatchRetriesTotal: 0,
              },
              contendedDocuments: [],
              totalContendedDocuments: 0,
              truncated: false,
              recentEvents: [],
            },
          ],
        });
      }
      throw new Error("unexpected url: " + url);
    };

    await app.init();
    assert.equal(app.state.view, "explorer");
    assert.equal(app.state.contentionSseStatus, "idle");
    assert.equal(
      harness.eventSources.filter((s) => s.url === "/emulator/v1/events").length,
      1
    );
    assert.equal(
      harness.eventSources.filter(
        (s) => s.url === "/emulator/v1/contention/events"
      ).length,
      0
    );
    assert.equal(contentionFetches, 0);

    const contentionTab = findClickHandler(app.renderTopbar(), "Contention");
    assert.ok(contentionTab, "expected Contention button in topbar");
    await contentionTab();
    assert.equal(app.state.view, "contention");
    assert.equal(
      window.location.pathname,
      "/projects/p/databases/(default)/contention"
    );
    assert.equal(contentionFetches, 1);
    let contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 1);
    assert.equal(contentionStreams[0].closed, false);
    assert.equal(app.state.contentionSseStatus, "connecting");

    contentionStreams[0].emit("open");
    assert.equal(app.state.contentionSseStatus, "live");
    assert.equal(
      contentionFetches,
      1,
      "initial SSE open must not double-fetch"
    );

    const queryTab = findClickHandler(app.renderTopbar(), "Query Runner");
    assert.ok(queryTab);
    await queryTab();
    assert.equal(app.state.view, "query");
    assert.equal(contentionStreams[0].closed, true);
    assert.equal(contentionStreams[0].closeCount, 1);
    assert.equal(app.state.contentionSseStatus, "idle");
    assert.equal(
      window.location.pathname,
      "/projects/p/databases/(default)/documents"
    );

    await app.switchView("contention");
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 2);
    assert.equal(contentionStreams[1].closed, false);
    assert.equal(contentionFetches, 2);

    contentionStreams[1].emit("error");
    assert.equal(app.state.contentionSseStatus, "error");
    contentionStreams[1].emit("open");
    await nextTick();
    assert.equal(app.state.contentionSseStatus, "live");
    assert.equal(
      contentionFetches,
      3,
      "error -> live reconnect must refresh contention"
    );

    await app.toggleSsePaused();
    assert.equal(app.state.ssePaused, true);
    assert.equal(contentionStreams[1].closed, true);
    assert.equal(app.state.contentionSseStatus, "idle");

    await app.toggleSsePaused();
    assert.equal(app.state.ssePaused, false);
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 3);
    assert.equal(contentionStreams[2].closed, false);
    assert.equal(contentionFetches, 4);

    harness.visibilityState = "hidden";
    for (const fn of harness.docVisibilityListeners) {
      await fn();
    }
    await nextTick();
    assert.equal(contentionStreams[2].closed, true);
    assert.equal(app.state.contentionSseStatus, "idle");

    await app.switchView("explorer");
    await app.switchView("contention");
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(
      contentionStreams.length,
      3,
      "must not open contention SSE while document is hidden"
    );

    harness.visibilityState = "visible";
    for (const fn of harness.docVisibilityListeners) {
      await fn();
    }
    await nextTick();
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 4);
    assert.equal(contentionStreams[3].closed, false);
    assert.equal(contentionFetches, 6);

    window.location.pathname = "/projects/p/databases/(default)/documents/users";
    for (const fn of harness.popstateListeners) {
      fn();
    }
    await nextTick();
    assert.equal(app.state.view, "explorer");
    assert.equal(contentionStreams[3].closed, true);
    assert.deepEqual(app.state.segments, ["users"]);

    await app.switchView("contention");
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 5);
    const fetchesBeforeVisOutside = contentionFetches;
    await app.switchView("explorer");
    assert.equal(contentionStreams[4].closed, true);
    harness.visibilityState = "hidden";
    await app.handleVisibilityChange();
    harness.visibilityState = "visible";
    await app.handleVisibilityChange();
    assert.equal(
      contentionFetches,
      fetchesBeforeVisOutside,
      "visibilitychange outside contention must not fetch contention"
    );

    app.state.activeDatabaseId = null;
    await app.switchView("contention");
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(
      contentionStreams.length,
      5,
      "must require both activeProjectId and activeDatabaseId"
    );
    app.state.activeDatabaseId = "(default)";
    await app.switchView("contention");
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(contentionStreams.length, 6);

    // 10. Unpausing SSE when enqueueSseBatch switches DB avoids duplicate refreshContention().
    await app.toggleSsePaused();
    assert.equal(contentionStreams[5].closed, true);
    const origFetch = harness.fetchImpl;
    const db2Root = "projects/p/databases/db2/documents";
    let db2ContentionFetches = 0;
    harness.fetchImpl = async (url, init) => {
      if (url === "/emulator/v1") return jsonResponse([db2Root]);
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
      if (url.startsWith("/emulator/v1/contention")) {
        db2ContentionFetches++;
        return jsonResponse({ databases: [] });
      }
      return origFetch(url, init);
    };
    await app.toggleSsePaused();
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.equal(
      db2ContentionFetches,
      1,
      "unpause resync DB switch must refresh contention exactly once"
    );

    // 11. Synchronous EventSource constructor throw leaves contentionSseStatus === "error" cleanly.
    await app.switchView("explorer");
    harness.fetchImpl = origFetch;
    let esThrowAttempts = 0;
    harness.eventSourceFactory = (url) => {
      if (url === "/emulator/v1/contention/events") {
        esThrowAttempts++;
        throw new Error("EventSource disabled");
      }
      return new MockEventSource(url);
    };
    await app.switchView("contention");
    assert.equal(app.state.contentionSseStatus, "error");
    assert.equal(
      esThrowAttempts,
      1,
      "synchronous EventSource throw must not retry in renderApp loop"
    );
    harness.eventSourceFactory = null;
    await app.switchView("explorer");

    // 12. Paused SSE suppresses automatic visibilitychange refreshContention while preserving manual Refresh.
    await app.switchView("contention");
    await app.toggleSsePaused();
    assert.equal(app.state.ssePaused, true);
    const fetchesWhilePausedBeforeVis = contentionFetches;
    harness.visibilityState = "hidden";
    for (const fn of harness.docVisibilityListeners) {
      await fn();
    }
    harness.visibilityState = "visible";
    for (const fn of harness.docVisibilityListeners) {
      await fn();
    }
    assert.equal(
      contentionFetches,
      fetchesWhilePausedBeforeVis,
      "visibilitychange while ssePaused must not automatically refresh contention"
    );
    contentionStreams = harness.eventSources.filter(
      (s) => s.url === "/emulator/v1/contention/events"
    );
    assert.equal(
      contentionStreams.every((s) => s.closed),
      true,
      "visibilitychange while ssePaused must keep contention SSE closed"
    );
    const manualRefreshBtn = findClickHandler(app.renderWorkspace(), "Refresh");
    assert.ok(
      manualRefreshBtn,
      "expected manual Refresh button while SSE paused"
    );
    await manualRefreshBtn();
    assert.equal(
      contentionFetches,
      fetchesWhilePausedBeforeVis + 1,
      "manual Refresh must still fetch contention while ssePaused"
    );
    await app.toggleSsePaused();

    // 13. Switching DB via selectDatabase while in Contention view sets loading=true immediately.
    const db3Root = "projects/p/databases/db3/documents";
    const db3MetaDef = deferred();
    const db3ColsDef = deferred();
    harness.fetchImpl = async (url, init) => {
      if (url === "/emulator/v1") {
        return jsonResponse([rootPath, db2Root, db3Root]);
      }
      if (url === "/v1/projects/p/databases/db3") return db3MetaDef.promise;
      if (url === "/v1/projects/p/databases/db3/documents:listCollectionIds") {
        return db3ColsDef.promise;
      }
      if (url.startsWith("/emulator/v1/contention")) {
        return jsonResponse({ databases: [] });
      }
      return origFetch(url, init);
    };
    const selectDb3Promise = app.selectDatabase("p", "db3", [], true);
    assert.equal(
      app.state.contention.loading,
      true,
      "selectDatabase in contention view must set loading=true immediately"
    );
    assert.equal(app.state.contention.snapshot, null);
    const pendingDbSwitchMarkup = renderTemplateMarkup(app.renderWorkspace());
    assert.ok(
      pendingDbSwitchMarkup.includes("Loading contention snapshot..."),
      "pending DB switch in contention view must show loading state"
    );
    assert.equal(
      pendingDbSwitchMarkup.includes("No active lock contention"),
      false,
      "pending DB switch in contention view must not flash empty contention copy"
    );
    db3MetaDef.resolve(
      jsonResponse({
        name: "projects/p/databases/db3",
        uid: "u3",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      })
    );
    db3ColsDef.resolve(jsonResponse({ collectionIds: [] }));
    await selectDb3Promise;
    assert.equal(app.state.contention.loading, false);
    await app.switchView("explorer");
  });

  test("regression_contention_debounced_refetch_and_stale_guard", async () => {
    const timers = installFakeTimers();
    try {
      const rootPath = "projects/p/databases/(default)/documents";
      let contentionFetches = 0;
      /** @type {() => Promise<unknown>} */
      let nextSnapshotFn = async () =>
        jsonResponse({
          databases: [
            {
              database: rootPath,
              summary: {
                woundsTotal: 0,
                timeoutsTotal: 0,
                upgradeAbortsTotal: 0,
                writeBatchRetriesTotal: 0,
              },
              contendedDocuments: [],
              totalContendedDocuments: 0,
              truncated: false,
              recentEvents: [],
            },
          ],
        });

      harness.fetchImpl = async (url) => {
        if (url === "/emulator/v1") return jsonResponse([rootPath]);
        if (url === "/v1/projects/p/databases/(default)") {
          return jsonResponse({
            name: "projects/p/databases/(default)",
            uid: "u1",
            type: "FIRESTORE_NATIVE",
            databaseEdition: "STANDARD",
            concurrencyMode: "PESSIMISTIC",
          });
        }
        if (
          url ===
          "/v1/projects/p/databases/(default)/documents:listCollectionIds"
        ) {
          return jsonResponse({ collectionIds: [] });
        }
        if (url.startsWith("/emulator/v1/contention")) {
          contentionFetches++;
          return nextSnapshotFn();
        }
        throw new Error("unexpected url: " + url);
      };

      window.location.pathname = "/projects/p/databases/(default)/documents";
      await app.init();
      await app.switchView("contention");
      const es = harness.eventSources.find(
        (s) => s.url === "/emulator/v1/contention/events"
      );
      assert.ok(es);
      es.emit("open");
      assert.equal(contentionFetches, 1);

      // 1. Rapid burst coalesces into 1 refetch after 200ms.
      es.emit("contention", {
        data: JSON.stringify({ type: "contention_changed", database: rootPath }),
      });
      await timers.advanceTimers(40);
      es.emit("contention", {
        data: JSON.stringify({ type: "contention_changed", database: rootPath }),
      });
      await timers.advanceTimers(40);
      es.emit("contention", {
        data: JSON.stringify({ type: "database_cleared", database: rootPath }),
      });
      es.emit("resync", {
        data: JSON.stringify({ type: "resync", skipped: 2 }),
      });
      es.emit("contention", {
        data: JSON.stringify({ type: "contention_changed", database: rootPath }),
      });

      await timers.advanceTimers(199);
      assert.equal(
        contentionFetches,
        1,
        "debounced burst must not fetch before 200ms trailing quiet window"
      );

      await timers.advanceTimers(1);
      assert.equal(
        contentionFetches,
        2,
        "rapid burst must coalesce into exactly 1 GET /emulator/v1/contention at 200ms"
      );

      // 2. Sustained >5Hz contention events still flush at maxWaitMs=1000.
      for (let step = 0; step < 9; step++) {
        es.emit("contention", {
          data: JSON.stringify({
            type: "contention_changed",
            database: rootPath,
          }),
        });
        await timers.advanceTimers(100);
      }
      assert.equal(
        contentionFetches,
        2,
        "must not flush before maxWaitMs=1000 while events arrive every 100ms"
      );
      es.emit("contention", {
        data: JSON.stringify({ type: "contention_changed", database: rootPath }),
      });
      await timers.advanceTimers(100);
      assert.equal(
        contentionFetches,
        3,
        "maxWaitMs=1000 must force refresh under continuous >5Hz contention"
      );
      await timers.advanceTimers(250);
      assert.equal(
        contentionFetches,
        3,
        "flushing at maxWaitMs must clear trailing timer cleanly"
      );

      // 3. Events for another DB or non-object payloads do not trigger fetches.
      es.emit("contention", { data: "null" });
      es.emit("contention", { data: '"str"' });
      es.emit("contention", {
        data: JSON.stringify({
          type: "contention_changed",
          database: "projects/p/databases/other/documents",
        }),
      });
      await timers.advanceTimers(1200);
      assert.equal(
        contentionFetches,
        3,
        "unrelated DB or non-object payloads must not trigger fetches"
      );

      // 4. Stale in-flight GET response must NOT overwrite newer snapshot.
      const slowFirst = deferred();
      const fastSecond = deferred();
      let seqMode = 0;
      nextSnapshotFn = () => {
        seqMode++;
        return seqMode === 1 ? slowFirst.promise : fastSecond.promise;
      };
      const p1 = app.refreshContention();
      const p2 = app.refreshContention();
      fastSecond.resolve(
        jsonResponse({
          databases: [
            {
              database: rootPath,
              summary: {
                woundsTotal: 7,
                timeoutsTotal: 0,
                upgradeAbortsTotal: 0,
                writeBatchRetriesTotal: 0,
              },
              contendedDocuments: [],
              totalContendedDocuments: 0,
              truncated: false,
              recentEvents: [],
            },
          ],
        })
      );
      await p2;
      slowFirst.resolve(
        jsonResponse({
          databases: [
            {
              database: rootPath,
              summary: {
                woundsTotal: 1,
                timeoutsTotal: 0,
                upgradeAbortsTotal: 0,
                writeBatchRetriesTotal: 0,
              },
              contendedDocuments: [],
              totalContendedDocuments: 0,
              truncated: false,
              recentEvents: [],
            },
          ],
        })
      );
      await p1;
      assert.equal(app.state.contention.snapshot?.databases[0].summary.woundsTotal, 7);

      // 5. Leaving view before debounce/maxWait timers fire clears timers cleanly.
      nextSnapshotFn = async () => jsonResponse({ databases: [] });
      const fetchesBeforeLeave = contentionFetches;
      es.emit("contention", {
        data: JSON.stringify({ type: "contention_changed", database: rootPath }),
      });
      await app.switchView("explorer");
      assert.equal(es.closed, true);
      await timers.advanceTimers(1500);
      assert.equal(
        contentionFetches,
        fetchesBeforeLeave,
        "closing contention SSE must cancel pending debounce and maxWait timers"
      );
    } finally {
      timers.restore();
    }
  });

  test("regression_contention_dom_only_waiter_ticker_lifecycle", async () => {
    const intervals = installFakeIntervals();
    const origDateNow = Date.now;
    let mockNow = 1_700_000_000_000;
    Date.now = () => mockNow;

    try {
      const waitNode = {
        attrs: /** @type {Record<string, string>} */ ({
          "data-wait-base-ms": "160",
          "data-wait-ms": "160",
          "data-wait-label": "160ms",
        }),
        textContent: "160ms",
        getAttribute(/** @type {string} */ name) {
          return this.attrs[name] ?? null;
        },
        setAttribute(/** @type {string} */ name, /** @type {unknown} */ val) {
          this.attrs[name] = String(val);
        },
      };
      let querySelectorCalls = 0;
      const mockWorkspaceEl = {
        querySelectorAll(/** @type {string} */ sel) {
          if (sel === "[data-wait-base-ms]") {
            querySelectorCalls++;
            return [waitNode];
          }
          return [];
        },
      };
      harness.getElementById = (id) =>
        id === "workspace" ? mockWorkspaceEl : null;

      const rootPath = "projects/p/databases/(default)/documents";
      const otherRoot = "projects/p/databases/other/documents";
      /** @type {Array<Record<string, unknown>>} */
      let currentWaiters = [];

      harness.fetchImpl = async (url) => {
        if (url === "/emulator/v1") return jsonResponse([rootPath, otherRoot]);
        if (url === "/v1/projects/p/databases/(default)") {
          return jsonResponse({
            name: "projects/p/databases/(default)",
            uid: "u1",
            type: "FIRESTORE_NATIVE",
            databaseEdition: "STANDARD",
            concurrencyMode: "PESSIMISTIC",
          });
        }
        if (url === "/v1/projects/p/databases/other") {
          return jsonResponse({
            name: "projects/p/databases/other",
            uid: "u2",
            type: "FIRESTORE_NATIVE",
            databaseEdition: "STANDARD",
            concurrencyMode: "PESSIMISTIC",
          });
        }
        if (url.endsWith(":listCollectionIds")) {
          return jsonResponse({ collectionIds: [] });
        }
        if (url.startsWith("/emulator/v1/contention")) {
          const dbName = url.includes("other") ? otherRoot : rootPath;
          return jsonResponse({
            databases: [
              {
                database: dbName,
                summary: {
                  woundsTotal: 0,
                  timeoutsTotal: 0,
                  upgradeAbortsTotal: 0,
                  writeBatchRetriesTotal: 0,
                },
                contendedDocuments:
                  currentWaiters.length > 0
                    ? [
                        {
                          document: `${dbName}/c/d1`,
                          writer: null,
                          readers: [],
                          readerCount: 1,
                          waiters: currentWaiters,
                          waiterCount: currentWaiters.length,
                        },
                      ]
                    : [],
                totalContendedDocuments: currentWaiters.length > 0 ? 1 : 0,
                truncated: false,
                recentEvents: [],
              },
            ],
          });
        }
        throw new Error("unexpected url: " + url);
      };

      await app.init();

      await app.switchView("contention");
      assert.equal(
        intervals.startedIntervals.length,
        0,
        "no ticker interval when waiters list is empty"
      );

      currentWaiters = [
        {
          sessionId: 7,
          txnId: "1:1",
          kind: "PESSIMISTIC_TXN",
          seq: 3,
          mode: "WRITE",
          waitedMs: 160,
        },
      ];
      await app.refreshContention();
      assert.equal(intervals.startedIntervals.length, 1);
      assert.equal(intervals.startedIntervals[0].ms, 1000);
      assert.equal(intervals.activeIntervals.size, 1);

      const tickId = intervals.startedIntervals[0].id;
      const tickFn = intervals.activeIntervals.get(tickId)?.fn;
      assert.ok(tickFn);
      const snapRefBeforeTicks = app.state.contention.snapshot;
      mockNow += 1000;
      tickFn();
      assert.equal(querySelectorCalls, 1);
      assert.equal(waitNode.attrs["data-wait-ms"], "1160");
      assert.equal(waitNode.attrs["data-wait-label"], "1.2s");
      assert.equal(waitNode.textContent, "1.2s");

      mockNow += 1000;
      tickFn();
      assert.equal(waitNode.attrs["data-wait-ms"], "2160");
      assert.equal(waitNode.attrs["data-wait-label"], "2.2s");
      assert.equal(waitNode.textContent, "2.2s");
      assert.equal(app.state.contention.snapshot, snapRefBeforeTicks);
      assert.equal(
        app.state.contention.snapshot?.databases[0].contendedDocuments[0].waiters[0]
          .waitedMs,
        160
      );

      currentWaiters = [];
      await app.refreshContention();
      assert.equal(
        intervals.activeIntervals.size,
        0,
        "interval must clear when waiters drop to 0"
      );

      currentWaiters = [
        {
          sessionId: 8,
          txnId: null,
          kind: "SINGLE_WRITE",
          seq: 4,
          mode: "WRITE",
          waitedMs: 25,
        },
      ];
      await app.refreshContention();
      assert.equal(intervals.activeIntervals.size, 1);

      harness.visibilityState = "hidden";
      for (const fn of harness.docVisibilityListeners) {
        await fn();
      }
      assert.equal(
        intervals.activeIntervals.size,
        0,
        "interval must clear when tab becomes hidden"
      );

      harness.visibilityState = "visible";
      for (const fn of harness.docVisibilityListeners) {
        await fn();
      }
      assert.equal(
        intervals.activeIntervals.size,
        1,
        "interval must restart when tab becomes visible with waiters"
      );

      const clearedBeforeDbSwitch = intervals.clearedIntervals.length;
      await app.selectDatabase("p", "other");
      assert.ok(
        intervals.clearedIntervals.length > clearedBeforeDbSwitch,
        "database change must clear previous ticker"
      );
      assert.equal(intervals.activeIntervals.size, 1);

      await app.switchView("query");
      assert.equal(
        intervals.activeIntervals.size,
        0,
        "leaving contention view must clear ticker interval"
      );
    } finally {
      Date.now = origDateNow;
      intervals.restore();
    }
  });

  test("regression_contention_clear_history_open_doc_and_rendering", async () => {
    const rootA = "projects/p/databases/(default)/documents";
    const rootB = "projects/other/databases/db2/documents";
    const nestedDoc = `${rootA}/rooms/r1/messages/m1`;
    const crossDbDoc = `${rootB}/orders/o1`;

    /** @type {string[]} */
    const deleteCalls = [];
    let clearedOnce = false;

    harness.fetchImpl = async (url, init) => {
      if (url === "/emulator/v1") return jsonResponse([rootA, rootB]);
      if (url === "/v1/projects/p/databases/(default)") {
        return jsonResponse({
          name: "projects/p/databases/(default)",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/other/databases/db2") {
        return jsonResponse({
          name: "projects/other/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url.endsWith(":listCollectionIds")) {
        if (url.includes("/rooms/r1:")) {
          return jsonResponse({ collectionIds: ["messages"] });
        }
        if (url.includes("/documents:")) {
          return jsonResponse({ collectionIds: ["rooms", "orders"] });
        }
        return jsonResponse({ collectionIds: [] });
      }
      if (url.includes("/documents/rooms?")) {
        return jsonResponse({ documents: [{ name: `${rootA}/rooms/r1` }] });
      }
      if (url.includes("/documents/rooms/r1/messages?")) {
        return jsonResponse({
          documents: [
            {
              name: nestedDoc,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (url === `/v1/${nestedDoc}`) {
        return jsonResponse({
          name: nestedDoc,
          fields: { body: { stringValue: "hi" } },
        });
      }
      if (url.includes("/documents/orders?")) {
        return jsonResponse({
          documents: [
            {
              name: crossDbDoc,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (url === `/v1/${crossDbDoc}`) {
        return jsonResponse({
          name: crossDbDoc,
          fields: { total: { integerValue: "42" } },
        });
      }
      if (
        url.startsWith("/emulator/v1/contention") &&
        init.method === "DELETE"
      ) {
        deleteCalls.push(url);
        clearedOnce = true;
        return jsonResponse({});
      }
      if (url.startsWith("/emulator/v1/contention")) {
        return jsonResponse({
          databases: [
            {
              database: rootA,
              summary: clearedOnce
                ? {
                    woundsTotal: 0,
                    timeoutsTotal: 0,
                    upgradeAbortsTotal: 0,
                    writeBatchRetriesTotal: 0,
                  }
                : {
                    woundsTotal: 3,
                    timeoutsTotal: 2,
                    upgradeAbortsTotal: 1,
                    writeBatchRetriesTotal: 4,
                  },
              contendedDocuments: [
                {
                  document: nestedDoc,
                  writer: {
                    sessionId: 11,
                    txnId: "11:1",
                    kind: "PESSIMISTIC_TXN",
                    seq: 1,
                    state: "ACTIVE",
                    woundable: false,
                  },
                  readers: [
                    {
                      sessionId: 12,
                      txnId: "12:1",
                      kind: "PESSIMISTIC_TXN",
                      seq: 2,
                      state: "ACTIVE",
                      woundable: true,
                      retained: true,
                      activeLeases: 1,
                    },
                  ],
                  readerCount: 1,
                  waiters: [
                    {
                      sessionId: 13,
                      txnId: "13:1",
                      kind: "PESSIMISTIC_TXN",
                      seq: 3,
                      mode: "WRITE",
                      waitedMs: 250,
                    },
                  ],
                  waiterCount: 1,
                },
              ],
              totalContendedDocuments: 1,
              truncated: false,
              recentEvents: clearedOnce
                ? []
                : [
                    {
                      id: 2,
                      kind: "WOUNDED",
                      timestamp: "2026-11-23T12:00:01Z",
                      docName: nestedDoc,
                      sessionId: 14,
                      txnId: "14:1",
                      sessionKind: "PESSIMISTIC_TXN",
                      seq: 5,
                      wounderSessionId: 11,
                      wounderTxnId: "11:1",
                      wounderKind: "PESSIMISTIC_TXN",
                      wounderSeq: 1,
                    },
                    {
                      id: 1,
                      kind: "WRITE_BATCH_RETRY",
                      timestamp: "2026-11-23T12:00:00Z",
                      docName: null,
                      sessionId: 15,
                      txnId: null,
                      sessionKind: "WRITE_BATCH",
                      seq: 6,
                      attempt: 1,
                    },
                  ],
            },
          ],
        });
      }
      throw new Error("unexpected url: " + url);
    };

    window.location.pathname = "/projects/p/databases/(default)/documents";
    await app.init();
    await app.switchView("contention");

    const topbarMarkup = renderTemplateMarkup(app.renderTopbar());
    assert.ok(topbarMarkup.includes("Contention"));
    assert.ok(topbarMarkup.includes('aria-pressed="true"'));

    const markup = renderTemplateMarkup(app.renderWorkspace());
    assert.ok(markup.includes("Wounds") && markup.includes(">3<"));
    assert.ok(markup.includes("Timeouts") && markup.includes(">2<"));
    assert.ok(markup.includes("Upgrade Aborts") && markup.includes(">1<"));
    assert.ok(markup.includes("WriteBatch Retries") && markup.includes(">4<"));
    assert.equal(markup.includes("slowWaitsTotal"), false);
    assert.equal(markup.includes("SLOW_WAIT"), false);
    assert.equal(markup.includes("OptimisticWriteSkew"), false);
    assert.ok(markup.includes('data-wait-base-ms="250"'));
    assert.ok(markup.includes("data-wait-ms="));
    assert.ok(
      /data-wait-label="\d+ms"\s*><\/span>/.test(markup),
      ".wait-elapsed must remain empty (no ChildPart inside)"
    );
    assert.ok(
      markup.includes('aria-label="Clear history (p/(default))"'),
      "WCAG 2.5.3 Label in Name"
    );
    assert.ok(markup.includes("retained"));
    assert.ok(markup.includes("wounded by"));
    assert.ok(markup.includes("WOUNDED"));
    assert.ok(markup.includes("WRITE_BATCH_RETRY"));
    assert.ok(markup.includes("attempt 1"));

    const clearBtn = findClickHandler(app.renderWorkspace(), "Clear history");
    assert.ok(clearBtn, "expected Clear history button");
    await clearBtn();
    assert.deepEqual(deleteCalls, [
      "/emulator/v1/contention?database=projects%2Fp%2Fdatabases%2F%28default%29%2Fdocuments",
    ]);
    assert.equal(app.state.contention.clearing, false);
    assert.equal(app.state.contention.snapshot?.databases[0].summary.woundsTotal, 0);
    assert.equal(
      app.state.contention.snapshot?.databases[0].contendedDocuments[0].waiters
        .length,
      1
    );

    const delCountBeforeGuards = deleteCalls.length;
    await app.switchView("explorer");
    await app.clearContentionHistory();
    assert.equal(
      deleteCalls.length,
      delCountBeforeGuards,
      "clearContentionHistory must no-op outside contention view"
    );
    await app.switchView("contention");

    const savedDbId = app.state.activeDatabaseId;
    app.state.activeDatabaseId = null;
    await app.clearContentionHistory();
    assert.equal(
      deleteCalls.length,
      delCountBeforeGuards,
      "clearContentionHistory must no-op when activeDatabaseId is null"
    );
    app.state.activeDatabaseId = savedDbId;

    const stalePreClearGet = deferred();
    const baseFetch = harness.fetchImpl;
    harness.fetchImpl = async (url, init) => {
      if (
        url.startsWith("/emulator/v1/contention") &&
        (init.method ?? "GET") === "GET"
      ) {
        if (!clearedOnce) return stalePreClearGet.promise;
      }
      return baseFetch(url, init);
    };
    clearedOnce = false;
    const preClearRefresh = app.refreshContention();
    await app.clearContentionHistory();
    stalePreClearGet.resolve(
      jsonResponse({
        databases: [
          {
            database: rootA,
            summary: {
              woundsTotal: 99,
              timeoutsTotal: 0,
              upgradeAbortsTotal: 0,
              writeBatchRetriesTotal: 0,
            },
            contendedDocuments: [],
            totalContendedDocuments: 0,
            truncated: false,
            recentEvents: [],
          },
        ],
      })
    );
    await preClearRefresh;
    assert.equal(
      app.state.contention.snapshot?.databases[0].summary.woundsTotal,
      0,
      "stale pre-clear GET must be ignored"
    );
    harness.fetchImpl = baseFetch;

    const allScopeBtn = findClickHandler(app.renderWorkspace(), "All Databases");
    assert.ok(allScopeBtn);
    const deferredAllGet = deferred();
    harness.fetchImpl = async (url, init) => {
      if (
        url === "/emulator/v1/contention" &&
        (init.method ?? "GET") === "GET"
      ) {
        return deferredAllGet.promise;
      }
      return baseFetch(url, init);
    };
    const scopePromise = allScopeBtn();
    assert.equal(app.state.contention.scope, "all");
    assert.equal(
      app.state.contention.snapshot,
      null,
      "scope switch must clear stale snapshot before fetch resolves"
    );
    deferredAllGet.resolve(
      baseFetch("/emulator/v1/contention", { method: "GET" })
    );
    await scopePromise;
    harness.fetchImpl = baseFetch;

    const clearAllBtn = findClickHandler(app.renderWorkspace(), "Clear history");
    assert.ok(clearAllBtn);
    await clearAllBtn();
    assert.equal(
      deleteCalls[deleteCalls.length - 1],
      "/emulator/v1/contention"
    );

    const docBtn = findClickHandler(app.renderWorkspace(), nestedDoc);
    assert.ok(docBtn, "expected semantic <button> for contended document path");
    docBtn();
    await nextTick(10);
    assert.equal(app.state.view, "explorer");
    assert.deepEqual(app.state.segments, ["rooms", "r1", "messages", "m1"]);
    assert.equal(app.state.contentionSseStatus, "idle");

    await app.openDocumentFromName(`${rootA}/rooms/../messages/m1`);
    assert.deepEqual(
      app.state.segments,
      ["rooms", "r1", "messages", "m1"],
      "dot-dot segment must be ignored"
    );
    await app.openDocumentFromName(`${rootA}/rooms//r1/m1`);
    assert.deepEqual(
      app.state.segments,
      ["rooms", "r1", "messages", "m1"],
      "empty segment must be ignored"
    );
    await app.openDocumentFromName(`${rootA}/rooms`);
    assert.deepEqual(
      app.state.segments,
      ["rooms", "r1", "messages", "m1"],
      "odd collection path must be ignored"
    );

    const newColDoc = `${rootA}/newcol/d1`;
    harness.fetchImpl = async (url, init) => {
      if (url === `/v1/${rootA}:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["rooms", "orders", "newcol"] });
      }
      if (url.includes("/documents/newcol?")) {
        return jsonResponse({
          documents: [
            {
              name: newColDoc,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (url === `/v1/${newColDoc}`) {
        return jsonResponse({ name: newColDoc, fields: {} });
      }
      return baseFetch(url, init);
    };
    await app.openDocumentFromName(newColDoc);
    assert.ok(
      app.state.rootCollections.includes("newcol"),
      "same-DB openDocumentFromName must refresh missing root collection"
    );
    assert.deepEqual(app.state.segments, ["newcol", "d1"]);
    harness.fetchImpl = baseFetch;

    await app.openDocumentFromName(crossDbDoc);
    assert.equal(app.state.activeProjectId, "other");
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.deepEqual(app.state.segments, ["orders", "o1"]);

    await app.switchView("contention");
    const prevFetch = harness.fetchImpl;
    harness.fetchImpl = async (url, init) => {
      if (
        url.startsWith("/emulator/v1/contention") &&
        init.method === "DELETE"
      ) {
        return jsonResponse(
          { error: { code: 500, status: "INTERNAL", message: "clear failed" } },
          500
        );
      }
      return prevFetch(url, init);
    };
    await app.clearContentionHistory();
    assert.equal(app.state.contention.clearing, false);
    assert.equal(app.state.contention.error, "[500 INTERNAL] clear failed");
    harness.fetchImpl = prevFetch;
    await app.switchView("explorer");

    const truncMarkup = renderTemplateMarkup(
      renderContention({
        projectId: "p",
        databaseId: "(default)",
        state: {
          scope: "active",
          snapshot: {
            databases: [
              {
                database: rootA,
                summary: {
                  woundsTotal: 0,
                  timeoutsTotal: 0,
                  upgradeAbortsTotal: 0,
                  writeBatchRetriesTotal: 0,
                },
                contendedDocuments: [
                  {
                    document: nestedDoc,
                    writer: null,
                    readers: [],
                    readerCount: 70,
                    waiters: [],
                    waiterCount: 0,
                  },
                ],
                totalContendedDocuments: 300,
                truncated: true,
                recentEvents: [],
              },
            ],
          },
          loading: false,
          clearing: false,
          error: "[500 INTERNAL] boom",
          lastFetchedAtMs: null,
          lastFetchedIso: null,
        },
        sseStatus: "error",
        ssePaused: false,
        onChangeScope: () => Promise.resolve(),
        onRefresh: () => {},
        onClearHistory: () => {},
        onOpenDocument: () => {},
      })
    );
    assert.ok(
      truncMarkup.includes('role="alert"') &&
        truncMarkup.includes("[500 INTERNAL] boom")
    );
    assert.ok(
      truncMarkup.includes("Snapshot truncated:") &&
        truncMarkup.includes("showing 1 of 300")
    );
    assert.ok(
      truncMarkup.includes(
        "participant lists capped at 64 readers / waiters per document"
      )
    );
    assert.ok(truncMarkup.includes("No contention events recorded."));

    const emptyMarkup = renderTemplateMarkup(
      renderContention({
        projectId: "p",
        databaseId: "(default)",
        state: {
          scope: "active",
          snapshot: { databases: [] },
          loading: false,
          clearing: false,
          error: null,
          lastFetchedAtMs: null,
          lastFetchedIso: null,
        },
        sseStatus: "live",
        ssePaused: false,
        onChangeScope: () => Promise.resolve(),
        onRefresh: () => {},
        onClearHistory: () => {},
        onOpenDocument: () => {},
      })
    );
    assert.ok(emptyMarkup.includes("No active lock contention"));
    assert.ok(emptyMarkup.includes("No contention events recorded."));

    let retryClicks = 0;
    const initialErrTpl = renderContention({
      projectId: "p",
      databaseId: "(default)",
      state: {
        scope: "active",
        snapshot: null,
        loading: false,
        clearing: false,
        error: "[503 UNAVAILABLE] contention endpoint failed",
        lastFetchedAtMs: null,
        lastFetchedIso: null,
      },
      sseStatus: "error",
      ssePaused: false,
      onChangeScope: () => Promise.resolve(),
      onRefresh: () => {
        retryClicks++;
      },
      onClearHistory: () => {},
      onOpenDocument: () => {},
    });
    const initialErrMarkup = renderTemplateMarkup(initialErrTpl);
    const dashCardMatches =
      initialErrMarkup.match(
        /class="contention-stat-value mono">\u{2014}<\/strong>/gu
      ) ?? [];
    assert.equal(
      dashCardMatches.length,
      4,
      "all 4 summary cards must show em-dash when initial snapshot failed: " +
        initialErrMarkup
    );
    assert.equal(
      initialErrMarkup.includes(">0</strong>"),
      false,
      "summary cards must not show 0 when snapshot is null with error"
    );
    assert.ok(
      initialErrMarkup.includes(
        "Failed to load contention snapshot: [503 UNAVAILABLE] contention endpoint failed"
      )
    );
    assert.equal(
      initialErrMarkup.includes("No active lock contention"),
      false,
      "error state must not render empty contention copy"
    );
    assert.equal(
      initialErrMarkup.includes("No contention events recorded."),
      false
    );
    const retryBtn = findClickHandler(initialErrTpl, "Retry");
    assert.ok(retryBtn, "expected Retry button in .contention-body");
    retryBtn();
    assert.equal(retryClicks, 1);
  });

  test("regression_contention_lit_html_waiter_rerender_and_tick", () => {
    const container = /** @type {HTMLElement} */ (
      /** @type {unknown} */ (new DomElement("MAIN"))
    );
    const rootPath = "projects/p/databases/(default)/documents";

    /**
     * @param {number | null} waitedMs
     * @returns {import('../api.js').ContentionSnapshot}
     */
    function makeSnapshot(waitedMs) {
      return {
        databases: [
          {
            database: rootPath,
            summary: {
              woundsTotal: 1,
              timeoutsTotal: 0,
              upgradeAbortsTotal: 0,
              writeBatchRetriesTotal: 0,
            },
            contendedDocuments:
              waitedMs === null
                ? []
                : [
                    {
                      document: `${rootPath}/users/alice`,
                      writer: null,
                      readers: [],
                      readerCount: 1,
                      waiters: [
                        {
                          sessionId: 9,
                          txnId: "9:1",
                          kind: "PESSIMISTIC_TXN",
                          seq: 2,
                          mode: "WRITE",
                          waitedMs,
                        },
                      ],
                      waiterCount: 1,
                    },
                  ],
            totalContendedDocuments: waitedMs === null ? 0 : 1,
            truncated: false,
            recentEvents: [],
          },
        ],
      };
    }

    const cState = createContentionState();
    let mockNow = 1_700_000_000_000;
    const origDateNow = Date.now;
    Date.now = () => mockNow;

    try {
      const renderView = () => {
        render(
          renderContention({
            projectId: "p",
            databaseId: "(default)",
            state: cState,
            sseStatus: "live",
            ssePaused: false,
            onChangeScope: () => Promise.resolve(),
            onRefresh: () => {},
            onClearHistory: () => {},
            onOpenDocument: () => {},
          }),
          container
        );
        updateWaiterElapsedDom(container, cState, mockNow);
      };

      cState.snapshot = makeSnapshot(160);
      cState.lastFetchedAtMs = mockNow;
      renderView();

      let waitNodes = container.querySelectorAll("[data-wait-base-ms]");
      assert.equal(waitNodes.length, 1);
      assert.equal(
        Array.from(waitNodes[0].childNodes).some((n) => n.nodeType === 8),
        false,
        "must not place lit-html Comment anchor inside .wait-elapsed"
      );
      assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "160");
      assert.equal(waitNodes[0].getAttribute("data-wait-label"), "160ms");
      assert.equal(waitNodes[0].textContent, "160ms");

      mockNow += 1000;
      updateWaiterElapsedDom(container, cState, mockNow);
      assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "1160");
      assert.equal(waitNodes[0].getAttribute("data-wait-label"), "1.2s");
      assert.equal(waitNodes[0].textContent, "1.2s");

      cState.snapshot = makeSnapshot(160);
      cState.lastFetchedAtMs = mockNow;
      renderView();
      waitNodes = container.querySelectorAll("[data-wait-base-ms]");
      assert.equal(waitNodes.length, 1);
      assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "160");
      assert.equal(waitNodes[0].getAttribute("data-wait-label"), "160ms");
      assert.equal(waitNodes[0].textContent, "160ms");

      cState.snapshot = makeSnapshot(null);
      cState.lastFetchedAtMs = mockNow;
      renderView();
      assert.equal(container.querySelectorAll("[data-wait-base-ms]").length, 0);

      cState.snapshot = makeSnapshot(400);
      cState.lastFetchedAtMs = mockNow;
      renderView();
      mockNow += 2000;
      updateWaiterElapsedDom(container, cState, mockNow);
      waitNodes = container.querySelectorAll("[data-wait-base-ms]");
      assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "2400");
      assert.equal(waitNodes[0].textContent, "2.4s");

      cState.snapshot = makeSnapshot(400);
      cState.lastFetchedAtMs = null;
      renderView();
      waitNodes = container.querySelectorAll("[data-wait-base-ms]");
      assert.equal(waitNodes.length, 1);
      assert.equal(waitNodes[0].getAttribute("data-wait-label"), "400ms");
      assert.equal(
        waitNodes[0].textContent,
        "400ms",
        "null lastFetchedAtMs must fall back to data-wait-label"
      );

      waitNodes[0].textContent = "stale";
      updateWaiterElapsedDom(container, null);
      assert.equal(waitNodes[0].textContent, "400ms");
      waitNodes[0].setAttribute("data-wait-base-ms", "");
      waitNodes[0].setAttribute("data-wait-label", "750ms");
      waitNodes[0].textContent = "stale";
      updateWaiterElapsedDom(container, null);
      assert.equal(waitNodes[0].textContent, "750ms");
      waitNodes[0].removeAttribute("data-wait-label");
      updateWaiterElapsedDom(container, null);
      assert.equal(waitNodes[0].textContent, "");
    } finally {
      Date.now = origDateNow;
    }
  });

  test("regression_contention_clear_history_loading_and_stale_error_guards", async () => {
    const db1Root = "projects/p/databases/db1/documents";
    const db2Root = "projects/p/databases/db2/documents";

    /** @type {() => Promise<unknown>} */
    let contentionGetHandler = async () =>
      jsonResponse({
        databases: [
          {
            database: db1Root,
            summary: {
              woundsTotal: 2,
              timeoutsTotal: 0,
              upgradeAbortsTotal: 0,
              writeBatchRetriesTotal: 0,
            },
            contendedDocuments: [],
            totalContendedDocuments: 0,
            truncated: false,
            recentEvents: [],
          },
        ],
      });
    /** @type {() => Promise<unknown>} */
    let contentionDeleteHandler = async () => jsonResponse({});
    let contentionGetCount = 0;

    harness.fetchImpl = async (url, init) => {
      const method = init.method ?? "GET";
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
      if (url === "/v1/projects/p/databases/db2") {
        return jsonResponse({
          name: "projects/p/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url.endsWith(":listCollectionIds")) {
        return jsonResponse({ collectionIds: [] });
      }
      if (url.startsWith("/emulator/v1/contention") && method === "DELETE") {
        return contentionDeleteHandler();
      }
      if (url.startsWith("/emulator/v1/contention") && method === "GET") {
        contentionGetCount++;
        return contentionGetHandler();
      }
      throw new Error("unexpected url: " + method + " " + url);
    };

    await app.init();
    await app.switchView("contention");
    assert.equal(app.state.contention.loading, false);

    // 1. Stale in-flight refreshContention() + failed clearContention() must NOT strand loading === true.
    const staleGetDef = deferred();
    const failDeleteDef = deferred();
    contentionGetHandler = () => staleGetDef.promise;
    contentionDeleteHandler = () => failDeleteDef.promise;

    const staleRefreshP = app.refreshContention();
    assert.equal(app.state.contention.loading, true);

    const clearFailP = app.clearContentionHistory();
    assert.equal(
      app.state.contention.loading,
      false,
      "clearContentionHistory must reset loading immediately after bumping seq"
    );
    assert.equal(app.state.contention.clearing, true);

    staleGetDef.resolve(
      jsonResponse({
        databases: [
          {
            database: db1Root,
            summary: {
              woundsTotal: 99,
              timeoutsTotal: 0,
              upgradeAbortsTotal: 0,
              writeBatchRetriesTotal: 0,
            },
            contendedDocuments: [],
            totalContendedDocuments: 0,
            truncated: false,
            recentEvents: [],
          },
        ],
      })
    );
    await staleRefreshP;
    assert.equal(app.state.contention.loading, false);
    assert.equal(
      app.state.contention.snapshot?.databases[0].summary.woundsTotal,
      2,
      "stale refresh must be ignored"
    );

    failDeleteDef.resolve(
      jsonResponse(
        { error: { code: 500, status: "INTERNAL", message: "clear failed" } },
        500
      )
    );
    await clearFailP;
    assert.equal(
      app.state.contention.loading,
      false,
      "failed clearContention after invalidated refresh must not strand loading"
    );
    assert.equal(app.state.contention.clearing, false);
    assert.equal(app.state.contention.error, "[500 INTERNAL] clear failed");
    const afterFailMarkup = renderTemplateMarkup(app.renderWorkspace());
    assert.equal(
      afterFailMarkup.includes("Refreshing..."),
      false,
      "Refresh button must remain enabled after failed clear"
    );

    // 2. Switching scope mid-clear prevents stale error and post-clear refresh.
    const scopeFailDelDef = deferred();
    contentionDeleteHandler = () => scopeFailDelDef.promise;
    contentionGetHandler = async () =>
      jsonResponse({
        databases: [
          {
            database: db2Root,
            summary: {
              woundsTotal: 5,
              timeoutsTotal: 1,
              upgradeAbortsTotal: 0,
              writeBatchRetriesTotal: 0,
            },
            contendedDocuments: [],
            totalContendedDocuments: 0,
            truncated: false,
            recentEvents: [],
          },
        ],
      });

    const clearActiveP = app.clearContentionHistory();
    const allBtn = findClickHandler(app.renderWorkspace(), "All Databases");
    assert.ok(allBtn);
    await allBtn();
    assert.equal(app.state.contention.scope, "all");
    assert.equal(app.state.contention.error, null);
    assert.equal(app.state.contention.snapshot?.databases[0].summary.woundsTotal, 5);

    scopeFailDelDef.resolve(
      jsonResponse(
        {
          error: { code: 500, status: "INTERNAL", message: "stale active error" },
        },
        500
      )
    );
    await clearActiveP;
    assert.equal(
      app.state.contention.error,
      null,
      "stale error from active scope must not overwrite all-scope state"
    );
    assert.equal(app.state.contention.clearing, false);

    const scopeOkDelDef = deferred();
    contentionDeleteHandler = () => scopeOkDelDef.promise;
    const clearAllP = app.clearContentionHistory();
    const activeBtn = findClickHandler(app.renderWorkspace(), "Active DB");
    assert.ok(activeBtn);
    await activeBtn();
    const getsAfterScopeSwitch = contentionGetCount;
    scopeOkDelDef.resolve(jsonResponse({}));
    await clearAllP;
    assert.equal(
      contentionGetCount,
      getsAfterScopeSwitch,
      "stale clear success after scope switch must not call refreshContention"
    );

    // 3. Switching view mid-clear blocks stale error and post-clear refresh.
    const viewFailDelDef = deferred();
    contentionDeleteHandler = () => viewFailDelDef.promise;
    const clearViewFailP = app.clearContentionHistory();
    await app.switchView("explorer");
    viewFailDelDef.resolve(
      jsonResponse(
        { error: { code: 500, status: "INTERNAL", message: "stale view error" } },
        500
      )
    );
    await clearViewFailP;
    assert.equal(app.state.view, "explorer");
    assert.equal(
      app.state.contention.error,
      null,
      "failed clear after leaving contention view must not set error"
    );
    assert.equal(app.state.contention.clearing, false);

    await app.switchView("contention");
    const viewCycleDelDef = deferred();
    contentionDeleteHandler = () => viewCycleDelDef.promise;
    const clearCycleP = app.clearContentionHistory();
    await app.switchView("explorer");
    await app.switchView("contention");
    const getsAfterReenter = contentionGetCount;
    viewCycleDelDef.resolve(
      jsonResponse(
        { error: { code: 500, status: "INTERNAL", message: "stale cycle error" } },
        500
      )
    );
    await clearCycleP;
    assert.equal(
      app.state.contention.error,
      null,
      "failed clear from prior view visit must not overwrite re-entered view error"
    );
    assert.equal(contentionGetCount, getsAfterReenter);

    // 4. Switching DB mid-clear prevents stale error and stale toast on new DB.
    const dbSwitchDelDef = deferred();
    contentionDeleteHandler = () => dbSwitchDelDef.promise;
    const clearDb1P = app.clearContentionHistory();
    app.state.toasts = [];
    await app.selectDatabase("p", "db2", [], true);
    assert.equal(app.state.activeDatabaseId, "db2");
    dbSwitchDelDef.resolve(
      jsonResponse(
        { error: { code: 500, status: "INTERNAL", message: "stale db1 error" } },
        500
      )
    );
    await clearDb1P;
    assert.equal(
      app.state.contention.error,
      null,
      "failed clear from db1 must not overwrite db2 contention error"
    );
    assert.equal(app.state.contention.clearing, false);
    assert.equal(
      app.state.toasts.length,
      0,
      "failed clear from db1 must not toast after switching to db2"
    );
  });
});
