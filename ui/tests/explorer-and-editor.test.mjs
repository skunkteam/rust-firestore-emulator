// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  deferred,
  findClickHandler,
  findElementByClass,
  findInputByAriaLabel,
  findInputByPlaceholder,
  findSelectByAriaLabel,
  flattenText,
  harness,
  jsonResponse,
  nextTick,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as api from "../api.js";
import * as app from "../app.js";
import * as editor from "../components/editor.js";
import { renderExplorer } from "../components/explorer.js";
import * as query from "../components/query.js";
import * as stateMod from "../state.js";
import * as values from "../values.js";

describe("Explorer, routing, and document editor regressions", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("bug_web_ui_1: rapid selectDatabase ignores slower DB1 overwrite", async () => {
    const db1MetaDef = deferred();
    const db1ColsDef = deferred();
    const db2MetaDef = deferred();
    const db2ColsDef = deferred();
    /** @type {string[]} */
    const fetched = [];

    harness.fetchImpl = async (url) => {
      fetched.push(url);
      if (url === "/v1/projects/p1/databases/db1") return db1MetaDef.promise;
      if (url === "/v1/projects/p1/databases/db1/documents:listCollectionIds") {
        return db1ColsDef.promise;
      }
      if (url === "/v1/projects/p2/databases/db2") return db2MetaDef.promise;
      if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {
        return db2ColsDef.promise;
      }
      if (url === "/emulator/v1") {
        return jsonResponse([
          "projects/p1/databases/db1/documents",
          "projects/p2/databases/db2/documents",
        ]);
      }
      throw new Error("unexpected url in 1A: " + url);
    };

    const p1 = app.selectDatabase("p1", "db1", ["col1"]);
    const p2 = app.selectDatabase("p2", "db2", []);
    assert.equal(
      app.state.activeDbMeta,
      null,
      "activeDbMeta cleared immediately on db switch"
    );

    db2MetaDef.resolve(
      jsonResponse({
        name: "projects/p2/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "ENTERPRISE",
        concurrencyMode: "OPTIMISTIC",
      })
    );
    db2ColsDef.resolve(jsonResponse({ collectionIds: ["orders"] }));
    await p2;

    db1MetaDef.resolve(
      jsonResponse({
        name: "projects/p1/databases/db1",
        uid: "u1",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      })
    );
    db1ColsDef.resolve(jsonResponse({ collectionIds: ["users"] }));
    await p1;

    assert.equal(app.state.activeProjectId, "p2");
    assert.equal(app.state.activeDatabaseId, "db2");
    assert.equal(
      /** @type {import('../api.js').DatabaseMetadata | null} */ (
        app.state.activeDbMeta
      )?.databaseEdition,
      "ENTERPRISE"
    );
    assert.deepEqual(app.state.rootCollections, ["orders"]);
    assert.equal(
      fetched.some((u) => u.includes("/db1/documents/col1")),
      false,
      "stale DB1 must not trigger hydrateVisiblePath"
    );
  });

  test("bug_web_ui_1: rapid Explorer segment navigation prevents col/doc mismatch", async () => {
    const usersListDef = deferred();
    const ordersListDef = deferred();
    /** @type {string[]} */
    const fetched = [];

    harness.fetchImpl = async (url) => {
      fetched.push(url);
      if (url === "/v1/projects/p2/databases/db2") {
        return jsonResponse({
          name: "projects/p2/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users", "orders"] });
      }
      if (url === "/emulator/v1") {
        return jsonResponse(["projects/p2/databases/db2/documents"]);
      }
      if (url.startsWith("/v1/projects/p2/databases/db2/documents/users?")) {
        return usersListDef.promise;
      }
      if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders?")) {
        return ordersListDef.promise;
      }
      if (url === "/v1/projects/p2/databases/db2/documents/orders/ord1") {
        return jsonResponse({
          name: "projects/p2/databases/db2/documents/orders/ord1",
          fields: {},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      if (
        url ===
        "/v1/projects/p2/databases/db2/documents/orders/ord1:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: [] });
      }
      throw new Error("unexpected url in 1B: " + url);
    };

    await app.selectDatabase("p2", "db2", [], true);
    const ws1 = app.renderWorkspace();
    const clickUsers = findClickHandler(ws1, "users");
    assert.ok(clickUsers, "expected click handler for users collection");
    clickUsers();

    app.state.view = "query";
    app.state.queryState.results = [
      { name: "projects/p2/databases/db2/documents/orders/ord1", fields: {} },
    ];
    const wsQuery = app.renderWorkspace();
    const openOrd1 = findClickHandler(wsQuery, "Open in Explorer");
    assert.ok(openOrd1, "expected Open in Explorer handler");
    openOrd1();

    ordersListDef.resolve(
      jsonResponse({
        documents: [
          {
            name: "projects/p2/databases/db2/documents/orders/ord1",
            createTime: "2026-01-01T00:00:00Z",
            updateTime: "2026-01-01T00:00:00Z",
          },
        ],
      })
    );
    await nextTick();

    usersListDef.resolve(
      jsonResponse({
        documents: [
          {
            name: "projects/p2/databases/db2/documents/users/alice",
            createTime: "2026-01-01T00:00:00Z",
            updateTime: "2026-01-01T00:00:00Z",
          },
        ],
      })
    );
    await nextTick();

    assert.deepEqual(app.state.segments, ["orders", "ord1"]);
    assert.equal(
      fetched.some((u) => u.includes("/users/ord1")),
      false,
      "must never mix old colPath users with new docId ord1"
    );
    assert.equal(
      app.state.documentLevels.has(
        "projects/p2/databases/db2/documents/users/ord1"
      ),
      false
    );
  });

  test("bug_web_ui_1: loadCollectionColumn sequence prevents stale overwrite", async () => {
    const colPath = "projects/p2/databases/db2/documents/orders";
    const parentPath = "projects/p2/databases/db2/documents";
    const slowDef = deferred();
    const fastDef = deferred();
    let call = 0;

    harness.fetchImpl = async (url) => {
      if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders?")) {
        call++;
        return call === 1 ? slowDef.promise : fastDef.promise;
      }
      throw new Error("unexpected url in 1C: " + url);
    };

    const pSlow = app.loadCollectionColumn(colPath, parentPath, "orders", false);
    const pFast = app.loadCollectionColumn(colPath, parentPath, "orders", false);

    fastDef.resolve(
      jsonResponse({
        documents: [
          {
            name: "projects/p2/databases/db2/documents/orders/fresh",
            createTime: "2026-01-02T00:00:00Z",
            updateTime: "2026-01-02T00:00:00Z",
          },
        ],
      })
    );
    await pFast;

    slowDef.resolve(
      jsonResponse({
        documents: [
          {
            name: "projects/p2/databases/db2/documents/orders/stale",
            createTime: "2026-01-01T00:00:00Z",
            updateTime: "2026-01-01T00:00:00Z",
          },
        ],
      })
    );
    await pSlow;

    assert.deepEqual(
      app.state.collectionColumns.get(colPath)?.documents.map((d) => d.id),
      ["fresh"]
    );
  });

  test("bug_web_ui_1: slow SSE loadDocumentLevel and toggleDatabaseSetting respect newer Save / DB switch", async () => {
    const docPath = "projects/p2/databases/db2/documents/orders/ord1";
    const slowSseGet = deferred();
    const slowPatchDb = deferred();
    let useSlowDocGet = false;

    harness.fetchImpl = async (url, init) => {
      if (url === "/v1/projects/p2/databases/db2") {
        if (init.method === "PATCH") return slowPatchDb.promise;
        return jsonResponse({
          name: "projects/p2/databases/db2",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["orders"] });
      }
      if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders?")) {
        return jsonResponse({
          documents: [
            {
              name: docPath,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (
        url === "/v1/projects/p2/databases/db2/documents/orders/ord1" &&
        (!init.method || init.method === "GET")
      ) {
        if (useSlowDocGet) return slowSseGet.promise;
        return jsonResponse({
          name: docPath,
          fields: {},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      if (
        url ===
        "/v1/projects/p2/databases/db2/documents/orders/ord1:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: [] });
      }
      if (
        url.startsWith("/v1/projects/p2/databases/db2/documents/orders/ord1?") &&
        init.method === "PATCH"
      ) {
        return jsonResponse({
          name: docPath,
          fields: { title: { stringValue: "saved-v2" } },
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-02T00:00:00Z",
        });
      }
      if (url === "/v1/projects/p3/databases/db3") {
        return jsonResponse({
          name: "projects/p3/databases/db3",
          uid: "u3",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: [] });
      }
      if (url === "/emulator/v1") {
        return jsonResponse([
          "projects/p2/databases/db2/documents",
          "projects/p3/databases/db3/documents",
        ]);
      }
      throw new Error("unexpected url in 1D: " + init.method + " " + url);
    };

    await app.selectDatabase("p2", "db2", ["orders", "ord1"], true);
    const level = app.state.documentLevels.get(docPath);
    assert.ok(level?.editorSession);

    useSlowDocGet = true;
    const pSseDoc = app.loadDocumentLevel(docPath, "ord1", true);

    const addFieldBtn = findClickHandler(app.renderWorkspace(), "+ Add Field");
    assert.ok(addFieldBtn);
    addFieldBtn();
    level.editorSession.nodes[0].key = "title";
    level.editorSession.nodes[0].text = "saved-v2";
    await app.saveDocumentFromInspector(level);
    assert.equal(level.editorSession.cleanDoc?.updateTime, "2026-01-02T00:00:00Z");

    slowSseGet.resolve(
      jsonResponse({
        name: docPath,
        fields: {},
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      })
    );
    await pSseDoc;
    assert.equal(level.editorSession.cleanDoc?.updateTime, "2026-01-02T00:00:00Z");
    assert.equal(level.editorSession.externalDoc, null);

    const pToggle = app.toggleDatabaseSetting("edition");
    await app.selectDatabase("p3", "db3", [], true);
    slowPatchDb.resolve(
      jsonResponse({
        name: "projects/p2/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      })
    );
    await pToggle;
    assert.equal(app.state.activeDbMeta?.name, "projects/p3/databases/db3");
    assert.equal(app.state.activeDbMeta?.databaseEdition, "ENTERPRISE");
  });

  test("bug_web_ui_1: handleSseBatch aborts on DB switch and database_cleared avoids /v1 calls", async () => {
    const oldColsDef = deferred();
    /** @type {string[]} */
    const fetched = [];
    harness.fetchImpl = async (url) => {
      fetched.push(url);
      if (url === "/v1/projects/p3/databases/db3") {
        return jsonResponse({
          name: "projects/p3/databases/db3",
          uid: "u3",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") {
        return oldColsDef.promise;
      }
      if (url === "/v1/projects/p4/databases/db4") {
        return jsonResponse({
          name: "projects/p4/databases/db4",
          uid: "u4",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p4/databases/db4/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["db4_col"] });
      }
      if (url === "/emulator/v1") {
        return jsonResponse([
          "projects/p3/databases/db3/documents",
          "projects/p4/databases/db4/documents",
        ]);
      }
      throw new Error("unexpected url in 1E: " + url);
    };

    app.state.activeProjectId = "p3";
    app.state.activeDatabaseId = "db3";
    app.state.segments = ["old_col"];
    const pBatch = app.handleSseBatch([
      {
        type: "documents_changed",
        database: "projects/p3/databases/db3/documents",
        paths: ["projects/p3/databases/db3/documents/old_col/d1"],
      },
    ]);
    await app.selectDatabase("p4", "db4", [], true);
    oldColsDef.resolve(jsonResponse({ collectionIds: ["stale_db3_col"] }));
    await pBatch;

    assert.deepEqual(app.state.rootCollections, ["db4_col"]);
    assert.equal(
      fetched.some((u) => u.includes("/db3/documents/old_col")),
      false
    );

    /** @type {string[]} */
    const fetched2 = [];
    harness.fetchImpl = async (url) => {
      fetched2.push(url);
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error("unexpected url in 1F: " + url);
    };
    app.state.activeProjectId = "p4";
    app.state.activeDatabaseId = "db4";
    await app.handleSseBatch([
      {
        type: "database_cleared",
        database: "projects/p4/databases/db4/documents",
      },
    ]);
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.equal(app.state.activeDbMeta, null);
    assert.equal(
      fetched2.some((u) => u.startsWith("/v1/")),
      false
    );
  });

  test("bug_web_ui_2: URL routing and path segment validation", async () => {
    for (const badPath of [
      "/projects/p/databases/(default)/documents/users/100%",
      "/projects/p/databases/(default)/documents/users/%ZZ",
      "/projects/p/databases/(default)/documents/users/%E0%A4%A",
      "/projects/%ZZ/databases/(default)/documents",
      "/projects/p/databases/(default)/documents/users//alice",
      "/projects/p/databases/(default)/documents/users/a%2Fb",
      "/projects/p/databases/(default)/documents/users/%2e%2e",
      "/projects/p/databases/(default)/documents/users/.",
      "/projects/../databases/(default)/documents/users/alice",
    ]) {
      assert.equal(
        app.parseBrowserRoute(badPath),
        null,
        "expected null for " + badPath
      );
    }

    assert.deepEqual(
      app.parseBrowserRoute(
        "/projects/p/databases/(default)/documents/users/100%25"
      ),
      { projectId: "p", databaseId: "(default)", segments: ["users", "100%"] }
    );

    window.location.pathname =
      "/projects/p/databases/(default)/documents/users/100%";
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error("unexpected url during init: " + url);
    };
    await app.init();
    assert.equal(app.state.activeProjectId, null);

    app.state.segments = ["users"];
    window.location.pathname =
      "/projects/p/databases/(default)/documents/users/%ZZ";
    for (const listener of harness.popstateListeners) {
      listener();
    }
    assert.deepEqual(app.state.segments, []);

    let fetchCount = 0;
    harness.fetchImpl = async () => {
      fetchCount++;
      return jsonResponse({});
    };

    app.state.modal = {
      kind: "open_db",
      projectId: "..",
      databaseId: "(default)",
      edition: "STANDARD",
      busy: false,
      error: null,
    };
    const submitDb = findClickHandler(app.renderModal(), "Open Database");
    assert.ok(submitDb);
    await submitDb();
    assert.ok(
      app.state.modal.error,
      "expected validation error for projectId '..'"
    );
    assert.equal(fetchCount, 0);

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.openCreateDocumentModal(
      "projects/p/databases/(default)/documents",
      "users",
      ""
    );
    assert.equal(app.state.modal?.kind, "create_doc");
    app.state.modal.docId = "..";
    const submitDoc = findClickHandler(app.renderModal(), "Save Document");
    assert.ok(submitDoc);
    await submitDoc();
    assert.ok(app.state.modal.error, "expected validation error for docId '..'");
    assert.equal(fetchCount, 0);
  });

  test("bug_web_ui_3: dirty editor protection across navigation and external deletion", async () => {
    const docPath = "projects/p/databases/(default)/documents/users/alice";
    const serverDoc = {
      name: docPath,
      fields: { name: { stringValue: "Alice" } },
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    };

    let docExists = true;
    harness.fetchImpl = async (url) => {
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
      if (url === "/emulator/v1") {
        return jsonResponse(["projects/p/databases/(default)/documents"]);
      }
      if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {
        return jsonResponse({
          documents: docExists
            ? [
                {
                  name: docPath,
                  createTime: serverDoc.createTime,
                  updateTime: serverDoc.updateTime,
                },
              ]
            : [],
        });
      }
      if (url === "/v1/projects/p/databases/(default)/documents/users/alice") {
        return docExists
          ? jsonResponse(serverDoc)
          : jsonResponse(
              { error: { code: 404, status: "NOT_FOUND", message: "not found" } },
              404
            );
      }
      if (
        url ===
        "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: ["posts"] });
      }
      if (
        url.startsWith(
          "/v1/projects/p/databases/(default)/documents/users/alice/posts?"
        )
      ) {
        return jsonResponse({ documents: [] });
      }
      throw new Error("unexpected url in bug 3: " + url);
    };

    await app.selectDatabase("p", "(default)", ["users", "alice"], true);
    const level = app.state.documentLevels.get(docPath);
    assert.ok(level?.editorSession);

    const nameInputEl = findInputByPlaceholder(
      app.renderWorkspace(),
      "string value"
    );
    assert.ok(nameInputEl?.handlers.input, "expected string value input handler");
    nameInputEl.handlers.input({ target: { value: "Alice-Edited" } });
    assert.equal(level.editorSession.isDirty, true);

    app.state.segments = ["users", "alice", "posts"];
    await app.hydrateVisiblePath();
    assert.equal(level.editorSession.isDirty, true);
    assert.equal(level.editorSession.nodes[0].text, "Alice-Edited");

    docExists = false;
    await app.handleSseBatch([
      {
        type: "documents_changed",
        database: "projects/p/databases/(default)/documents",
        paths: [docPath],
      },
    ]);

    assert.equal(level.exists, false);
    assert.equal(level.editorSession.isDirty, true);
    assert.equal(level.editorSession.deletedExternally, true);

    const explorerTpl = renderExplorer({
      projectId: "p",
      databaseId: "(default)",
      rootCollections: ["users"],
      rootLoading: false,
      segments: ["users", "alice"],
      collectionColumns: app.state.collectionColumns,
      documentLevels: app.state.documentLevels,
      onSelectSegments: () => {},
      onOpenStartCollection: () => {},
      onOpenAddDocument: () => {},
      onOpenDeleteCollection: () => {},
      onOpenDeleteDocument: () => {},
      onOpenQueryCollection: () => {},
      onLoadMoreDocuments: () => {},
      onSaveDocument: () => {},
      onFilterChange: () => {},
      onRerender: () => {},
    });
    const renderedText = flattenText(explorerTpl);
    assert.ok(
      renderedText.includes("deleted externally"),
      "expected external deletion conflict banner"
    );
    assert.equal(renderedText.includes("Non-existent ancestor document"), false);

    const keepBtn = findClickHandler(explorerTpl, "Keep my edits");
    assert.ok(keepBtn);
    keepBtn();
    assert.equal(level.editorSession.deletedExternally, false);
    assert.equal(level.editorSession.cleanDoc, null);
    assert.equal(level.editorSession.isDirty, true);
    assert.equal(level.editorSession.nodes[0].text, "Alice-Edited");

    level.editorSession.deletedExternally = true;
    const tpl2 = app.renderWorkspace();
    const discardBtn = findClickHandler(tpl2, "Discard my edits");
    assert.ok(discardBtn);
    discardBtn();
    assert.equal(level.editorSession.isDirty, false);
    assert.ok(
      flattenText(app.renderWorkspace()).includes("Non-existent ancestor document")
    );
  });

  test("bug_web_ui_4: delete modal truncates segments safely", async () => {
    harness.fetchImpl = async (url, init) => {
      if (init.method === "DELETE") return jsonResponse({});
      if (url.endsWith(":listCollectionIds")) {
        return jsonResponse({ collectionIds: ["users", "rooms"] });
      }
      if (url.includes("/documents/rooms?")) {
        return jsonResponse({ documents: [] });
      }
      if (url.includes("/documents/users?")) {
        return jsonResponse({ documents: [] });
      }
      if (url === "/v1/projects/p/databases/(default)/documents/users/orders") {
        return jsonResponse({
          name: "projects/p/databases/(default)/documents/users/orders",
          fields: {},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      throw new Error(
        "unexpected url in bug 4: " + (init.method ?? "GET") + " " + url
      );
    };

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.segments = ["rooms", "r1", "messages"];
    app.state.modal = {
      kind: "delete_doc",
      docPath: "projects/p/databases/(default)/documents/rooms/r1",
      recursive: true,
      hasSubcollections: true,
      busy: false,
      error: null,
    };
    const confirmDelDoc = findClickHandler(app.renderModal(), "Delete");
    assert.ok(confirmDelDoc);
    await confirmDelDoc();
    assert.deepEqual(app.state.segments, ["rooms"]);

    app.state.segments = ["users", "orders", "orders"];
    app.state.modal = {
      kind: "delete_col",
      colPath: "projects/p/databases/(default)/documents/users/orders/orders",
      colId: "orders",
      busy: false,
      error: null,
    };
    const confirmDelCol = findClickHandler(
      app.renderModal(),
      "Delete Collection"
    );
    assert.ok(confirmDelCol);
    await confirmDelCol();
    assert.deepEqual(app.state.segments, ["users", "orders"]);
  });

  test("bug_web_ui_5: RFC 3339 timestamp validation", () => {
    for (const bad of [
      "2026-02-31T00:00:00Z",
      "2026-02-29T00:00:00Z",
      "2100-02-29T00:00:00Z",
      "1900-02-29T00:00:00Z",
      "2026-04-31T00:00:00Z",
      "2026-01-01T24:00:00Z",
      "2026-01-01T00:60:00Z",
      "2026-01-01T00:00:60Z",
      "2026-01-01T00:00:00+24:00",
      "2026-01-01T00:00:00+00:60",
      "0000-01-01T00:00:00Z",
      "0001-01-01T00:00:00+01:00",
      "9999-12-31T23:59:59-01:00",
      "2026-01-01T00:00:00.1234567890Z",
    ]) {
      assert.equal(
        values.validateTimestamp(bad).valid,
        false,
        "expected invalid timestamp: " + bad
      );
    }

    for (const good of [
      "2024-02-29T23:59:59.123456789Z",
      "2000-02-29T00:00:00Z",
      "0001-01-01T00:00:00Z",
      "0001-01-01T00:00:00-01:00",
      "9999-12-31T23:59:59.999999999Z",
      "9999-12-31T23:59:59+01:00",
      "2026-11-10T15:30:00.000000001+05:30",
    ]) {
      assert.equal(
        values.validateTimestamp(good).valid,
        true,
        "expected valid timestamp: " + good
      );
    }
  });

  test("bug_web_ui_6: negative zero preservation across editor, API, and queries", async () => {
    const serialized = values.stringifyProtoJson(
      {
        fields: {
          neg: { doubleValue: -0 },
          pos: { doubleValue: 0 },
          geo: { geoPointValue: { latitude: -0, longitude: 0 } },
        },
      },
      2
    );
    assert.ok(serialized.includes('"doubleValue": -0.0'));
    assert.ok(serialized.includes('"doubleValue": 0'));
    assert.ok(serialized.includes('"latitude": -0.0'));
    assert.ok(serialized.includes('"longitude": 0'));

    const doc = {
      name: "projects/p/databases/(default)/documents/c/d",
      fields: {
        neg: { doubleValue: -0 },
        geo: { geoPointValue: { latitude: -0, longitude: 0 } },
        vec: {
          mapValue: {
            fields: {
              __type__: { stringValue: "__vector__" },
              value: { arrayValue: { values: [{ doubleValue: -0 }] } },
            },
          },
        },
      },
    };
    const session = editor.createEditorSession(doc.name, "p", "(default)", doc);
    assert.equal(
      session.isDirty,
      false,
      "GeoPoint -0.0 must not falsely mark clean doc dirty"
    );
    assert.ok(session.cleanJson.includes('"doubleValue": -0.0'));
    assert.ok(session.cleanJson.includes('"latitude": -0.0'));

    session.mode = "raw";
    const extracted = editor.extractSessionFields(session);
    assert.equal(extracted.valid, true);
    assert.ok(extracted.fields);
    assert.equal(Object.is(extracted.fields.neg.doubleValue, -0), true);
    assert.equal(
      Object.is(extracted.fields.geo.geoPointValue?.latitude, -0),
      true
    );

    /** @type {string[]} */
    const sentBodies = [];
    harness.fetchImpl = async (_url, init) => {
      sentBodies.push(String(init.body ?? ""));
      return jsonResponse({ name: doc.name, fields: extracted.fields });
    };

    await api.createDocument(
      "projects/p/databases/(default)/documents",
      "c",
      "d",
      extracted.fields
    );
    await api.updateDocument(doc.name, extracted.fields, ["neg"], null);

    const qState = query.createQueryRunnerState("c");
    const row = query.createFilterRow();
    row.fieldPath = "neg";
    row.op = "EQUAL";
    row.valueType = "doubleValue";
    row.valueText = "-0.0";
    qState.filters.push(row);
    const built = query.buildStructuredQueryFromVisual(qState, "p", "(default)");
    assert.equal(built.valid, true);
    assert.ok(built.query);
    await api.runQuery("projects/p/databases/(default)/documents", built.query);

    assert.equal(sentBodies.length, 3);
    for (const body of sentBodies) {
      assert.ok(
        body.includes("-0.0"),
        "expected -0.0 in serialized request body: " + body
      );
    }
  });

  test("bug_web_ui_7: __proto__ keys and whitespace key preservation", () => {
    const rawRes = values.validateRawProtoJsonFields(
      '{"fields":{"__proto__":{"stringValue":"x"}," a ":{"integerValue":"1"}}}'
    );
    assert.equal(rawRes.valid, true);
    assert.ok(rawRes.fields);
    assert.equal(Object.getPrototypeOf(rawRes.fields), Object.prototype);
    assert.deepEqual(Object.keys(rawRes.fields).sort(), [" a ", "__proto__"]);
    assert.equal(rawRes.fields["__proto__"].stringValue, "x");
    assert.equal(rawRes.fields[" a "].integerValue, "1");

    const spacedDoc = {
      name: "projects/p/databases/(default)/documents/c/d",
      fields: { " a ": { stringValue: "val" } },
    };
    const session = editor.createEditorSession(
      spacedDoc.name,
      "p",
      "(default)",
      spacedDoc
    );
    assert.equal(
      session.isDirty,
      false,
      "leading/trailing spaces in key must not falsely mark dirty"
    );

    session.nodes.push({
      id: 999,
      key: "__proto__",
      type: "stringValue",
      text: "protoVal",
      boolVal: false,
      latText: "0",
      lngText: "0",
      bytesMode: "base64",
      children: [],
    });
    const extracted = editor.extractSessionFields(session);
    assert.equal(extracted.valid, true);
    assert.ok(extracted.fields);
    assert.equal(Object.getPrototypeOf(extracted.fields), Object.prototype);
    assert.deepEqual(Object.keys(extracted.fields).sort(), [" a ", "__proto__"]);
    assert.equal(extracted.fields["__proto__"].stringValue, "protoVal");
  });

  test("regression_popstate_same_db_does_not_lock_saving_or_loading_states", async () => {
    const colPath = "projects/p/databases/(default)/documents/orders";
    const parentPath = "projects/p/databases/(default)/documents";
    const docPath = "projects/p/databases/(default)/documents/orders/ord1";

    /** @type {ReturnType<typeof deferred> | null} */
    let patchDef = null;
    /** @type {ReturnType<typeof deferred> | null} */
    let colReloadDef = null;

    harness.fetchImpl = async (url, init) => {
      if (url === "/emulator/v1") {
        return jsonResponse(["projects/p/databases/(default)/documents"]);
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
        return jsonResponse({ collectionIds: ["orders"] });
      }
      if (url.startsWith("/v1/projects/p/databases/(default)/documents/orders?")) {
        if (colReloadDef) return colReloadDef.promise;
        return jsonResponse({
          documents: [
            {
              name: docPath,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (
        url === "/v1/projects/p/databases/(default)/documents/orders/ord1" &&
        (!init.method || init.method === "GET")
      ) {
        return jsonResponse({
          name: docPath,
          fields: { status: { stringValue: "open" } },
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      if (
        url ===
        "/v1/projects/p/databases/(default)/documents/orders/ord1:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: [] });
      }
      if (
        url.startsWith("/v1/projects/p/databases/(default)/documents/orders/ord1") &&
        init.method === "PATCH"
      ) {
        assert.ok(patchDef);
        return patchDef.promise;
      }
      throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
    };

    window.location.pathname =
      "/projects/p/databases/(default)/documents/orders/ord1";
    await app.init();

    const level = app.state.documentLevels.get(docPath);
    const colState = app.state.collectionColumns.get(colPath);
    assert.ok(level?.editorSession);
    assert.ok(colState);

    const valInput = findInputByPlaceholder(app.renderWorkspace(), "string value");
    assert.ok(valInput?.handlers.input);
    valInput.handlers.input({ target: { value: "closed" } });
    assert.equal(level.editorSession.isDirty, true);

    patchDef = deferred();
    colReloadDef = deferred();
    const savePromise = app.saveDocumentFromInspector(level);
    const colPromise = app.loadCollectionColumn(
      colPath,
      parentPath,
      "orders",
      false
    );
    assert.equal(level.editorSession.saving, true);
    assert.equal(colState.loading, true);

    window.location.pathname = "/projects/p/databases/(default)/documents";
    for (const fn of harness.popstateListeners) {
      fn();
    }
    await nextTick();

    patchDef.resolve(
      jsonResponse({
        name: docPath,
        fields: { status: { stringValue: "closed" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      })
    );
    colReloadDef.resolve(jsonResponse({ documents: [] }));
    await savePromise;
    await colPromise;

    assert.equal(
      level.editorSession.saving,
      false,
      "same-DB popstate navigation must not leave cached editorSession.saving permanently stuck true"
    );
    assert.equal(
      colState.loading,
      false,
      "same-DB popstate navigation must not leave cached colState.loading permanently stuck true"
    );
  });

  test("regression_clear_database_modal_does_not_resurrect_cleared_db_on_concurrent_sse", async () => {
    const listDef1 = deferred();
    const listDef2 = deferred();
    let listCalls = 0;
    /** @type {string[]} */
    const fetched = [];

    harness.fetchImpl = async (url, init) => {
      fetched.push((init.method ?? "GET") + " " + url);
      if (
        url === "/emulator/v1/projects/p/databases/(default)/documents" &&
        init.method === "DELETE"
      ) {
        return jsonResponse({});
      }
      if (url === "/emulator/v1") {
        listCalls++;
        return listCalls === 1 ? listDef1.promise : listDef2.promise;
      }
      if (url === "/v1/projects/p/databases/(default)") {
        return jsonResponse({
          name: "projects/p/databases/(default)",
          uid: "u-resurrected",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: [] });
      }
      throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
    };

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.databases = [
      {
        projectId: "p",
        databaseId: "(default)",
        rootPath: "projects/p/databases/(default)/documents",
      },
    ];
    app.state.modal = {
      kind: "clear_db",
      projectId: "p",
      databaseId: "(default)",
      busy: false,
      error: null,
    };

    const clearBtn = findClickHandler(app.renderModal(), "Clear Database");
    assert.ok(clearBtn, "expected Clear Database button handler");
    const clearPromise = clearBtn();

    await nextTick();
    assert.equal(listCalls, 1);

    const ssePromise = app.handleSseBatch([
      {
        type: "database_cleared",
        database: "projects/p/databases/(default)/documents",
      },
    ]);
    assert.equal(listCalls, 2);

    listDef1.resolve(jsonResponse([]));
    await nextTick();
    listDef2.resolve(jsonResponse([]));
    await clearPromise;
    await ssePromise;

    assert.equal(
      fetched.some((entry) => entry.includes(" /v1/projects/p/databases/(default)")),
      false,
      "confirmClearDb must not read stale state.databases and resurrect the cleared database: " +
        JSON.stringify(fetched)
    );
    assert.equal(app.state.activeProjectId, null);
  });

  test("regression_deleted_externally_keep_empty_edits_and_save_recreates_document", async () => {
    const docPath = "projects/p/databases/(default)/documents/users/alice";
    let docExists = true;
    let patchCount = 0;

    harness.fetchImpl = async (url, init) => {
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
      if (url === "/emulator/v1") {
        return jsonResponse(["projects/p/databases/(default)/documents"]);
      }
      if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {
        return jsonResponse({
          documents: docExists
            ? [
                {
                  name: docPath,
                  createTime: "2026-01-01T00:00:00Z",
                  updateTime: "2026-01-01T00:00:00Z",
                },
              ]
            : [],
        });
      }
      if (
        url === "/v1/projects/p/databases/(default)/documents/users/alice" &&
        (!init.method || init.method === "GET")
      ) {
        return docExists
          ? jsonResponse({
              name: docPath,
              fields: { name: { stringValue: "Alice" } },
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            })
          : jsonResponse(
              { error: { code: 404, status: "NOT_FOUND", message: "not found" } },
              404
            );
      }
      if (
        url ===
        "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: [] });
      }
      if (
        url.startsWith(
          "/v1/projects/p/databases/(default)/documents/users/alice"
        ) &&
        init.method === "PATCH"
      ) {
        patchCount++;
        docExists = true;
        return jsonResponse({
          name: docPath,
          fields: {},
          createTime: "2026-01-02T00:00:00Z",
          updateTime: "2026-01-02T00:00:00Z",
        });
      }
      throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
    };

    await app.selectDatabase("p", "(default)", ["users", "alice"], true);
    const level = app.state.documentLevels.get(docPath);
    assert.ok(level?.editorSession);

    const rawTab = findClickHandler(app.renderWorkspace(), "Raw ProtoJSON");
    assert.ok(rawTab);
    rawTab();
    level.editorSession.rawText = '{"fields":{}}';
    const structuredTab = findClickHandler(
      app.renderWorkspace(),
      "Structured Fields"
    );
    assert.ok(structuredTab);
    structuredTab();
    assert.equal(level.editorSession.nodes.length, 0);
    assert.equal(level.editorSession.isDirty, true);

    docExists = false;
    await app.handleSseBatch([
      {
        type: "documents_changed",
        database: "projects/p/databases/(default)/documents",
        paths: [docPath],
      },
    ]);
    assert.equal(level.exists, false);
    assert.equal(level.editorSession.deletedExternally, true);

    const keepBtn = findClickHandler(app.renderWorkspace(), "Keep my edits");
    assert.ok(keepBtn, "expected Keep my edits button");
    keepBtn();

    assert.equal(
      level.editorSession.isDirty,
      true,
      "Keep my edits on an externally deleted document with empty fields must stay dirty so the user can recreate it"
    );
    assert.equal(
      flattenText(app.renderWorkspace()).includes("Non-existent ancestor document"),
      false,
      "editor must remain visible instead of collapsing into Non-existent ancestor document card"
    );

    await app.saveDocumentFromInspector(level);
    assert.equal(
      patchCount,
      1,
      "Save Changes must send PATCH to recreate the empty document"
    );
    assert.equal(level.exists, true);
  });

  test("regression_save_document_clears_loading_and_ignores_concurrent_inflight_sse_get", async () => {
    const docPath = "projects/p/databases/(default)/documents/users/alice";
    /** @type {() => Promise<unknown>} */
    let getHandler = async () =>
      jsonResponse({
        name: docPath,
        fields: { name: { stringValue: "Alice-v1" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      });
    /** @type {() => Promise<unknown>} */
    let patchHandler = async () =>
      jsonResponse({
        name: docPath,
        fields: { name: { stringValue: "Alice-v2" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      });

    harness.fetchImpl = async (url, init) => {
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
      if (url === "/emulator/v1") {
        return jsonResponse(["projects/p/databases/(default)/documents"]);
      }
      if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {
        return jsonResponse({
          documents: [
            {
              name: docPath,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (
        url === "/v1/projects/p/databases/(default)/documents/users/alice" &&
        (!init.method || init.method === "GET")
      ) {
        return getHandler();
      }
      if (
        url ===
        "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds"
      ) {
        return jsonResponse({ collectionIds: [] });
      }
      if (
        url.startsWith(
          "/v1/projects/p/databases/(default)/documents/users/alice"
        ) &&
        init.method === "PATCH"
      ) {
        return patchHandler();
      }
      throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
    };

    await app.selectDatabase("p", "(default)", ["users", "alice"], true);
    const level = app.state.documentLevels.get(docPath);
    assert.ok(level?.editorSession);

    const sseDef1 = deferred();
    getHandler = () => sseDef1.promise;
    const sseP1 = app.loadDocumentLevel(docPath, "alice", true);
    assert.equal(level.loading, true);

    const input1 = findInputByPlaceholder(app.renderWorkspace(), "string value");
    assert.ok(input1?.handlers.input);
    input1.handlers.input({ target: { value: "Alice-v2" } });
    await app.saveDocumentFromInspector(level);

    sseDef1.resolve(
      jsonResponse({
        name: docPath,
        fields: { name: { stringValue: "Alice-v1" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      })
    );
    await sseP1;
    assert.equal(
      level.loading,
      false,
      "superseded loadDocumentLevel must not leave level.loading stuck true after saveDocumentFromInspector"
    );

    const patchDef = deferred();
    const sseDef2 = deferred();
    patchHandler = () => patchDef.promise;
    getHandler = () => sseDef2.promise;

    const input2 = findInputByPlaceholder(app.renderWorkspace(), "string value");
    assert.ok(input2?.handlers.input);
    input2.handlers.input({ target: { value: "Alice-v3" } });
    const saveP = app.saveDocumentFromInspector(level);

    const sseP2 = app.loadDocumentLevel(docPath, "alice", true);

    patchDef.resolve(
      jsonResponse({
        name: docPath,
        fields: { name: { stringValue: "Alice-v3" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-03T00:00:00Z",
      })
    );
    await saveP;
    assert.equal(level.editorSession.cleanDoc?.updateTime, "2026-01-03T00:00:00Z");

    sseDef2.resolve(
      jsonResponse({
        name: docPath,
        fields: { name: { stringValue: "Alice-v2" } },
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      })
    );
    await sseP2;

    assert.equal(
      level.editorSession.cleanDoc?.updateTime,
      "2026-01-03T00:00:00Z",
      "concurrent SSE GET started while PATCH was in flight must not overwrite newer saved document"
    );
  });

  test("regression_list_collection_ids_pagination_and_guards", async () => {
    /** @type {string[]} */
    const warns = [];
    const origWarn = console.warn;
    console.warn = (...args) => warns.push(args.map(String).join(" "));

    try {
      // 1. Paginates across nextPageToken while sending {} on initial request without warning.
      {
        warns.length = 0;
        /** @type {string[]} */
        const bodies = [];
        harness.fetchImpl = async (url, init) => {
          assert.equal(
            url,
            "/v1/projects/p/databases/(default)/documents:listCollectionIds"
          );
          assert.equal(init.method, "POST");
          const bodyStr = String(init.body ?? "");
          bodies.push(bodyStr);
          const req = /** @type {{ pageToken?: string }} */ (JSON.parse(bodyStr));
          if (!req.pageToken) {
            return jsonResponse({
              collectionIds: ["c1", "c2"],
              nextPageToken: "tok1",
            });
          }
          if (req.pageToken === "tok1") {
            return jsonResponse({ collectionIds: ["c3"], nextPageToken: "tok2" });
          }
          if (req.pageToken === "tok2") {
            return jsonResponse({ collectionIds: ["c4"] });
          }
          throw new Error("unexpected pageToken: " + req.pageToken);
        };

        const cols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents"
        );
        assert.deepEqual(cols, ["c1", "c2", "c3", "c4"]);
        assert.deepEqual(bodies, [
          "{}",
          '{"pageToken":"tok1"}',
          '{"pageToken":"tok2"}',
        ]);
        assert.equal(warns.length, 0, "normal pagination must not emit warnings");
      }

      // 2. Duplicate nextPageToken stops cleanly and emits console.warn.
      {
        warns.length = 0;
        let calls = 0;
        harness.fetchImpl = async (_url, init) => {
          calls++;
          const req = /** @type {{ pageToken?: string }} */ (
            JSON.parse(String(init.body ?? "{}"))
          );
          if (!req.pageToken) {
            return jsonResponse({ collectionIds: ["a"], nextPageToken: "dup" });
          }
          return jsonResponse({ collectionIds: ["b"], nextPageToken: "dup" });
        };

        const cols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents"
        );
        assert.deepEqual(cols, ["a", "b"]);
        assert.equal(calls, 2);
        assert.equal(
          warns.length,
          1,
          "duplicate nextPageToken must emit console.warn"
        );
        assert.ok(
          warns[0].includes("duplicate nextPageToken"),
          "unexpected warn: " + warns[0]
        );
      }

      // 3. Max page cap (1000 pages) warns iff nextPageToken remains.
      {
        warns.length = 0;
        let calls = 0;
        harness.fetchImpl = async () => {
          calls++;
          return jsonResponse({
            collectionIds: ["col_" + calls],
            nextPageToken: "tok_" + calls,
          });
        };

        const cols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents"
        );
        assert.equal(calls, 1000);
        assert.equal(cols.length, 1000);
        assert.equal(cols[0], "col_1");
        assert.equal(cols[999], "col_1000");
        assert.equal(
          warns.length,
          1,
          "hitting page cap with remaining nextPageToken must emit console.warn"
        );
        assert.ok(warns[0].includes("1000"), "unexpected warn: " + warns[0]);

        warns.length = 0;
        calls = 0;
        harness.fetchImpl = async () => {
          calls++;
          return jsonResponse({
            collectionIds: ["ok_" + calls],
            nextPageToken: calls < 1000 ? "ok_tok_" + calls : "",
          });
        };
        const exactCols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents"
        );
        assert.equal(calls, 1000);
        assert.equal(exactCols.length, 1000);
        assert.equal(
          warns.length,
          0,
          "completing exactly on page 1000 without nextPageToken must not warn"
        );
      }

      // 4. shouldAbort and AbortSignal halt pagination immediately between pages.
      {
        warns.length = 0;
        let calls = 0;
        let abortNow = false;
        harness.fetchImpl = async () => {
          calls++;
          abortNow = true;
          return jsonResponse({
            collectionIds: ["p1"],
            nextPageToken: "tok_next",
          });
        };
        const cols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents",
          { shouldAbort: () => abortNow }
        );
        assert.equal(
          calls,
          1,
          "shouldAbort must prevent fetching subsequent pages"
        );
        assert.deepEqual(cols, []);
        assert.equal(warns.length, 0);

        const ac = new AbortController();
        let signalCalls = 0;
        /** @type {unknown} */
        let seenSignal = null;
        harness.fetchImpl = async (_url, init) => {
          signalCalls++;
          seenSignal = init.signal;
          ac.abort();
          return jsonResponse({
            collectionIds: ["s1"],
            nextPageToken: "tok_s2",
          });
        };
        const sigCols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents",
          { signal: ac.signal }
        );
        assert.equal(signalCalls, 1, "aborted signal must halt after first page");
        assert.equal(
          seenSignal,
          ac.signal,
          "listCollectionIds must forward signal to fetch"
        );
        assert.deepEqual(sigCols, []);

        let preAbortedCalls = 0;
        harness.fetchImpl = async () => {
          preAbortedCalls++;
          return jsonResponse({ collectionIds: ["x"] });
        };
        const preCols = await api.listCollectionIds(
          "projects/p/databases/(default)/documents",
          { signal: ac.signal }
        );
        assert.equal(preAbortedCalls, 0);
        assert.deepEqual(preCols, []);
      }

      // 5. selectDatabase, loadDocumentLevel, and handleSseBatch abort stale paginated listCollectionIds.
      {
        const db1Page1Def = deferred();
        let db1ColPages = 0;
        harness.fetchImpl = async (url, init) => {
          if (url === "/v1/projects/p1/databases/db1") {
            return jsonResponse({
              name: "projects/p1/databases/db1",
              uid: "u1",
              type: "FIRESTORE_NATIVE",
              databaseEdition: "STANDARD",
              concurrencyMode: "PESSIMISTIC",
            });
          }
          if (url === "/v1/projects/p1/databases/db1/documents:listCollectionIds") {
            db1ColPages++;
            const body = /** @type {{ pageToken?: string }} */ (
              JSON.parse(String(init.body ?? "{}"))
            );
            if (!body.pageToken) return db1Page1Def.promise;
            throw new Error("stale db1 page 2 must never be requested");
          }
          if (url === "/v1/projects/p2/databases/db2") {
            return jsonResponse({
              name: "projects/p2/databases/db2",
              uid: "u2",
              type: "FIRESTORE_NATIVE",
              databaseEdition: "STANDARD",
              concurrencyMode: "PESSIMISTIC",
            });
          }
          if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {
            return jsonResponse({ collectionIds: ["fresh_col"] });
          }
          if (url === "/emulator/v1") {
            return jsonResponse([
              "projects/p1/databases/db1/documents",
              "projects/p2/databases/db2/documents",
            ]);
          }
          throw new Error("unexpected url in 5a: " + url);
        };

        const pSelect1 = app.selectDatabase("p1", "db1", [], true);
        await nextTick();
        const pSelect2 = app.selectDatabase("p2", "db2", [], true);
        db1Page1Def.resolve(
          jsonResponse({ collectionIds: ["c1"], nextPageToken: "page2" })
        );
        await Promise.all([pSelect1, pSelect2]);
        assert.equal(db1ColPages, 1);
        assert.deepEqual(app.state.rootCollections, ["fresh_col"]);

        // 5b. loadDocumentLevel doc switch (navEpoch bump) aborts paginated subcollection load before page 2.
        const docAPath = "projects/p2/databases/db2/documents/fresh_col/docA";
        const docBPath = "projects/p2/databases/db2/documents/fresh_col/docB";
        const docAPage1Def = deferred();
        let docAColPages = 0;
        harness.fetchImpl = async (url, init) => {
          if (url === `/v1/${docAPath}` || url === `/v1/${docBPath}`) {
            return jsonResponse({ name: url.slice(4), fields: {} });
          }
          if (url === `/v1/${docAPath}:listCollectionIds`) {
            docAColPages++;
            const body = /** @type {{ pageToken?: string }} */ (
              JSON.parse(String(init.body ?? "{}"))
            );
            if (!body.pageToken) return docAPage1Def.promise;
            throw new Error("stale docA subcollection page 2 must never be requested");
          }
          if (url === `/v1/${docBPath}:listCollectionIds`) {
            return jsonResponse({ collectionIds: ["subB"] });
          }
          if (url.startsWith("/v1/projects/p2/databases/db2/documents/fresh_col?")) {
            return jsonResponse({ documents: [] });
          }
          throw new Error("unexpected url in 5b: " + url);
        };

        app.state.segments = ["fresh_col", "docA"];
        const pDocA = app.loadDocumentLevel(docAPath, "docA", true);
        await nextTick();
        app.state.segments = ["fresh_col", "docB"];
        stateMod.bumpNavEpoch();
        const pDocB = app.loadDocumentLevel(docBPath, "docB", true);
        docAPage1Def.resolve(
          jsonResponse({ collectionIds: ["subA1"], nextPageToken: "sub_page2" })
        );
        await Promise.all([pDocA, pDocB]);
        assert.equal(docAColPages, 1);
        assert.equal(app.state.documentLevels.get(docAPath)?.loading, false);
        assert.deepEqual(
          app.state.documentLevels.get(docAPath)?.subcollections,
          []
        );
        assert.deepEqual(
          app.state.documentLevels.get(docBPath)?.subcollections,
          ["subB"]
        );

        // 5c. handleSseBatch aborts paginated root listCollectionIds when active DB switches mid-pagination.
        const ssePage1Def = deferred();
        let sseDb2ColPages = 0;
        harness.fetchImpl = async (url, init) => {
          if (url === "/emulator/v1") {
            return jsonResponse([
              "projects/p2/databases/db2/documents",
              "projects/p3/databases/db3/documents",
            ]);
          }
          if (url === "/v1/projects/p2/databases/db2") {
            return jsonResponse({
              name: "projects/p2/databases/db2",
              uid: "u2",
              type: "FIRESTORE_NATIVE",
              databaseEdition: "STANDARD",
              concurrencyMode: "PESSIMISTIC",
            });
          }
          if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {
            sseDb2ColPages++;
            const body = /** @type {{ pageToken?: string }} */ (
              JSON.parse(String(init.body ?? "{}"))
            );
            if (!body.pageToken) return ssePage1Def.promise;
            throw new Error("stale SSE db2 page 2 must never be requested");
          }
          if (url === "/v1/projects/p3/databases/db3") {
            return jsonResponse({
              name: "projects/p3/databases/db3",
              uid: "u3",
              type: "FIRESTORE_NATIVE",
              databaseEdition: "STANDARD",
              concurrencyMode: "PESSIMISTIC",
            });
          }
          if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") {
            return jsonResponse({ collectionIds: ["db3_col"] });
          }
          throw new Error("unexpected url in 5c: " + url);
        };

        const pSse = app.handleSseBatch([{ type: "resync" }]);
        await nextTick();
        const pSelect3 = app.selectDatabase("p3", "db3", [], true);
        ssePage1Def.resolve(
          jsonResponse({ collectionIds: ["stale_p2"], nextPageToken: "tok_sse_2" })
        );
        await Promise.all([pSse, pSelect3]);
        assert.equal(sseDb2ColPages, 1);
        assert.deepEqual(app.state.rootCollections, ["db3_col"]);
      }
    } finally {
      console.warn = origWarn;
    }
  });

  test("regression_load_collection_column_preserves_pagination_on_refresh", async () => {
    const colPath = "projects/p/databases/(default)/documents/items";
    const parentPath = "projects/p/databases/(default)/documents";
    const allDocs = Array.from({ length: 450 }, (_, i) => ({
      name: `${colPath}/doc-${String(i).padStart(3, "0")}`,
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }));

    let clampLimit = 0;
    /** @type {Array<{ pageSize: number, pageToken: string }>} */
    const requestedPages = [];

    harness.fetchImpl = async (url) => {
      const parsed = new URL(url, "http://localhost");
      const pageSize = Number(parsed.searchParams.get("pageSize") ?? "0");
      const pageToken = parsed.searchParams.get("pageToken") ?? "";
      requestedPages.push({ pageSize, pageToken });

      let start = 0;
      if (pageToken) {
        const idx = allDocs.findIndex((d) => d.name.endsWith("/" + pageToken));
        start = idx >= 0 ? idx + 1 : allDocs.length;
      }
      const effectiveSize =
        clampLimit > 0 ? Math.min(pageSize, clampLimit) : pageSize;
      const slice = allDocs.slice(start, start + effectiveSize);
      const hasMore = start + slice.length < allDocs.length;
      const nextPageToken =
        hasMore && slice.length > 0
          ? (slice[slice.length - 1].name.split("/").pop() ?? "")
          : "";
      return jsonResponse({ documents: slice, nextPageToken });
    };

    await app.loadCollectionColumn(colPath, parentPath, "items", false);
    const colState = app.state.collectionColumns.get(colPath);
    assert.ok(colState);
    assert.equal(colState.documents.length, 200);
    assert.equal(colState.nextPageToken, "doc-199");

    await app.loadCollectionColumn(colPath, parentPath, "items", true);
    assert.equal(colState.documents.length, 400);
    assert.equal(colState.nextPageToken, "doc-399");

    requestedPages.length = 0;
    await app.loadCollectionColumn(colPath, parentPath, "items", false);
    assert.deepEqual(requestedPages, [{ pageSize: 400, pageToken: "" }]);
    assert.equal(colState.documents.length, 400);
    assert.equal(colState.documents[399].id, "doc-399");
    assert.equal(colState.nextPageToken, "doc-399");

    clampLimit = 150;
    requestedPages.length = 0;
    await app.loadCollectionColumn(colPath, parentPath, "items", false);
    assert.deepEqual(requestedPages, [
      { pageSize: 400, pageToken: "" },
      { pageSize: 250, pageToken: "doc-149" },
      { pageSize: 100, pageToken: "doc-299" },
    ]);
    assert.equal(colState.documents.length, 400);
    assert.equal(colState.documents[0].id, "doc-000");
    assert.equal(colState.documents[399].id, "doc-399");
    assert.equal(colState.nextPageToken, "doc-399");
  });

  test("regression_uint8_to_base64_chunked_large_arrays", () => {
    for (const size of [0, 1, 0x7fff, 0x8000, 0x8001, 0x10000, 100000]) {
      const bytes = new Uint8Array(size);
      for (let i = 0; i < size; i++) {
        bytes[i] = (i * 31 + 7) & 0xff;
      }
      const expected = Buffer.from(bytes).toString("base64");
      assert.equal(
        values.uint8ToBase64(bytes),
        expected,
        "mismatch for size " + size
      );
    }

    const largeText = "a".repeat(70000);
    const encoded = values.encodeBytes(largeText, "utf8");
    assert.equal(encoded.valid, true);
    assert.equal(
      encoded.base64,
      Buffer.from(largeText, "utf8").toString("base64")
    );
  });

  test("regression_editor_key_value_errors_and_bytes_mode_error", () => {
    const docPath = "projects/p/databases/(default)/documents/items/d1";
    const session = editor.createEditorSession(docPath, "p", "(default)", {
      name: docPath,
      fields: {
        alpha: { integerValue: "10" },
        beta: { integerValue: "20" },
        gamma: { stringValue: "ok" },
      },
    });

    session.nodes[1].key = "alpha";
    session.nodes[2].key = "";

    let check = editor.draftNodesToFields(session.nodes, "p", "(default)");
    assert.equal(check.valid, false);
    assert.equal(check.errors.get(session.nodes[0].id), undefined);
    assert.deepEqual(check.errors.get(session.nodes[1].id), {
      keyError: 'Duplicate field name "alpha"',
    });
    assert.deepEqual(check.errors.get(session.nodes[2].id), {
      keyError: "Field name is required",
    });

    const extracted = editor.extractSessionFields(session);
    assert.equal(extracted.valid, false);
    assert.equal(typeof extracted.error, "string");
    assert.ok(extracted.error?.includes('Duplicate field name "alpha"'));

    let markup = renderTemplateMarkup(
      editor.renderHybridEditor({ session, onChange: () => {} })
    );
    const rowMatches = markup.split('<div class="field-row">').slice(1);
    assert.equal(rowMatches.length, 3);
    assert.equal(
      rowMatches[0].includes("input-error"),
      false,
      "valid first field must have no input-error"
    );
    assert.ok(
      rowMatches[1].includes('class="input field-key-input input-error"'),
      "duplicate key must highlight .field-key-input"
    );
    assert.equal(
      rowMatches[1].includes('class="input input-error"'),
      false,
      "duplicate key with valid value must NOT highlight value input"
    );
    assert.ok(
      rowMatches[2].includes('class="input field-key-input input-error"'),
      "empty key must highlight .field-key-input"
    );
    assert.equal(
      rowMatches[2].includes('class="input input-error"'),
      false,
      "empty key with valid value must NOT highlight value input"
    );

    session.nodes[1].text = "not-an-int";
    check = editor.draftNodesToFields(session.nodes, "p", "(default)");
    assert.equal(check.valid, false);
    const combinedErr = check.errors.get(session.nodes[1].id);
    assert.ok(combinedErr?.keyError?.includes('Duplicate field name "alpha"'));
    assert.ok(
      combinedErr?.valueError,
      "valueError must be recorded alongside keyError"
    );

    markup = renderTemplateMarkup(
      editor.renderHybridEditor({ session, onChange: () => {} })
    );
    const bothRows = markup.split('<div class="field-row">').slice(1);
    assert.ok(bothRows[1].includes('class="input field-key-input input-error"'));
    assert.ok(bothRows[1].includes('class="input input-error"'));
    assert.ok(bothRows[1].includes('Duplicate field name "alpha"'));
    assert.ok(bothRows[1].includes(combinedErr.valueError));

    session.nodes[1].key = "beta";
    session.nodes[2].key = "gamma";
    markup = renderTemplateMarkup(
      editor.renderHybridEditor({ session, onChange: () => {} })
    );
    const valueOnlyRows = markup.split('<div class="field-row">').slice(1);
    assert.equal(
      valueOnlyRows[1].includes('class="input field-key-input input-error"'),
      false,
      "pure value error must NOT highlight .field-key-input"
    );
    assert.ok(
      valueOnlyRows[1].includes('class="input input-error"'),
      "pure value error must highlight value input"
    );

    // Rejected switchBytesMode("utf8") on non-UTF-8 bytes surfaces inline error without dirtying.
    const binDoc = {
      name: docPath,
      fields: {
        raw: { bytesValue: "/+8=" },
      },
    };
    const binSession = editor.createEditorSession(docPath, "p", "(default)", binDoc);
    assert.equal(binSession.isDirty, false);
    assert.equal(binSession.nodes[0].bytesMode, "base64");
    assert.equal(binSession.nodes[0].bytesModeError, undefined);

    let rerenderCount = 0;
    const renderBin = () =>
      editor.renderHybridEditor({
        session: binSession,
        onChange: () => {
          rerenderCount++;
        },
      });

    const bytesSelect = findSelectByAriaLabel(renderBin(), "Bytes encoding mode");
    assert.ok(
      bytesSelect?.handlers.change,
      "bytes encoding mode select handler must exist"
    );
    const mockSelectTarget = { value: "utf8" };
    bytesSelect.handlers.change({ target: mockSelectTarget });

    assert.equal(
      binSession.nodes[0].bytesMode,
      "base64",
      "mode stays base64 when UTF-8 decode fails"
    );
    assert.equal(
      mockSelectTarget.value,
      "base64",
      "DOM select value snaps back to base64"
    );
    const getBytesModeError = () => binSession.nodes[0].bytesModeError;
    assert.ok(
      getBytesModeError()?.includes("UTF-8"),
      "bytesModeError must explain UTF-8 rejection"
    );
    assert.equal(
      binSession.isDirty,
      false,
      "rejected switchBytesMode must NOT dirty a clean session"
    );
    assert.equal(
      editor.extractSessionFields(binSession).valid,
      true,
      "valid base64 bytes remain saveable"
    );
    assert.ok(rerenderCount >= 1);

    const rejectedMarkup = renderTemplateMarkup(renderBin());
    assert.ok(rejectedMarkup.includes("Cannot switch to UTF-8"));
    assert.ok(rejectedMarkup.includes('class="select input-error"'));
    assert.equal(
      rejectedMarkup.includes('class="input input-error"'),
      false,
      "valid base64 value input must NOT be marked input-error on bytesModeError"
    );

    // (a) Switching to valid "hex" mode clears bytesModeError and preserves clean state.
    const bytesSelect2 = findSelectByAriaLabel(
      renderBin(),
      "Bytes encoding mode"
    );
    assert.ok(bytesSelect2?.handlers.change);
    bytesSelect2.handlers.change({ target: { value: "hex" } });
    assert.equal(binSession.nodes[0].bytesMode, "hex");
    assert.equal(binSession.nodes[0].text, "ffef");
    assert.equal(binSession.nodes[0].bytesModeError, undefined);
    assert.equal(binSession.isDirty, false);

    // (b) Editing value text clears bytesModeError.
    findSelectByAriaLabel(renderBin(), "Bytes encoding mode")?.handlers.change({
      target: { value: "utf8" },
    });
    assert.ok(getBytesModeError()?.includes("UTF-8"));
    findInputByAriaLabel(renderBin(), "Bytes value")?.handlers.input({
      target: { value: "6869" },
    });
    assert.equal(
      binSession.nodes[0].bytesModeError,
      undefined,
      "editing value clears bytesModeError"
    );
    assert.equal(binSession.isDirty, true);

    // (c) Revert clears bytesModeError & restores clean state.
    editor.syncSessionWithDocument(binSession, binDoc);
    assert.equal(binSession.isDirty, false);
    assert.equal(binSession.nodes[0].bytesModeError, undefined);

    // (d) Structured -> Raw -> Structured clears bytesModeError.
    findSelectByAriaLabel(renderBin(), "Bytes encoding mode")?.handlers.change({
      target: { value: "utf8" },
    });
    assert.ok(getBytesModeError()?.includes("UTF-8"));
    const toRaw = findClickHandler(renderBin(), "Raw ProtoJSON");
    assert.ok(toRaw);
    toRaw();
    assert.equal(binSession.mode, "raw");
    assert.equal(
      binSession.nodes[0].bytesModeError,
      undefined,
      "switching to Raw clears bytesModeError"
    );
    const toStructured = findClickHandler(renderBin(), "Structured Fields");
    assert.ok(toStructured);
    toStructured();
    assert.equal(binSession.nodes[0].bytesModeError, undefined);
    assert.equal(binSession.isDirty, false);

    // (e) Changing field type clears bytesModeError.
    findSelectByAriaLabel(renderBin(), "Bytes encoding mode")?.handlers.change({
      target: { value: "utf8" },
    });
    assert.ok(getBytesModeError()?.includes("UTF-8"));
    const typeSelect = findElementByClass(renderBin(), "field-type-select");
    assert.ok(typeSelect?.handlers.change);
    typeSelect.handlers.change({ target: { value: "stringValue" } });
    assert.equal(binSession.nodes[0].type, "stringValue");
    assert.equal(
      binSession.nodes[0].bytesModeError,
      undefined,
      "changing type clears bytesModeError"
    );

    const manualCheck = editor.draftNodesToFields(
      [
        {
          id: 999,
          key: "k",
          type: "bytesValue",
          text: "AAEC",
          boolVal: false,
          latText: "0",
          lngText: "0",
          bytesMode: "base64",
          children: [],
        },
      ],
      "p",
      "(default)"
    );
    assert.equal(manualCheck.valid, true);
  });
});
