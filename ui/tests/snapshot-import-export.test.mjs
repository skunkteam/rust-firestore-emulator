// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  DomElement,
  deferred,
  findClickHandler,
  findElement,
  flattenText,
  harness,
  installFakeIntervals,
  installFakeTimers,
  jsonResponse,
  nextTick,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as api from "../api.js";
import * as stateMod from "../state.js";
import { createQueryRunnerState } from "../components/query-model.js";
import { ensureSubcollections } from "../components/query-suggestions.js";

/**
 * @typedef {{
 *   tagName: string,
 *   href: string,
 *   download: string,
 *   style: Record<string, string>,
 *   clicked: boolean,
 *   click: () => void,
 *   remove: () => void,
 * }} MockAnchorElement
 */

const topbarEl = new DomElement("HEADER");
const workspaceEl = new DomElement("MAIN");
const modalRootEl = new DomElement("DIV");
const toastRootEl = new DomElement("DIV");

harness.getElementById = (id) =>
  id === "topbar"
    ? topbarEl
    : id === "workspace"
      ? workspaceEl
      : id === "modal-root"
        ? modalRootEl
        : id === "toast-root"
          ? toastRootEl
          : null;
harness.fetchImpl = async (url) => {
  if (url === "/emulator/v1") return jsonResponse([]);
  return jsonResponse({});
};

const app = await import("../app.js");
await nextTick();

const docObj = /** @type {Record<string, unknown>} */ (
  /** @type {unknown} */ (globalThis.document)
);
const urlObj = /** @type {Record<string, unknown>} */ (
  /** @type {unknown} */ (URL)
);
const hasOwnBody = Object.prototype.hasOwnProperty.call(docObj, "body");
const origDocBody = docObj.body;
const origDocCreateElement = /** @type {(tag: string) => unknown} */ (
  docObj.createElement
);
const hasOwnCreateObjectURL = Object.prototype.hasOwnProperty.call(
  urlObj,
  "createObjectURL"
);
const origCreateObjectURL = urlObj.createObjectURL;
const hasOwnRevokeObjectURL = Object.prototype.hasOwnProperty.call(
  urlObj,
  "revokeObjectURL"
);
const origRevokeObjectURL = urlObj.revokeObjectURL;

let nextObjectUrlSeq = 0;
/** @type {Array<{ url: string, blob: unknown }>} */
let createdObjectUrls = [];
/** @type {string[]} */
let revokedObjectUrls = [];
/** @type {MockAnchorElement[]} */
let createdAnchors = [];
/** @type {unknown[]} */
let appendedAnchors = [];
/** @type {MockAnchorElement[]} */
let removedAnchors = [];
/** @type {{ appendChild: (el: unknown) => unknown }} */
let mockDocBody;

/**
 * Builds a mock Fetch Response supporting `.blob()`, `.text()`, and `.json()`.
 * @param {string | Uint8Array} body
 * @param {number} [status]
 * @param {Record<string, string>} [headers]
 */
function blobResponse(body, status = 200, headers = {}) {
  const raw =
    typeof body === "string"
      ? new TextEncoder().encode(body)
      : body instanceof Uint8Array
        ? body
        : new Uint8Array(body);
  const bytes = new Uint8Array(raw.byteLength);
  bytes.set(raw);
  const blob = new Blob([bytes], { type: "application/octet-stream" });
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      /** @param {string} k */
      get(k) {
        const target = k.toLowerCase();
        for (const [hk, hv] of Object.entries(headers)) {
          if (hk.toLowerCase() === target) return hv;
        }
        return null;
      },
    },
    async blob() {
      return blob;
    },
    async text() {
      return new TextDecoder().decode(bytes);
    },
    async json() {
      return JSON.parse(new TextDecoder().decode(bytes));
    },
  };
}

/**
 * Finds the first `<button>` in `node` whose text contains `labelText` and returns disabled state.
 * @param {unknown} node
 * @param {string} labelText
 * @returns {{ disabled: boolean, attrs: string, text: string } | null}
 */
function findButtonMeta(node, labelText) {
  const markup = renderTemplateMarkup(node);
  const re = /<button\b([^>]*)>([\s\S]*?)<\/button>/g;
  let match;
  while ((match = re.exec(markup)) !== null) {
    const attrs = match[1];
    const text = match[2]
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (text.includes(labelText)) {
      return {
        disabled: /\bdisabled(?:\s*=|\b)/.test(attrs),
        attrs,
        text,
      };
    }
  }
  return null;
}

/**
 * Finds the `@change` handler on `<input type="file">`.
 * @param {unknown} node
 * @returns {((...args: unknown[]) => unknown) | null}
 */
function findFileChangeHandler(node) {
  const el = findElement(
    node,
    (item) => item.tagName === "input" && item.attrs.type === "file"
  );
  return el?.handlers.change ?? null;
}

/**
 * Reads `app.state.modal` without TS control-flow narrowing across callbacks.
 * @returns {import('../state.js').AppState['modal']}
 */
function currentModal() {
  return app.state.modal;
}

/**
 * Reads `app.state.activeDbMeta` without TS control-flow narrowing across callbacks.
 * @returns {import('../state.js').AppState['activeDbMeta']}
 */
function currentActiveDbMeta() {
  return app.state.activeDbMeta;
}

/**
 * Clears a URL log array without triggering TS `never[]` control-flow narrowing.
 * @param {string[]} list
 */
function clearList(list) {
  list.length = 0;
}

/**
 * Checks whether `list` contains `entry` without TS `never[]` narrowing on `[]`.
 * @param {string[]} list
 * @param {string} entry
 * @returns {boolean}
 */
function hasUrl(list, entry) {
  return list.includes(entry);
}

/**
 * Installs per-test anchor/body/objectURL mocks and restores `harness.getElementById`.
 */
function installSnapshotBrowserMocks() {
  nextObjectUrlSeq = 0;
  createdObjectUrls = [];
  revokedObjectUrls = [];
  createdAnchors = [];
  appendedAnchors = [];
  removedAnchors = [];

  harness.getElementById = (id) =>
    id === "topbar"
      ? topbarEl
      : id === "workspace"
        ? workspaceEl
        : id === "modal-root"
          ? modalRootEl
          : id === "toast-root"
            ? toastRootEl
            : null;
  docObj.getElementById = (/** @type {string} */ id) =>
    harness.getElementById(id);

  mockDocBody = {
    appendChild(el) {
      appendedAnchors.push(el);
      return el;
    },
  };
  docObj.body = mockDocBody;

  docObj.createElement = (/** @type {string} */ tag) => {
    if (String(tag).toLowerCase() === "a") {
      /** @type {MockAnchorElement} */
      const anchor = {
        tagName: "A",
        href: "",
        download: "",
        style: {},
        clicked: false,
        click() {
          this.clicked = true;
        },
        remove() {
          removedAnchors.push(this);
        },
      };
      createdAnchors.push(anchor);
      return anchor;
    }
    return origDocCreateElement(tag);
  };

  urlObj.createObjectURL = (/** @type {unknown} */ blob) => {
    const url = `blob:mock/${++nextObjectUrlSeq}`;
    createdObjectUrls.push({ url, blob });
    return url;
  };
  urlObj.revokeObjectURL = (/** @type {unknown} */ url) => {
    revokedObjectUrls.push(String(url));
  };
}

/**
 * Restores browser globals mutated by snapshot tests.
 */
function restoreSnapshotBrowserMocks() {
  docObj.createElement = origDocCreateElement;
  if (hasOwnBody) docObj.body = origDocBody;
  else delete docObj.body;

  if (hasOwnCreateObjectURL) urlObj.createObjectURL = origCreateObjectURL;
  else delete urlObj.createObjectURL;

  if (hasOwnRevokeObjectURL) urlObj.revokeObjectURL = origRevokeObjectURL;
  else delete urlObj.revokeObjectURL;
}

describe("Snapshot import/export API, SSE sync, modals, and a11y", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
    installSnapshotBrowserMocks();
  });

  afterEach(async () => {
    restoreSnapshotBrowserMocks();
    await resetHarness(app, stateMod);
  });

  test("snapshot_api_export_and_import_contracts", async () => {
    /** @type {Array<{ url: string, init: RequestInit }>} */
    const calls = [];
    /** @type {() => unknown} */
    let nextResponse = () => jsonResponse({});

    harness.fetchImpl = async (url, init) => {
      calls.push({ url, init: init ?? {} });
      return nextResponse();
    };

    // 1. No-arg exportSnapshot ("All Databases") + Content-Disposition parsing variants.
    /** @type {Array<[string | null, string]>} */
    const filenameCases = [
      [
        'attachment; filename="demo-_default_-20261126T000000Z.fsemu"',
        "demo-_default_-20261126T000000Z.fsemu",
      ],
      ["attachment; filename=unquoted.fsemu; size=12", "unquoted.fsemu"],
      ['attachment; filename="../../nested/dir/snap.fsemu"', "snap.fsemu"],
      ['attachment; filename="C:\\fakepath\\win.fsemu"', "win.fsemu"],
      ['attachment; filename=""', "firestore-emulator-snapshot.fsemu"],
      [null, "firestore-emulator-snapshot.fsemu"],
    ];
    for (const [header, expectedFilename] of filenameCases) {
      /** @type {Record<string, string>} */
      const headers = header ? { "Content-Disposition": header } : {};
      nextResponse = () => blobResponse(new Uint8Array([1, 2, 3]), 200, headers);
      const res = await api.exportSnapshot();
      assert.equal(calls.at(-1)?.url, "/emulator/v1/export");
      assert.equal(res.filename, expectedFilename);
      assert.equal(res.blob.size, 3);
    }

    // 2. Repeatable ?database= parameters on exportSnapshot.
    nextResponse = () =>
      blobResponse("abc", 200, {
        "Content-Disposition": 'attachment; filename="single.fsemu"',
      });
    const targeted = await api.exportSnapshot([
      "projects/p1/databases/(default)/documents",
      "projects/p2/databases/db2/documents",
    ]);
    assert.equal(targeted.filename, "single.fsemu");
    const parsedUrl = new URL(
      calls.at(-1)?.url ?? "",
      "http://localhost"
    );
    assert.equal(parsedUrl.pathname, "/emulator/v1/export");
    assert.deepEqual(parsedUrl.searchParams.getAll("database"), [
      "projects/p1/databases/(default)/documents",
      "projects/p2/databases/db2/documents",
    ]);

    // 3. ApiError propagation via shared parseApiError (JSON RestError & raw HTTP fallback).
    nextResponse = () =>
      jsonResponse(
        { error: { code: 404, status: "NOT_FOUND", message: "missing db" } },
        404
      );
    await assert.rejects(
      () => api.exportSnapshot(["projects/missing/databases/d/documents"]),
      (err) =>
        err instanceof api.ApiError &&
        err.code === 404 &&
        err.status === "NOT_FOUND" &&
        err.message === "missing db"
    );

    nextResponse = () => blobResponse("plain gateway failure", 502);
    await assert.rejects(
      () => api.exportSnapshot(),
      (err) =>
        err instanceof api.ApiError &&
        err.code === 502 &&
        err.status === "HTTP_ERROR" &&
        err.message === "plain gateway failure"
    );

    // 4. importSnapshot sends Content-Type: application/octet-stream and raw File/Blob body.
    const uploadBlob = new Blob([new Uint8Array([9, 8, 7, 6])], {
      type: "application/octet-stream",
    });
    const summaryPayload = {
      databases: [
        {
          name: "projects/demo/databases/(default)/documents",
          projectId: "demo",
          databaseId: "(default)",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
          documentCount: 4,
          replaced: true,
        },
      ],
      totalDatabases: 1,
      totalDocuments: 4,
    };
    nextResponse = () => jsonResponse(summaryPayload, 200);
    const summary = await api.importSnapshot(uploadBlob);
    const importCall = calls.at(-1);
    assert.equal(importCall?.url, "/emulator/v1/import");
    assert.equal(importCall?.init.method, "POST");
    assert.equal(
      /** @type {Record<string, string> | undefined} */ (
        importCall?.init.headers
      )?.["Content-Type"],
      "application/octet-stream"
    );
    assert.equal(importCall?.init.body, uploadBlob);
    assert.deepEqual(summary, summaryPayload);

    nextResponse = () =>
      jsonResponse(
        {
          error: {
            code: 409,
            status: "ABORTED",
            message: "import in progress",
          },
        },
        409
      );
    await assert.rejects(
      () => api.importSnapshot(uploadBlob),
      (err) =>
        err instanceof api.ApiError &&
        err.code === 409 &&
        err.status === "ABORTED"
    );
  });

  test("snapshot_sse_database_replaced_reload_dirty_protection_and_precedence", async () => {
    const activeRoot = "projects/p1/databases/(default)/documents";
    const otherRoot = "projects/p2/databases/other/documents";
    const dirtyUpdateDocPath = `${activeRoot}/users/alice`;
    const dirtyDeleteDocPath = `${activeRoot}/users/ghost`;
    const cleanDocPath = `${activeRoot}/users/bob`;

    let serverMeta = {
      name: "projects/p1/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    };
    const rootCols = ["users"];
    let docsInUsers = [
      {
        name: dirtyUpdateDocPath,
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      },
      {
        name: dirtyDeleteDocPath,
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      },
      {
        name: cleanDocPath,
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      },
    ];
    /** @type {Map<string, import('../values.js').FirestoreDocument>} */
    const docResponses = new Map();
    docResponses.set(dirtyUpdateDocPath, {
      name: dirtyUpdateDocPath,
      fields: { role: { stringValue: "v1" } },
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    });
    docResponses.set(dirtyDeleteDocPath, {
      name: dirtyDeleteDocPath,
      fields: { temp: { stringValue: "alive" } },
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    });
    docResponses.set(cleanDocPath, {
      name: cleanDocPath,
      fields: { role: { stringValue: "clean-v1" } },
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    });
    let dbList = [activeRoot, otherRoot];
    /** @type {string[]} */
    const fetchedUrls = [];

    harness.fetchImpl = async (url) => {
      fetchedUrls.push(url);
      if (url === "/emulator/v1") return jsonResponse(dbList);
      if (url === "/v1/projects/p1/databases/(default)") {
        return jsonResponse(serverMeta);
      }
      if (url === `/v1/${activeRoot}:listCollectionIds`) {
        return jsonResponse({ collectionIds: rootCols });
      }
      if (url.startsWith(`/v1/${activeRoot}/users?`)) {
        return jsonResponse({ documents: docsInUsers });
      }
      if (url === `/v1/${dirtyUpdateDocPath}`) {
        const doc = docResponses.get(dirtyUpdateDocPath);
        return doc
          ? jsonResponse(doc)
          : jsonResponse(
              { error: { code: 404, status: "NOT_FOUND", message: "missing" } },
              404
            );
      }
      if (url === `/v1/${dirtyDeleteDocPath}`) {
        const doc = docResponses.get(dirtyDeleteDocPath);
        return doc
          ? jsonResponse(doc)
          : jsonResponse(
              { error: { code: 404, status: "NOT_FOUND", message: "missing" } },
              404
            );
      }
      if (url === `/v1/${cleanDocPath}`) {
        const doc = docResponses.get(cleanDocPath);
        return doc
          ? jsonResponse(doc)
          : jsonResponse(
              { error: { code: 404, status: "NOT_FOUND", message: "missing" } },
              404
            );
      }
      if (url.endsWith(":listCollectionIds")) {
        return jsonResponse({ collectionIds: [] });
      }
      if (url === "/v1/projects/p2/databases/other") {
        return jsonResponse({
          name: "projects/p2/databases/other",
          uid: "u2",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === `/v1/${otherRoot}:listCollectionIds`) {
        return jsonResponse({ collectionIds: [] });
      }
      throw new Error(`unexpected url: ${url}`);
    };

    // Load clean doc, dirty-updated doc, and dirty-deleted doc into documentLevels.
    await app.selectDatabase("p1", "(default)", ["users", "bob"], true);
    await app.loadDocumentLevel(dirtyDeleteDocPath, "ghost", false);
    const ghostLevel = app.state.documentLevels.get(dirtyDeleteDocPath);
    assert.ok(ghostLevel?.editorSession);
    ghostLevel.editorSession.nodes[0].text = "ghost-unsaved";
    ghostLevel.editorSession.isDirty = true;

    app.state.segments = ["users", "alice"];
    await app.hydrateVisiblePath();
    const aliceLevel = app.state.documentLevels.get(dirtyUpdateDocPath);
    assert.ok(aliceLevel?.editorSession);
    aliceLevel.editorSession.nodes[0].text = "alice-unsaved";
    aliceLevel.editorSession.isDirty = true;

    // Populate Query Runner state so we verify selective replacement leaves it intact vs active replacement resets it.
    app.state.queryState = createQueryRunnerState("users");
    const bobDoc = docResponses.get(cleanDocPath);
    assert.ok(bobDoc);
    app.state.queryState.results = [bobDoc];

    // 1. Non-active database_replaced refreshes db list ONLY; active selection & queryState stay untouched.
    fetchedUrls.length = 0;
    await app.handleSseBatch([
      { type: "database_replaced", database: otherRoot },
    ]);
    assert.equal(app.state.activeProjectId, "p1");
    assert.equal(app.state.activeDatabaseId, "(default)");
    assert.equal(app.state.queryState.collectionId, "users");
    assert.equal(app.state.queryState.results?.length, 1);
    assert.equal(
      fetchedUrls.some((u) => u.startsWith("/v1/projects/p1/")),
      false,
      "non-active database_replaced must not reload active database"
    );

    // 2. Active database_replaced retains selection, updates metadata, resets queryState, and preserves dirty editors.
    serverMeta = {
      ...serverMeta,
      databaseEdition: "ENTERPRISE",
      concurrencyMode: "OPTIMISTIC",
    };
    docResponses.set(dirtyUpdateDocPath, {
      name: dirtyUpdateDocPath,
      fields: { role: { stringValue: "v2-restored" } },
      createTime: "2026-01-02T00:00:00Z",
      updateTime: "2026-01-02T00:00:00Z",
    });
    docResponses.delete(dirtyDeleteDocPath);
    docResponses.set(cleanDocPath, {
      name: cleanDocPath,
      fields: { role: { stringValue: "clean-v2" } },
      createTime: "2026-01-02T00:00:00Z",
      updateTime: "2026-01-02T00:00:00Z",
    });
    docsInUsers = [
      {
        name: dirtyUpdateDocPath,
        createTime: "2026-01-02T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      },
      {
        name: cleanDocPath,
        createTime: "2026-01-02T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      },
    ];

    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.equal(app.state.activeProjectId, "p1");
    assert.equal(app.state.activeDatabaseId, "(default)");
    assert.equal(app.state.activeDbMeta?.databaseEdition, "ENTERPRISE");
    assert.equal(app.state.activeDbMeta?.concurrencyMode, "OPTIMISTIC");
    assert.equal(app.state.queryState.collectionId, "");
    assert.equal(app.state.queryState.results, null);

    assert.equal(aliceLevel.editorSession.isDirty, true);
    assert.equal(aliceLevel.editorSession.nodes[0].text, "alice-unsaved");
    assert.equal(
      aliceLevel.editorSession.externalDoc?.fields?.role?.stringValue,
      "v2-restored"
    );

    // Also verify non-visible dirty ghost document retains dirty state and picks up deletedExternally when visited.
    assert.equal(app.state.documentLevels.has(dirtyDeleteDocPath), true);
    await app.loadDocumentLevel(dirtyDeleteDocPath, "ghost", true);
    assert.equal(ghostLevel.editorSession.isDirty, true);
    assert.equal(ghostLevel.editorSession.deletedExternally, true);
    assert.equal(ghostLevel.editorSession.nodes[0].text, "ghost-unsaved");

    // Clean non-visible bob was pruned and fetches clean-v2 when loaded.
    assert.equal(app.state.documentLevels.has(cleanDocPath), false);
    await app.loadDocumentLevel(cleanDocPath, "bob", true);
    assert.equal(
      app.state.documentLevels.get(cleanDocPath)?.editorSession?.cleanDoc
        ?.fields?.role?.stringValue,
      "clean-v2"
    );

    // 3. database_cleared wins over database_replaced regardless of event order within one batch.
    for (const batch of /** @type {import('../api.js').EmulatorSseEvent[][]} */ ([
      [
        { type: "database_replaced", database: activeRoot },
        { type: "database_cleared", database: activeRoot },
      ],
      [
        { type: "database_cleared", database: activeRoot },
        { type: "database_replaced", database: activeRoot },
      ],
    ])) {
      app.state.activeProjectId = "p1";
      app.state.activeDatabaseId = "(default)";
      app.state.segments = ["users", "alice"];
      dbList = [otherRoot];
      await app.handleSseBatch(batch);
      assert.equal(
        app.state.activeProjectId,
        "p2",
        "cleared active DB must switch to remaining DB instead of rehydrating old DB"
      );
      assert.equal(app.state.activeDatabaseId, "other");
      assert.deepEqual(app.state.segments, []);
    }

    // And when no databases remain, cleared wins with null selection.
    app.state.activeProjectId = "p1";
    app.state.activeDatabaseId = "(default)";
    app.state.segments = ["users", "alice"];
    dbList = [];
    await app.handleSseBatch([
      { type: "database_cleared", database: activeRoot },
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
  });

  test("snapshot_sse_database_replaced_truncates_clean_paths_and_prunes_caches", async () => {
    const activeRoot = "projects/p/databases/(default)/documents";
    const rootCols = ["rooms", "users", "orders"];
    const existingDocs = new Set([
      `${activeRoot}/users/u205`,
      `${activeRoot}/orders/ord1`,
    ]);
    const subcolsByDoc = new Map([
      [`${activeRoot}/rooms/r1`, ["messages"]],
      [`${activeRoot}/orders/ord1`, []],
    ]);

    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([activeRoot]);
      if (url === "/v1/projects/p/databases/(default)") {
        return jsonResponse({
          name: "projects/p/databases/(default)",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === `/v1/${activeRoot}:listCollectionIds`) {
        return jsonResponse({ collectionIds: rootCols });
      }
      if (url.includes(":listCollectionIds")) {
        const docPath = url.slice(
          "/v1/".length,
          -":listCollectionIds".length
        );
        return jsonResponse({
          collectionIds: subcolsByDoc.get(docPath) ?? [],
        });
      }
      if (url.includes("?")) {
        // Return empty page-1 listing intentionally to prove truncation uses DocumentLevelState, NOT page-1 collectionColumns!
        return jsonResponse({ documents: [] });
      }
      const docPath = url.slice("/v1/".length);
      if (existingDocs.has(docPath)) {
        return jsonResponse({
          name: docPath,
          fields: {},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      return jsonResponse(
        { error: { code: 404, status: "NOT_FOUND", message: "missing" } },
        404
      );
    };

    await app.selectDatabase("p", "(default)", [], true);

    // 1. Phantom parent r1 survives (`exists: false, subcollections: ["messages"]`), deleted leaf m1 truncates to ["rooms", "r1", "messages"].
    app.state.segments = ["rooms", "r1", "messages", "m1"];
    app.state.collectionColumns.set(`${activeRoot}/stale_col`, {
      collectionPath: `${activeRoot}/stale_col`,
      collectionId: "stale_col",
      parentPath: activeRoot,
      documents: [],
      nextPageToken: "",
      filterText: "",
      loading: false,
    });
    app.state.documentLevels.set(`${activeRoot}/stale_col/s1`, {
      docPath: `${activeRoot}/stale_col/s1`,
      docId: "s1",
      exists: true,
      loading: false,
      subcollections: [],
      editorSession: null,
    });
    app.state.documentLevels.set(`${activeRoot}/drafts/keep_dirty`, {
      docPath: `${activeRoot}/drafts/keep_dirty`,
      docId: "keep_dirty",
      exists: true,
      loading: false,
      subcollections: [],
      editorSession: /** @type {import('../components/editor.js').EditorSession} */ (
        /** @type {unknown} */ ({ isDirty: true })
      ),
    });

    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.deepEqual(app.state.segments, ["rooms", "r1", "messages"]);
    assert.equal(
      globalThis.window.location.pathname,
      "/projects/p/databases/(default)/documents/rooms/r1/messages"
    );
    assert.equal(
      app.state.collectionColumns.has(`${activeRoot}/stale_col`),
      false
    );
    assert.equal(
      app.state.documentLevels.has(`${activeRoot}/stale_col/s1`),
      false
    );
    assert.equal(
      app.state.documentLevels.has(`${activeRoot}/rooms/r1/messages/m1`),
      false
    );
    assert.equal(app.state.documentLevels.has(`${activeRoot}/rooms/r1`), true);
    assert.equal(
      app.state.documentLevels.has(`${activeRoot}/drafts/keep_dirty`),
      true
    );

    // 2. Document beyond page 1 (absent from collectionColumns) survives via DocumentLevelState.exists === true.
    app.state.segments = ["users", "u205"];
    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.deepEqual(app.state.segments, ["users", "u205"]);

    // 3. Missing subcollection under surviving doc truncates to the surviving doc.
    app.state.segments = ["orders", "ord1", "items", "i1"];
    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.deepEqual(app.state.segments, ["orders", "ord1"]);
    assert.equal(
      app.state.collectionColumns.has(`${activeRoot}/orders/ord1/items`),
      false
    );

    // 4. Dirty document along path prevents segment truncation even if root collection disappeared.
    app.state.segments = ["legacy", "doc1", "sub", "s1"];
    app.state.documentLevels.set(`${activeRoot}/legacy/doc1`, {
      docPath: `${activeRoot}/legacy/doc1`,
      docId: "doc1",
      exists: true,
      loading: false,
      subcollections: ["sub"],
      editorSession: /** @type {import('../components/editor.js').EditorSession} */ (
        /** @type {unknown} */ ({ isDirty: true, cleanDoc: null })
      ),
    });
    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.deepEqual(app.state.segments, ["legacy", "doc1", "sub", "s1"]);

    // 5. Clean path with missing root collection truncates all the way to [].
    const legacyDoc = app.state.documentLevels.get(`${activeRoot}/legacy/doc1`);
    assert.ok(legacyDoc?.editorSession);
    legacyDoc.editorSession.isDirty = false;
    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.deepEqual(app.state.segments, []);
    assert.equal(
      globalThis.window.location.pathname,
      "/projects/p/databases/(default)/documents"
    );
    assert.equal(
      app.state.documentLevels.has(`${activeRoot}/legacy/doc1`),
      false
    );
  });

  test("snapshot_ui_modals_topbar_paused_queue_and_repeated_file_reset", async () => {
    const dbA = "projects/a/databases/d1/documents";
    const dbZ = "projects/z/databases/d1/documents";
    const timers = installFakeTimers();

    try {
      // 1. Topbar & Empty Hero visibility when 0 databases exist.
      app.state.databases = [];
      app.state.activeProjectId = null;
      app.state.activeDatabaseId = null;
      app.state.activeDbMeta = null;

      const topbarEmpty = app.renderTopbar();
      assert.ok(
        findClickHandler(topbarEmpty, "Import"),
        "Import always visible in topbar"
      );
      assert.equal(
        findClickHandler(topbarEmpty, "Export"),
        null,
        "Export hidden when 0 databases exist"
      );

      const heroEmpty = app.renderWorkspace();
      assert.ok(findClickHandler(heroEmpty, "+ Open / Create Database"));
      const heroImportBtn = findClickHandler(heroEmpty, "Import Snapshot");
      assert.ok(heroImportBtn, "Import Snapshot visible in empty hero");
      heroImportBtn();
      assert.equal(app.state.modal?.kind, "import_snapshot");
      const importMarkup = renderTemplateMarkup(app.renderModal());
      assert.ok(importMarkup.includes('role="dialog"'));
      assert.ok(importMarkup.includes('aria-modal="true"'));
      assert.ok(
        importMarkup.includes('aria-labelledby="modal-title-import-snapshot"')
      );
      assert.equal(/<label(?![^>]*\bfor=)/.test(importMarkup), false);
      app.state.modal = null;

      // 2. Topbar shows Export when databases exist even before active selection; Current Database is disabled without active DB.
      app.state.databases = [
        { projectId: "a", databaseId: "d1", rootPath: dbA },
      ];
      const topbarWithDbs = app.renderTopbar();
      const exportTopbarBtn = findClickHandler(topbarWithDbs, "Export");
      assert.ok(exportTopbarBtn, "Export visible when databases.length > 0");
      exportTopbarBtn();
      assert.equal(app.state.modal?.kind, "export_snapshot");
      const exportMarkup = renderTemplateMarkup(app.renderModal());
      assert.ok(exportMarkup.includes('role="dialog"'));
      assert.ok(exportMarkup.includes('aria-modal="true"'));
      assert.ok(
        exportMarkup.includes('aria-labelledby="modal-title-export-snapshot"')
      );
      assert.equal(
        findButtonMeta(app.renderModal(), "Current Database")?.disabled,
        true
      );
      assert.equal(
        findButtonMeta(app.renderModal(), "All Databases")?.disabled,
        false
      );

      // 3. Export modal download flow (Current Database + All Databases + busy disabling + deferred revoke + optional document.body + error handling).
      /** @type {'current-ok' | 'all-ok' | 'fail'} */
      let exportMode = "current-ok";
      /** @type {ReturnType<typeof deferred> | null} */
      let exportDeferred = null;
      /** @type {Array<{ url: string, init: RequestInit }>} */
      const exportCalls = [];
      harness.fetchImpl = async (url, init) => {
        exportCalls.push({ url, init: init ?? {} });
        if (exportDeferred) {
          await exportDeferred.promise;
        }
        if (exportMode === "fail") {
          return jsonResponse(
            {
              error: { code: 404, status: "NOT_FOUND", message: "db missing" },
            },
            404
          );
        }
        const fname =
          exportMode === "current-ok" ? "a-d1.fsemu" : "all.fsemu";
        return blobResponse(new Uint8Array([1, 2, 3]), 200, {
          "Content-Disposition": `attachment; filename="${fname}"`,
        });
      };

      app.state.activeProjectId = "a";
      app.state.activeDatabaseId = "d1";
      assert.equal(
        findButtonMeta(app.renderModal(), "Current Database")?.disabled,
        false
      );

      exportDeferred = deferred();
      const currentBtn = findClickHandler(app.renderModal(), "Current Database");
      assert.ok(currentBtn);
      const currentPromise = currentBtn();
      assert.equal(app.state.modal?.busy, true);
      const busyExportTpl = app.renderModal();
      assert.equal(
        findButtonMeta(busyExportTpl, "Exporting...")?.disabled,
        true
      );
      assert.equal(
        findButtonMeta(busyExportTpl, "All Databases")?.disabled,
        true
      );
      assert.equal(findButtonMeta(busyExportTpl, "Cancel")?.disabled, true);
      assert.equal(findButtonMeta(busyExportTpl, "✕")?.disabled, true);
      exportDeferred.resolve(undefined);
      exportDeferred = null;
      await currentPromise;
      assert.equal(app.state.modal, null);
      assert.equal(
        exportCalls.at(-1)?.url,
        "/emulator/v1/export?database=projects%2Fa%2Fdatabases%2Fd1%2Fdocuments"
      );
      assert.equal(createdAnchors.at(-1)?.download, "a-d1.fsemu");
      assert.equal(createdAnchors.at(-1)?.clicked, true);
      assert.equal(appendedAnchors.length, 1);
      assert.equal(removedAnchors.length, 1);
      assert.equal(
        revokedObjectUrls.length,
        0,
        "object URL revocation must be deferred"
      );
      assert.ok(
        app.state.toasts.some(
          (t) => t.tone === "info" && t.text.includes("a-d1.fsemu")
        )
      );
      assert.ok(
        Array.from(timers.scheduled.values()).some((t) => t.ms >= 60_000)
      );
      await timers.advanceTimers(60_000);
      assert.equal(revokedObjectUrls.length, 1);

      // Export All Databases with document.body absent (Node / detached guard).
      const savedBody = docObj.body;
      docObj.body = null;
      exportMode = "all-ok";
      findClickHandler(app.renderTopbar(), "Export")?.();
      await findClickHandler(app.renderModal(), "All Databases")?.();
      docObj.body = savedBody;
      assert.equal(exportCalls.at(-1)?.url, "/emulator/v1/export");
      assert.equal(createdAnchors.at(-1)?.download, "all.fsemu");

      // Failed export keeps modal open with inline error.
      exportMode = "fail";
      findClickHandler(app.renderTopbar(), "Export")?.();
      await findClickHandler(app.renderModal(), "All Databases")?.();
      const failedExportModal = currentModal();
      assert.equal(failedExportModal?.kind, "export_snapshot");
      assert.equal(failedExportModal.busy, false);
      assert.ok(failedExportModal.error?.includes("db missing"));
      app.state.modal = null;

      // 4. Import modal: dirty warning, repeated selection reset on failure & success, ssePaused override, auto-select & zero-DB no-op.
      app.state.documentLevels.set(`${dbA}/c/d`, {
        docPath: `${dbA}/c/d`,
        docId: "d",
        exists: true,
        loading: false,
        subcollections: [],
        editorSession: /** @type {import('../components/editor.js').EditorSession} */ (
          /** @type {unknown} */ ({ isDirty: true })
        ),
      });
      findClickHandler(app.renderTopbar(), "Import")?.();
      const dirtyModalText = flattenText(app.renderModal());
      assert.ok(
        dirtyModalText.includes("unsaved edits"),
        "expected dirty-editor warning in import modal"
      );
      assert.equal(
        findButtonMeta(app.renderModal(), "Import Snapshot")?.disabled,
        true
      );

      const mockFile = new Blob([new Uint8Array([70, 83])], {
        type: "application/octet-stream",
      });
      const mockInput = {
        value: "C:\\fakepath\\checkpoint.fsemu",
        files: [mockFile],
      };
      const onFileChange = findFileChangeHandler(app.renderModal());
      assert.ok(onFileChange, "expected @change handler on file input");
      onFileChange({ target: mockInput });
      assert.equal(
        app.state.modal?.kind === "import_snapshot" && app.state.modal.file,
        mockFile
      );
      assert.equal(
        findButtonMeta(app.renderModal(), "Import Snapshot")?.disabled,
        false
      );

      // First upload fails -> busy disables buttons while in flight, then input.value and m.file reset so same file can be selected again.
      let importAttempt = 0;
      /** @type {ReturnType<typeof deferred> | null} */
      let importDeferred = null;
      let currentMetaEdition = "STANDARD";
      let currentCols = ["before"];
      let currentDbList = [dbA];

      harness.fetchImpl = async (url, init) => {
        if (url === "/emulator/v1/import") {
          if (importDeferred) {
            await importDeferred.promise;
          }
          importAttempt++;
          assert.equal(
            /** @type {Record<string, string> | undefined} */ (init?.headers)?.[
              "Content-Type"
            ],
            "application/octet-stream"
          );
          assert.equal(init?.body, mockFile);
          if (importAttempt === 1) {
            return jsonResponse(
              {
                error: {
                  code: 400,
                  status: "INVALID_ARGUMENT",
                  message: "bad checksum",
                },
              },
              400
            );
          }
          currentMetaEdition = "ENTERPRISE";
          currentCols = ["after"];
          return jsonResponse({
            databases: [
              {
                name: dbA,
                projectId: "a",
                databaseId: "d1",
                databaseEdition: "ENTERPRISE",
                concurrencyMode: "OPTIMISTIC",
                documentCount: 3,
                replaced: true,
              },
            ],
            totalDatabases: 1,
            totalDocuments: 3,
          });
        }
        if (url === "/emulator/v1") return jsonResponse(currentDbList);
        if (url === "/v1/projects/a/databases/d1") {
          return jsonResponse({
            name: "projects/a/databases/d1",
            uid: "ua",
            type: "FIRESTORE_NATIVE",
            databaseEdition: currentMetaEdition,
            concurrencyMode:
              currentMetaEdition === "ENTERPRISE"
                ? "OPTIMISTIC"
                : "PESSIMISTIC",
          });
        }
        if (url === `/v1/${dbA}:listCollectionIds`) {
          return jsonResponse({ collectionIds: currentCols });
        }
        throw new Error(`unexpected url: ${url}`);
      };

      importDeferred = deferred();
      const importBtn = findClickHandler(app.renderModal(), "Import Snapshot");
      assert.ok(importBtn);
      const failImportPromise = importBtn();
      assert.equal(app.state.modal?.busy, true);
      const busyImportTpl = app.renderModal();
      assert.equal(
        findButtonMeta(busyImportTpl, "Importing...")?.disabled,
        true
      );
      assert.equal(findButtonMeta(busyImportTpl, "Cancel")?.disabled, true);
      assert.equal(findButtonMeta(busyImportTpl, "✕")?.disabled, true);
      importDeferred.resolve(undefined);
      importDeferred = null;
      await failImportPromise;
      assert.equal(app.state.modal?.kind, "import_snapshot");
      assert.equal(app.state.modal.busy, false);
      assert.equal(app.state.modal.file, null);
      assert.equal(
        mockInput.value,
        "",
        "failed import must clear file input .value"
      );
      assert.ok(app.state.modal.error?.includes("bad checksum"));

      // Re-selecting the same file works cleanly.
      mockInput.value = "C:\\fakepath\\checkpoint.fsemu";
      findFileChangeHandler(app.renderModal())?.({ target: mockInput });
      assert.equal(app.state.modal.file, mockFile);
      assert.equal(app.state.modal.error, null);

      // Enable ssePaused: incoming SSE batch is ignored, while UI import STILL refreshes!
      app.state.ssePaused = true;
      app.state.rootCollections = ["before"];
      await app.handleSseBatch([{ type: "database_replaced", database: dbA }]);
      assert.deepEqual(
        app.state.rootCollections,
        ["before"],
        "background SSE must stay paused"
      );

      await findClickHandler(app.renderModal(), "Import Snapshot")?.();
      assert.equal(app.state.modal, null);
      assert.equal(
        mockInput.value,
        "",
        "successful import must clear file input .value"
      );
      assert.equal(currentActiveDbMeta()?.databaseEdition, "ENTERPRISE");
      assert.deepEqual(
        app.state.rootCollections,
        ["after"],
        "UI import must refresh even when ssePaused"
      );
      assert.ok(
        app.state.toasts.some(
          (t) =>
            t.tone === "success" && t.text.includes("1 database, 3 documents")
        )
      );

      // 5. Live UI import (ssePaused === false) auto-selects first sorted DB when no DB was active.
      app.state.ssePaused = false;
      app.state.activeProjectId = null;
      app.state.activeDatabaseId = null;
      app.state.activeDbMeta = null;
      app.state.databases = [];
      app.state.documentLevels.clear();
      currentDbList = [dbZ, dbA];

      harness.fetchImpl = async (url) => {
        if (url === "/emulator/v1/import") {
          return jsonResponse({
            databases: [
              {
                name: dbZ,
                projectId: "z",
                databaseId: "d1",
                databaseEdition: "STANDARD",
                concurrencyMode: "PESSIMISTIC",
                documentCount: 1,
                replaced: false,
              },
              {
                name: dbA,
                projectId: "a",
                databaseId: "d1",
                databaseEdition: "ENTERPRISE",
                concurrencyMode: "OPTIMISTIC",
                documentCount: 2,
                replaced: false,
              },
            ],
            totalDatabases: 2,
            totalDocuments: 3,
          });
        }
        if (url === "/emulator/v1") return jsonResponse([dbZ, dbA]);
        if (url === "/v1/projects/a/databases/d1") {
          return jsonResponse({
            name: "projects/a/databases/d1",
            uid: "ua",
            type: "FIRESTORE_NATIVE",
            databaseEdition: "ENTERPRISE",
            concurrencyMode: "OPTIMISTIC",
          });
        }
        if (url === `/v1/${dbA}:listCollectionIds`) {
          return jsonResponse({ collectionIds: ["first_col"] });
        }
        throw new Error(`unexpected url in auto-select: ${url}`);
      };

      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "f", files: [mockFile] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();
      assert.equal(app.state.activeProjectId, "a");
      assert.equal(app.state.activeDatabaseId, "d1");
      assert.deepEqual(app.state.rootCollections, ["first_col"]);

      // 6. Zero-database import is a complete no-op.
      let zeroDbFetchCount = 0;
      app.state.activeProjectId = null;
      app.state.activeDatabaseId = null;
      app.state.databases = [];
      harness.fetchImpl = async (url) => {
        zeroDbFetchCount++;
        assert.equal(url, "/emulator/v1/import");
        return jsonResponse({
          databases: [],
          totalDatabases: 0,
          totalDocuments: 0,
        });
      };
      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "z", files: [mockFile] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();
      assert.equal(zeroDbFetchCount, 1);
      assert.equal(app.state.activeProjectId, null);
    } finally {
      timers.restore();
    }
  });

  test("regression_sse_database_replaced_nav_epoch_change_still_prunes_and_renders", async () => {
    const activeRoot = "projects/p/databases/(default)/documents";
    const otherRoot = "projects/p/databases/other/documents";
    /** @type {ReturnType<typeof deferred> | null} */
    let colsDeferred = null;
    /** @type {ReturnType<typeof deferred> | null} */
    let colReloadDeferred = null;
    const rootCols = ["users"];
    /** @type {string[]} */
    const fetchedUrls = [];

    harness.fetchImpl = async (url) => {
      fetchedUrls.push(url);
      if (url === "/emulator/v1") return jsonResponse([activeRoot, otherRoot]);
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
      if (url === `/v1/${activeRoot}:listCollectionIds`) {
        if (colsDeferred) return colsDeferred.promise;
        return jsonResponse({ collectionIds: rootCols });
      }
      if (url === `/v1/${otherRoot}:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["other_col"] });
      }
      if (url.startsWith(`/v1/${activeRoot}/users?`)) {
        if (colReloadDeferred) return colReloadDeferred.promise;
        return jsonResponse({
          documents: [
            {
              name: `${activeRoot}/users/alice`,
              createTime: "2026-01-01T00:00:00Z",
              updateTime: "2026-01-01T00:00:00Z",
            },
          ],
        });
      }
      if (url === `/v1/${activeRoot}/users/alice`) {
        return jsonResponse({
          name: `${activeRoot}/users/alice`,
          fields: { name: { stringValue: "Alice" } },
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      if (url === `/v1/${activeRoot}/users/alice:listCollectionIds`) {
        return jsonResponse({ collectionIds: [] });
      }
      throw new Error(`unexpected url: ${url}`);
    };

    await app.selectDatabase("p", "(default)", ["users", "alice"], true);

    let trackedRootCols = app.state.rootCollections;
    let rootColsRenderReads = 0;
    Object.defineProperty(app.state, "rootCollections", {
      configurable: true,
      enumerable: true,
      get() {
        rootColsRenderReads++;
        return trackedRootCols;
      },
      set(v) {
        trackedRootCols = v;
      },
    });

    try {
      // Seed extra stale & dirty caches before replacement.
      app.state.collectionColumns.set(`${activeRoot}/stale_col`, {
        collectionPath: `${activeRoot}/stale_col`,
        collectionId: "stale_col",
        parentPath: activeRoot,
        documents: [],
        nextPageToken: "",
        filterText: "",
        loading: false,
      });
      app.state.documentLevels.set(`${activeRoot}/stale_col/s1`, {
        docPath: `${activeRoot}/stale_col/s1`,
        docId: "s1",
        exists: true,
        loading: false,
        subcollections: [],
        editorSession: null,
      });
      app.state.documentLevels.set(`${activeRoot}/drafts/dirty1`, {
        docPath: `${activeRoot}/drafts/dirty1`,
        docId: "dirty1",
        exists: true,
        loading: false,
        subcollections: [],
        editorSession: /** @type {import('../components/editor.js').EditorSession} */ (
          /** @type {unknown} */ ({ isDirty: true })
        ),
      });

      // 1. navEpoch changes to root mid-listCollectionIds during database_replaced:
      //    stops hydrating old ["users", "alice"], skips truncation, STILL prunes caches and calls renderApp().
      colsDeferred = deferred();
      fetchedUrls.length = 0;
      const sseBatch1 = app.handleSseBatch([
        { type: "database_replaced", database: activeRoot },
      ]);
      while (!fetchedUrls.includes(`/v1/${activeRoot}:listCollectionIds`)) {
        await nextTick();
      }
      app.state.segments = [];
      stateMod.bumpNavEpoch();
      rootColsRenderReads = 0;
      fetchedUrls.length = 0;

      colsDeferred.resolve(jsonResponse({ collectionIds: ["new_col"] }));
      colsDeferred = null;
      await sseBatch1;

      assert.deepEqual(app.state.rootCollections, ["new_col"]);
      assert.deepEqual(app.state.segments, []);
      assert.equal(
        fetchedUrls.some((u) => u.includes("/users")),
        false,
        "must not hydrate stale segments after navEpoch change"
      );
      assert.equal(
        app.state.collectionColumns.size,
        0,
        "invisible collectionColumns must still be pruned"
      );
      assert.equal(
        app.state.documentLevels.has(`${activeRoot}/users/alice`),
        false,
        "clean non-visible documentLevels must still be pruned"
      );
      assert.equal(
        app.state.documentLevels.has(`${activeRoot}/stale_col/s1`),
        false,
        "stale non-visible documentLevels must still be pruned"
      );
      assert.equal(
        app.state.documentLevels.has(`${activeRoot}/drafts/dirty1`),
        true,
        "dirty documentLevels must still be preserved"
      );
      assert.ok(
        rootColsRenderReads >= 1,
        "renderApp() must still run when navEpoch changes mid-listCollectionIds"
      );

      // 2. navEpoch changes mid-segmentsSnapshot hydration loop (during loadCollectionColumn):
      //    stops hydrating doc level, skips truncation, STILL prunes caches and calls renderApp().
      app.state.segments = ["users", "alice"];
      app.state.collectionColumns.set(`${activeRoot}/stale_col_2`, {
        collectionPath: `${activeRoot}/stale_col_2`,
        collectionId: "stale_col_2",
        parentPath: activeRoot,
        documents: [],
        nextPageToken: "",
        filterText: "",
        loading: false,
      });
      colReloadDeferred = deferred();
      fetchedUrls.length = 0;
      const sseBatch2 = app.handleSseBatch([
        { type: "database_replaced", database: activeRoot },
      ]);
      while (!fetchedUrls.some((u) => u.startsWith(`/v1/${activeRoot}/users?`))) {
        await nextTick();
      }
      app.state.segments = [];
      stateMod.bumpNavEpoch();
      rootColsRenderReads = 0;
      fetchedUrls.length = 0;

      colReloadDeferred.resolve(jsonResponse({ documents: [] }));
      colReloadDeferred = null;
      await sseBatch2;

      assert.equal(
        fetchedUrls.some((u) => u.includes("/users/alice")),
        false,
        "must stop hydrating remaining segments after mid-loop navEpoch change"
      );
      assert.equal(app.state.collectionColumns.size, 0);
      assert.ok(
        rootColsRenderReads >= 1,
        "renderApp() must run after mid-loop navEpoch break"
      );

      // 3. dbEpoch changes mid-listCollectionIds must still abort silently without overwriting new DB or calling renderApp().
      colsDeferred = deferred();
      fetchedUrls.length = 0;
      const sseBatch3 = app.handleSseBatch([
        { type: "database_replaced", database: activeRoot },
      ]);
      while (!fetchedUrls.includes(`/v1/${activeRoot}:listCollectionIds`)) {
        await nextTick();
      }
      await app.selectDatabase("p", "other", [], true);
      rootColsRenderReads = 0;
      colsDeferred.resolve(jsonResponse({ collectionIds: ["stale_p_col"] }));
      colsDeferred = null;
      await sseBatch3;

      assert.equal(
        rootColsRenderReads,
        0,
        "stale dbEpoch must return silently without renderApp()"
      );
      assert.deepEqual(app.state.rootCollections, ["other_col"]);
    } finally {
      Object.defineProperty(app.state, "rootCollections", {
        value: trackedRootCols,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  });

  test("regression_contention_cleared_or_refreshed_on_database_replaced", async () => {
    const intervals = installFakeIntervals();
    const db1 = "projects/p/databases/db1/documents";
    const db2 = "projects/p/databases/db2/documents";
    let db1Edition = "STANDARD";
    /** @type {Array<Record<string, unknown>>} */
    let db1Waiters = [];
    let contentionFetches = 0;

    let trackedMeta = app.state.activeDbMeta;
    let metaReads = 0;

    try {
      harness.fetchImpl = async (url, init) => {
        if (url === "/emulator/v1") return jsonResponse([db1, db2]);
        if (url === "/v1/projects/p/databases/db1") {
          return jsonResponse({
            name: "projects/p/databases/db1",
            uid: "u1",
            type: "FIRESTORE_NATIVE",
            databaseEdition: db1Edition,
            concurrencyMode:
              db1Edition === "ENTERPRISE" ? "OPTIMISTIC" : "PESSIMISTIC",
          });
        }
        if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {
          return jsonResponse({ collectionIds: [] });
        }
        if (url.startsWith("/emulator/v1/contention")) {
          contentionFetches++;
          return jsonResponse({
            databases: [
              {
                database: db1,
                summary: {
                  woundsTotal: 2,
                  timeoutsTotal: 0,
                  upgradeAbortsTotal: 0,
                  writeBatchRetriesTotal: 0,
                },
                contendedDocuments:
                  db1Waiters.length > 0
                    ? [
                        {
                          document: `${db1}/c/d1`,
                          writer: null,
                          readers: [],
                          readerCount: 1,
                          waiters: db1Waiters,
                          waiterCount: db1Waiters.length,
                        },
                      ]
                    : [],
                totalContendedDocuments: db1Waiters.length > 0 ? 1 : 0,
                truncated: false,
                recentEvents: [],
              },
            ],
          });
        }
        if (url === "/emulator/v1/import" && init?.method === "POST") {
          const bodyObj = /** @type {{ _db2Only?: boolean } | null} */ (
            /** @type {unknown} */ (init.body ?? null)
          );
          const isDb2Only = Boolean(bodyObj?._db2Only);
          return jsonResponse({
            databases: [
              {
                name: isDb2Only ? db2 : db1,
                projectId: "p",
                databaseId: isDb2Only ? "db2" : "db1",
                databaseEdition: "STANDARD",
                concurrencyMode: "PESSIMISTIC",
                documentCount: 1,
                replaced: true,
              },
            ],
            totalDatabases: 1,
            totalDocuments: 1,
            importId: isDb2Only ? 2 : 1,
          });
        }
        throw new Error(`unexpected url: ${init?.method ?? "GET"} ${url}`);
      };

      await app.init();
      assert.equal(app.state.activeDatabaseId, "db1");

      // 1. Explorer view: cached contention snapshot is cleared when affected, preserved when unrelated.
      app.state.view = "explorer";
      app.state.contention.scope = "active";
      app.state.contention.snapshot = { databases: [] };
      app.state.contention.lastFetchedAtMs = 1_700_000_000_000;
      app.state.contention.lastFetchedIso = "2026-01-01T00:00:00.000Z";
      app.state.contention.error = "stale error";

      await app.handleSseBatch([
        { type: "database_replaced", database: db2, importId: 10 },
      ]);
      assert.ok(
        app.state.contention.snapshot !== null,
        "unrelated replacement under scope=active must keep cached contention"
      );

      app.state.contention.scope = "all";
      metaReads = 0;
      trackedMeta = app.state.activeDbMeta;
      Object.defineProperty(app.state, "activeDbMeta", {
        configurable: true,
        enumerable: true,
        get() {
          metaReads++;
          return trackedMeta;
        },
        set(v) {
          trackedMeta = v;
        },
      });

      await app.handleSseBatch([
        { type: "database_replaced", database: db2, importId: 11 },
      ]);
      assert.equal(
        app.state.contention.snapshot,
        null,
        "non-active replacement under scope=all must clear cached contention"
      );
      assert.equal(app.state.contention.lastFetchedAtMs, null);
      assert.equal(app.state.contention.lastFetchedIso, null);
      assert.equal(app.state.contention.error, null);
      assert.ok(
        metaReads >= 1,
        "renderApp() must run after non-active replacement clears contention"
      );

      // Replacement of active DB under scope=active in Explorer view clears cached contention.
      app.state.contention.scope = "active";
      app.state.contention.snapshot = { databases: [] };
      app.state.contention.lastFetchedAtMs = 1_700_000_000_000;
      app.state.contention.lastFetchedIso = "2026-01-01T00:00:00.000Z";
      await app.handleSseBatch([
        { type: "database_replaced", database: db1, importId: 12 },
      ]);
      assert.equal(
        app.state.contention.snapshot,
        null,
        "active DB replacement in Explorer must clear cached contention"
      );
      assert.equal(app.state.contention.lastFetchedAtMs, null);
      assert.equal(app.state.contention.lastFetchedIso, null);

      // database_config_changed alone updates activeDbMeta and calls renderApp() without entering collection hydration.
      db1Edition = "ENTERPRISE";
      metaReads = 0;
      await app.handleSseBatch([
        { type: "database_config_changed", database: db1 },
      ]);
      assert.equal(app.state.activeDbMeta?.databaseEdition, "ENTERPRISE");
      assert.ok(metaReads >= 1, "database_config_changed must call renderApp()");

      // 2. Contention view with ssePaused=true: UI import replacing active DB refreshes contention & stops dead waiter ticker.
      db1Waiters = [
        {
          sessionId: 1,
          txnId: "1:1",
          kind: "PESSIMISTIC_TXN",
          seq: 1,
          mode: "WRITE",
          waitedMs: 500,
        },
      ];
      await app.switchView("contention");
      assert.equal(intervals.activeIntervals.size, 1, "expected active waiter ticker");

      await app.toggleSsePaused();
      assert.equal(app.state.ssePaused, true);
      assert.equal(
        intervals.activeIntervals.size,
        1,
        "waiter ticker remains active until snapshot updates"
      );

      const fetchesBeforeActiveImport = contentionFetches;
      db1Waiters = [];
      const mockFile1 = new Blob([new Uint8Array([1])], {
        type: "application/octet-stream",
      });
      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "db1.fsemu", files: [mockFile1] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();

      assert.equal(
        contentionFetches,
        fetchesBeforeActiveImport + 1,
        "UI import replacing active DB while ssePaused must refresh contention"
      );
      assert.equal(
        intervals.activeIntervals.size,
        0,
        "post-import contention refresh must stop dead waiter ticker"
      );

      // 3. Contention view with scope=all & ssePaused=true: UI import replacing non-active db2 also refreshes & stops ticker.
      db1Waiters = [
        {
          sessionId: 2,
          txnId: "2:1",
          kind: "PESSIMISTIC_TXN",
          seq: 2,
          mode: "WRITE",
          waitedMs: 300,
        },
      ];
      await app.refreshContention();
      assert.equal(intervals.activeIntervals.size, 1);

      const allScopeBtn = findClickHandler(
        app.renderWorkspace(),
        "All Databases"
      );
      assert.ok(allScopeBtn);
      await allScopeBtn();
      assert.equal(app.state.contention.scope, "all");
      assert.equal(intervals.activeIntervals.size, 1);

      const fetchesBeforeOtherImport = contentionFetches;
      db1Waiters = [];
      const mockFile2 = Object.assign(
        new Blob([new Uint8Array([2])], { type: "application/octet-stream" }),
        { _db2Only: true }
      );
      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "db2.fsemu", files: [mockFile2] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();

      assert.equal(
        contentionFetches,
        fetchesBeforeOtherImport + 1,
        "UI import replacing non-active DB under scope=all while ssePaused must refresh contention"
      );
      assert.equal(
        intervals.activeIntervals.size,
        0,
        "scope=all post-import refresh must stop dead waiter ticker"
      );
    } finally {
      Object.defineProperty(app.state, "activeDbMeta", {
        value: trackedMeta,
        writable: true,
        enumerable: true,
        configurable: true,
      });
      intervals.restore();
    }
  });

  test("regression_import_id_correlation_dedup_and_bounds", async () => {
    const dbA = "projects/p/databases/dbA/documents";
    const dbB = "projects/p/databases/dbB/documents";
    /** @type {(() => unknown) | null} */
    let nextSummaryFn = null;
    /** @type {string[]} */
    const fetchedUrls = [];

    harness.fetchImpl = async (url, init) => {
      fetchedUrls.push(`${init?.method ?? "GET"} ${url}`);
      if (url === "/emulator/v1/import" && init?.method === "POST") {
        assert.ok(nextSummaryFn);
        return jsonResponse(nextSummaryFn());
      }
      if (url === "/emulator/v1") return jsonResponse([dbA, dbB]);
      if (url === "/v1/projects/p/databases/dbA") {
        return jsonResponse({
          name: "projects/p/databases/dbA",
          uid: "ua",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === "/v1/projects/p/databases/dbA/documents:listCollectionIds") {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url.startsWith(`/v1/${dbA}/users?`)) {
        return jsonResponse({ documents: [] });
      }
      throw new Error(`unexpected url: ${url}`);
    };

    await app.init();
    assert.equal(app.state.activeDatabaseId, "dbA");

    const mockFile = new Blob([new Uint8Array([1, 2])], {
      type: "application/octet-stream",
    });
    /** @param {unknown} summary */
    async function runUiImport(summary) {
      nextSummaryFn = () => summary;
      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "snap.fsemu", files: [mockFile] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();
    }

    // 1. Live UI import with importId=42 deduplicates subsequent SSE database_replaced & database_created.
    clearList(fetchedUrls);
    await runUiImport({
      databases: [
        {
          name: dbA,
          projectId: "p",
          databaseId: "dbA",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 1,
          replaced: true,
        },
        {
          name: dbB,
          projectId: "p",
          databaseId: "dbB",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 0,
          replaced: false,
        },
      ],
      totalDatabases: 2,
      totalDocuments: 1,
      importId: 42,
    });
    assert.ok(fetchedUrls.includes(`POST /v1/${dbA}:listCollectionIds`));

    // User runs/sets queryState after local import completes; delayed SSE with same importId=42 must not wipe it or refetch.
    app.state.queryState = createQueryRunnerState("orders");
    app.state.queryState.results = [{ name: `${dbA}/orders/o1`, fields: {} }];
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 42 },
      { type: "database_created", database: dbB, importId: 42 },
    ]);
    assert.deepEqual(
      fetchedUrls,
      [],
      "same importId SSE must cause zero duplicate fetches"
    );
    assert.equal(
      app.state.queryState.collectionId,
      "orders",
      "queryState must survive deduplicated SSE"
    );
    assert.equal(app.state.queryState.results?.length, 1);

    // 2. External import with new importId=43 still reloads active DB and resets queryState.
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 43 },
    ]);
    assert.ok(hasUrl(fetchedUrls, `POST /v1/${dbA}:listCollectionIds`));
    assert.equal(app.state.queryState.collectionId, "");
    assert.equal(app.state.queryState.results, null);

    // 3. SSE arriving BEFORE HTTP response deduplicates local onImportSuccess callback.
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 50 },
      { type: "database_created", database: dbB, importId: 50 },
    ]);
    assert.ok(hasUrl(fetchedUrls, `POST /v1/${dbA}:listCollectionIds`));

    app.state.queryState = createQueryRunnerState("kept_after_sse");
    clearList(fetchedUrls);
    await runUiImport({
      databases: [
        {
          name: dbA,
          projectId: "p",
          databaseId: "dbA",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 1,
          replaced: true,
        },
        {
          name: dbB,
          projectId: "p",
          databaseId: "dbB",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 0,
          replaced: false,
        },
      ],
      totalDatabases: 2,
      totalDocuments: 1,
      importId: 50,
    });
    assert.deepEqual(
      fetchedUrls,
      ["POST /emulator/v1/import"],
      "onImportSuccess must skip all entries already applied by SSE"
    );
    assert.equal(app.state.queryState.collectionId, "kept_after_sse");

    // 4. Unified appliedImports FIFO Map: cap = 64 imports for both local and SSE,
    //    per-DB tracking inside each importId Set prevents >256-DB eviction, and init() clears it.
    for (let id = 100; id < 165; id++) {
      await runUiImport({
        databases: [
          {
            name: dbB,
            projectId: "p",
            databaseId: "dbB",
            databaseEdition: "STANDARD",
            concurrencyMode: "PESSIMISTIC",
            documentCount: 0,
            replaced: true,
          },
        ],
        totalDatabases: 1,
        totalDocuments: 0,
        importId: id,
      });
    }
    // id=100 was evicted (65th entry pushed out 100), while id=164 remains retained.
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 164 },
    ]);
    assert.deepEqual(
      fetchedUrls,
      [],
      "most recent 64 local importIds must remain deduplicated"
    );
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 100 },
    ]);
    assert.ok(
      hasUrl(fetchedUrls, "GET /emulator/v1"),
      "oldest local importId=100 must be evicted at cap 64"
    );

    for (let id = 200; id < 265; id++) {
      await app.handleSseBatch([
        { type: "database_created", database: dbB, importId: id },
      ]);
    }
    // id=200 was evicted (65th entry pushed out 200), while id=264 remains retained.
    clearList(fetchedUrls);
    await runUiImport({
      databases: [
        {
          name: dbB,
          projectId: "p",
          databaseId: "dbB",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 0,
          replaced: false,
        },
      ],
      totalDatabases: 1,
      totalDocuments: 0,
      importId: 264,
    });
    assert.deepEqual(
      fetchedUrls,
      ["POST /emulator/v1/import"],
      "most recent 64 SSE importIds must remain deduplicated"
    );

    clearList(fetchedUrls);
    await runUiImport({
      databases: [
        {
          name: dbB,
          projectId: "p",
          databaseId: "dbB",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 0,
          replaced: false,
        },
      ],
      totalDatabases: 1,
      totalDocuments: 0,
      importId: 200,
    });
    assert.ok(
      hasUrl(fetchedUrls, "GET /emulator/v1"),
      "oldest SSE importId=200 must be evicted at cap 64"
    );

    // Single >256-DB import: SSE arrives before HTTP 200 for all 300 DBs (d000=dbA..d299).
    // Earlier DBs (including d000=dbA) must NEVER be evicted from the importId's Set!
    const bulkDbs = Array.from({ length: 300 }, (_, idx) => {
      const id = idx === 0 ? "dbA" : `bulk_${String(idx).padStart(3, "0")}`;
      return {
        name: `projects/p/databases/${id}/documents`,
        projectId: "p",
        databaseId: id,
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
        documentCount: 0,
        replaced: idx === 0,
      };
    });
    await app.handleSseBatch(
      bulkDbs.map((d) => ({
        type: d.replaced ? "database_replaced" : "database_created",
        database: d.name,
        importId: 500,
      }))
    );
    clearList(fetchedUrls);
    await runUiImport({
      databases: bulkDbs,
      totalDatabases: bulkDbs.length,
      totalDocuments: 0,
      importId: 500,
    });
    assert.deepEqual(
      fetchedUrls,
      ["POST /emulator/v1/import"],
      ">256-DB single import must retain every DB in its importId Set without mid-import eviction"
    );

    // Partial SSE before HTTP 200 on >256-DB import only applies the remaining unseen DB.
    await app.handleSseBatch(
      bulkDbs.slice(0, 299).map((d) => ({
        type: d.replaced ? "database_replaced" : "database_created",
        database: d.name,
        importId: 501,
      }))
    );
    const partialBulkSummary = [
      ...bulkDbs.slice(0, 299),
      {
        name: dbB,
        projectId: "p",
        databaseId: "dbB",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
        documentCount: 0,
        replaced: false,
      },
    ];
    clearList(fetchedUrls);
    await runUiImport({
      databases: partialBulkSummary,
      totalDatabases: partialBulkSummary.length,
      totalDocuments: 0,
      importId: 501,
    });
    assert.deepEqual(
      fetchedUrls,
      ["POST /emulator/v1/import", "GET /emulator/v1"],
      "partial >256-DB SSE-before-HTTP must only apply the unseen newly-created dbB"
    );

    // init() clears appliedImports so importId=164 and importId=500 are no longer suppressed.
    await app.init();
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 164 },
    ]);
    assert.ok(
      hasUrl(fetchedUrls, "GET /emulator/v1"),
      "init() must clear appliedImports"
    );
    clearList(fetchedUrls);
    await app.handleSseBatch([
      { type: "database_replaced", database: dbA, importId: 500 },
    ]);
    assert.ok(
      hasUrl(fetchedUrls, "GET /emulator/v1"),
      "init() must clear bulk SSE importId"
    );
  });

  test("regression_export_revoke_object_url_finally_and_manual_timer_cancel", async () => {
    const timers = installFakeTimers();
    try {
      // 1. Verify clearTimeout cancels manual >=1000ms timers cleanly.
      let cancelledTimerRan = false;
      const syntheticId = setTimeout(() => {
        cancelledTimerRan = true;
      }, 60_000);
      clearTimeout(syntheticId);
      await timers.advanceTimers(120_000);
      assert.equal(
        cancelledTimerRan,
        false,
        "clearTimeout must cancel manual >=1000ms timer"
      );

      // 2. Verify a.click() throwing inside export still removes anchor and schedules >=60_000ms revokeObjectURL in finally.
      const prevCreateElement = /** @type {(tag: string) => unknown} */ (
        docObj.createElement
      );
      docObj.createElement = (/** @type {string} */ tag) => {
        const el = prevCreateElement(tag);
        if (String(tag).toLowerCase() === "a") {
          /** @type {MockAnchorElement} */ (el).click = () => {
            throw new Error("download blocked");
          };
        }
        return el;
      };

      app.state.databases = [
        {
          projectId: "a",
          databaseId: "d1",
          rootPath: "projects/a/databases/d1/documents",
        },
      ];
      app.state.activeProjectId = "a";
      app.state.activeDatabaseId = "d1";

      harness.fetchImpl = async () =>
        blobResponse(new Uint8Array([1, 2, 3]), 200, {
          "Content-Disposition": 'attachment; filename="fail-click.fsemu"',
        });

      findClickHandler(app.renderTopbar(), "Export")?.();
      assert.equal(app.state.modal?.kind, "export_snapshot");
      await findClickHandler(app.renderModal(), "Current Database")?.();

      assert.equal(
        removedAnchors.length,
        1,
        "anchor must be removed in finally even when click() throws"
      );
      assert.equal(createdObjectUrls.length, 1);
      assert.equal(
        revokedObjectUrls.length,
        0,
        "revokeObjectURL must be deferred by >= 60_000ms"
      );
      const revokeTimer = Array.from(timers.scheduled.values()).find(
        (t) => t.ms >= 60_000
      );
      assert.ok(
        revokeTimer,
        "expected >= 60_000ms revokeObjectURL timer in finally"
      );
      await timers.advanceTimers(60_000);
      assert.deepEqual(revokedObjectUrls, [createdObjectUrls[0].url]);

      assert.equal(app.state.modal?.kind, "export_snapshot");
      assert.equal(app.state.modal.busy, false);
      assert.equal(app.state.modal.busyScope, null);
      assert.ok(app.state.modal.error?.includes("download blocked"));
    } finally {
      timers.restore();
    }
  });

  test("regression_export_modal_busy_scope_per_button", async () => {
    app.state.databases = [
      {
        projectId: "a",
        databaseId: "d1",
        rootPath: "projects/a/databases/d1/documents",
      },
    ];
    app.state.activeProjectId = "a";
    app.state.activeDatabaseId = "d1";

    /** @type {ReturnType<typeof deferred> | null} */
    let exportDeferred = null;
    let shouldFail = false;
    harness.fetchImpl = async () => {
      if (exportDeferred) {
        await exportDeferred.promise;
      }
      if (shouldFail) {
        return jsonResponse(
          { error: { code: 500, status: "INTERNAL", message: "export failed" } },
          500
        );
      }
      return blobResponse(new Uint8Array([1, 2, 3]), 200, {
        "Content-Disposition": 'attachment; filename="snap.fsemu"',
      });
    };

    findClickHandler(app.renderTopbar(), "Export")?.();
    assert.equal(app.state.modal?.kind, "export_snapshot");
    assert.equal(app.state.modal.busyScope, null);

    // 1. Clicking "All Databases" shows "Exporting..." ONLY on All Databases, keeping "Current Database" label intact.
    exportDeferred = deferred();
    const allBtn = findClickHandler(app.renderModal(), "All Databases");
    assert.ok(allBtn);
    const allPromise = allBtn();
    assert.equal(app.state.modal.busy, true);
    assert.equal(app.state.modal.busyScope, "all");

    const allBusyTpl = app.renderModal();
    assert.equal(findButtonMeta(allBusyTpl, "Exporting...")?.disabled, true);
    assert.equal(findButtonMeta(allBusyTpl, "All Databases"), null);
    assert.equal(
      findButtonMeta(allBusyTpl, "Current Database")?.disabled,
      true,
      "Current Database must retain its label while disabled during All Databases export"
    );

    // Error resets busyScope=null and restores both labels.
    shouldFail = true;
    exportDeferred.resolve(undefined);
    exportDeferred = null;
    await allPromise;
    assert.equal(app.state.modal?.kind, "export_snapshot");
    assert.equal(app.state.modal.busy, false);
    assert.equal(app.state.modal.busyScope, null);
    assert.ok(app.state.modal.error?.includes("export failed"));
    const afterErrorTpl = app.renderModal();
    assert.equal(
      findButtonMeta(afterErrorTpl, "Current Database")?.disabled,
      false
    );
    assert.equal(
      findButtonMeta(afterErrorTpl, "All Databases")?.disabled,
      false
    );
    assert.equal(findButtonMeta(afterErrorTpl, "Exporting..."), null);

    // 2. Clicking "Current Database" shows "Exporting..." ONLY on Current Database, keeping "All Databases" label intact.
    shouldFail = false;
    exportDeferred = deferred();
    const curBtn = findClickHandler(app.renderModal(), "Current Database");
    assert.ok(curBtn);
    const curPromise = curBtn();
    assert.equal(app.state.modal.busy, true);
    assert.equal(app.state.modal.busyScope, "current");

    const curBusyTpl = app.renderModal();
    assert.equal(findButtonMeta(curBusyTpl, "Exporting...")?.disabled, true);
    assert.equal(findButtonMeta(curBusyTpl, "Current Database"), null);
    assert.equal(
      findButtonMeta(curBusyTpl, "All Databases")?.disabled,
      true,
      "All Databases must retain its label while disabled during Current Database export"
    );

    exportDeferred.resolve(undefined);
    exportDeferred = null;
    await curPromise;
    assert.equal(app.state.modal, null);
  });

  test("regression_import_created_db_preserves_contention_and_live_sse_avoids_double_fetch", async () => {
    const db1 = "projects/p/databases/db1/documents";
    const dbNew = "projects/p/databases/dbNew/documents";
    /** @type {string[]} */
    const fetchedUrls = [];
    let contentionFetches = 0;
    /** @type {ReturnType<typeof deferred> | null} */
    let contentionDeferred = null;
    /** @type {(() => unknown) | null} */
    let nextSummaryFn = null;

    harness.fetchImpl = async (url, init) => {
      fetchedUrls.push(`${init?.method ?? "GET"} ${url}`);
      if (url === "/emulator/v1") return jsonResponse([db1]);
      if (url === "/v1/projects/p/databases/db1") {
        return jsonResponse({
          name: "projects/p/databases/db1",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === `/v1/${db1}:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url.startsWith(`/v1/${db1}/users?`)) {
        return jsonResponse({ documents: [] });
      }
      if (url === `/v1/${db1}/users/alice`) {
        return jsonResponse({
          name: `${db1}/users/alice`,
          fields: {},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        });
      }
      if (url === `/v1/${db1}/users/alice:listCollectionIds`) {
        return jsonResponse({ collectionIds: [] });
      }
      if (url.startsWith("/emulator/v1/contention")) {
        contentionFetches++;
        if (contentionDeferred) {
          return contentionDeferred.promise;
        }
        return jsonResponse({
          databases: [
            {
              database: db1,
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
        });
      }
      if (url === "/emulator/v1/import" && init?.method === "POST") {
        assert.ok(nextSummaryFn);
        return jsonResponse(nextSummaryFn());
      }
      throw new Error(`unexpected url: ${init?.method ?? "GET"} ${url}`);
    };

    await app.init();
    await app.switchView("contention");
    const allScopeBtn = findClickHandler(
      app.renderWorkspace(),
      "All Databases"
    );
    assert.ok(allScopeBtn);
    await allScopeBtn();
    assert.equal(app.state.contention.scope, "all");

    const contentionStream = harness.eventSources
      .filter((s) => s.url === "/emulator/v1/contention/events")
      .at(-1);
    assert.ok(contentionStream);
    contentionStream.emit("open");
    assert.equal(app.state.contentionSseStatus, "live");

    // 1. UI import with replaced=false must emit database_created and NOT wipe contention snapshot.
    const mockFile = new Blob([new Uint8Array([1, 2])], {
      type: "application/octet-stream",
    });
    /** @param {unknown} summary */
    async function runUiImport(summary) {
      nextSummaryFn = () => summary;
      findClickHandler(app.renderTopbar(), "Import")?.();
      findFileChangeHandler(app.renderModal())?.({
        target: { value: "snap.fsemu", files: [mockFile] },
      });
      await findClickHandler(app.renderModal(), "Import Snapshot")?.();
    }

    const initialSnapshot = app.state.contention.snapshot;
    const initialFetchedAt = app.state.contention.lastFetchedAtMs;
    assert.ok(initialSnapshot !== null && initialFetchedAt !== null);
    const fetchesBeforeCreate = contentionFetches;

    await runUiImport({
      databases: [
        {
          name: dbNew,
          projectId: "p",
          databaseId: "dbNew",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 2,
          replaced: false,
        },
      ],
      totalDatabases: 1,
      totalDocuments: 2,
      importId: 80,
    });
    assert.equal(
      app.state.contention.snapshot,
      initialSnapshot,
      "d.replaced=false must not wipe contention snapshot"
    );
    assert.equal(app.state.contention.lastFetchedAtMs, initialFetchedAt);
    assert.equal(
      contentionFetches,
      fetchesBeforeCreate,
      "d.replaced=false must not trigger contention fetch"
    );

    // 2. When contentionSseStatus === "live" and SSE is unpaused, database_replaced avoids double-fetch and preserves snapshot.
    const fetchesBeforeLiveReplace = contentionFetches;
    await app.handleSseBatch([
      { type: "database_replaced", database: db1, importId: 81 },
    ]);
    assert.equal(
      contentionFetches,
      fetchesBeforeLiveReplace,
      "live contention SSE must prevent duplicate refreshContention() in handleSseBatch"
    );
    assert.ok(
      app.state.contention.snapshot !== null,
      "live contention SSE must not blank snapshot before SSE invalidation"
    );

    await runUiImport({
      databases: [
        {
          name: db1,
          projectId: "p",
          databaseId: "db1",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
          documentCount: 1,
          replaced: true,
        },
      ],
      totalDatabases: 1,
      totalDocuments: 1,
      importId: 82,
    });
    assert.equal(
      contentionFetches,
      fetchesBeforeLiveReplace,
      "live contention SSE must prevent duplicate refreshContention() on UI import too"
    );

    // Contention SSE DatabaseCleared invalidation refreshes contention exactly once.
    const timers = installFakeTimers();
    try {
      contentionStream.emit("contention", {
        data: JSON.stringify({ type: "database_cleared", database: db1 }),
      });
      await timers.advanceTimers(300);
      assert.equal(contentionFetches, fetchesBeforeLiveReplace + 1);
    } finally {
      timers.restore();
    }

    // 3. When contentionSseStatus !== "live", fallback Contention sync runs BEFORE Explorer hydration.
    app.state.segments = ["users", "alice"];
    contentionStream.emit("error");
    assert.equal(app.state.contentionSseStatus, "error");

    contentionDeferred = deferred();
    fetchedUrls.length = 0;
    const batchPromise = app.handleSseBatch([
      { type: "database_replaced", database: db1, importId: 83 },
    ]);
    while (!fetchedUrls.some((u) => u.startsWith("GET /emulator/v1/contention"))) {
      await nextTick();
    }
    assert.equal(
      app.state.contention.snapshot,
      null,
      "fallback contention refresh clears stale snapshot"
    );
    assert.equal(
      fetchedUrls.some(
        (u) => u.includes(":listCollectionIds") || u.includes("/users")
      ),
      false,
      "Explorer hydration must NOT start before fallback Contention fetch"
    );

    contentionDeferred.resolve(jsonResponse({ databases: [] }));
    contentionDeferred = null;
    await batchPromise;

    const contentionIdx = fetchedUrls.findIndex((u) =>
      u.startsWith("GET /emulator/v1/contention")
    );
    const listColsIdx = fetchedUrls.findIndex(
      (u) => u === `POST /v1/${db1}:listCollectionIds`
    );
    const colListIdx = fetchedUrls.findIndex((u) =>
      u.startsWith(`GET /v1/${db1}/users?`)
    );
    const docGetIdx = fetchedUrls.findIndex(
      (u) => u === `GET /v1/${db1}/users/alice`
    );
    assert.ok(contentionIdx >= 0);
    assert.ok(
      listColsIdx > contentionIdx,
      "Contention sync must complete before listCollectionIds"
    );
    assert.ok(colListIdx > listColsIdx);
    assert.ok(docGetIdx > colListIdx);
  });

  test("regression_sse_empty_auto_select_same_batch_recovery_and_list_cols_error", async () => {
    const dbA = "projects/a/databases/d1/documents";
    const dbZ = "projects/z/databases/d1/documents";
    /** @type {string[]} */
    let dbList = [];
    let colsA = ["orders"];
    let failListCols = false;
    /** @type {string[]} */
    const fetchedUrls = [];

    harness.fetchImpl = async (url) => {
      fetchedUrls.push(url);
      if (url === "/emulator/v1") return jsonResponse(dbList);
      if (url === "/v1/projects/a/databases/d1") {
        return jsonResponse({
          name: "projects/a/databases/d1",
          uid: "ua",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "ENTERPRISE",
          concurrencyMode: "OPTIMISTIC",
        });
      }
      if (url === `/v1/${dbA}:listCollectionIds`) {
        if (failListCols) {
          return jsonResponse(
            { error: { code: 500, status: "INTERNAL", message: "boom" } },
            500
          );
        }
        return jsonResponse({ collectionIds: colsA });
      }
      if (url.startsWith(`/v1/${dbA}/`)) {
        if (url.endsWith(":listCollectionIds")) {
          return jsonResponse({ collectionIds: [] });
        }
        if (url.includes("?")) return jsonResponse({ documents: [] });
        return jsonResponse(
          { error: { code: 404, status: "NOT_FOUND", message: "missing" } },
          404
        );
      }
      throw new Error(`unexpected url: ${url}`);
    };

    // 1. Empty emulator auto-selects first sorted DB on SSE [database_created].
    await app.init();
    assert.equal(app.state.activeProjectId, null);
    assert.equal(app.state.activeDatabaseId, null);
    assert.deepEqual(app.state.databases, []);

    dbList = [dbZ, dbA];
    await app.handleSseBatch([
      { type: "database_created", database: dbZ },
      { type: "database_created", database: dbA },
    ]);
    assert.equal(app.state.activeProjectId, "a");
    assert.equal(app.state.activeDatabaseId, "d1");
    assert.equal(app.state.activeDbMeta?.databaseEdition, "ENTERPRISE");
    assert.deepEqual(app.state.rootCollections, ["orders"]);

    // 2. Same-batch [database_cleared, database_created] recovers cleanly onto recreated DB.
    app.state.segments = ["orders", "o1"];
    app.state.toasts = [];
    colsA = ["fresh_col"];
    dbList = [dbA];
    await app.handleSseBatch([
      { type: "database_cleared", database: dbA },
      { type: "database_created", database: dbA },
    ]);
    assert.equal(app.state.activeProjectId, "a");
    assert.equal(app.state.activeDatabaseId, "d1");
    assert.deepEqual(app.state.segments, []);
    assert.deepEqual(app.state.rootCollections, ["fresh_col"]);
    assert.ok(app.state.toasts.some((t) => t.text.includes("was cleared")));

    // 3. listCollectionIds failure skips truncateStaleSegmentsAfterReplace.
    app.state.segments = ["fresh_col", "f1"];
    app.state.rootCollections = ["fresh_col"];
    failListCols = true;
    await app.handleSseBatch([{ type: "database_replaced", database: dbA }]);
    assert.deepEqual(
      app.state.segments,
      ["fresh_col", "f1"],
      "listCollectionIds error must preserve segments instead of truncating"
    );
    assert.deepEqual(app.state.rootCollections, ["fresh_col"]);

    // 4. Successful listCollectionIds where root collection vanished skips segment fetches before truncating.
    failListCols = false;
    colsA = ["other_col"];
    fetchedUrls.length = 0;
    await app.handleSseBatch([{ type: "database_replaced", database: dbA }]);
    assert.deepEqual(app.state.segments, []);
    assert.equal(
      fetchedUrls.some((u) => u.includes("/fresh_col")),
      false,
      "clean vanished root collection must skip segment hydration fetches before truncation"
    );
  });

  test("regression_a11y_snapshot_modals_alert_describedby_autofocus_and_copy", async () => {
    harness.fetchImpl = async (url) => {
      if (url === "/emulator/v1") return jsonResponse([]);
      throw new Error(`unexpected url: ${url}`);
    };

    await app.init();

    // 1. Empty-hero copy and topbar Import/Export tooltips.
    const heroText = flattenText(app.renderWorkspace());
    assert.ok(
      heroText.includes(
        "Connect your test suite to automatically populate databases, create a database right now, or import a saved .fsemu snapshot."
      ),
      "empty hero copy must mention snapshot import"
    );

    app.state.databases = [
      {
        projectId: "a",
        databaseId: "d1",
        rootPath: "projects/a/databases/d1/documents",
      },
    ];
    const topbarMarkup = renderTemplateMarkup(app.renderTopbar());
    assert.ok(topbarMarkup.includes('title="Import a .fsemu snapshot"'));
    assert.ok(
      topbarMarkup.includes('title="Export databases to a .fsemu snapshot"')
    );

    // 2. Modal error banner uses .error-banner.modal-banner and role="alert".
    app.state.modal = {
      kind: "import_snapshot",
      file: null,
      busy: false,
      error: "Failed to parse snapshot",
    };
    const errMarkup = renderTemplateMarkup(app.renderModal());
    assert.ok(
      errMarkup.includes('class="error-banner modal-banner"') &&
        errMarkup.includes('role="alert"'),
      "modal error banner must include modal-banner class and role=alert"
    );

    // 3. Import modal warning IDs, updated copy, and aria-describedby wiring.
    assert.ok(errMarkup.includes('id="import-snapshot-overwrite-warning"'));
    assert.equal(
      errMarkup.includes('id="import-snapshot-dirty-warning"'),
      false
    );
    assert.ok(
      errMarkup.includes(
        "Importing completely replaces (never merges) each database in the snapshot, deleting extra documents and overwriting settings. Databases not in the snapshot remain untouched."
      )
    );
    assert.ok(
      errMarkup.includes(
        'aria-describedby="import-snapshot-overwrite-warning"'
      ),
      "file input must reference overwrite warning when no dirty editors exist"
    );

    app.state.documentLevels.set("projects/a/databases/d1/documents/c/d", {
      docPath: "projects/a/databases/d1/documents/c/d",
      docId: "d",
      exists: true,
      loading: false,
      subcollections: [],
      editorSession: /** @type {import('../components/editor.js').EditorSession} */ (
        /** @type {unknown} */ ({ isDirty: true })
      ),
    });
    const dirtyImportMarkup = renderTemplateMarkup(app.renderModal());
    assert.ok(
      dirtyImportMarkup.includes('id="import-snapshot-overwrite-warning"')
    );
    assert.ok(
      dirtyImportMarkup.includes('id="import-snapshot-dirty-warning"')
    );
    assert.ok(
      dirtyImportMarkup.includes(
        "You have unsaved edits in an open document editor. If this import updates or deletes that document, your local draft is kept with an option to keep, reload, or discard it."
      )
    );
    assert.ok(
      dirtyImportMarkup.includes(
        'aria-describedby="import-snapshot-overwrite-warning import-snapshot-dirty-warning"'
      ),
      "file input must reference both warnings when dirty editors exist"
    );
    app.state.documentLevels.clear();
    app.state.modal = null;

    // 4. Export Snapshot [data-autofocus] attribute & syncModalAccessibility runtime focus.
    app.state.activeProjectId = "a";
    app.state.activeDatabaseId = "d1";
    findClickHandler(app.renderTopbar(), "Export")?.();
    assert.equal(app.state.modal?.kind, "export_snapshot");
    assert.equal(
      /** @type {DomElement | null} */ (
        harness.activeElement
      )?.textContent?.trim(),
      "Current Database",
      "Export modal with active DB must focus Current Database via [data-autofocus]"
    );
    const exportWithActiveMarkup = renderTemplateMarkup(app.renderModal());
    assert.ok(exportWithActiveMarkup.includes("data-autofocus"));

    harness.docKeydownListeners[0]?.({ key: "Escape", preventDefault() {} });
    assert.equal(app.state.modal, null);

    app.state.activeProjectId = null;
    app.state.activeDatabaseId = null;
    findClickHandler(app.renderTopbar(), "Export")?.();
    assert.equal(currentModal()?.kind, "export_snapshot");
    assert.equal(
      /** @type {DomElement | null} */ (
        harness.activeElement
      )?.textContent?.trim(),
      "All Databases",
      "Export modal without active DB must focus All Databases via [data-autofocus]"
    );

    harness.docKeydownListeners[0]?.({ key: "Escape", preventDefault() {} });
    assert.equal(app.state.modal, null);

    // Open/Create Database modal has no [data-autofocus] and continues focusing first enabled input.
    findClickHandler(app.renderTopbar(), "+ Open / Create Database")?.();
    assert.equal(currentModal()?.kind, "open_db");
    assert.equal(
      /** @type {DomElement | null} */ (harness.activeElement)?.getAttribute(
        "id"
      ),
      "modal-open-db-project",
      "input modal without [data-autofocus] must still focus first enabled input"
    );
  });

  test("regression_database_replaced_resets_query_suggestions_and_aborts_active_query", async () => {
    const activeRoot = "projects/p1/databases/(default)/documents";
    /** @type {AbortSignal | null} */
    let capturedSignal;
    /** @type {ReturnType<typeof deferred> | null} */
    let queryDeferred = null;

    harness.fetchImpl = async (url, init) => {
      if (url === "/emulator/v1") return jsonResponse([activeRoot]);
      if (url === "/v1/projects/p1/databases/(default)") {
        return jsonResponse({
          name: "projects/p1/databases/(default)",
          uid: "u1",
          type: "FIRESTORE_NATIVE",
          databaseEdition: "STANDARD",
          concurrencyMode: "PESSIMISTIC",
        });
      }
      if (url === `/v1/${activeRoot}:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["users"] });
      }
      if (url === `/v1/${activeRoot}/users/alice:listCollectionIds`) {
        return jsonResponse({ collectionIds: ["posts"] });
      }
      if (url === `/v1/${activeRoot}:runQuery`) {
        capturedSignal = /** @type {AbortSignal | null} */ (
          init?.signal ?? null
        );
        if (queryDeferred) return queryDeferred.promise;
        return jsonResponse([]);
      }
      throw new Error(`unexpected url: ${url}`);
    };

    await app.selectDatabase("p1", "(default)", [], true);

    await ensureSubcollections(
      app.state.querySuggestions,
      {
        rootPath: activeRoot,
        rootCollections: app.state.rootCollections,
        onRerender: () => {},
      },
      "users/alice"
    );
    assert.equal(app.state.querySuggestions.subcollectionsByParent.size, 1);
    const epochBefore = app.state.querySuggestions.epoch;

    // 1. In-flight runQuery is aborted synchronously when active DB is replaced; resolving later does not populate stale results.
    app.state.queryState = createQueryRunnerState("users");
    queryDeferred = deferred();
    capturedSignal = null;
    const queryPromise1 = app.executeCurrentQuery();
    while (!capturedSignal) {
      await nextTick();
    }
    const signal1 = /** @type {AbortSignal} */ (capturedSignal);
    assert.equal(app.state.queryState.running, true);
    assert.equal(signal1.aborted, false);

    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.equal(signal1.aborted, true, "active query signal must be aborted");
    assert.ok(
      app.state.querySuggestions.epoch > epochBefore,
      "querySuggestions epoch must advance on active database_replaced"
    );
    assert.equal(
      app.state.querySuggestions.subcollectionsByParent.size,
      0,
      "cached subcollection suggestions must be cleared"
    );
    assert.equal(app.state.queryState.collectionId, "");
    assert.equal(app.state.queryState.running, false);

    queryDeferred.resolve(
      jsonResponse([
        {
          document: {
            name: `${activeRoot}/users/stale`,
            fields: {},
            createTime: "2026-01-01T00:00:00Z",
            updateTime: "2026-01-01T00:00:00Z",
          },
        },
      ])
    );
    queryDeferred = null;
    await queryPromise1;
    assert.equal(app.state.queryState.results, null);
    assert.equal(app.state.queryState.error, null);
    assert.equal(app.state.queryState.running, false);

    // 2. Rejecting the aborted query after database_replaced also leaves new queryState.error untouched.
    app.state.queryState = createQueryRunnerState("users");
    queryDeferred = deferred();
    capturedSignal = null;
    const queryPromise2 = app.executeCurrentQuery();
    while (!capturedSignal) {
      await nextTick();
    }
    const signal2 = /** @type {AbortSignal} */ (capturedSignal);
    await app.handleSseBatch([
      { type: "database_replaced", database: activeRoot },
    ]);
    assert.equal(signal2.aborted, true);

    queryDeferred.reject(new Error("aborted request failed"));
    queryDeferred = null;
    await queryPromise2;
    assert.equal(app.state.queryState.results, null);
    assert.equal(app.state.queryState.error, null);
    assert.equal(app.state.queryState.running, false);
  });
});
