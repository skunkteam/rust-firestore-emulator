#![allow(unused_crate_dependencies)]

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
};

fn run_node_ui_test(script: &str) {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut child = Command::new("node")
        .arg("--input-type=module")
        .arg("-")
        .current_dir(&repo_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn node for UI regression test");

    child
        .stdin
        .as_mut()
        .expect("stdin must be piped")
        .write_all(script.as_bytes())
        .expect("failed to write JS test script to node stdin");

    let output = child
        .wait_with_output()
        .expect("failed waiting for node process");
    assert!(
        output.status.success(),
        "node UI test failed (exit {:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const NODE_BOOTSTRAP: &str = r#"
import assert from "node:assert/strict";

const popstateListeners = [];
let fetchImpl = async () => {
  throw new Error("unexpected fetch call");
};

globalThis.document = {
  createTreeWalker: () => ({ currentNode: null, nextNode: () => null }),
  getElementById: () => null,
};
globalThis.window = {
  EMULATOR_VERSION: "test",
  location: { pathname: "/" },
  history: {
    pushState(_state, _title, url) {
      globalThis.window.location.pathname = String(url);
    },
    replaceState(_state, _title, url) {
      globalThis.window.location.pathname = String(url);
    },
  },
  addEventListener(type, fn) {
    if (type === "popstate") popstateListeners.push(fn);
  },
};
globalThis.EventSource = class {
  addEventListener() {}
  close() {}
};
globalThis.fetch = (url, init) => fetchImpl(String(url), init ?? {});

function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function jsonResponse(body, status = 200) {
  const text = typeof body === "string" ? body : JSON.stringify(body);
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 404 ? "Not Found" : "OK",
    text: async () => text,
  };
}

function flattenText(node) {
  if (node == null || typeof node === "symbol") return "";
  if (typeof node === "string" || typeof node === "number" || typeof node === "boolean") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(flattenText).join("");
  }
  if (typeof node === "object" && Array.isArray(node.strings) && Array.isArray(node.values)) {
    let out = "";
    for (let i = 0; i < node.strings.length; i++) {
      out += node.strings[i];
      if (i < node.values.length) {
        out += flattenText(node.values[i]);
      }
    }
    return out;
  }
  return "";
}

function findClickHandler(node, labelText) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const hit = findClickHandler(item, labelText);
      if (hit) return hit;
    }
    return null;
  }
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {
    for (let i = 0; i < node.values.length; i++) {
      const v = node.values[i];
      if (typeof v === "function" && /@click=\s*$/.test(node.strings[i])) {
        // Ensure this @click= is on a <button> or <li> element (not a parent <div class="modal-backdrop">).
        let tagBefore = "";
        for (let k = i; k >= 0; k--) {
          tagBefore = node.strings[k] + tagBefore;
          const lastLt = tagBefore.lastIndexOf("<");
          if (lastLt >= 0) {
            tagBefore = tagBefore.slice(lastLt);
            break;
          }
        }
        const tagMatch = /^<(button|li)\b/.exec(tagBefore);
        if (tagMatch) {
          const closeTag = `</${tagMatch[1]}>`;
          let inner = "";
          for (let j = i + 1; j < node.strings.length; j++) {
            const s = node.strings[j];
            const closeIdx = s.indexOf(closeTag);
            if (closeIdx >= 0) {
              inner += s.slice(0, closeIdx);
              break;
            }
            inner += s;
            if (j < node.values.length) {
              inner += flattenText(node.values[j]);
            }
          }
          if (inner.includes(labelText)) {
            return v;
          }
        }
      }
      const nested = findClickHandler(v, labelText);
      if (nested) return nested;
    }
  }
  return null;
}

function findInputHandler(node, placeholderSubstr) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const hit = findInputHandler(item, placeholderSubstr);
      if (hit) return hit;
    }
    return null;
  }
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {
    for (let i = 0; i < node.values.length; i++) {
      const v = node.values[i];
      if (typeof v === "function" && /@input=\s*$/.test(node.strings[i])) {
        let before = "";
        for (let k = i; k >= 0; k--) {
          before = node.strings[k] + before;
          if (before.includes("<input") || before.includes("<textarea")) break;
        }
        if (before.includes(placeholderSubstr)) {
          return v;
        }
      }
      const nested = findInputHandler(v, placeholderSubstr);
      if (nested) return nested;
    }
  }
  return null;
}
"#;

#[test]
fn bug_web_ui_1_async_race_conditions() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

// 1A. Rapid selectDatabase: slower DB1 must NOT overwrite DB2 metadata/collections.
{{
  const db1MetaDef = deferred();
  const db1ColsDef = deferred();
  const db2MetaDef = deferred();
  const db2ColsDef = deferred();
  const fetched = [];

  fetchImpl = async (url) => {{
    fetched.push(url);
    if (url === "/v1/projects/p1/databases/db1") return db1MetaDef.promise;
    if (url === "/v1/projects/p1/databases/db1/documents:listCollectionIds") return db1ColsDef.promise;
    if (url === "/v1/projects/p2/databases/db2") return db2MetaDef.promise;
    if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") return db2ColsDef.promise;
    if (url === "/emulator/v1") {{
      return jsonResponse([
        "projects/p1/databases/db1/documents",
        "projects/p2/databases/db2/documents",
      ]);
    }}
    throw new Error("unexpected url in 1A: " + url);
  }};

  const p1 = app.selectDatabase("p1", "db1", ["col1"]);
  const p2 = app.selectDatabase("p2", "db2", []);
  assert.equal(app.state.activeDbMeta, null, "activeDbMeta cleared immediately on db switch");

  db2MetaDef.resolve(jsonResponse({{
    name: "projects/p2/databases/db2",
    uid: "u2",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "ENTERPRISE",
    concurrencyMode: "OPTIMISTIC",
  }}));
  db2ColsDef.resolve(jsonResponse({{ collectionIds: ["orders"] }}));
  await p2;

  db1MetaDef.resolve(jsonResponse({{
    name: "projects/p1/databases/db1",
    uid: "u1",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "STANDARD",
    concurrencyMode: "PESSIMISTIC",
  }}));
  db1ColsDef.resolve(jsonResponse({{ collectionIds: ["users"] }}));
  await p1;

  assert.equal(app.state.activeProjectId, "p2");
  assert.equal(app.state.activeDatabaseId, "db2");
  assert.equal(app.state.activeDbMeta.databaseEdition, "ENTERPRISE");
  assert.deepEqual(app.state.rootCollections, ["orders"]);
  assert.equal(
    fetched.some((u) => u.includes("/db1/documents/col1")),
    false,
    "stale DB1 must not trigger hydrateVisiblePath"
  );
}}

// 1B. Rapid Explorer segment navigation: slower users load must NOT pair old "users" with new "ord1".
{{
  const usersListDef = deferred();
  const ordersListDef = deferred();
  const fetched = [];

  fetchImpl = async (url) => {{
    fetched.push(url);
    if (url === "/v1/projects/p2/databases/db2") {{
      return jsonResponse({{
        name: "projects/p2/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "ENTERPRISE",
        concurrencyMode: "OPTIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: ["users", "orders"] }});
    }}
    if (url === "/emulator/v1") {{
      return jsonResponse(["projects/p2/databases/db2/documents"]);
    }}
    if (url.startsWith("/v1/projects/p2/databases/db2/documents/users?")) return usersListDef.promise;
    if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders?")) return ordersListDef.promise;
    if (url === "/v1/projects/p2/databases/db2/documents/orders/ord1") {{
      return jsonResponse({{
        name: "projects/p2/databases/db2/documents/orders/ord1",
        fields: {{}},
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      }});
    }}
    if (url === "/v1/projects/p2/databases/db2/documents/orders/ord1:listCollectionIds") {{
      return jsonResponse({{ collectionIds: [] }});
    }}
    throw new Error("unexpected url in 1B: " + url);
  }};

  await app.selectDatabase("p2", "db2", [], true);
  // Click root collection "users" in Explorer (triggers onSelectSegments(["users"]))
  const ws1 = app.renderWorkspace();
  const clickUsers = findClickHandler(ws1, "users");
  assert.ok(clickUsers, "expected click handler for users collection");
  clickUsers();
  // Immediately navigate to ["orders", "ord1"] via Query result open handler or collection click -> orders -> ord1
  app.state.view = "query";
  app.state.queryState.results = [{{ name: "projects/p2/databases/db2/documents/orders/ord1", fields: {{}} }}];
  const wsQuery = app.renderWorkspace();
  const openOrd1 = findClickHandler(wsQuery, "Open in Explorer");
  assert.ok(openOrd1, "expected Open in Explorer handler");
  openOrd1();

  ordersListDef.resolve(jsonResponse({{
    documents: [{{
      name: "projects/p2/databases/db2/documents/orders/ord1",
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }}],
  }}));
  await new Promise((r) => setTimeout(r, 0));

  usersListDef.resolve(jsonResponse({{
    documents: [{{
      name: "projects/p2/databases/db2/documents/users/alice",
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }}],
  }}));
  await new Promise((r) => setTimeout(r, 0));

  assert.deepEqual(app.state.segments, ["orders", "ord1"]);
  assert.equal(
    fetched.some((u) => u.includes("/users/ord1")),
    false,
    "must never mix old colPath users with new docId ord1"
  );
  assert.equal(
    app.state.documentLevels.has("projects/p2/databases/db2/documents/users/ord1"),
    false
  );
}}

// 1C. loadCollectionColumn per-column request sequence: older slow list must not overwrite newer list.
{{
  const colPath = "projects/p2/databases/db2/documents/orders";
  const parentPath = "projects/p2/databases/db2/documents";
  const slowDef = deferred();
  const fastDef = deferred();
  let call = 0;

  fetchImpl = async (url) => {{
    if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders?")) {{
      call++;
      return call === 1 ? slowDef.promise : fastDef.promise;
    }}
    throw new Error("unexpected url in 1C: " + url);
  }};

  const pSlow = app.loadCollectionColumn(colPath, parentPath, "orders", false);
  const pFast = app.loadCollectionColumn(colPath, parentPath, "orders", false);

  fastDef.resolve(jsonResponse({{
    documents: [{{
      name: "projects/p2/databases/db2/documents/orders/fresh",
      createTime: "2026-01-02T00:00:00Z",
      updateTime: "2026-01-02T00:00:00Z",
    }}],
  }}));
  await pFast;

  slowDef.resolve(jsonResponse({{
    documents: [{{
      name: "projects/p2/databases/db2/documents/orders/stale",
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }}],
  }}));
  await pSlow;

  assert.deepEqual(
    app.state.collectionColumns.get(colPath).documents.map((d) => d.id),
    ["fresh"]
  );
}}

// 1D. Slow SSE loadDocumentLevel must not overwrite a newer user Save, and toggleDatabaseSetting must ignore stale DB switch.
{{
  const docPath = "projects/p2/databases/db2/documents/orders/ord1";
  const slowSseGet = deferred();
  const slowPatchDb = deferred();

  fetchImpl = async (url, init) => {{
    if (url === "/v1/projects/p2/databases/db2/documents/orders/ord1" && (!init.method || init.method === "GET")) {{
      return slowSseGet.promise;
    }}
    if (url === "/v1/projects/p2/databases/db2/documents/orders/ord1:listCollectionIds") {{
      return jsonResponse({{ collectionIds: [] }});
    }}
    if (url.startsWith("/v1/projects/p2/databases/db2/documents/orders/ord1?") && init.method === "PATCH") {{
      return jsonResponse({{
        name: docPath,
        fields: {{ title: {{ stringValue: "saved-v2" }} }},
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-02T00:00:00Z",
      }});
    }}
    if (url === "/v1/projects/p2/databases/db2" && init.method === "PATCH") {{
      return slowPatchDb.promise;
    }}
    if (url === "/v1/projects/p3/databases/db3") {{
      return jsonResponse({{
        name: "projects/p3/databases/db3",
        uid: "u3",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "ENTERPRISE",
        concurrencyMode: "OPTIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: [] }});
    }}
    if (url === "/emulator/v1") {{
      return jsonResponse([
        "projects/p2/databases/db2/documents",
        "projects/p3/databases/db3/documents",
      ]);
    }}
    throw new Error("unexpected url in 1D: " + init.method + " " + url);
  }};

  const level = app.state.documentLevels.get(docPath);
  const pSseDoc = app.loadDocumentLevel(docPath, "ord1", true);

  // User adds a field and saves before slow SSE GET resolves
  const addFieldBtn = findClickHandler(app.renderWorkspace(), "+ Add Field");
  assert.ok(addFieldBtn);
  addFieldBtn();
  level.editorSession.nodes[0].key = "title";
  level.editorSession.nodes[0].text = "saved-v2";
  await app.saveDocumentFromInspector(level);
  assert.equal(level.editorSession.cleanDoc.updateTime, "2026-01-02T00:00:00Z");

  slowSseGet.resolve(jsonResponse({{
    name: docPath,
    fields: {{}},
    createTime: "2026-01-01T00:00:00Z",
    updateTime: "2026-01-01T00:00:00Z",
  }}));
  await pSseDoc;
  assert.equal(level.editorSession.cleanDoc.updateTime, "2026-01-02T00:00:00Z");
  assert.equal(level.editorSession.externalDoc, null);

  // Now toggleDatabaseSetting on db2, switch to db3, resolve db2 PATCH late
  const pToggle = app.toggleDatabaseSetting("edition");
  await app.selectDatabase("p3", "db3", [], true);
  slowPatchDb.resolve(jsonResponse({{
    name: "projects/p2/databases/db2",
    uid: "u2",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "STANDARD",
    concurrencyMode: "PESSIMISTIC",
  }}));
  await pToggle;
  assert.equal(app.state.activeDbMeta.name, "projects/p3/databases/db3");
  assert.equal(app.state.activeDbMeta.databaseEdition, "ENTERPRISE");
}}

// 1E. handleSseBatch must abort if active DB changes while awaiting listCollectionIds.
{{
  const oldColsDef = deferred();
  const fetched = [];
  fetchImpl = async (url) => {{
    fetched.push(url);
    if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") return oldColsDef.promise;
    if (url === "/v1/projects/p4/databases/db4") {{
      return jsonResponse({{
        name: "projects/p4/databases/db4",
        uid: "u4",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p4/databases/db4/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: ["db4_col"] }});
    }}
    if (url === "/emulator/v1") {{
      return jsonResponse([
        "projects/p3/databases/db3/documents",
        "projects/p4/databases/db4/documents",
      ]);
    }}
    throw new Error("unexpected url in 1E: " + url);
  }};

  app.state.segments = ["old_col"];
  const pBatch = app.handleSseBatch([{{
    type: "documents_changed",
    database: "projects/p3/databases/db3/documents",
    paths: ["projects/p3/databases/db3/documents/old_col/d1"],
  }}]);
  await app.selectDatabase("p4", "db4", [], true);
  oldColsDef.resolve(jsonResponse({{ collectionIds: ["stale_db3_col"] }}));
  await pBatch;

  assert.deepEqual(app.state.rootCollections, ["db4_col"]);
  assert.equal(fetched.some((u) => u.includes("/db3/documents/old_col")), false);
}}

// 1F. handleSseBatch database_cleared resets active state without calling /v1/projects/ for cleared DB.
{{
  const fetched = [];
  fetchImpl = async (url) => {{
    fetched.push(url);
    if (url === "/emulator/v1") return jsonResponse([]);
    throw new Error("unexpected url in 1F: " + url);
  }};
  app.state.activeProjectId = "p4";
  app.state.activeDatabaseId = "db4";
  await app.handleSseBatch([{{
    type: "database_cleared",
    database: "projects/p4/databases/db4/documents",
  }}]);
  assert.equal(app.state.activeProjectId, null);
  assert.equal(app.state.activeDatabaseId, null);
  assert.equal(app.state.activeDbMeta, null);
  assert.equal(fetched.some((u) => u.startsWith("/v1/")), false);
}}

console.log("bug_web_ui_1 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_2_url_routing_and_segment_validation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

// Malformed percent encodings, empty segments, encoded slashes, and dot segments must return null cleanly.
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
]) {{
  assert.equal(app.parseBrowserRoute(badPath), null, "expected null for " + badPath);
}}

assert.deepEqual(
  app.parseBrowserRoute("/projects/p/databases/(default)/documents/users/100%25"),
  {{ projectId: "p", databaseId: "(default)", segments: ["users", "100%"] }}
);

// init() with a malformed URL must not throw URIError.
globalThis.window.location.pathname = "/projects/p/databases/(default)/documents/users/100%";
fetchImpl = async (url) => {{
  if (url === "/emulator/v1") return jsonResponse([]);
  throw new Error("unexpected url during init: " + url);
}};
await app.init();
assert.equal(app.state.activeProjectId, null);

// popstate with a malformed URL must not throw URIError.
app.state.segments = ["users"];
globalThis.window.location.pathname = "/projects/p/databases/(default)/documents/users/%ZZ";
for (const listener of popstateListeners) {{
  listener();
}}
assert.deepEqual(app.state.segments, []);

// Modals must reject "." and ".." without calling fetch.
let fetchCount = 0;
fetchImpl = async () => {{
  fetchCount++;
  return jsonResponse({{}});
}};

app.state.modal = {{
  kind: "open_db",
  projectId: "..",
  databaseId: "(default)",
  edition: "STANDARD",
  busy: false,
  error: null,
}};
const submitDb = findClickHandler(app.renderModal(), "Open Database");
assert.ok(submitDb);
await submitDb();
assert.ok(app.state.modal.error, "expected validation error for projectId '..'");
assert.equal(fetchCount, 0);

app.state.activeProjectId = "p";
app.state.activeDatabaseId = "(default)";
app.openCreateDocumentModal("projects/p/databases/(default)/documents", "users", "");
app.state.modal.docId = "..";
const submitDoc = findClickHandler(app.renderModal(), "Save Document");
assert.ok(submitDoc);
await submitDoc();
assert.ok(app.state.modal.error, "expected validation error for docId '..'");
assert.equal(fetchCount, 0);

console.log("bug_web_ui_2 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_3_dirty_editor_protection() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const {{ renderExplorer }} = await import("./ui/components/explorer.js");

const docPath = "projects/p/databases/(default)/documents/users/alice";
const serverDoc = {{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-01T00:00:00Z",
}};

let docExists = true;
fetchImpl = async (url) => {{
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  if (url === "/emulator/v1") {{
    return jsonResponse(["projects/p/databases/(default)/documents"]);
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {{
    return jsonResponse({{
      documents: docExists
        ? [{{ name: docPath, createTime: serverDoc.createTime, updateTime: serverDoc.updateTime }}]
        : [],
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice") {{
    return docExists
      ? jsonResponse(serverDoc)
      : jsonResponse({{ error: {{ code: 404, status: "NOT_FOUND", message: "not found" }} }}, 404);
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["posts"] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users/alice/posts?")) {{
    return jsonResponse({{ documents: [] }});
  }}
  throw new Error("unexpected url in bug 3: " + url);
}};

await app.selectDatabase("p", "(default)", ["users", "alice"], true);
const level = app.state.documentLevels.get(docPath);
assert.ok(level?.editorSession);

// Mutate editor value through the input handler so isDirty becomes true.
const nameInput = findInputHandler(app.renderWorkspace(), "string value");
assert.ok(nameInput, "expected string value input handler");
nameInput({{ target: {{ value: "Alice-Edited" }} }});
assert.equal(level.editorSession.isDirty, true);

// 3A. Navigating into subcollection "posts" via hydrateVisiblePath must preserve dirty edits on alice.
app.state.segments = ["users", "alice", "posts"];
await app.hydrateVisiblePath();
assert.equal(level.editorSession.isDirty, true);
assert.equal(level.editorSession.nodes[0].text, "Alice-Edited");

// 3B. External deletion (404 via SSE) while dirty must NOT hide the editor behind the phantom card.
docExists = false;
await app.handleSseBatch([{{
  type: "documents_changed",
  database: "projects/p/databases/(default)/documents",
  paths: [docPath],
}}]);

assert.equal(level.exists, false);
assert.equal(level.editorSession.isDirty, true);
assert.equal(level.editorSession.deletedExternally, true);

const explorerTpl = renderExplorer({{
  projectId: "p",
  databaseId: "(default)",
  rootCollections: ["users"],
  rootLoading: false,
  segments: ["users", "alice"],
  collectionColumns: app.state.collectionColumns,
  documentLevels: app.state.documentLevels,
  onSelectSegments: () => {{}},
  onOpenStartCollection: () => {{}},
  onOpenAddDocument: () => {{}},
  onOpenDeleteCollection: () => {{}},
  onOpenDeleteDocument: () => {{}},
  onOpenQueryCollection: () => {{}},
  onLoadMoreDocuments: () => {{}},
  onSaveDocument: () => {{}},
  onFilterChange: () => {{}},
  onRerender: () => {{}},
}});
const renderedText = flattenText(explorerTpl);
assert.ok(renderedText.includes("deleted externally"), "expected external deletion conflict banner");
assert.equal(renderedText.includes("Non-existent ancestor document"), false);

// Clicking "Keep my edits" clears cleanDoc/deletedExternally while preserving unsaved nodes.
const keepBtn = findClickHandler(explorerTpl, "Keep my edits");
assert.ok(keepBtn);
keepBtn();
assert.equal(level.editorSession.deletedExternally, false);
assert.equal(level.editorSession.cleanDoc, null);
assert.equal(level.editorSession.isDirty, true);
assert.equal(level.editorSession.nodes[0].text, "Alice-Edited");

// Clicking "Discard my edits" when deleted externally transitions cleanly to non-existent state.
level.editorSession.deletedExternally = true;
const tpl2 = app.renderWorkspace();
const discardBtn = findClickHandler(tpl2, "Discard my edits");
assert.ok(discardBtn);
discardBtn();
assert.equal(level.editorSession.isDirty, false);
assert.ok(flattenText(app.renderWorkspace()).includes("Non-existent ancestor document"));

console.log("bug_web_ui_3 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_4_delete_segment_truncation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

fetchImpl = async (url, init) => {{
  if (init.method === "DELETE") return jsonResponse({{}});
  if (url.endsWith(":listCollectionIds")) return jsonResponse({{ collectionIds: ["users", "rooms"] }});
  if (url.includes("/documents/rooms?")) return jsonResponse({{ documents: [] }});
  if (url.includes("/documents/users?")) return jsonResponse({{ documents: [] }});
  if (url === "/v1/projects/p/databases/(default)/documents/users/orders") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)/documents/users/orders",
      fields: {{}},
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }});
  }}
  throw new Error("unexpected url in bug 4: " + (init.method ?? "GET") + " " + url);
}};

// 4A. Recursive delete_doc from subcollection view ["rooms", "r1", "messages"] -> must slice to ["rooms"]
app.state.activeProjectId = "p";
app.state.activeDatabaseId = "(default)";
app.state.segments = ["rooms", "r1", "messages"];
app.state.modal = {{
  kind: "delete_doc",
  docPath: "projects/p/databases/(default)/documents/rooms/r1",
  recursive: true,
  hasSubcollections: true,
  busy: false,
  error: null,
}};
const confirmDelDoc = findClickHandler(app.renderModal(), "Delete");
assert.ok(confirmDelDoc);
await confirmDelDoc();
assert.deepEqual(app.state.segments, ["rooms"]);

// 4B. delete_col with duplicate ancestor segment name ["users", "orders", "orders"] -> must slice to ["users", "orders"]
app.state.segments = ["users", "orders", "orders"];
app.state.modal = {{
  kind: "delete_col",
  colPath: "projects/p/databases/(default)/documents/users/orders/orders",
  colId: "orders",
  busy: false,
  error: null,
}};
const confirmDelCol = findClickHandler(app.renderModal(), "Delete Collection");
assert.ok(confirmDelCol);
await confirmDelCol();
assert.deepEqual(app.state.segments, ["users", "orders"]);

console.log("bug_web_ui_4 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_5_timestamp_validation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const {{ validateTimestamp }} = await import("./ui/values.js");

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
]) {{
  assert.equal(validateTimestamp(bad).valid, false, "expected invalid timestamp: " + bad);
}}

for (const good of [
  "2024-02-29T23:59:59.123456789Z",
  "2000-02-29T00:00:00Z",
  "0001-01-01T00:00:00Z",
  "0001-01-01T00:00:00-01:00",
  "9999-12-31T23:59:59.999999999Z",
  "9999-12-31T23:59:59+01:00",
  "2026-11-10T15:30:00.000000001+05:30",
]) {{
  assert.equal(validateTimestamp(good).valid, true, "expected valid timestamp: " + good);
}}

console.log("bug_web_ui_5 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_6_negative_zero_preservation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const values = await import("./ui/values.js");
const api = await import("./ui/api.js");
const editor = await import("./ui/components/editor.js");
const query = await import("./ui/components/query.js");

const serialized = values.stringifyProtoJson({{
  fields: {{
    neg: {{ doubleValue: -0 }},
    pos: {{ doubleValue: 0 }},
    geo: {{ geoPointValue: {{ latitude: -0, longitude: 0 }} }},
  }},
}}, 2);
assert.ok(serialized.includes('"doubleValue": -0.0'));
assert.ok(serialized.includes('"doubleValue": 0'));
assert.ok(serialized.includes('"latitude": -0.0'));
assert.ok(serialized.includes('"longitude": 0'));

const doc = {{
  name: "projects/p/databases/(default)/documents/c/d",
  fields: {{
    neg: {{ doubleValue: -0 }},
    geo: {{ geoPointValue: {{ latitude: -0, longitude: 0 }} }},
    vec: {{
      mapValue: {{
        fields: {{
          __type__: {{ stringValue: "__vector__" }},
          value: {{ arrayValue: {{ values: [{{ doubleValue: -0 }}] }} }},
        }},
      }},
    }},
  }},
}};
const session = editor.createEditorSession(doc.name, "p", "(default)", doc);
assert.equal(session.isDirty, false, "GeoPoint -0.0 must not falsely mark clean doc dirty");
assert.ok(session.cleanJson.includes('"doubleValue": -0.0'));
assert.ok(session.cleanJson.includes('"latitude": -0.0'));

// Switching to Raw and validating back preserves -0.
session.mode = "raw";
const extracted = editor.extractSessionFields(session);
assert.equal(extracted.valid, true);
assert.equal(Object.is(extracted.fields.neg.doubleValue, -0), true);
assert.equal(Object.is(extracted.fields.geo.geoPointValue.latitude, -0), true);

// API requests preserve -0.0 in HTTP bodies.
const sentBodies = [];
fetchImpl = async (_url, init) => {{
  sentBodies.push(init.body);
  return jsonResponse({{ name: doc.name, fields: extracted.fields }});
}};

await api.createDocument("projects/p/databases/(default)/documents", "c", "d", extracted.fields);
await api.updateDocument(doc.name, extracted.fields, ["neg"], null);

const qState = query.createQueryRunnerState("c");
qState.filters.push({{
  id: 1,
  fieldPath: "neg",
  op: "EQUAL",
  valueType: "doubleValue",
  valueText: "-0.0",
  latText: "0",
  lngText: "0",
}});
const built = query.buildStructuredQueryFromVisual(qState, "p", "(default)");
assert.equal(built.valid, true);
await api.runQuery("projects/p/databases/(default)/documents", built.query);

assert.equal(sentBodies.length, 3);
for (const body of sentBodies) {{
  assert.ok(body.includes("-0.0"), "expected -0.0 in serialized request body: " + body);
}}

console.log("bug_web_ui_6 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_7_proto_key_and_whitespace_preservation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const values = await import("./ui/values.js");
const editor = await import("./ui/components/editor.js");

const rawRes = values.validateRawProtoJsonFields(
  '{{"fields":{{"__proto__":{{"stringValue":"x"}}," a ":{{ "integerValue": "1" }}}}}}'
);
assert.equal(rawRes.valid, true);
assert.equal(Object.getPrototypeOf(rawRes.fields), Object.prototype);
assert.deepEqual(Object.keys(rawRes.fields).sort(), [" a ", "__proto__"]);
assert.equal(rawRes.fields["__proto__"].stringValue, "x");
assert.equal(rawRes.fields[" a "].integerValue, "1");

const spacedDoc = {{
  name: "projects/p/databases/(default)/documents/c/d",
  fields: {{ " a ": {{ stringValue: "val" }} }},
}};
const session = editor.createEditorSession(spacedDoc.name, "p", "(default)", spacedDoc);
assert.equal(session.isDirty, false, "leading/trailing spaces in key must not falsely mark dirty");

session.nodes.push({{
  id: 999,
  key: "__proto__",
  type: "stringValue",
  text: "protoVal",
  boolVal: false,
  latText: "0",
  lngText: "0",
  bytesMode: "base64",
  children: [],
}});
const extracted = editor.extractSessionFields(session);
assert.equal(extracted.valid, true);
assert.equal(Object.getPrototypeOf(extracted.fields), Object.prototype);
assert.deepEqual(Object.keys(extracted.fields).sort(), [" a ", "__proto__"]);
assert.equal(extracted.fields["__proto__"].stringValue, "protoVal");

console.log("bug_web_ui_7 passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn bug_web_ui_8_miller_column_header_overflow_css() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");

    assert!(
        css.contains(".miller-col") && css.contains("overflow: hidden;"),
        "expected .miller-col to enforce overflow: hidden"
    );
    assert!(
        css.contains(".col-header > span")
            && css.contains("min-width: 0;")
            && css.contains("text-overflow: ellipsis;")
            && css.contains("white-space: nowrap;"),
        "expected .col-header > span truncation rules"
    );
    assert!(
        css.contains(".col-header-actions") && css.contains("flex-shrink: 0;"),
        "expected .col-header-actions to enforce flex-shrink: 0"
    );
}

#[test]
fn regression_popstate_same_db_does_not_lock_saving_or_loading_states() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const colPath = "projects/p/databases/(default)/documents/orders";
const parentPath = "projects/p/databases/(default)/documents";
const docPath = "projects/p/databases/(default)/documents/orders/ord1";

let patchDef = null;
let colReloadDef = null;

fetchImpl = async (url, init) => {{
  if (url === "/emulator/v1") {{
    return jsonResponse(["projects/p/databases/(default)/documents"]);
  }}
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["orders"] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/orders?")) {{
    if (colReloadDef) return colReloadDef.promise;
    return jsonResponse({{
      documents: [{{
        name: docPath,
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      }}],
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/orders/ord1" && (!init.method || init.method === "GET")) {{
    return jsonResponse({{
      name: docPath,
      fields: {{ status: {{ stringValue: "open" }} }},
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/orders/ord1:listCollectionIds") {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/orders/ord1") && init.method === "PATCH") {{
    return patchDef.promise;
  }}
  throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
}};

globalThis.window.location.pathname = "/projects/p/databases/(default)/documents/orders/ord1";
await app.init();

const level = app.state.documentLevels.get(docPath);
const colState = app.state.collectionColumns.get(colPath);
assert.ok(level?.editorSession);
assert.ok(colState);

// Mark ord1 dirty and start an in-flight Save + an in-flight collection reload.
const valInput = findInputHandler(app.renderWorkspace(), "string value");
assert.ok(valInput);
valInput({{ target: {{ value: "closed" }} }});
assert.equal(level.editorSession.isDirty, true);

patchDef = deferred();
colReloadDef = deferred();
const savePromise = app.saveDocumentFromInspector(level);
const colPromise = app.loadCollectionColumn(colPath, parentPath, "orders", false);
assert.equal(level.editorSession.saving, true);
assert.equal(colState.loading, true);

// Browser Back/Forward within the SAME database ("p" / "(default)") to root documents view.
globalThis.window.location.pathname = "/projects/p/databases/(default)/documents";
for (const fn of popstateListeners) {{
  fn();
}}
await new Promise((r) => setTimeout(r, 0));

// Now settle the in-flight PATCH and collection GET.
patchDef.resolve(jsonResponse({{
  name: docPath,
  fields: {{ status: {{ stringValue: "closed" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-02T00:00:00Z",
}}));
colReloadDef.resolve(jsonResponse({{ documents: [] }}));
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
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_clear_database_modal_does_not_resurrect_cleared_db_on_concurrent_sse() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const listDef1 = deferred();
const listDef2 = deferred();
let listCalls = 0;
const fetched = [];

fetchImpl = async (url, init) => {{
  fetched.push((init.method ?? "GET") + " " + url);
  if (url === "/emulator/v1/projects/p/databases/(default)/documents" && init.method === "DELETE") {{
    return jsonResponse({{}});
  }}
  if (url === "/emulator/v1") {{
    listCalls++;
    return listCalls === 1 ? listDef1.promise : listDef2.promise;
  }}
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u-resurrected",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
}};

app.state.activeProjectId = "p";
app.state.activeDatabaseId = "(default)";
app.state.databases = [{{
  projectId: "p",
  databaseId: "(default)",
  rootPath: "projects/p/databases/(default)/documents",
}}];
app.state.modal = {{
  kind: "clear_db",
  projectId: "p",
  databaseId: "(default)",
  busy: false,
  error: null,
}};

const clearBtn = findClickHandler(app.renderModal(), "Clear Database");
assert.ok(clearBtn, "expected Clear Database button handler");
const clearPromise = clearBtn();

// Wait a microtask so DELETE completes and confirmClearDb awaits GET /emulator/v1 (#1).
await new Promise((r) => setTimeout(r, 0));
assert.equal(listCalls, 1);

// Concurrent SSE database_cleared arrives and starts GET /emulator/v1 (#2), bumping dbListSeq.
const ssePromise = app.handleSseBatch([{{
  type: "database_cleared",
  database: "projects/p/databases/(default)/documents",
}}]);
assert.equal(listCalls, 2);

// Resolve both GET /emulator/v1 calls with [] (no databases remain).
listDef1.resolve(jsonResponse([]));
await new Promise((r) => setTimeout(r, 0));
listDef2.resolve(jsonResponse([]));
await clearPromise;
await ssePromise;

assert.equal(
  fetched.some((entry) => entry.includes(" /v1/projects/p/databases/(default)")),
  false,
  "confirmClearDb must not read stale state.databases and resurrect the cleared database: " + JSON.stringify(fetched)
);
assert.equal(app.state.activeProjectId, null);
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_deleted_externally_keep_empty_edits_and_save_recreates_document() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const docPath = "projects/p/databases/(default)/documents/users/alice";
let docExists = true;
let patchCount = 0;

fetchImpl = async (url, init) => {{
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  if (url === "/emulator/v1") {{
    return jsonResponse(["projects/p/databases/(default)/documents"]);
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {{
    return jsonResponse({{
      documents: docExists
        ? [{{ name: docPath, createTime: "2026-01-01T00:00:00Z", updateTime: "2026-01-01T00:00:00Z" }}]
        : [],
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice" && (!init.method || init.method === "GET")) {{
    return docExists
      ? jsonResponse({{
          name: docPath,
          fields: {{ name: {{ stringValue: "Alice" }} }},
          createTime: "2026-01-01T00:00:00Z",
          updateTime: "2026-01-01T00:00:00Z",
        }})
      : jsonResponse({{ error: {{ code: 404, status: "NOT_FOUND", message: "not found" }} }}, 404);
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds") {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users/alice") && init.method === "PATCH") {{
    patchCount++;
    docExists = true;
    return jsonResponse({{
      name: docPath,
      fields: {{}},
      createTime: "2026-01-02T00:00:00Z",
      updateTime: "2026-01-02T00:00:00Z",
    }});
  }}
  throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
}};

await app.selectDatabase("p", "(default)", ["users", "alice"], true);
const level = app.state.documentLevels.get(docPath);
assert.ok(level?.editorSession);

// User deletes the only field so the edited document is an empty `{{}}` document (`isDirty === true`).
const rawTab = findClickHandler(app.renderWorkspace(), "Raw ProtoJSON");
assert.ok(rawTab);
rawTab();
level.editorSession.rawText = '{{"fields":{{}}}}';
const structuredTab = findClickHandler(app.renderWorkspace(), "Structured Fields");
assert.ok(structuredTab);
structuredTab();
assert.equal(level.editorSession.nodes.length, 0);
assert.equal(level.editorSession.isDirty, true);

// Document is deleted externally via SSE.
docExists = false;
await app.handleSseBatch([{{
  type: "documents_changed",
  database: "projects/p/databases/(default)/documents",
  paths: [docPath],
}}]);
assert.equal(level.exists, false);
assert.equal(level.editorSession.deletedExternally, true);

// User clicks "Keep my edits" intending to keep and save the empty `{{}}` document.
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
assert.equal(patchCount, 1, "Save Changes must send PATCH to recreate the empty document");
assert.equal(level.exists, true);
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_save_document_clears_loading_and_ignores_concurrent_inflight_sse_get() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const docPath = "projects/p/databases/(default)/documents/users/alice";
let getHandler = async () => jsonResponse({{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice-v1" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-01T00:00:00Z",
}});
let patchHandler = async () => jsonResponse({{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice-v2" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-02T00:00:00Z",
}});

fetchImpl = async (url, init) => {{
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  if (url === "/emulator/v1") {{
    return jsonResponse(["projects/p/databases/(default)/documents"]);
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {{
    return jsonResponse({{
      documents: [{{ name: docPath, createTime: "2026-01-01T00:00:00Z", updateTime: "2026-01-01T00:00:00Z" }}],
    }});
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice" && (!init.method || init.method === "GET")) {{
    return getHandler();
  }}
  if (url === "/v1/projects/p/databases/(default)/documents/users/alice:listCollectionIds") {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users/alice") && init.method === "PATCH") {{
    return patchHandler();
  }}
  throw new Error("unexpected url: " + (init.method ?? "GET") + " " + url);
}};

await app.selectDatabase("p", "(default)", ["users", "alice"], true);
const level = app.state.documentLevels.get(docPath);
assert.ok(level?.editorSession);

// Part A: An SSE loadDocumentLevel starts (`level.loading = true`), then user saves before SSE GET settles.
const sseDef1 = deferred();
getHandler = () => sseDef1.promise;
const sseP1 = app.loadDocumentLevel(docPath, "alice", true);
assert.equal(level.loading, true);

const input1 = findInputHandler(app.renderWorkspace(), "string value");
assert.ok(input1);
input1({{ target: {{ value: "Alice-v2" }} }});
await app.saveDocumentFromInspector(level);

sseDef1.resolve(jsonResponse({{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice-v1" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-01T00:00:00Z",
}}));
await sseP1;
assert.equal(
  level.loading,
  false,
  "superseded loadDocumentLevel must not leave level.loading stuck true after saveDocumentFromInspector"
);

// Part B: User starts saveDocumentFromInspector (in-flight PATCH), SSE loadDocumentLevel starts BEFORE PATCH resolves,
// then PATCH resolves with newer v3, then stale pre-PATCH SSE GET resolves with older v2.
const patchDef = deferred();
const sseDef2 = deferred();
patchHandler = () => patchDef.promise;
getHandler = () => sseDef2.promise;

const input2 = findInputHandler(app.renderWorkspace(), "string value");
assert.ok(input2);
input2({{ target: {{ value: "Alice-v3" }} }});
const saveP = app.saveDocumentFromInspector(level);

// SSE GET begins while PATCH is still in flight on the wire.
const sseP2 = app.loadDocumentLevel(docPath, "alice", true);

patchDef.resolve(jsonResponse({{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice-v3" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-03T00:00:00Z",
}}));
await saveP;
assert.equal(level.editorSession.cleanDoc.updateTime, "2026-01-03T00:00:00Z");

sseDef2.resolve(jsonResponse({{
  name: docPath,
  fields: {{ name: {{ stringValue: "Alice-v2" }} }},
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-02T00:00:00Z",
}}));
await sseP2;

assert.equal(
  level.editorSession.cleanDoc.updateTime,
  "2026-01-03T00:00:00Z",
  "concurrent SSE GET started while PATCH was in flight must not overwrite newer saved document"
);
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_raw_protojson_overflow_float_is_rejected() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const values = await import("./ui/values.js");

const res = values.validateRawProtoJsonFields('{{"fields":{{"d":{{"doubleValue":1e500}}}}}}');
assert.equal(
  res.valid,
  false,
  "JSON numeric literal 1e500 overflows IEEE-754 double to Infinity (which stringifyProtoJson turns into null) and must be rejected unless quoted as \"Infinity\""
);
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn query_builder_array_operators_validation_and_error_banners() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const query = await import("./ui/components/query.js");
const values = await import("./ui/values.js");

const makeRow = (op, valueText) => ({{
  id: 1,
  fieldPath: "tags",
  op,
  valueType: "stringValue",
  valueText,
  latText: "0",
  lngText: "0",
}});

// 1. IN, ARRAY_CONTAINS_ANY, and NOT_IN emit selected operator and encode integers, -0.0, floats,
//    unsafe floats, explicit ±Infinity and 2^53 doubleValue, and single-level arrayValue (IN/NOT_IN).
for (const op of ["IN", "ARRAY_CONTAINS_ANY", "NOT_IN"]) {{
  const qState = query.createQueryRunnerState("items");
  const baseArr =
    '[42, -0, 1.5, 1e25, {{"doubleValue": "Infinity"}}, {{"doubleValue": "-Infinity"}}, null, true, "ok", {{"doubleValue": 9007199254740992}}';
  qState.filters.push(makeRow(
    op,
    op === "ARRAY_CONTAINS_ANY"
      ? `${{baseArr}}]`
      : `${{baseArr}}, {{"arrayValue": {{"values": [{{"integerValue": "7"}}]}}}}]`
  ));
  const built = query.buildStructuredQueryFromVisual(qState, "p", "(default)");
  assert.equal(built.valid, true, "expected valid build for " + op + ": " + built.error);
  const ff = built.query.where.fieldFilter;
  assert.equal(ff.op, op);
  const items = ff.value.arrayValue.values;
  assert.deepEqual(items[0], {{ integerValue: "42" }});
  assert.equal(Object.is(items[1].doubleValue, -0), true);
  assert.deepEqual(items[2], {{ doubleValue: 1.5 }});
  assert.deepEqual(items[3], {{ doubleValue: 1e25 }});
  assert.deepEqual(items[4], {{ doubleValue: "Infinity" }});
  assert.deepEqual(items[5], {{ doubleValue: "-Infinity" }});
  assert.deepEqual(items[6], {{ nullValue: null }});
  assert.deepEqual(items[7], {{ booleanValue: true }});
  assert.deepEqual(items[8], {{ stringValue: "ok" }});
  assert.deepEqual(items[9], {{ doubleValue: 9007199254740992 }});
  if (op !== "ARRAY_CONTAINS_ANY") {{
    assert.deepEqual(items[10], {{ arrayValue: {{ values: [{{ integerValue: "7" }}] }} }});
  }}

  const serialized = values.stringifyProtoJson(built.query);
  assert.ok(serialized.includes('"doubleValue":-0.0'), "expected -0.0 in " + serialized);
  assert.ok(serialized.includes('"doubleValue":"Infinity"'), "expected Infinity string in " + serialized);
  assert.ok(serialized.includes('"doubleValue":"-Infinity"'), "expected -Infinity string in " + serialized);
}}

// 2. Rejects unquoted |n| >= 2^53 integers (except inside doubleValue), overflowing numeric
//    literals in both shorthand and ProtoJSON, nested arrays, and invalid plain objects.
assert.equal(
  values.validateRawProtoJsonFields('{{"fields":{{"d":{{"doubleValue":9007199254740992}}}}}}').valid,
  true
);
assert.equal(
  values.validateRawProtoJsonFields('{{"fields":{{"i":{{"integerValue":9007199254740993}}}}}}').valid,
  false
);
for (const [op, rawText, expectedSubstr] of [
  ["IN", "[9007199254740992]", "Unquoted 64-bit integer 9007199254740992 in IN"],
  ["ARRAY_CONTAINS_ANY", "[9007199254740993]", "ARRAY_CONTAINS_ANY"],
  ["NOT_IN", "[-9007199254740993]", "NOT_IN"],
  ["IN", '[{{"integerValue": 9007199254740993}}]', "Unquoted 64-bit integer 9007199254740993 in IN"],
  ["IN", "[1e309]", "overflows IEEE-754 double"],
  ["NOT_IN", "[-1e309]", "overflows IEEE-754 double"],
  ["IN", '[{{"doubleValue": 1e309}}]', "overflows IEEE-754 double"],
  ["IN", "[[1]]", 'must use explicit ProtoJSON {{"arrayValue": {{"values": [...]}}}}'],
  ["NOT_IN", "[[1]]", 'must use explicit ProtoJSON {{"arrayValue": {{"values": [...]}}}}'],
  ["ARRAY_CONTAINS_ANY", "[[1]]", "Nested arrays are not supported in ARRAY_CONTAINS_ANY"],
  ["ARRAY_CONTAINS_ANY", '[{{"arrayValue":{{"values":[{{"integerValue":"1"}}]}}}}]', "nested arrays"],
  ["IN", '[{{"arrayValue":{{"values":[{{"arrayValue":{{"values":[{{"integerValue":"1"}}]}}}}]}}}}]', "nested arrays"],
  ["ARRAY_CONTAINS_ANY", '[{{"foo": 1}}]', "unsupported Value key"],
  ["NOT_IN", "not json", 'Filter "tags NOT_IN"'],
  ["ARRAY_CONTAINS_ANY", "{{}}", 'Filter "tags ARRAY_CONTAINS_ANY" requires a JSON array'],
]) {{
  const qState = query.createQueryRunnerState("items");
  qState.filters.push(makeRow(op, rawText));
  const res = query.buildStructuredQueryFromVisual(qState, "p", "(default)");
  assert.equal(res.valid, false, "expected invalid result for " + op + " " + rawText);
  assert.ok(
    res.error.includes(expectedSubstr),
    `expected error containing ${{JSON.stringify(expectedSubstr)}}, got ${{res.error}}`
  );
}}

// 3. renderFilterRow initializes "[]" when switching into an array operator and renders JSON input.
{{
  const qState = query.createQueryRunnerState("items");
  qState.filters.push(makeRow("EQUAL", "alice"));
  const tpl1 = query.renderQueryRunner({{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["items"],
    state: qState,
    onRerender: () => {{}},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }});
  const findSelectChange = (node) => {{
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) {{
      for (const item of node) {{
        const hit = findSelectChange(item);
        if (hit) return hit;
      }}
      return null;
    }}
    if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
      for (let i = 0; i < node.values.length; i++) {{
        const v = node.values[i];
        if (typeof v === "function" && /@change=\s*$/.test(node.strings[i])) {{
          let before = "";
          for (let k = i; k >= 0; k--) {{
            before = node.strings[k] + before;
            if (before.includes("<select")) break;
          }}
          if (before.includes("<select") && node.values[i - 1] === "EQUAL") {{
            return v;
          }}
        }}
        const nested = findSelectChange(v);
        if (nested) return nested;
      }}
    }}
    return null;
  }};
  const onOpChange = findSelectChange(tpl1);
  assert.ok(onOpChange, "expected operator <select> @change handler");
  onOpChange({{ target: {{ value: "NOT_IN" }} }});
  assert.equal(qState.filters[0].op, "NOT_IN");
  assert.equal(qState.filters[0].valueText, "[]");
  const tpl2 = query.renderQueryRunner({{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["items"],
    state: qState,
    onRerender: () => {{}},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }});
  assert.ok(findInputHandler(tpl2, "JSON array"), "expected JSON array input after switch");
}}

// 4. executeCurrentQuery formats ApiError banners with "[code status] message".
{{
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "(default)";
  app.state.queryState = query.createQueryRunnerState("items");
  fetchImpl = async (url) => {{
    assert.ok(url.endsWith(":runQuery"));
    return jsonResponse(
      {{ error: {{ code: 400, status: "INVALID_ARGUMENT", message: "bad filter" }} }},
      400
    );
  }};
  await app.executeCurrentQuery();
  assert.equal(app.state.queryState.error, "[400 INVALID_ARGUMENT] bad filter");
}}

console.log("query_builder_array_operators_validation_and_error_banners passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_test_suite_script_sigterm_exit_code_and_eslint_flat_config_rules() {
    // Part A: `test-suite/package.json` `test:rust-emulator` must exit non-zero on SIGTERM instead
    // of swallowing TERM in `trap cleanup EXIT INT TERM`.
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let pkg_text = fs::read_to_string(repo_root.join("test-suite/package.json"))
        .expect("failed to read test-suite/package.json");
    let pkg_json: serde_json::Value =
        serde_json::from_str(&pkg_text).expect("invalid test-suite/package.json");
    let raw_script = pkg_json["scripts"]["test:rust-emulator"]
        .as_str()
        .expect("missing scripts.test:rust-emulator");

    let node_poll_start = raw_script
        .find("node -e ")
        .expect("expected readiness poll in test:rust-emulator");
    let node_poll_end = raw_script[node_poll_start..]
        .find("|| return 1;")
        .map(|idx| node_poll_start + idx + "|| return 1;".len())
        .expect("expected `|| return 1;` at end of readiness poll");

    let mut stubbed_script = String::new();
    stubbed_script.push_str(&raw_script[..node_poll_start]);
    stubbed_script.push_str(
        "echo READY; sleep 2 & WAIT_PID=$!; wait \"$WAIT_PID\"; kill \"$WAIT_PID\" 2>/dev/null || \
         true;",
    );
    stubbed_script.push_str(&raw_script[node_poll_end..]);
    let stubbed_script = stubbed_script
        .replace("../target/debug/firestore-emulator", "sleep 30")
        .replace(
            "RUST_EMULATOR=true NODE_OPTIONS='--experimental-vm-modules' jest \"$@\"",
            "true",
        );

    let mut child = Command::new("/bin/bash")
        .arg("-c")
        .arg(&stubbed_script)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn bash for test:rust-emulator signal test");

    let stdout = child.stdout.take().expect("stdout piped");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("failed reading READY line");
    assert_eq!(line.trim(), "READY", "unexpected startup output: {line:?}");

    let kill_status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("failed to send SIGTERM via kill");
    assert!(kill_status.success(), "kill -TERM failed");

    let exit_status = child.wait().expect("failed waiting for bash child");
    assert!(
        !exit_status.success(),
        "test:rust-emulator swallowed SIGTERM and exited with status 0 instead of non-zero"
    );

    // Part B: `test-suite/eslint.config.cjs` must preserve core `eslint:recommended` rules (e.g.
    // `no-debugger`, `no-unreachable`, `no-constant-condition`).
    let eslint_check = r#"
import assert from "node:assert/strict";
import Module from "node:module";
import path from "node:path";

const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === "@typescript-eslint/eslint-plugin") {
    return {
      configs: {
        "flat/recommended-type-checked": [
          { name: "typescript-eslint/base" },
          {
            name: "typescript-eslint/eslint-recommended",
            rules: {
              "no-var": "error",
              "prefer-const": "error",
            },
          },
          {
            name: "typescript-eslint/recommended-type-checked",
            rules: {
              "@typescript-eslint/no-explicit-any": "error",
            },
          },
        ],
      },
    };
  }
  if (request === "eslint-config-prettier/flat") {
    return { name: "eslint-config-prettier", rules: {} };
  }
  if (request === "@eslint/js") {
    return {
      configs: {
        recommended: {
          rules: {
            "no-debugger": "error",
            "no-unreachable": "error",
            "no-constant-condition": "error",
          },
        },
      },
    };
  }
  return origLoad.call(this, request, parent, isMain);
};

const requireCjs = Module.createRequire(path.resolve("./test-suite/eslint.config.cjs"));
const flatConfig = requireCjs("./eslint.config.cjs");
assert.ok(Array.isArray(flatConfig), "eslint.config.cjs must export a flat-config array");

const mergedRules = Object.assign(
  {},
  ...flatConfig.filter((entry) => entry && entry.rules).map((entry) => entry.rules)
);
for (const coreRule of ["no-debugger", "no-unreachable", "no-constant-condition"]) {
  assert.equal(
    mergedRules[coreRule],
    "error",
    `eslint.config.cjs dropped eslint:recommended rule "${coreRule}" during flat-config migration`
  );
}
"#;
    run_node_ui_test(eslint_check);
}

#[test]
fn regression_list_collection_ids_pagination_and_guards() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const api = await import("./ui/api.js");

// 1. Paginates across nextPageToken while sending exact {{}} on the initial request.
{{
  const bodies = [];
  fetchImpl = async (url, init) => {{
    assert.equal(url, "/v1/projects/p/databases/(default)/documents:listCollectionIds");
    assert.equal(init.method, "POST");
    bodies.push(init.body);
    const req = JSON.parse(init.body);
    if (!req.pageToken) {{
      return jsonResponse({{ collectionIds: ["c1", "c2"], nextPageToken: "tok1" }});
    }}
    if (req.pageToken === "tok1") {{
      return jsonResponse({{ collectionIds: ["c3"], nextPageToken: "tok2" }});
    }}
    if (req.pageToken === "tok2") {{
      return jsonResponse({{ collectionIds: ["c4"] }});
    }}
    throw new Error("unexpected pageToken: " + req.pageToken);
  }};

  const cols = await api.listCollectionIds("projects/p/databases/(default)/documents");
  assert.deepEqual(cols, ["c1", "c2", "c3", "c4"]);
  assert.deepEqual(bodies, ["{{}}", '{{"pageToken":"tok1"}}', '{{"pageToken":"tok2"}}']);
}}

// 2. Duplicate nextPageToken stops cleanly instead of looping infinitely.
{{
  let calls = 0;
  fetchImpl = async (_url, init) => {{
    calls++;
    const req = JSON.parse(init.body);
    if (!req.pageToken) {{
      return jsonResponse({{ collectionIds: ["a"], nextPageToken: "dup" }});
    }}
    return jsonResponse({{ collectionIds: ["b"], nextPageToken: "dup" }});
  }};

  const cols = await api.listCollectionIds("projects/p/databases/(default)/documents");
  assert.deepEqual(cols, ["a", "b"]);
  assert.equal(calls, 2);
}}

// 3. Max page cap (1000 pages) guards against unbounded unique token chains.
{{
  let calls = 0;
  fetchImpl = async () => {{
    calls++;
    return jsonResponse({{
      collectionIds: ["col_" + calls],
      nextPageToken: "tok_" + calls,
    }});
  }};

  const cols = await api.listCollectionIds("projects/p/databases/(default)/documents");
  assert.equal(calls, 1000);
  assert.equal(cols.length, 1000);
  assert.equal(cols[0], "col_1");
  assert.equal(cols[999], "col_1000");
}}

console.log("regression_list_collection_ids_pagination_and_guards passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_sse_unpause_reconnect_resync_and_cleared_db_non_recreation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
let lastEs = null;
globalThis.EventSource = class {{
  constructor(url) {{
    this.url = url;
    this.listeners = new Map();
    lastEs = this;
  }}
  addEventListener(type, fn) {{
    const list = this.listeners.get(type) ?? [];
    list.push(fn);
    this.listeners.set(type, list);
  }}
  emit(type, data) {{
    for (const fn of this.listeners.get(type) ?? []) {{
      fn(data);
    }}
  }}
  close() {{}}
}};

const app = await import("./ui/app.js");

const db1Root = "projects/p/databases/db1/documents";
const db2Root = "projects/p/databases/db2/documents";
let databases = [db1Root];
let db1Docs = ["alice"];
const getDbCalls = [];
let db1ListCalls = 0;

fetchImpl = async (url) => {{
  if (url === "/emulator/v1") {{
    return jsonResponse(databases);
  }}
  if (url === "/v1/projects/p/databases/db1") {{
    getDbCalls.push("db1");
    return jsonResponse({{
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/db2") {{
    getDbCalls.push("db2");
    return jsonResponse({{
      name: "projects/p/databases/db2",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "ENTERPRISE",
      concurrencyMode: "OPTIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: ["orders"] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/db1/documents/users?")) {{
    db1ListCalls++;
    return jsonResponse({{
      documents: db1Docs.map((id) => ({{
        name: `${{db1Root}}/users/${{id}}`,
        createTime: "2026-01-01T00:00:00Z",
        updateTime: "2026-01-01T00:00:00Z",
      }})),
    }});
  }}
  throw new Error("unexpected url: " + url);
}};

globalThis.window.location.pathname = "/projects/p/databases/db1/documents/users";
await app.init();
assert.ok(lastEs, "EventSource must be connected on init");
assert.equal(getDbCalls.length, 1);
assert.equal(db1ListCalls, 1);

// 1. Initial "open" during/after init() must NOT trigger an extra resync.
lastEs.emit("open");
await app.flushSseQueue();
assert.equal(getDbCalls.length, 1, "initial open must not trigger resync");
assert.equal(db1ListCalls, 1, "initial open must not re-list collection");

// 2. Transition "error" -> "live" must enqueue a full active-DB resync via sseQueue.
db1Docs = ["alice", "bob"];
lastEs.emit("error");
assert.equal(app.state.sseStatus, "error");
lastEs.emit("open");
assert.equal(app.state.sseStatus, "live");
await app.flushSseQueue();

assert.equal(getDbCalls.length, 2, "reconnect error -> live must refresh metadata");
assert.equal(db1ListCalls, 2, "reconnect error -> live must refresh expanded column");
assert.deepEqual(
  app.state.collectionColumns.get(`${{db1Root}}/users`).documents.map((d) => d.id),
  ["alice", "bob"]
);

// 3. Pausing must NOT trigger resync, and reconnect while paused must be ignored.
const pauseBtn = findClickHandler(app.renderTopbar(), "Live");
assert.ok(pauseBtn);
await pauseBtn();
assert.equal(app.state.ssePaused, true);

db1Docs = ["alice", "bob", "carol"];
lastEs.emit("error");
lastEs.emit("open");
await app.flushSseQueue();
assert.equal(db1ListCalls, 2, "paused state must suppress resync on reconnect");

// 4. Unpausing (true -> false) triggers full resync via sseQueue when DB still exists.
const resumeBtn = findClickHandler(app.renderTopbar(), "Live Paused");
assert.ok(resumeBtn);
await resumeBtn();
assert.equal(app.state.ssePaused, false);
assert.deepEqual(
  app.state.collectionColumns.get(`${{db1Root}}/users`).documents.map((d) => d.id),
  ["alice", "bob", "carol"]
);

// 5. Cleared-DB non-recreation on fullResyncActiveDb:
//    (a) When active DB disappeared and another DB exists, clear active state and auto-select remaining DB without getDatabase(db1).
const getDbCountBeforeSwitch = getDbCalls.length;
databases = [db2Root];
await app.handleSseBatch([{{ type: "resync" }}]);
assert.equal(
  getDbCalls.slice(getDbCountBeforeSwitch).includes("db1"),
  false,
  "must never call getDatabase(db1) when db1 is absent after resync"
);
assert.equal(app.state.activeDatabaseId, "db2");
assert.deepEqual(app.state.rootCollections, ["orders"]);

//    (b) When active DB disappeared on unpause and NO DBs remain, clear active state without recreating db2.
const pauseBtn2 = findClickHandler(app.renderTopbar(), "Live");
await pauseBtn2();
databases = [];
const getDbCountBeforeClear = getDbCalls.length;
const resumeBtn2 = findClickHandler(app.renderTopbar(), "Live Paused");
await resumeBtn2();
assert.equal(
  getDbCalls.length,
  getDbCountBeforeClear,
  "must never call getDatabase(db2) when db2 is absent on unpause resync"
);
assert.equal(app.state.activeProjectId, null);
assert.equal(app.state.activeDatabaseId, null);
assert.equal(globalThis.window.location.pathname, "/");

console.log("regression_sse_unpause_reconnect_resync_and_cleared_db_non_recreation passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_load_collection_column_preserves_pagination_on_refresh() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const colPath = "projects/p/databases/(default)/documents/items";
const parentPath = "projects/p/databases/(default)/documents";
const allDocs = Array.from({{ length: 450 }}, (_, i) => ({{
  name: `${{colPath}}/doc-${{String(i).padStart(3, "0")}}`,
  createTime: "2026-01-01T00:00:00Z",
  updateTime: "2026-01-01T00:00:00Z",
}}));

let clampLimit = 0;
const requestedPages = [];

fetchImpl = async (url) => {{
  const parsed = new URL(url, "http://localhost");
  const pageSize = Number(parsed.searchParams.get("pageSize") ?? "0");
  const pageToken = parsed.searchParams.get("pageToken") ?? "";
  requestedPages.push({{ pageSize, pageToken }});

  let start = 0;
  if (pageToken) {{
    const idx = allDocs.findIndex((d) => d.name.endsWith("/" + pageToken));
    start = idx >= 0 ? idx + 1 : allDocs.length;
  }}
  const effectiveSize = clampLimit > 0 ? Math.min(pageSize, clampLimit) : pageSize;
  const slice = allDocs.slice(start, start + effectiveSize);
  const hasMore = start + slice.length < allDocs.length;
  const nextPageToken = hasMore && slice.length > 0
    ? slice[slice.length - 1].name.split("/").pop()
    : "";
  return jsonResponse({{ documents: slice, nextPageToken }});
}};

// 1. Initial load fetches 200 docs, then Load more appends 200 more (total 400).
await app.loadCollectionColumn(colPath, parentPath, "items", false);
let colState = app.state.collectionColumns.get(colPath);
assert.equal(colState.documents.length, 200);
assert.equal(colState.nextPageToken, "doc-199");

await app.loadCollectionColumn(colPath, parentPath, "items", true);
assert.equal(colState.documents.length, 400);
assert.equal(colState.nextPageToken, "doc-399");

// 2. Non-append SSE refresh with unclamped server preserves all 400 documents in 1 request.
requestedPages.length = 0;
await app.loadCollectionColumn(colPath, parentPath, "items", false);
assert.deepEqual(requestedPages, [{{ pageSize: 400, pageToken: "" }}]);
assert.equal(colState.documents.length, 400);
assert.equal(colState.documents[399].id, "doc-399");
assert.equal(colState.nextPageToken, "doc-399");

// 3. Non-append SSE refresh when server clamps responses to 150 docs follows nextPageToken up to 400.
clampLimit = 150;
requestedPages.length = 0;
await app.loadCollectionColumn(colPath, parentPath, "items", false);
assert.deepEqual(requestedPages, [
  {{ pageSize: 400, pageToken: "" }},
  {{ pageSize: 250, pageToken: "doc-149" }},
  {{ pageSize: 100, pageToken: "doc-299" }},
]);
assert.equal(colState.documents.length, 400);
assert.equal(colState.documents[0].id, "doc-000");
assert.equal(colState.documents[399].id, "doc-399");
assert.equal(colState.nextPageToken, "doc-399");

console.log("regression_load_collection_column_preserves_pagination_on_refresh passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_uint8_to_base64_chunked_large_arrays() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const {{ uint8ToBase64, encodeBytes }} = await import("./ui/values.js");

for (const size of [0, 1, 0x7fff, 0x8000, 0x8001, 0x10000, 100000]) {{
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) {{
    bytes[i] = (i * 31 + 7) & 0xff;
  }}
  const expected = Buffer.from(bytes).toString("base64");
  assert.equal(uint8ToBase64(bytes), expected, "mismatch for size " + size);
}}

const largeText = "a".repeat(70000);
const encoded = encodeBytes(largeText, "utf8");
assert.equal(encoded.valid, true);
assert.equal(encoded.base64, Buffer.from(largeText, "utf8").toString("base64"));

console.log("regression_uint8_to_base64_chunked_large_arrays passed");
"#
    );

    run_node_ui_test(&script);
}
