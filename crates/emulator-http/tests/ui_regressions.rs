#![allow(unused_crate_dependencies)]

use std::{
    io::Write,
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
    let css = std::fs::read_to_string(repo_root.join("ui/styles.css"))
        .expect("failed to read ui/styles.css");

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
