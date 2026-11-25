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
const docKeydownListeners = [];
const docVisibilityListeners = [];
let mockActiveElement = null;
let mockVisibilityState = "visible";
let fetchImpl = async () => {
  throw new Error("unexpected fetch call");
};

globalThis.document = {
  createTreeWalker: () => ({ currentNode: null, nextNode: () => null }),
  getElementById: () => null,
  get visibilityState() {
    return mockVisibilityState;
  },
  get activeElement() {
    return mockActiveElement;
  },
  set activeElement(el) {
    mockActiveElement = el;
  },
  addEventListener(type, fn) {
    if (type === "keydown") docKeydownListeners.push(fn);
    if (type === "visibilitychange") docVisibilityListeners.push(fn);
  },
  removeEventListener(type, fn) {
    if (type === "keydown") {
      const idx = docKeydownListeners.indexOf(fn);
      if (idx >= 0) docKeydownListeners.splice(idx, 1);
    }
    if (type === "visibilitychange") {
      const idx = docVisibilityListeners.indexOf(fn);
      if (idx >= 0) docVisibilityListeners.splice(idx, 1);
    }
  },
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

function renderTemplateMarkup(node) {
  const LIT_NOTHING = Symbol.for("lit-nothing");
  if (node == null || node === LIT_NOTHING || typeof node === "symbol") return "";
  if (typeof node === "string" || typeof node === "number" || typeof node === "boolean") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(renderTemplateMarkup).join("");
  }
  if (typeof node === "object" && Array.isArray(node.strings) && Array.isArray(node.values)) {
    let out = "";
    for (let i = 0; i < node.strings.length; i++) {
      let chunk = node.strings[i];
      if (i < node.values.length) {
        const val = node.values[i];
        const boolMatch = /\s+\?([A-Za-z0-9_:-]+)=$/.exec(chunk);
        if (boolMatch) {
          chunk = chunk.slice(0, boolMatch.index) + (val && val !== LIT_NOTHING ? ` ${boolMatch[1]}` : "");
          out += chunk;
          continue;
        }
        if (val === LIT_NOTHING) {
          const attrMatch = /\s+[A-Za-z0-9_:-]+=$/.exec(chunk);
          if (attrMatch) {
            chunk = chunk.slice(0, attrMatch.index);
          }
          out += chunk;
          continue;
        }
        out += chunk;
        if (typeof val === "function") {
          out += "[fn]";
        } else if (typeof val === "string" && /=\s*$/.test(chunk)) {
          out += JSON.stringify(val);
        } else {
          out += renderTemplateMarkup(val);
        }
      } else {
        out += chunk;
      }
    }
    return out;
  }
  return "";
}

function findElementEventHandlers(node, classSubstr) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const hit = findElementEventHandlers(item, classSubstr);
      if (hit) return hit;
    }
    return null;
  }
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {
    for (let i = 0; i < node.strings.length; i++) {
      if (node.strings[i].includes(classSubstr)) {
        const handlers = {};
        for (let k = i; k < node.values.length; k++) {
          const s = node.strings[k];
          if (k > i && s.includes(">")) break;
          const m = /@([a-z]+)=\s*$/.exec(s);
          if (m && typeof node.values[k] === "function") {
            handlers[m[1]] = node.values[k];
          }
        }
        if (Object.keys(handlers).length > 0) return handlers;
      }
    }
    for (const v of node.values) {
      const hit = findElementEventHandlers(v, classSubstr);
      if (hit) return hit;
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
const app = await import("./ui/app.js");

const warns = [];
const origWarn = console.warn;
console.warn = (...args) => warns.push(args.map(String).join(" "));

try {{
// 1. Paginates across nextPageToken while sending exact {{}} on the initial request without warning.
{{
  warns.length = 0;
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
  assert.equal(warns.length, 0, "normal pagination must not emit warnings");
}}

// 2. Duplicate nextPageToken stops cleanly and emits console.warn instead of looping infinitely.
{{
  warns.length = 0;
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
  assert.equal(warns.length, 1, "duplicate nextPageToken must emit console.warn");
  assert.ok(warns[0].includes("duplicate nextPageToken"), "unexpected warn: " + warns[0]);
}}

// 3. Max page cap (1000 pages) guards against unbounded unique token chains and warns iff nextPageToken remains.
{{
  warns.length = 0;
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
  assert.equal(warns.length, 1, "hitting page cap with remaining nextPageToken must emit console.warn");
  assert.ok(warns[0].includes("1000"), "unexpected warn: " + warns[0]);

  // Exactly 1000 pages finishing with empty nextPageToken must NOT warn.
  warns.length = 0;
  calls = 0;
  fetchImpl = async () => {{
    calls++;
    return jsonResponse({{
      collectionIds: ["ok_" + calls],
      nextPageToken: calls < 1000 ? "ok_tok_" + calls : "",
    }});
  }};
  const exactCols = await api.listCollectionIds("projects/p/databases/(default)/documents");
  assert.equal(calls, 1000);
  assert.equal(exactCols.length, 1000);
  assert.equal(warns.length, 0, "completing exactly on page 1000 without nextPageToken must not warn");
}}

// 4. shouldAbort and AbortSignal halt pagination immediately between pages without spurious warnings.
{{
  warns.length = 0;
  let calls = 0;
  let abortNow = false;
  fetchImpl = async () => {{
    calls++;
    abortNow = true;
    return jsonResponse({{ collectionIds: ["p1"], nextPageToken: "tok_next" }});
  }};
  const cols = await api.listCollectionIds("projects/p/databases/(default)/documents", {{
    shouldAbort: () => abortNow,
  }});
  assert.equal(calls, 1, "shouldAbort must prevent fetching subsequent pages");
  assert.deepEqual(cols, []);
  assert.equal(warns.length, 0);

  const ac = new AbortController();
  let signalCalls = 0;
  let seenSignal = null;
  fetchImpl = async (_url, init) => {{
    signalCalls++;
    seenSignal = init.signal;
    ac.abort();
    return jsonResponse({{ collectionIds: ["s1"], nextPageToken: "tok_s2" }});
  }};
  const sigCols = await api.listCollectionIds("projects/p/databases/(default)/documents", {{
    signal: ac.signal,
  }});
  assert.equal(signalCalls, 1, "aborted signal must halt after first page");
  assert.equal(seenSignal, ac.signal, "listCollectionIds must forward signal to fetch");
  assert.deepEqual(sigCols, []);

  // Pre-aborted signal skips even the first fetch.
  let preAbortedCalls = 0;
  fetchImpl = async () => {{
    preAbortedCalls++;
    return jsonResponse({{ collectionIds: ["x"] }});
  }};
  const preCols = await api.listCollectionIds("projects/p/databases/(default)/documents", {{
    signal: ac.signal,
  }});
  assert.equal(preAbortedCalls, 0);
  assert.deepEqual(preCols, []);
}}

// 5. selectDatabase, loadDocumentLevel, and handleSseBatch abort stale paginated listCollectionIds on DB/doc switch.
{{
  // 5a. selectDatabase DB switch aborts multi-page root listCollectionIds before page 2.
  const db1Page1Def = deferred();
  let db1ColPages = 0;
  fetchImpl = async (url, init) => {{
    if (url === "/v1/projects/p1/databases/db1") {{
      return jsonResponse({{
        name: "projects/p1/databases/db1",
        uid: "u1",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p1/databases/db1/documents:listCollectionIds") {{
      db1ColPages++;
      const body = JSON.parse(init.body ?? "{{}}");
      if (!body.pageToken) return db1Page1Def.promise;
      throw new Error("stale db1 page 2 must never be requested");
    }}
    if (url === "/v1/projects/p2/databases/db2") {{
      return jsonResponse({{
        name: "projects/p2/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: ["fresh_col"] }});
    }}
    if (url === "/emulator/v1") {{
      return jsonResponse(["projects/p1/databases/db1/documents", "projects/p2/databases/db2/documents"]);
    }}
    throw new Error("unexpected url in 5a: " + url);
  }};

  const pSelect1 = app.selectDatabase("p1", "db1", [], true);
  await new Promise((r) => setTimeout(r, 0));
  const pSelect2 = app.selectDatabase("p2", "db2", [], true);
  db1Page1Def.resolve(jsonResponse({{ collectionIds: ["c1"], nextPageToken: "page2" }}));
  await Promise.all([pSelect1, pSelect2]);
  assert.equal(db1ColPages, 1);
  assert.deepEqual(app.state.rootCollections, ["fresh_col"]);

  // 5b. loadDocumentLevel doc switch (navEpoch bump) aborts paginated subcollection load before page 2.
  const docAPath = "projects/p2/databases/db2/documents/fresh_col/docA";
  const docBPath = "projects/p2/databases/db2/documents/fresh_col/docB";
  const docAPage1Def = deferred();
  let docAColPages = 0;
  fetchImpl = async (url, init) => {{
    if (url === `/v1/${{docAPath}}` || url === `/v1/${{docBPath}}`) {{
      return jsonResponse({{ name: url.slice(4), fields: {{}} }});
    }}
    if (url === `/v1/${{docAPath}}:listCollectionIds`) {{
      docAColPages++;
      const body = JSON.parse(init.body ?? "{{}}");
      if (!body.pageToken) return docAPage1Def.promise;
      throw new Error("stale docA subcollection page 2 must never be requested");
    }}
    if (url === `/v1/${{docBPath}}:listCollectionIds`) {{
      return jsonResponse({{ collectionIds: ["subB"] }});
    }}
    if (url.startsWith("/v1/projects/p2/databases/db2/documents/fresh_col?")) {{
      return jsonResponse({{ documents: [] }});
    }}
    throw new Error("unexpected url in 5b: " + url);
  }};

  app.state.segments = ["fresh_col", "docA"];
  const pDocA = app.loadDocumentLevel(docAPath, "docA", true);
  await new Promise((r) => setTimeout(r, 0));
  // Simulate switching to docB via Explorer segment selection (bumps navEpoch).
  const {{ bumpNavEpoch }} = await import("./ui/state.js");
  app.state.segments = ["fresh_col", "docB"];
  bumpNavEpoch();
  const pDocB = app.loadDocumentLevel(docBPath, "docB", true);
  docAPage1Def.resolve(jsonResponse({{ collectionIds: ["subA1"], nextPageToken: "sub_page2" }}));
  await Promise.all([pDocA, pDocB]);
  assert.equal(docAColPages, 1);
  assert.equal(app.state.documentLevels.get(docAPath).loading, false);
  assert.deepEqual(app.state.documentLevels.get(docAPath).subcollections, []);
  assert.deepEqual(app.state.documentLevels.get(docBPath).subcollections, ["subB"]);

  // 5c. handleSseBatch aborts paginated root listCollectionIds when active DB switches mid-pagination.
  const ssePage1Def = deferred();
  let sseDb2ColPages = 0;
  fetchImpl = async (url, init) => {{
    if (url === "/emulator/v1") {{
      return jsonResponse(["projects/p2/databases/db2/documents", "projects/p3/databases/db3/documents"]);
    }}
    if (url === "/v1/projects/p2/databases/db2") {{
      return jsonResponse({{
        name: "projects/p2/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p2/databases/db2/documents:listCollectionIds") {{
      sseDb2ColPages++;
      const body = JSON.parse(init.body ?? "{{}}");
      if (!body.pageToken) return ssePage1Def.promise;
      throw new Error("stale SSE db2 page 2 must never be requested");
    }}
    if (url === "/v1/projects/p3/databases/db3") {{
      return jsonResponse({{
        name: "projects/p3/databases/db3",
        uid: "u3",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p3/databases/db3/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: ["db3_col"] }});
    }}
    throw new Error("unexpected url in 5c: " + url);
  }};

  const pSse = app.handleSseBatch([{{ type: "resync" }}]);
  await new Promise((r) => setTimeout(r, 0));
  const pSelect3 = app.selectDatabase("p3", "db3", [], true);
  ssePage1Def.resolve(jsonResponse({{ collectionIds: ["stale_p2"], nextPageToken: "tok_sse_2" }}));
  await Promise.all([pSse, pSelect3]);
  assert.equal(sseDb2ColPages, 1);
  assert.deepEqual(app.state.rootCollections, ["db3_col"]);
}}
}} finally {{
  console.warn = origWarn;
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
fn regression_sse_resync_error_and_inflight_select_database_guard() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const db1Root = "projects/p/databases/db1/documents";
const dbNewRoot = "projects/p/databases/db_new/documents";
let failDbList = false;

fetchImpl = async (url) => {{
  if (url === "/emulator/v1") {{
    if (failDbList) {{
      return jsonResponse({{ error: {{ code: 503, status: "UNAVAILABLE", message: "db list down" }} }}, 503);
    }}
    return jsonResponse([db1Root]);
  }}
  if (url === "/v1/projects/p/databases/db1") {{
    if (failDbList) {{
      throw new Error("must not call getDatabase when refreshDatabaseList failed");
    }}
    return jsonResponse({{
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") {{
    if (failDbList) {{
      throw new Error("must not call listCollectionIds when refreshDatabaseList failed");
    }}
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  throw new Error("unexpected url: " + url);
}};

await app.selectDatabase("p", "db1", [], true);
assert.equal(app.state.activeDatabaseId, "db1");
assert.ok(app.state.activeDbMeta);
assert.deepEqual(app.state.rootCollections, ["users"]);

// 1. Transient /emulator/v1 error during resync notifies error toast and preserves active DB state.
failDbList = true;
app.state.toasts = [];
await app.handleSseBatch([{{ type: "resync" }}]);
assert.equal(app.state.activeProjectId, "p");
assert.equal(app.state.activeDatabaseId, "db1");
assert.ok(app.state.activeDbMeta, "activeDbMeta must not be cleared on refreshDatabaseList error");
assert.deepEqual(app.state.rootCollections, ["users"]);
assert.equal(app.state.databases.length, 1);
assert.ok(
  app.state.toasts.some((t) => t.tone === "error" && t.text.includes("db list down")),
  "expected error toast on refreshDatabaseList failure"
);

// Also verify enqueueSseBatch (via toggleSsePaused unpause) surfaces the error toast cleanly without clobbering state.
app.state.toasts = [];
app.state.ssePaused = true;
await app.toggleSsePaused();
assert.equal(app.state.activeDatabaseId, "db1");
assert.ok(
  app.state.toasts.some((t) => t.tone === "error" && t.text.includes("db list down")),
  "expected error toast when unpause resync fails"
);

// 2. In-flight selectDatabase for a newly created DB must NOT be mistaken for a cleared DB on SSE resync.
failDbList = false;
const dbNewMetaDef = deferred();
const dbNewColsDef = deferred();
let dbNewGetCount = 0;
let includeDbNewInList = false;

fetchImpl = async (url) => {{
  if (url === "/emulator/v1") {{
    return jsonResponse(includeDbNewInList ? [db1Root, dbNewRoot] : [db1Root]);
  }}
  if (url === "/v1/projects/p/databases/db_new") {{
    dbNewGetCount++;
    return dbNewMetaDef.promise;
  }}
  if (url === "/v1/projects/p/databases/db_new/documents:listCollectionIds") {{
    return dbNewColsDef.promise;
  }}
  throw new Error("unexpected url in inflight check: " + url);
}};

app.state.toasts = [];
const selectPromise = app.selectDatabase("p", "db_new", [], true);
assert.equal(app.state.rootLoading, true);
assert.equal(app.state.activeDbMeta, null);

await app.handleSseBatch([{{ type: "resync" }}]);
assert.equal(app.state.activeProjectId, "p");
assert.equal(app.state.activeDatabaseId, "db_new");
assert.equal(globalThis.window.location.pathname, "/projects/p/databases/db_new/documents");
assert.equal(dbNewGetCount, 1, "handleSseBatch must not duplicate getDatabase while rootLoading is true");
assert.equal(
  app.state.toasts.some((t) => t.text.includes("was cleared")),
  false,
  "in-flight selectDatabase must never emit cleared-DB toast"
);

includeDbNewInList = true;
dbNewMetaDef.resolve(jsonResponse({{
  name: "projects/p/databases/db_new",
  uid: "u_new",
  type: "FIRESTORE_NATIVE",
  databaseEdition: "ENTERPRISE",
  concurrencyMode: "OPTIMISTIC",
}}));
dbNewColsDef.resolve(jsonResponse({{ collectionIds: ["items"] }}));
await selectPromise;

assert.equal(app.state.rootLoading, false);
assert.equal(app.state.activeDatabaseId, "db_new");
assert.equal(app.state.activeDbMeta.name, "projects/p/databases/db_new");
assert.deepEqual(app.state.rootCollections, ["items"]);

// 3. Even when rootLoading is false, null activeDbMeta (e.g. failed initial DB load) is not treated as cleared.
app.state.activeDbMeta = null;
app.state.toasts = [];
includeDbNewInList = false;
await app.handleSseBatch([{{ type: "resync" }}]);
assert.equal(app.state.activeDatabaseId, "db_new");
assert.equal(app.state.toasts.some((t) => t.text.includes("was cleared")), false);

console.log("regression_sse_resync_error_and_inflight_select_database_guard passed");
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

fn extract_css_rule_block<'a>(css: &'a str, selector: &str) -> &'a str {
    let needle = format!("\n{selector} {{");
    let start = if css.starts_with(&format!("{selector} {{")) {
        0
    } else {
        css.find(&needle)
            .map(|idx| idx + 1)
            .unwrap_or_else(|| panic!("missing CSS rule block for `{selector}`"))
    };
    let rest = &css[start..];
    let end = rest
        .find('}')
        .unwrap_or_else(|| panic!("unterminated CSS rule block for `{selector}`"));
    &rest[..=end]
}

#[test]
fn regression_a11y_miller_columns_forms_and_icon_labels() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");

    // Preserve bug_web_ui_8 CSS invariants and verify button reset + ellipsis + :focus-visible
    // rules against the exact selector blocks.
    let miller_col_rule = extract_css_rule_block(&css, ".miller-col");
    assert!(miller_col_rule.contains("overflow: hidden;"));
    let col_header_span_rule = extract_css_rule_block(&css, ".col-header > span");
    assert!(col_header_span_rule.contains("min-width: 0;"));
    let col_item_rule = extract_css_rule_block(&css, ".col-item");
    assert!(col_item_rule.contains("border: none;"));
    assert!(col_item_rule.contains("background: transparent;"));
    assert!(col_item_rule.contains("width: 100%;"));
    assert!(col_item_rule.contains("min-width: 0;"));
    assert!(col_item_rule.contains("text-align: left;"));
    let item_label_rule = extract_css_rule_block(&css, ".item-label");
    assert!(item_label_rule.contains("min-width: 0;"));
    assert!(item_label_rule.contains("overflow: hidden;"));
    assert!(item_label_rule.contains("text-overflow: ellipsis;"));
    assert!(css.contains(".col-item:focus-visible"));

    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const {{ renderExplorer }} = await import("./ui/components/explorer.js");
const {{ createEditorSession, renderHybridEditor }} = await import("./ui/components/editor.js");
const {{ createQueryRunnerState, renderQueryRunner }} = await import("./ui/components/query.js");

const rootPath = "projects/p/databases/(default)/documents";
const usersColPath = `${{rootPath}}/users`;
const aliceDocPath = `${{usersColPath}}/alice`;
const postsColPath = `${{aliceDocPath}}/posts`;
const post1DocPath = `${{postsColPath}}/p1`;

const collectionColumns = new Map([
  [usersColPath, {{
    collectionPath: usersColPath,
    collectionId: "users",
    parentPath: rootPath,
    documents: [
      {{ id: "alice", fullName: aliceDocPath, phantom: false }},
      {{ id: "ghost", fullName: `${{usersColPath}}/ghost`, phantom: true }},
    ],
    nextPageToken: "",
    filterText: "",
    loading: false,
  }}],
  [postsColPath, {{
    collectionPath: postsColPath,
    collectionId: "posts",
    parentPath: aliceDocPath,
    documents: [
      {{ id: "p1", fullName: post1DocPath, phantom: false }},
    ],
    nextPageToken: "",
    filterText: "",
    loading: false,
  }}],
]);

const session = createEditorSession(post1DocPath, "p", "(default)", {{
  name: post1DocPath,
  fields: {{ title: {{ stringValue: "Hello" }} }},
}});

const documentLevels = new Map([
  [aliceDocPath, {{
    docPath: aliceDocPath,
    docId: "alice",
    exists: true,
    loading: false,
    subcollections: ["posts", "tags"],
    editorSession: null,
  }}],
  [post1DocPath, {{
    docPath: post1DocPath,
    docId: "p1",
    exists: true,
    loading: false,
    subcollections: [],
    editorSession: session,
  }}],
]);

// 1. Explorer uses semantic <button type="button" class="col-item ..."> inside <li> across all 3 columns,
//    sets aria-current="true" ONLY on selected items, and sets descriptive aria-labels on icon-only controls.
let selectedPath = null;
const explorerTpl = renderExplorer({{
  projectId: "p",
  databaseId: "(default)",
  rootCollections: ["users", "orders"],
  rootLoading: false,
  segments: ["users", "alice", "posts", "p1"],
  collectionColumns,
  documentLevels,
  onSelectSegments: (s) => {{ selectedPath = s; }},
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
const explorerMarkup = renderTemplateMarkup(explorerTpl);

assert.equal(explorerMarkup.includes('<li class="col-item'), false, "<li> must wrap <button class=col-item>");
assert.ok(explorerMarkup.includes('<nav aria-label="Breadcrumb" class="breadcrumbs">'));
assert.ok(explorerMarkup.includes('aria-current="page"'));
assert.ok(explorerMarkup.includes('aria-label="Delete collection users"'));
assert.ok(explorerMarkup.includes('aria-label="Delete collection posts"'));

// Count aria-current="true" occurrences on col-item buttons: selected root "users", selected doc "alice",
// selected ancestor subcol "posts", selected leaf doc "p1" => 4 selected col-item buttons!
const currentTrueMatches = explorerMarkup.match(/aria-current="true"/g) ?? [];
assert.equal(currentTrueMatches.length, 4, "expected aria-current=true on all 4 selected Miller items");

// findClickHandler compatibility is preserved with <button type="button" class="col-item">.
const clickOrders = findClickHandler(explorerTpl, "orders");
assert.ok(clickOrders, "findClickHandler must still match <button class=col-item>");
clickOrders();
assert.deepEqual(selectedPath, ["orders"]);

// 2. Editor icon-only remove field button has descriptive aria-label.
const editorMarkup = renderTemplateMarkup(renderHybridEditor({{
  session,
  onChange: () => {{}},
}}));
assert.ok(editorMarkup.includes('aria-label="Remove field title"'));

// 3. Query Runner has proper label for/id associations and icon-only aria-labels.
const qState = createQueryRunnerState("users");
qState.filters.push({{
  id: 1,
  fieldPath: "age",
  op: "EQUAL",
  valueType: "integerValue",
  valueText: "21",
  latText: "0",
  lngText: "0",
}});
qState.orders.push({{ id: 2, fieldPath: "age", direction: "ASCENDING" }});
const queryMarkup = renderTemplateMarkup(renderQueryRunner({{
  projectId: "p",
  databaseId: "(default)",
  knownCollections: ["users"],
  state: qState,
  onRerender: () => {{}},
  onExecuteQuery: () => {{}},
  onOpenResultDocument: () => {{}},
}}));
assert.ok(queryMarkup.includes('for="query-parent-subpath"') && queryMarkup.includes('id="query-parent-subpath"'));
assert.ok(queryMarkup.includes('for="query-collection-id"') && queryMarkup.includes('id="query-collection-id"'));
assert.ok(queryMarkup.includes('for="query-all-descendants"') && queryMarkup.includes('id="query-all-descendants"'));
assert.ok(queryMarkup.includes('for="query-limit"') && queryMarkup.includes('id="query-limit"'));
assert.ok(queryMarkup.includes('aria-label="Remove filter 1"'));
assert.ok(queryMarkup.includes('aria-label="Remove sort order 1"'));

// 4. Topbar database selector & toast dismiss button have descriptive aria-labels, and .sse-badge omits redundant aria-pressed.
app.state.databases = [{{ projectId: "p", databaseId: "(default)", rootPath }}];
app.state.toasts = [{{ id: 1, text: "Saved", tone: "success" }}];
const topbarMarkup = renderTemplateMarkup(app.renderTopbar());
const toastsMarkup = renderTemplateMarkup(app.renderToasts());
assert.ok(topbarMarkup.includes('aria-label="Select database"'));
assert.equal(topbarMarkup.includes("aria-pressed"), false, ".sse-badge dynamic label must not pair aria-pressed");
assert.ok(toastsMarkup.includes('aria-label="Dismiss notification"'));

console.log("regression_a11y_miller_columns_forms_and_icon_labels passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_a11y_modals_aria_labels_escape_focus_and_backdrop() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const {{ createEditorSession }} = await import("./ui/components/editor.js");
const {{ getModalFocusableElements, handleModalKeydown }} = await import("./ui/components/modals.js");

// 1. All 5 modal kinds expose role="dialog", aria-modal="true", aria-labelledby matching title id,
//    aria-label="Close dialog", and unique for/id label associations on every control.
const modalFixtures = [
  {{
    kind: "open_db",
    modal: {{ kind: "open_db", projectId: "p", databaseId: "(default)", edition: "STANDARD", busy: false, error: null }},
    titleId: "modal-title-open-db",
    expectedForIds: ["modal-open-db-project", "modal-open-db-database", "modal-open-db-edition"],
  }},
  {{
    kind: "create_doc",
    modal: {{
      kind: "create_doc",
      parentPath: "projects/p/databases/(default)/documents",
      collectionId: "",
      lockCollection: false,
      docId: "d1",
      lockDocId: false,
      session: createEditorSession("", "p", "(default)", null),
      busy: false,
      error: null,
    }},
    titleId: "modal-title-create-doc",
    expectedForIds: ["modal-create-doc-collection", "modal-create-doc-id"],
  }},
  {{
    kind: "delete_doc",
    modal: {{
      kind: "delete_doc",
      docPath: "projects/p/databases/(default)/documents/users/alice",
      recursive: false,
      hasSubcollections: true,
      busy: false,
      error: null,
    }},
    titleId: "modal-title-delete-doc",
    expectedForIds: ["modal-delete-doc-recursive"],
  }},
  {{
    kind: "delete_col",
    modal: {{
      kind: "delete_col",
      colPath: "projects/p/databases/(default)/documents/users",
      colId: "users",
      busy: false,
      error: null,
    }},
    titleId: "modal-title-delete-col",
    expectedForIds: [],
  }},
  {{
    kind: "clear_db",
    modal: {{
      kind: "clear_db",
      projectId: "p",
      databaseId: "(default)",
      busy: false,
      error: null,
    }},
    titleId: "modal-title-clear-db",
    expectedForIds: [],
  }},
];

for (const fx of modalFixtures) {{
  app.state.modal = fx.modal;
  const markup = renderTemplateMarkup(app.renderModal());
  assert.ok(markup.includes('role="dialog"'), `${{fx.kind}} missing role=dialog`);
  assert.ok(markup.includes('aria-modal="true"'), `${{fx.kind}} missing aria-modal=true`);
  assert.ok(markup.includes(`aria-labelledby="${{fx.titleId}}"`), `${{fx.kind}} missing aria-labelledby`);
  assert.ok(markup.includes(`<h2 id="${{fx.titleId}}"`), `${{fx.kind}} missing matching heading id`);
  assert.ok(markup.includes('aria-label="Close dialog"'), `${{fx.kind}} missing Close dialog aria-label`);
  assert.equal(markup.includes('aria-label="Close dialog" disabled'), false, `${{fx.kind}} idle Close dialog must not be disabled`);
  assert.equal(/<label(?![^>]*\bfor=)/.test(markup), false, `${{fx.kind}} has unassociated <label>`);
  for (const id of fx.expectedForIds) {{
    assert.ok(markup.includes(`for="${{id}}"`), `${{fx.kind}} missing for=${{id}}`);
    assert.ok(markup.includes(`id="${{id}}"`), `${{fx.kind}} missing id=${{id}}`);
  }}
  fx.modal.busy = true;
  const busyMarkup = renderTemplateMarkup(app.renderModal());
  assert.ok(busyMarkup.includes('aria-label="Close dialog" disabled'), `${{fx.kind}} busy Close dialog must be disabled`);
  fx.modal.busy = false;
}}

// 2. Safe backdrop dismissal: drag-release from inside modal onto backdrop MUST NOT dismiss.
{{
  app.state.modal = {{
    kind: "clear_db",
    projectId: "p",
    databaseId: "(default)",
    busy: false,
    error: null,
  }};
  const handlers = findElementEventHandlers(app.renderModal(), "modal-backdrop");
  assert.ok(handlers?.mousedown && handlers?.click, "expected mousedown & click on .modal-backdrop");

  const backdropEl = {{ kind: "backdrop" }};
  const innerCardEl = {{ kind: "card" }};

  // Drag from inside modal out to backdrop -> must remain open.
  handlers.mousedown({{ target: innerCardEl, currentTarget: backdropEl }});
  handlers.click({{ target: backdropEl, currentTarget: backdropEl }});
  assert.ok(app.state.modal !== null, "drag-release onto backdrop must not dismiss modal");

  // Press on backdrop and release inside card -> must remain open.
  handlers.mousedown({{ target: backdropEl, currentTarget: backdropEl }});
  handlers.click({{ target: innerCardEl, currentTarget: backdropEl }});
  assert.ok(app.state.modal !== null, "press on backdrop released inside card must not dismiss modal");

  // While busy, direct click on backdrop must NOT dismiss.
  app.state.modal.busy = true;
  handlers.mousedown({{ target: backdropEl, currentTarget: backdropEl }});
  handlers.click({{ target: backdropEl, currentTarget: backdropEl }});
  assert.ok(app.state.modal !== null, "busy modal must not dismiss on backdrop click");

  // Direct press and release on backdrop when not busy MUST dismiss.
  app.state.modal.busy = false;
  handlers.mousedown({{ target: backdropEl, currentTarget: backdropEl }});
  handlers.click({{ target: backdropEl, currentTarget: backdropEl }});
  assert.equal(app.state.modal, null, "direct backdrop click must close modal");
}}

// 3. Guarded Escape close (respecting !m.busy), keydown listener cleanup, and previousFocus restoration.
{{
  app.state.modal = null;
  const triggerBtn = {{
    isConnected: true,
    focusCalls: 0,
    focus() {{
      this.focusCalls++;
      mockActiveElement = this;
    }},
  }};
  mockActiveElement = triggerBtn;

  const openModalBtn = findClickHandler(app.renderTopbar(), "+ Open / Create Database");
  assert.ok(openModalBtn);
  openModalBtn();
  assert.equal(app.state.modal?.kind, "open_db");
  assert.equal(docKeydownListeners.length, 1, "opening modal registers document keydown listener");

  // Busy modal ignores Escape.
  app.state.modal.busy = true;
  let preventedBusy = false;
  docKeydownListeners[0]({{ key: "Escape", preventDefault() {{ preventedBusy = true; }} }});
  assert.ok(app.state.modal !== null, "Escape must not close busy modal");
  assert.equal(preventedBusy, false);

  // IME composition Escape (isComposing: true) must NOT dismiss an idle modal.
  app.state.modal.busy = false;
  let preventedComposing = false;
  docKeydownListeners[0]({{ key: "Escape", isComposing: true, preventDefault() {{ preventedComposing = true; }} }});
  assert.ok(app.state.modal !== null, "isComposing Escape must not close modal");
  assert.equal(preventedComposing, false);

  // Idle modal closes on Escape, detaches keydown listener, and restores previousFocus.
  app.state.modal.busy = false;
  let preventedIdle = false;
  docKeydownListeners[0]({{ key: "Escape", preventDefault() {{ preventedIdle = true; }} }});
  assert.equal(preventedIdle, true);
  assert.equal(app.state.modal, null);
  assert.equal(docKeydownListeners.length, 0, "closing modal removes document keydown listener");
  assert.equal(triggerBtn.focusCalls, 1, "closing modal restores focus to trigger button");
}}

// 4. Tab and Shift+Tab focus wrap within the dialog container.
{{
  const el1 = {{ focusCount: 0, hasAttribute: () => false, focus() {{ this.focusCount++; mockActiveElement = this; }} }};
  const el2 = {{ focusCount: 0, hasAttribute: () => false, focus() {{ this.focusCount++; mockActiveElement = this; }} }};
  const el3 = {{ focusCount: 0, hasAttribute: () => false, focus() {{ this.focusCount++; mockActiveElement = this; }} }};
  const outsideEl = {{ focus() {{ mockActiveElement = this; }} }};
  const focusables = [el1, el2, el3];
  const dialogEl = {{
    contains: (target) => focusables.includes(target),
    querySelectorAll: () => focusables,
    focus() {{ mockActiveElement = this; }},
  }};
  const containerEl = {{
    querySelector: (sel) => (sel === '[role="dialog"]' ? dialogEl : null),
  }};

  app.state.modal = {{ kind: "delete_col", colPath: "p/d/c", colId: "c", busy: false, error: null }};

  // Tab on last element wraps to first.
  mockActiveElement = el3;
  let tabLastPrevented = false;
  handleModalKeydown({{ key: "Tab", shiftKey: false, preventDefault() {{ tabLastPrevented = true; }} }}, app.state, containerEl, () => {{}});
  assert.equal(tabLastPrevented, true);
  assert.equal(mockActiveElement, el1);

  // Shift+Tab on first element wraps to last.
  let shiftTabFirstPrevented = false;
  handleModalKeydown({{ key: "Tab", shiftKey: true, preventDefault() {{ shiftTabFirstPrevented = true; }} }}, app.state, containerEl, () => {{}});
  assert.equal(shiftTabFirstPrevented, true);
  assert.equal(mockActiveElement, el3);

  // Tab on middle element lets native focus advance without preventDefault.
  mockActiveElement = el2;
  let tabMiddlePrevented = false;
  handleModalKeydown({{ key: "Tab", shiftKey: false, preventDefault() {{ tabMiddlePrevented = true; }} }}, app.state, containerEl, () => {{}});
  assert.equal(tabMiddlePrevented, false);

  // Tab when focus is outside dialog pulls focus to first element inside dialog.
  mockActiveElement = outsideEl;
  let tabOutsidePrevented = false;
  handleModalKeydown({{ key: "Tab", shiftKey: false, preventDefault() {{ tabOutsidePrevented = true; }} }}, app.state, containerEl, () => {{}});
  assert.equal(tabOutsidePrevented, true);
  assert.equal(mockActiveElement, el1);

  // Modified Tab shortcuts (Ctrl/Meta/Alt+Tab) are ignored by focus wrap.
  mockActiveElement = el3;
  for (const mod of ["ctrlKey", "metaKey", "altKey"]) {{
    let modPrevented = false;
    handleModalKeydown({{ key: "Tab", [mod]: true, preventDefault() {{ modPrevented = true; }} }}, app.state, containerEl, () => {{}});
    assert.equal(modPrevented, false, mod + "+Tab must not be intercepted");
  }}
  assert.equal(mockActiveElement, el3);

  // [hidden] elements and non-focusable elements are excluded by getModalFocusableElements.
  const hiddenEl = {{ hasAttribute: (attr) => attr === "hidden", focus() {{}} }};
  const noFocusEl = {{ hasAttribute: () => false }};
  const hiddenCheckDialog = {{
    querySelectorAll: () => [hiddenEl, el1, noFocusEl, el2],
  }};
  assert.deepEqual(
    getModalFocusableElements(hiddenCheckDialog),
    [el1, el2],
    "getModalFocusableElements must filter out [hidden] and non-focusable elements"
  );
  assert.deepEqual(getModalFocusableElements({{}}), []);

  // When focusables.length === 0 (e.g. only [hidden] elements), Tab prevents default and focuses dialogEl.
  let emptyDialogFocusCalls = 0;
  const emptyDialogEl = {{
    querySelectorAll: () => [hiddenEl],
    focus() {{ emptyDialogFocusCalls++; mockActiveElement = this; }},
  }};
  const emptyContainerEl = {{
    querySelector: (sel) => (sel === '[role="dialog"]' ? emptyDialogEl : null),
  }};
  mockActiveElement = outsideEl;
  let emptyTabPrevented = false;
  handleModalKeydown(
    {{ key: "Tab", shiftKey: false, preventDefault() {{ emptyTabPrevented = true; }} }},
    app.state,
    emptyContainerEl,
    () => {{}}
  );
  assert.equal(emptyTabPrevented, true, "Tab with zero focusables must preventDefault");
  assert.equal(emptyDialogFocusCalls, 1, "Tab with zero focusables must focus dialog container");
  assert.equal(mockActiveElement, emptyDialogEl);

  // Detached trigger element (isConnected: false) must not receive focus on close.
  app.state.modal = null;
  const detachedTrigger = {{
    isConnected: false,
    focusCalls: 0,
    focus() {{
      this.focusCalls++;
    }},
  }};
  mockActiveElement = detachedTrigger;
  const openBtn2 = findClickHandler(app.renderTopbar(), "+ Open / Create Database");
  openBtn2();
  docKeydownListeners[0]({{ key: "Escape", preventDefault() {{}} }});
  assert.equal(app.state.modal, null);
  assert.equal(detachedTrigger.focusCalls, 0, "disconnected trigger must not be refocused");
}}

console.log("regression_a11y_modals_aria_labels_escape_focus_and_backdrop passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_sse_lifecycle_and_routing() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const streams = [];
globalThis.EventSource = class {{
  constructor(url) {{
    this.url = url;
    this.closed = false;
    this.closeCount = 0;
    this.listeners = new Map();
    streams.push(this);
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
  close() {{
    this.closed = true;
    this.closeCount++;
  }}
}};

const app = await import("./ui/app.js");

// 1. parseBrowserRoute supports /projects/.../databases/.../contention while keeping /documents exact.
assert.deepEqual(
  app.parseBrowserRoute("/projects/p/databases/(default)/contention"),
  {{ projectId: "p", databaseId: "(default)", segments: [], view: "contention" }}
);
assert.deepEqual(
  app.parseBrowserRoute("/projects/p/databases/(default)/contention/"),
  {{ projectId: "p", databaseId: "(default)", segments: [], view: "contention" }}
);
assert.equal(app.parseBrowserRoute("/projects/../databases/(default)/contention"), null);
assert.equal(app.parseBrowserRoute("/projects/p/databases/(default)/contention/extra"), null);
assert.deepEqual(
  app.parseBrowserRoute("/projects/p/databases/(default)/documents/users/alice"),
  {{ projectId: "p", databaseId: "(default)", segments: ["users", "alice"] }}
);

const rootPath = "projects/p/databases/(default)/documents";
let contentionFetches = 0;
fetchImpl = async (url) => {{
  if (url === "/emulator/v1") {{
    return jsonResponse([rootPath]);
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
    return jsonResponse({{ collectionIds: ["users"] }});
  }}
  if (url.startsWith("/v1/projects/p/databases/(default)/documents/users?")) {{
    return jsonResponse({{ documents: [] }});
  }}
  if (url.startsWith("/emulator/v1/contention")) {{
    contentionFetches++;
    return jsonResponse({{
      databases: [{{
        database: rootPath,
        summary: {{ woundsTotal: 0, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
        contendedDocuments: [],
        totalContendedDocuments: 0,
        truncated: false,
        recentEvents: [],
      }}],
    }});
  }}
  throw new Error("unexpected url: " + url);
}};

// 2. Starting on explorer must NOT open contention SSE or fetch /emulator/v1/contention.
await app.init();
assert.equal(app.state.view, "explorer");
assert.equal(app.state.contentionSseStatus, "idle");
assert.equal(streams.filter((s) => s.url === "/emulator/v1/events").length, 1);
assert.equal(streams.filter((s) => s.url === "/emulator/v1/contention/events").length, 0);
assert.equal(contentionFetches, 0);

// 3. Clicking Contention tab opens contention SSE, syncs URL, and fetches once (without double-fetch on initial open).
const contentionTab = findClickHandler(app.renderTopbar(), "Contention");
assert.ok(contentionTab, "expected Contention button in topbar");
await contentionTab();
assert.equal(app.state.view, "contention");
assert.equal(globalThis.window.location.pathname, "/projects/p/databases/(default)/contention");
assert.equal(contentionFetches, 1);
let contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 1);
assert.equal(contentionStreams[0].closed, false);
assert.equal(app.state.contentionSseStatus, "connecting");

contentionStreams[0].emit("open");
assert.equal(app.state.contentionSseStatus, "live");
assert.equal(contentionFetches, 1, "initial SSE open must not double-fetch");

// 4. Leaving Contention to Query Runner closes contention SSE and resets status to idle.
const queryTab = findClickHandler(app.renderTopbar(), "Query Runner");
assert.ok(queryTab);
await queryTab();
assert.equal(app.state.view, "query");
assert.equal(contentionStreams[0].closed, true);
assert.equal(contentionStreams[0].closeCount, 1);
assert.equal(app.state.contentionSseStatus, "idle");
assert.equal(globalThis.window.location.pathname, "/projects/p/databases/(default)/documents");

// 5. Re-entering Contention opens a new stream; error -> live triggers reconnect refresh.
await app.switchView("contention");
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 2);
assert.equal(contentionStreams[1].closed, false);
assert.equal(contentionFetches, 2);

contentionStreams[1].emit("error");
assert.equal(app.state.contentionSseStatus, "error");
contentionStreams[1].emit("open");
await new Promise((r) => setTimeout(r, 0));
assert.equal(app.state.contentionSseStatus, "live");
assert.equal(contentionFetches, 3, "error -> live reconnect must refresh contention");

// 6. Pausing SSE closes contention SSE; unpausing reopens and refreshes contention.
await app.toggleSsePaused();
assert.equal(app.state.ssePaused, true);
assert.equal(contentionStreams[1].closed, true);
assert.equal(app.state.contentionSseStatus, "idle");

await app.toggleSsePaused();
assert.equal(app.state.ssePaused, false);
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 3);
assert.equal(contentionStreams[2].closed, false);
assert.equal(contentionFetches, 4);

// 7. Tab hidden closes contention SSE; becoming visible reopens and refreshes.
mockVisibilityState = "hidden";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
await new Promise((r) => setTimeout(r, 0));
assert.equal(contentionStreams[2].closed, true);
assert.equal(app.state.contentionSseStatus, "idle");

// Switching away & back while hidden must keep contention SSE closed.
await app.switchView("explorer");
await app.switchView("contention");
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 3, "must not open contention SSE while document is hidden");

mockVisibilityState = "visible";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
await new Promise((r) => setTimeout(r, 0));
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 4);
assert.equal(contentionStreams[3].closed, false);
assert.equal(contentionFetches, 6);

// 8. popstate back to /documents/users switches to explorer and closes contention SSE.
globalThis.window.location.pathname = "/projects/p/databases/(default)/documents/users";
for (const fn of popstateListeners) {{
  fn();
}}
await new Promise((r) => setTimeout(r, 0));
assert.equal(app.state.view, "explorer");
assert.equal(contentionStreams[3].closed, true);
assert.deepEqual(app.state.segments, ["users"]);

// 9. Missing activeDatabaseId keeps contention SSE closed, and handleVisibilityChange outside contention does not fetch.
await app.switchView("contention");
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 5);
const fetchesBeforeVisOutside = contentionFetches;
await app.switchView("explorer");
assert.equal(contentionStreams[4].closed, true);
mockVisibilityState = "hidden";
await app.handleVisibilityChange();
mockVisibilityState = "visible";
await app.handleVisibilityChange();
assert.equal(contentionFetches, fetchesBeforeVisOutside, "visibilitychange outside contention must not fetch contention");

// When activeDatabaseId is unset, entering contention must NOT open SSE.
app.state.activeDatabaseId = null;
await app.switchView("contention");
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 5, "must require both activeProjectId and activeDatabaseId");
app.state.activeDatabaseId = "(default)";
await app.switchView("contention");
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(contentionStreams.length, 6);

// 10. Unpausing SSE when enqueueSseBatch switches to another DB avoids duplicate refreshContention().
await app.toggleSsePaused();
assert.equal(contentionStreams[5].closed, true);
const origFetch = fetchImpl;
const db2Root = "projects/p/databases/db2/documents";
let db2ContentionFetches = 0;
fetchImpl = async (url) => {{
  if (url === "/emulator/v1") return jsonResponse([db2Root]);
  if (url === "/v1/projects/p/databases/db2") {{
    return jsonResponse({{
      name: "projects/p/databases/db2",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/emulator/v1/contention")) {{
    db2ContentionFetches++;
    return jsonResponse({{ databases: [] }});
  }}
  return origFetch(url);
}};
await app.toggleSsePaused();
assert.equal(app.state.activeDatabaseId, "db2");
assert.equal(db2ContentionFetches, 1, "unpause resync DB switch must refresh contention exactly once");

// 11. Synchronous EventSource constructor throw leaves contentionSseStatus === "error" cleanly.
await app.switchView("explorer");
fetchImpl = origFetch;
const OrigEs = globalThis.EventSource;
let esThrowAttempts = 0;
globalThis.EventSource = class {{
  constructor(url) {{
    if (url === "/emulator/v1/contention/events") {{
      esThrowAttempts++;
      throw new Error("EventSource disabled");
    }}
    return new OrigEs(url);
  }}
}};
await app.switchView("contention");
assert.equal(app.state.contentionSseStatus, "error");
assert.equal(esThrowAttempts, 1, "synchronous EventSource throw must not retry in renderApp loop");
globalThis.EventSource = OrigEs;
await app.switchView("explorer");

// 12. Paused SSE suppresses automatic visibilitychange refreshContention while preserving manual Refresh.
await app.switchView("contention");
await app.toggleSsePaused();
assert.equal(app.state.ssePaused, true);
const fetchesWhilePausedBeforeVis = contentionFetches;
mockVisibilityState = "hidden";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
mockVisibilityState = "visible";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
assert.equal(
  contentionFetches,
  fetchesWhilePausedBeforeVis,
  "visibilitychange while ssePaused must not automatically refresh contention"
);
contentionStreams = streams.filter((s) => s.url === "/emulator/v1/contention/events");
assert.equal(
  contentionStreams.every((s) => s.closed),
  true,
  "visibilitychange while ssePaused must keep contention SSE closed"
);
const manualRefreshBtn = findClickHandler(app.renderWorkspace(), "Refresh");
assert.ok(manualRefreshBtn, "expected manual Refresh button while SSE paused");
await manualRefreshBtn();
assert.equal(
  contentionFetches,
  fetchesWhilePausedBeforeVis + 1,
  "manual Refresh must still fetch contention while ssePaused"
);
await app.toggleSsePaused();

// 13. Switching DB via selectDatabase while in Contention view sets loading=true immediately so
//     empty-state copy does not flash during getDatabase()/listCollectionIds() awaits.
const db3Root = "projects/p/databases/db3/documents";
const db3MetaDef = deferred();
const db3ColsDef = deferred();
fetchImpl = async (url) => {{
  if (url === "/emulator/v1") return jsonResponse([rootPath, db2Root, db3Root]);
  if (url === "/v1/projects/p/databases/db3") return db3MetaDef.promise;
  if (url === "/v1/projects/p/databases/db3/documents:listCollectionIds") return db3ColsDef.promise;
  if (url.startsWith("/emulator/v1/contention")) {{
    return jsonResponse({{ databases: [] }});
  }}
  return origFetch(url);
}};
const selectDb3Promise = app.selectDatabase("p", "db3", [], true);
assert.equal(app.state.contention.loading, true, "selectDatabase in contention view must set loading=true immediately");
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
db3MetaDef.resolve(jsonResponse({{
  name: "projects/p/databases/db3",
  uid: "u3",
  type: "FIRESTORE_NATIVE",
  databaseEdition: "STANDARD",
  concurrencyMode: "PESSIMISTIC",
}}));
db3ColsDef.resolve(jsonResponse({{ collectionIds: [] }}));
await selectDb3Promise;
assert.equal(app.state.contention.loading, false);
await app.switchView("explorer");

console.log("regression_contention_sse_lifecycle_and_routing passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_debounced_refetch_and_stale_guard() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
let virtualNow = 0;
let nextTimerId = 1;
const scheduledTimers = new Map();
const origSetTimeout = globalThis.setTimeout;
const origClearTimeout = globalThis.clearTimeout;

globalThis.setTimeout = (fn, ms = 0) => {{
  const id = nextTimerId++;
  scheduledTimers.set(id, {{ fn, runAt: virtualNow + Number(ms), seq: id }});
  return id;
}};
globalThis.clearTimeout = (id) => {{
  scheduledTimers.delete(id);
}};

async function flushMicrotasks() {{
  for (let i = 0; i < 6; i++) {{
    await Promise.resolve();
  }}
}}

async function advanceTimers(deltaMs) {{
  const target = virtualNow + deltaMs;
  while (true) {{
    let next = null;
    for (const entry of scheduledTimers.values()) {{
      if (entry.runAt <= target) {{
        if (!next || entry.runAt < next.runAt || (entry.runAt === next.runAt && entry.seq < next.seq)) {{
          next = entry;
        }}
      }}
    }}
    if (!next) break;
    scheduledTimers.delete(next.seq);
    virtualNow = next.runAt;
    next.fn();
    await flushMicrotasks();
  }}
  virtualNow = target;
}}

const streams = [];
globalThis.EventSource = class {{
  constructor(url) {{
    this.url = url;
    this.closed = false;
    this.listeners = new Map();
    streams.push(this);
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
  close() {{
    this.closed = true;
  }}
}};

const app = await import("./ui/app.js");

const rootPath = "projects/p/databases/(default)/documents";
let contentionFetches = 0;
let nextSnapshotFn = () => jsonResponse({{
  databases: [{{
    database: rootPath,
    summary: {{ woundsTotal: 0, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}});

fetchImpl = async (url) => {{
  if (url === "/emulator/v1") return jsonResponse([rootPath]);
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
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/emulator/v1/contention")) {{
    contentionFetches++;
    return nextSnapshotFn();
  }}
  throw new Error("unexpected url: " + url);
}};

globalThis.window.location.pathname = "/projects/p/databases/(default)/documents";
await app.init();
await app.switchView("contention");
const es = streams.find((s) => s.url === "/emulator/v1/contention/events");
assert.ok(es);
es.emit("open");
assert.equal(contentionFetches, 1);

// 1. Rapid burst of contention_changed / database_cleared / resync events coalesces into 1 refetch after 200ms.
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});
await advanceTimers(40);
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});
await advanceTimers(40);
es.emit("contention", {{ data: JSON.stringify({{ type: "database_cleared", database: rootPath }}) }});
es.emit("resync", {{ data: JSON.stringify({{ type: "resync", skipped: 2 }}) }});
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});

await advanceTimers(199);
assert.equal(contentionFetches, 1, "debounced burst must not fetch before 200ms trailing quiet window");

await advanceTimers(1);
assert.equal(contentionFetches, 2, "rapid burst must coalesce into exactly 1 GET /emulator/v1/contention at 200ms");

// 2. Sustained >5Hz contention events (every 100ms) must still flush at maxWaitMs=1000 instead of starving.
for (let step = 0; step < 9; step++) {{
  es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});
  await advanceTimers(100);
}}
assert.equal(contentionFetches, 2, "must not flush before maxWaitMs=1000 while events arrive every 100ms");
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});
await advanceTimers(100);
assert.equal(contentionFetches, 3, "maxWaitMs=1000 must force refresh under continuous >5Hz contention");
await advanceTimers(250);
assert.equal(contentionFetches, 3, "flushing at maxWaitMs must clear trailing timer cleanly");

// 3. Events for another database (and non-object JSON payloads) do not trigger extra fetches in active scope.
es.emit("contention", {{ data: "null" }});
es.emit("contention", {{ data: '"str"' }});
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: "projects/p/databases/other/documents" }}) }});
await advanceTimers(1200);
assert.equal(contentionFetches, 3, "unrelated DB or non-object payloads must not trigger fetches");

// 4. Stale in-flight GET response must NOT overwrite newer snapshot.
const slowFirst = deferred();
const fastSecond = deferred();
let seqMode = 0;
nextSnapshotFn = () => {{
  seqMode++;
  return seqMode === 1 ? slowFirst.promise : fastSecond.promise;
}};
const p1 = app.refreshContention();
const p2 = app.refreshContention();
fastSecond.resolve(jsonResponse({{
  databases: [{{
    database: rootPath,
    summary: {{ woundsTotal: 7, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}}));
await p2;
slowFirst.resolve(jsonResponse({{
  databases: [{{
    database: rootPath,
    summary: {{ woundsTotal: 1, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}}));
await p1;
assert.equal(app.state.contention.snapshot.databases[0].summary.woundsTotal, 7);

// 5. Leaving view before debounce/maxWait timers fire clears timers cleanly (no fetch after leave).
nextSnapshotFn = () => jsonResponse({{ databases: [] }});
const fetchesBeforeLeave = contentionFetches;
es.emit("contention", {{ data: JSON.stringify({{ type: "contention_changed", database: rootPath }}) }});
await app.switchView("explorer");
assert.equal(es.closed, true);
await advanceTimers(1500);
assert.equal(contentionFetches, fetchesBeforeLeave, "closing contention SSE must cancel pending debounce and maxWait timers");

globalThis.setTimeout = origSetTimeout;
globalThis.clearTimeout = origClearTimeout;
console.log("regression_contention_debounced_refetch_and_stale_guard passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_dom_only_waiter_ticker_lifecycle() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
let nextIntervalId = 1;
const activeIntervals = new Map();
const startedIntervals = [];
const clearedIntervals = [];
const origSetInterval = globalThis.setInterval;
const origClearInterval = globalThis.clearInterval;

globalThis.setInterval = (fn, ms) => {{
  const id = nextIntervalId++;
  activeIntervals.set(id, {{ fn, ms }});
  startedIntervals.push({{ id, ms }});
  return id;
}};
globalThis.clearInterval = (id) => {{
  if (activeIntervals.has(id)) {{
    activeIntervals.delete(id);
    clearedIntervals.push(id);
  }}
  return origClearInterval(id);
}};

let mockNow = 1_700_000_000_000;
Date.now = () => mockNow;

const waitNode = {{
  attrs: {{ "data-wait-base-ms": "160", "data-wait-ms": "160", "data-wait-label": "160ms" }},
  textContent: "160ms",
  getAttribute(name) {{
    return this.attrs[name] ?? null;
  }},
  setAttribute(name, val) {{
    this.attrs[name] = String(val);
  }},
}};
let querySelectorCalls = 0;
const mockWorkspaceEl = {{
  querySelectorAll(sel) {{
    if (sel === "[data-wait-base-ms]") {{
      querySelectorCalls++;
      return [waitNode];
    }}
    return [];
  }},
}};

const app = await import("./ui/app.js");
document.getElementById = (id) => (id === "workspace" ? mockWorkspaceEl : null);

const rootPath = "projects/p/databases/(default)/documents";
const otherRoot = "projects/p/databases/other/documents";
let currentWaiters = [];

fetchImpl = async (url) => {{
  if (url === "/emulator/v1") return jsonResponse([rootPath, otherRoot]);
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/other") {{
    return jsonResponse({{
      name: "projects/p/databases/other",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url.endsWith(":listCollectionIds")) {{
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.startsWith("/emulator/v1/contention")) {{
    const dbName = url.includes("other") ? otherRoot : rootPath;
    return jsonResponse({{
      databases: [{{
        database: dbName,
        summary: {{ woundsTotal: 0, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
        contendedDocuments: currentWaiters.length > 0
          ? [{{
              document: `${{dbName}}/c/d1`,
              writer: null,
              readers: [],
              readerCount: 1,
              waiters: currentWaiters,
              waiterCount: currentWaiters.length,
            }}]
          : [],
        totalContendedDocuments: currentWaiters.length > 0 ? 1 : 0,
        truncated: false,
        recentEvents: [],
      }}],
    }});
  }}
  throw new Error("unexpected url: " + url);
}};

await app.init();

// 1. Snapshot with 0 waiters must NOT start the 1000ms ticker.
await app.switchView("contention");
assert.equal(startedIntervals.length, 0, "no ticker interval when waiters list is empty");

// 2. Snapshot with 1 waiter starts exactly one 1000ms interval.
currentWaiters = [{{
  sessionId: 7,
  txnId: "1:1",
  kind: "PESSIMISTIC_TXN",
  seq: 3,
  mode: "WRITE",
  waitedMs: 160,
}}];
await app.refreshContention();
assert.equal(startedIntervals.length, 1);
assert.equal(startedIntervals[0].ms, 1000);
assert.equal(activeIntervals.size, 1);

// 3. Running ticks updates ONLY DOM nodes directly without re-fetching or overwriting snapshot waitedMs.
const tickId = startedIntervals[0].id;
const tickFn = activeIntervals.get(tickId).fn;
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
assert.equal(app.state.contention.snapshot.databases[0].contendedDocuments[0].waiters[0].waitedMs, 160);

// 4. Clearing waiters stops the interval immediately.
currentWaiters = [];
await app.refreshContention();
assert.equal(activeIntervals.size, 0, "interval must clear when waiters drop to 0");

// 5. Re-enabling waiter starts interval, then hidden visibility clears it, then visible restarts it.
currentWaiters = [{{ sessionId: 8, txnId: null, kind: "SINGLE_WRITE", seq: 4, mode: "WRITE", waitedMs: 25 }}];
await app.refreshContention();
assert.equal(activeIntervals.size, 1);

mockVisibilityState = "hidden";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
assert.equal(activeIntervals.size, 0, "interval must clear when tab becomes hidden");

mockVisibilityState = "visible";
for (const fn of docVisibilityListeners) {{
  await fn();
}}
assert.equal(activeIntervals.size, 1, "interval must restart when tab becomes visible with waiters");

// 6. Database change stops existing interval before new snapshot arrives, then restarts for new DB waiters.
const clearedBeforeDbSwitch = clearedIntervals.length;
await app.selectDatabase("p", "other");
assert.ok(clearedIntervals.length > clearedBeforeDbSwitch, "database change must clear previous ticker");
assert.equal(activeIntervals.size, 1);

// 7. Switching view away from contention clears the interval immediately.
await app.switchView("query");
assert.equal(activeIntervals.size, 0, "leaving contention view must clear ticker interval");

globalThis.setInterval = origSetInterval;
console.log("regression_contention_dom_only_waiter_ticker_lifecycle passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_clear_history_open_doc_and_rendering() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const {{ renderContention }} = await import("./ui/components/contention.js");

const rootA = "projects/p/databases/(default)/documents";
const rootB = "projects/other/databases/db2/documents";
const nestedDoc = `${{rootA}}/rooms/r1/messages/m1`;
const crossDbDoc = `${{rootB}}/orders/o1`;

const deleteCalls = [];
const fetchedUrls = [];
let clearedOnce = false;

fetchImpl = async (url, init) => {{
  fetchedUrls.push(`${{init.method ?? "GET"}} ${{url}}`);
  if (url === "/emulator/v1") return jsonResponse([rootA, rootB]);
  if (url === "/v1/projects/p/databases/(default)") {{
    return jsonResponse({{
      name: "projects/p/databases/(default)",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/other/databases/db2") {{
    return jsonResponse({{
      name: "projects/other/databases/db2",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "ENTERPRISE",
      concurrencyMode: "OPTIMISTIC",
    }});
  }}
  if (url.endsWith(":listCollectionIds")) {{
    if (url.includes("/rooms/r1:")) return jsonResponse({{ collectionIds: ["messages"] }});
    if (url.includes("/documents:")) return jsonResponse({{ collectionIds: ["rooms", "orders"] }});
    return jsonResponse({{ collectionIds: [] }});
  }}
  if (url.includes("/documents/rooms?")) {{
    return jsonResponse({{ documents: [{{ name: `${{rootA}}/rooms/r1` }}] }});
  }}
  if (url.includes("/documents/rooms/r1/messages?")) {{
    return jsonResponse({{
      documents: [{{ name: nestedDoc, createTime: "2026-01-01T00:00:00Z", updateTime: "2026-01-01T00:00:00Z" }}],
    }});
  }}
  if (url === `/v1/${{nestedDoc}}`) {{
    return jsonResponse({{ name: nestedDoc, fields: {{ body: {{ stringValue: "hi" }} }} }});
  }}
  if (url.includes("/documents/orders?")) {{
    return jsonResponse({{
      documents: [{{ name: crossDbDoc, createTime: "2026-01-01T00:00:00Z", updateTime: "2026-01-01T00:00:00Z" }}],
    }});
  }}
  if (url === `/v1/${{crossDbDoc}}`) {{
    return jsonResponse({{ name: crossDbDoc, fields: {{ total: {{ integerValue: "42" }} }} }});
  }}
  if (url.startsWith("/emulator/v1/contention") && init.method === "DELETE") {{
    deleteCalls.push(url);
    clearedOnce = true;
    return jsonResponse({{}});
  }}
  if (url.startsWith("/emulator/v1/contention")) {{
    return jsonResponse({{
      databases: [{{
        database: rootA,
        summary: clearedOnce
          ? {{ woundsTotal: 0, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }}
          : {{ woundsTotal: 3, timeoutsTotal: 2, upgradeAbortsTotal: 1, writeBatchRetriesTotal: 4 }},
        contendedDocuments: [{{
          document: nestedDoc,
          writer: {{
            sessionId: 11,
            txnId: "11:1",
            kind: "PESSIMISTIC_TXN",
            seq: 1,
            state: "ACTIVE",
            woundable: false,
          }},
          readers: [{{
            sessionId: 12,
            txnId: "12:1",
            kind: "PESSIMISTIC_TXN",
            seq: 2,
            state: "ACTIVE",
            woundable: true,
            retained: true,
            activeLeases: 1,
          }}],
          readerCount: 1,
          waiters: [{{
            sessionId: 13,
            txnId: "13:1",
            kind: "PESSIMISTIC_TXN",
            seq: 3,
            mode: "WRITE",
            waitedMs: 250,
          }}],
          waiterCount: 1,
        }}],
        totalContendedDocuments: 1,
        truncated: false,
        recentEvents: clearedOnce ? [] : [
          {{
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
          }},
          {{
            id: 1,
            kind: "WRITE_BATCH_RETRY",
            timestamp: "2026-11-23T12:00:00Z",
            docName: null,
            sessionId: 15,
            txnId: null,
            sessionKind: "WRITE_BATCH",
            seq: 6,
            attempt: 1,
          }},
        ],
      }}],
    }});
  }}
  throw new Error("unexpected url: " + url);
}};

globalThis.window.location.pathname = "/projects/p/databases/(default)/documents";
await app.init();
await app.switchView("contention");

// 1. Verify topbar ARIA & rendered contention UI (summary, writer, readers, waiters, WOUNDED, WRITE_BATCH_RETRY).
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
assert.ok(markup.includes('data-wait-ms='));
assert.ok(/data-wait-label="\d+ms"\s*><\/span>/.test(markup), ".wait-elapsed must remain empty (no ChildPart inside)");
assert.ok(markup.includes('aria-label="Clear history (p/(default))"'), "WCAG 2.5.3 Label in Name");
assert.ok(markup.includes("retained"));
assert.ok(markup.includes("wounded by"));
assert.ok(markup.includes("WOUNDED"));
assert.ok(markup.includes("WRITE_BATCH_RETRY"));
assert.ok(markup.includes("attempt 1"));

// 2. Clicking "Clear history" in active scope sends DELETE with ?database=... and preserves active waiters.
const clearBtn = findClickHandler(app.renderWorkspace(), "Clear history");
assert.ok(clearBtn, "expected Clear history button");
await clearBtn();
assert.deepEqual(deleteCalls, [
  "/emulator/v1/contention?database=projects%2Fp%2Fdatabases%2F%28default%29%2Fdocuments",
]);
assert.equal(app.state.contention.clearing, false);
assert.equal(app.state.contention.snapshot.databases[0].summary.woundsTotal, 0);
assert.equal(app.state.contention.snapshot.databases[0].contendedDocuments[0].waiters.length, 1);

// 2b. Guard clearContentionHistory against wrong view, missing DB, and pre-clear GET race.
const delCountBeforeGuards = deleteCalls.length;
await app.switchView("explorer");
await app.clearContentionHistory();
assert.equal(deleteCalls.length, delCountBeforeGuards, "clearContentionHistory must no-op outside contention view");
await app.switchView("contention");

const savedDbId = app.state.activeDatabaseId;
app.state.activeDatabaseId = null;
await app.clearContentionHistory();
assert.equal(deleteCalls.length, delCountBeforeGuards, "clearContentionHistory must no-op when activeDatabaseId is null");
app.state.activeDatabaseId = savedDbId;

// Pre-clear GET race: an in-flight GET returning stale woundsTotal=99 must be discarded after clearContentionHistory().
const stalePreClearGet = deferred();
const baseFetch = fetchImpl;
fetchImpl = async (url, init) => {{
  if (url.startsWith("/emulator/v1/contention") && (init.method ?? "GET") === "GET") {{
    if (!clearedOnce) return stalePreClearGet.promise;
  }}
  return baseFetch(url, init);
}};
clearedOnce = false;
const preClearRefresh = app.refreshContention();
await app.clearContentionHistory();
stalePreClearGet.resolve(jsonResponse({{
  databases: [{{
    database: rootA,
    summary: {{ woundsTotal: 99, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}}));
await preClearRefresh;
assert.equal(app.state.contention.snapshot.databases[0].summary.woundsTotal, 0, "stale pre-clear GET must be ignored");
fetchImpl = baseFetch;

// 3. Switching scope to All Databases resets snapshot synchronously while loading and clears without ?database=.
const allScopeBtn = findClickHandler(app.renderWorkspace(), "All Databases");
assert.ok(allScopeBtn);
const deferredAllGet = deferred();
fetchImpl = async (url, init) => {{
  if (url === "/emulator/v1/contention" && (init.method ?? "GET") === "GET") {{
    return deferredAllGet.promise;
  }}
  return baseFetch(url, init);
}};
const scopePromise = allScopeBtn();
assert.equal(app.state.contention.scope, "all");
assert.equal(app.state.contention.snapshot, null, "scope switch must clear stale snapshot before fetch resolves");
deferredAllGet.resolve(baseFetch("/emulator/v1/contention", {{ method: "GET" }}));
await scopePromise;
fetchImpl = baseFetch;

const clearAllBtn = findClickHandler(app.renderWorkspace(), "Clear history");
await clearAllBtn();
assert.equal(deleteCalls[deleteCalls.length - 1], "/emulator/v1/contention");

// 4. Clicking document button calls openDocumentFromName -> switches to explorer & hydrates nested document.
const docBtn = findClickHandler(app.renderWorkspace(), nestedDoc);
assert.ok(docBtn, "expected semantic <button> for contended document path");
docBtn();
await new Promise((r) => setTimeout(r, 10));
assert.equal(app.state.view, "explorer");
assert.deepEqual(app.state.segments, ["rooms", "r1", "messages", "m1"]);
assert.equal(app.state.contentionSseStatus, "idle");

// 4b. openDocumentFromName rejects invalid/odd paths and refreshes missing root collection in same-DB branch.
await app.openDocumentFromName(`${{rootA}}/rooms/../messages/m1`);
assert.deepEqual(app.state.segments, ["rooms", "r1", "messages", "m1"], "dot-dot segment must be ignored");
await app.openDocumentFromName(`${{rootA}}/rooms//r1/m1`);
assert.deepEqual(app.state.segments, ["rooms", "r1", "messages", "m1"], "empty segment must be ignored");
await app.openDocumentFromName(`${{rootA}}/rooms`);
assert.deepEqual(app.state.segments, ["rooms", "r1", "messages", "m1"], "odd collection path must be ignored");

const newColDoc = `${{rootA}}/newcol/d1`;
fetchImpl = async (url, init) => {{
  if (url === `/v1/${{rootA}}:listCollectionIds`) {{
    return jsonResponse({{ collectionIds: ["rooms", "orders", "newcol"] }});
  }}
  if (url.includes("/documents/newcol?")) {{
    return jsonResponse({{ documents: [{{ name: newColDoc, createTime: "2026-01-01T00:00:00Z", updateTime: "2026-01-01T00:00:00Z" }}] }});
  }}
  if (url === `/v1/${{newColDoc}}`) {{
    return jsonResponse({{ name: newColDoc, fields: {{}} }});
  }}
  return baseFetch(url, init);
}};
await app.openDocumentFromName(newColDoc);
assert.ok(app.state.rootCollections.includes("newcol"), "same-DB openDocumentFromName must refresh missing root collection");
assert.deepEqual(app.state.segments, ["newcol", "d1"]);
fetchImpl = baseFetch;

// Cross-database openDocumentFromName switches active DB and opens target doc.
await app.openDocumentFromName(crossDbDoc);
assert.equal(app.state.activeProjectId, "other");
assert.equal(app.state.activeDatabaseId, "db2");
assert.deepEqual(app.state.segments, ["orders", "o1"]);

// DELETE error during clearContentionHistory sets error and resets clearing flag.
await app.switchView("contention");
const prevFetch = fetchImpl;
fetchImpl = async (url, init) => {{
  if (url.startsWith("/emulator/v1/contention") && init.method === "DELETE") {{
    return jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "clear failed" }} }}, 500);
  }}
  return prevFetch(url, init);
}};
await app.clearContentionHistory();
assert.equal(app.state.contention.clearing, false);
assert.equal(app.state.contention.error, "[500 INTERNAL] clear failed");
fetchImpl = prevFetch;
await app.switchView("explorer");

// 5. Truncation banner (simultaneous doc + participant truncation), empty state, and error banner rendering.
const truncMarkup = renderTemplateMarkup(renderContention({{
  projectId: "p",
  databaseId: "(default)",
  state: {{
    scope: "active",
    snapshot: {{
      databases: [{{
        database: rootA,
        summary: {{ woundsTotal: 0, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
        contendedDocuments: [{{
          document: nestedDoc,
          writer: null,
          readers: [],
          readerCount: 70,
          waiters: [],
          waiterCount: 0,
        }}],
        totalContendedDocuments: 300,
        truncated: true,
        recentEvents: [],
      }}],
    }},
    loading: false,
    clearing: false,
    error: "[500 INTERNAL] boom",
    lastFetchedAtMs: null,
    lastFetchedIso: null,
  }},
  sseStatus: "error",
  ssePaused: false,
  onChangeScope: () => {{}},
  onRefresh: () => {{}},
  onClearHistory: () => {{}},
  onOpenDocument: () => {{}},
}}));
assert.ok(truncMarkup.includes('role="alert"') && truncMarkup.includes("[500 INTERNAL] boom"));
assert.ok(truncMarkup.includes("Snapshot truncated:") && truncMarkup.includes("showing 1 of 300"));
assert.ok(truncMarkup.includes("participant lists capped at 64 readers / waiters per document"));
assert.ok(truncMarkup.includes("No contention events recorded."));

const emptyMarkup = renderTemplateMarkup(renderContention({{
  projectId: "p",
  databaseId: "(default)",
  state: {{
    scope: "active",
    snapshot: {{ databases: [] }},
    loading: false,
    clearing: false,
    error: null,
    lastFetchedAtMs: null,
    lastFetchedIso: null,
  }},
  sseStatus: "live",
  ssePaused: false,
  onChangeScope: () => {{}},
  onRefresh: () => {{}},
  onClearHistory: () => {{}},
  onOpenDocument: () => {{}},
}}));
assert.ok(emptyMarkup.includes("No active lock contention"));
assert.ok(emptyMarkup.includes("No contention events recorded."));

// 6. When snapshot === null && !loading && error, render "—" in all 4 summary cards and explicit error + Retry in .contention-body.
let retryClicks = 0;
const initialErrTpl = renderContention({{
  projectId: "p",
  databaseId: "(default)",
  state: {{
    scope: "active",
    snapshot: null,
    loading: false,
    clearing: false,
    error: "[503 UNAVAILABLE] contention endpoint failed",
    lastFetchedAtMs: null,
    lastFetchedIso: null,
  }},
  sseStatus: "error",
  ssePaused: false,
  onChangeScope: () => {{}},
  onRefresh: () => {{
    retryClicks++;
  }},
  onClearHistory: () => {{}},
  onOpenDocument: () => {{}},
}});
const initialErrMarkup = renderTemplateMarkup(initialErrTpl);
const dashCardMatches = initialErrMarkup.match(/class="contention-stat-value mono">\u{{2014}}<\/strong>/gu) ?? [];
assert.equal(dashCardMatches.length, 4, "all 4 summary cards must show em-dash when initial snapshot failed: " + initialErrMarkup);
assert.equal(initialErrMarkup.includes(">0</strong>"), false, "summary cards must not show 0 when snapshot is null with error");
assert.ok(initialErrMarkup.includes("Failed to load contention snapshot: [503 UNAVAILABLE] contention endpoint failed"));
assert.equal(initialErrMarkup.includes("No active lock contention"), false, "error state must not render empty contention copy");
assert.equal(initialErrMarkup.includes("No contention events recorded."), false);
const retryBtn = findClickHandler(initialErrTpl, "Retry");
assert.ok(retryBtn, "expected Retry button in .contention-body");
retryBtn();
assert.equal(retryClicks, 1);

console.log("regression_contention_clear_history_open_doc_and_rendering passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_lit_html_waiter_rerender_and_tick() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");
    assert!(
        css.contains(".wait-elapsed {"),
        "expected .wait-elapsed CSS rule"
    );
    assert!(
        !css.contains(".wait-elapsed:empty::before"),
        "CSS must not own .wait-elapsed text via :empty::before"
    );
    assert!(
        !css.contains("attr(data-wait-label)"),
        "CSS must not duplicate data-wait-label text via content: attr(...)"
    );

    let script = format!(
        r#"{NODE_BOOTSTRAP}
class DomNode {{
  constructor(nodeType) {{
    this.nodeType = nodeType;
    this.parentNode = null;
    this.childNodes = [];
  }}
  get firstChild() {{
    return this.childNodes[0] ?? null;
  }}
  get nextSibling() {{
    if (!this.parentNode) return null;
    const list = this.parentNode.childNodes;
    const idx = list.indexOf(this);
    return idx >= 0 && idx + 1 < list.length ? list[idx + 1] : null;
  }}
  insertBefore(node, refNode) {{
    if (node.nodeType === 11) {{
      const kids = [...node.childNodes];
      for (const k of kids) this.insertBefore(k, refNode);
      node.childNodes = [];
      return node;
    }}
    if (node.parentNode) node.remove();
    node.parentNode = this;
    const idx = refNode ? this.childNodes.indexOf(refNode) : -1;
    if (idx >= 0) {{
      this.childNodes.splice(idx, 0, node);
    }} else {{
      this.childNodes.push(node);
    }}
    return node;
  }}
  append(...items) {{
    for (const item of items) {{
      const node = typeof item === "string" ? new DomText(item) : item;
      this.insertBefore(node, null);
    }}
  }}
  remove() {{
    if (!this.parentNode) return;
    const list = this.parentNode.childNodes;
    const idx = list.indexOf(this);
    if (idx >= 0) list.splice(idx, 1);
    this.parentNode = null;
  }}
  cloneNode(deep) {{
    const copy = this._cloneShallow();
    if (deep) {{
      for (const child of this.childNodes) {{
        copy.insertBefore(child.cloneNode(true), null);
      }}
    }}
    return copy;
  }}
}}

class DomText extends DomNode {{
  constructor(data) {{
    super(3);
    this.data = String(data);
  }}
  _cloneShallow() {{
    return new DomText(this.data);
  }}
}}

class DomComment extends DomNode {{
  constructor(data) {{
    super(8);
    this.data = String(data);
  }}
  _cloneShallow() {{
    return new DomComment(this.data);
  }}
}}

class DomFragment extends DomNode {{
  constructor() {{
    super(11);
  }}
  _cloneShallow() {{
    return new DomFragment();
  }}
}}

const VOID_TAGS = new Set(["INPUT", "BR", "HR", "IMG"]);

function parseHtmlToFragment(htmlStr) {{
  const frag = new DomFragment();
  const stack = [frag];
  const tokenRe = /<!--([\s\S]*?)-->|<\?([^>]*)>|<\/([A-Za-z0-9-]+)\s*>|<([A-Za-z0-9-]+)((?:\s+[^\s=/>]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>`]+))?)*)\s*(\/?)>|([^<]+)/g;
  let match;
  while ((match = tokenRe.exec(htmlStr)) !== null) {{
    const parent = stack[stack.length - 1];
    if (match[1] !== undefined) {{
      parent.insertBefore(new DomComment(match[1]), null);
    }} else if (match[2] !== undefined) {{
      parent.insertBefore(new DomComment("?" + match[2]), null);
    }} else if (match[3] !== undefined) {{
      const closeTag = match[3].toUpperCase();
      while (stack.length > 1) {{
        const top = stack.pop();
        if (top.tagName === closeTag) break;
      }}
    }} else if (match[4] !== undefined) {{
      const tag = match[4].toUpperCase();
      const el = new DomElement(tag);
      const attrStr = match[5] ?? "";
      const attrRe = /([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
      let am;
      while ((am = attrRe.exec(attrStr)) !== null) {{
        const val = am[2] ?? am[3] ?? am[4] ?? "";
        el.setAttribute(am[1], val);
      }}
      parent.insertBefore(el, null);
      if (!match[6] && !VOID_TAGS.has(tag)) {{
        stack.push(el);
      }}
    }} else if (match[7] !== undefined) {{
      parent.insertBefore(new DomText(match[7]), null);
    }}
  }}
  return frag;
}}

class DomElement extends DomNode {{
  constructor(tagName) {{
    super(1);
    this.tagName = tagName;
    this._attrs = new Map();
    if (tagName === "TEMPLATE") {{
      this.content = new DomFragment();
    }}
  }}
  set innerHTML(val) {{
    this.content = parseHtmlToFragment(String(val));
  }}
  get textContent() {{
    let out = "";
    for (const c of this.childNodes) {{
      out += c.nodeType === 3 ? c.data : c.nodeType === 1 ? c.textContent : "";
    }}
    return out;
  }}
  set textContent(val) {{
    for (const c of [...this.childNodes]) c.remove();
    if (val !== "" && val !== null && val !== undefined) {{
      this.insertBefore(new DomText(String(val)), null);
    }}
  }}
  hasAttributes() {{
    return this._attrs.size > 0;
  }}
  getAttributeNames() {{
    return Array.from(this._attrs.keys());
  }}
  getAttribute(name) {{
    return this._attrs.has(name) ? this._attrs.get(name) : null;
  }}
  hasAttribute(name) {{
    return this._attrs.has(name);
  }}
  setAttribute(name, val) {{
    this._attrs.set(name, String(val));
  }}
  removeAttribute(name) {{
    this._attrs.delete(name);
  }}
  toggleAttribute(name, force) {{
    if (force) this._attrs.set(name, "");
    else this._attrs.delete(name);
  }}
  addEventListener() {{}}
  removeEventListener() {{}}
  querySelectorAll(selector) {{
    const attrName = selector.startsWith("[") && selector.endsWith("]")
      ? selector.slice(1, -1)
      : null;
    const className = selector.startsWith(".") ? selector.slice(1) : null;
    const out = [];
    const visit = (node) => {{
      for (const c of node.childNodes) {{
        if (c.nodeType === 1) {{
          if (attrName && c.hasAttribute(attrName)) out.push(c);
          else if (className && (c.getAttribute("class") ?? "").split(/\s+/).includes(className)) out.push(c);
          visit(c);
        }}
      }}
    }};
    visit(this);
    return out;
  }}
  _cloneShallow() {{
    const el = new DomElement(this.tagName);
    for (const [k, v] of this._attrs) el._attrs.set(k, v);
    return el;
  }}
}}

function collectPreOrder(root, whatToShow) {{
  const nodes = [];
  const walk = (n) => {{
    for (const c of n.childNodes) {{
      if (whatToShow === 129 ? (c.nodeType === 1 || c.nodeType === 8) : true) {{
        nodes.push(c);
      }}
      walk(c);
    }}
  }};
  walk(root);
  return nodes;
}}

globalThis.document = {{
  nodeType: 9,
  childNodes: [],
  createComment: (data) => new DomComment(data),
  createTextNode: (data) => new DomText(data),
  createElement: (tag) => new DomElement(String(tag).toUpperCase()),
  importNode: (node, deep) => node.cloneNode(Boolean(deep)),
  createTreeWalker(root, whatToShow) {{
    return {{
      _root: root,
      _current: root,
      get currentNode() {{
        return this._current;
      }},
      set currentNode(node) {{
        this._root = node;
        this._current = node;
      }},
      nextNode() {{
        const list = collectPreOrder(this._root, whatToShow);
        if (this._current === this._root) {{
          this._current = list[0] ?? null;
          return this._current;
        }}
        const idx = list.indexOf(this._current);
        this._current = idx >= 0 && idx + 1 < list.length ? list[idx + 1] : null;
        return this._current;
      }},
    }};
  }},
}};

const {{ render }} = await import("./ui/lit-html.js");
const {{ createContentionState, renderContention, updateWaiterElapsedDom }} = await import("./ui/components/contention.js");

const container = new DomElement("MAIN");
const rootPath = "projects/p/databases/(default)/documents";

function makeSnapshot(waitedMs) {{
  return {{
    databases: [{{
      database: rootPath,
      summary: {{ woundsTotal: 1, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
      contendedDocuments: waitedMs === null ? [] : [{{
        document: `${{rootPath}}/users/alice`,
        writer: null,
        readers: [],
        readerCount: 1,
        waiters: [{{
          sessionId: 9,
          txnId: "9:1",
          kind: "PESSIMISTIC_TXN",
          seq: 2,
          mode: "WRITE",
          waitedMs,
        }}],
        waiterCount: 1,
      }}],
      totalContendedDocuments: waitedMs === null ? 0 : 1,
      truncated: false,
      recentEvents: [],
    }}],
  }};
}}

const cState = createContentionState();
let mockNow = 1_700_000_000_000;
const origDateNow = Date.now;
Date.now = () => mockNow;

const renderView = () => {{
  render(renderContention({{
    projectId: "p",
    databaseId: "(default)",
    state: cState,
    sseStatus: "live",
    ssePaused: false,
    onChangeScope: () => {{}},
    onRefresh: () => {{}},
    onClearHistory: () => {{}},
    onOpenDocument: () => {{}},
  }}), container);
  updateWaiterElapsedDom(container, cState, mockNow);
}};

// Cycle 1: Render with waitedMs=160 -> verify no Comment anchor inside .wait-elapsed, and text reconciled to 160ms.
cState.snapshot = makeSnapshot(160);
cState.lastFetchedAtMs = mockNow;
renderView();

let waitNodes = container.querySelectorAll("[data-wait-base-ms]");
assert.equal(waitNodes.length, 1);
assert.equal(waitNodes[0].childNodes.some((n) => n.nodeType === 8), false, "must not place lit-html Comment anchor inside .wait-elapsed");
assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "160");
assert.equal(waitNodes[0].getAttribute("data-wait-label"), "160ms");
assert.equal(waitNodes[0].textContent, "160ms");

// Tick +1000ms directly via updateWaiterElapsedDom (DOM-only).
mockNow += 1000;
updateWaiterElapsedDom(container, cState, mockNow);
assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "1160");
assert.equal(waitNodes[0].getAttribute("data-wait-label"), "1.2s");
assert.equal(waitNodes[0].textContent, "1.2s");

// Cycle 2: Subsequent lit-html re-render with a new snapshot whose formatted string ("160ms") matches Cycle 1's _$AH cache!
// Must NOT throw TypeError on missing Comment node and must update DOM back to 160ms cleanly.
cState.snapshot = makeSnapshot(160);
cState.lastFetchedAtMs = mockNow;
renderView();
waitNodes = container.querySelectorAll("[data-wait-base-ms]");
assert.equal(waitNodes.length, 1);
assert.equal(waitNodes[0].getAttribute("data-wait-ms"), "160");
assert.equal(waitNodes[0].getAttribute("data-wait-label"), "160ms");
assert.equal(waitNodes[0].textContent, "160ms");

// Cycle 3: Empty waiters -> waiter returns -> tick -> re-render.
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

// Cycle 4: When lastFetchedAtMs is null, updateWaiterElapsedDom owns fallback reconciliation from data-wait-label
// (resetting stale ticked text without CSS :empty::before).
cState.snapshot = makeSnapshot(400);
cState.lastFetchedAtMs = null;
renderView();
waitNodes = container.querySelectorAll("[data-wait-base-ms]");
assert.equal(waitNodes.length, 1);
assert.equal(waitNodes[0].getAttribute("data-wait-label"), "400ms");
assert.equal(waitNodes[0].textContent, "400ms", "null lastFetchedAtMs must fall back to data-wait-label");

// Also verify null fetchedOrState argument directly, non-numeric base fallback to data-wait-label, and missing label fallback.
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

Date.now = origDateNow;
console.log("regression_contention_lit_html_waiter_rerender_and_tick passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_contention_clear_history_loading_and_stale_error_guards() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");

const db1Root = "projects/p/databases/db1/documents";
const db2Root = "projects/p/databases/db2/documents";

let contentionGetHandler = async () => jsonResponse({{
  databases: [{{
    database: db1Root,
    summary: {{ woundsTotal: 2, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}});
let contentionDeleteHandler = async () => jsonResponse({{}});
let contentionGetCount = 0;

fetchImpl = async (url, init) => {{
  const method = init.method ?? "GET";
  if (url === "/emulator/v1") return jsonResponse([db1Root, db2Root]);
  if (url === "/v1/projects/p/databases/db1") {{
    return jsonResponse({{
      name: "projects/p/databases/db1",
      uid: "u1",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url === "/v1/projects/p/databases/db2") {{
    return jsonResponse({{
      name: "projects/p/databases/db2",
      uid: "u2",
      type: "FIRESTORE_NATIVE",
      databaseEdition: "STANDARD",
      concurrencyMode: "PESSIMISTIC",
    }});
  }}
  if (url.endsWith(":listCollectionIds")) return jsonResponse({{ collectionIds: [] }});
  if (url.startsWith("/emulator/v1/contention") && method === "DELETE") {{
    return contentionDeleteHandler(url);
  }}
  if (url.startsWith("/emulator/v1/contention") && method === "GET") {{
    contentionGetCount++;
    return contentionGetHandler(url);
  }}
  throw new Error("unexpected url: " + method + " " + url);
}};

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
assert.equal(app.state.contention.loading, false, "clearContentionHistory must reset loading immediately after bumping seq");
assert.equal(app.state.contention.clearing, true);

staleGetDef.resolve(jsonResponse({{
  databases: [{{
    database: db1Root,
    summary: {{ woundsTotal: 99, timeoutsTotal: 0, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}}));
await staleRefreshP;
assert.equal(app.state.contention.loading, false);
assert.equal(app.state.contention.snapshot.databases[0].summary.woundsTotal, 2, "stale refresh must be ignored");

failDeleteDef.resolve(jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "clear failed" }} }}, 500));
await clearFailP;
assert.equal(app.state.contention.loading, false, "failed clearContention after invalidated refresh must not strand loading");
assert.equal(app.state.contention.clearing, false);
assert.equal(app.state.contention.error, "[500 INTERNAL] clear failed");
const afterFailMarkup = renderTemplateMarkup(app.renderWorkspace());
assert.equal(afterFailMarkup.includes("Refreshing..."), false, "Refresh button must remain enabled after failed clear");

// 2. Switching scope mid-clear prevents stale error from overwriting newer scope state and suppresses stale post-clear refresh.
const scopeFailDelDef = deferred();
contentionDeleteHandler = () => scopeFailDelDef.promise;
contentionGetHandler = async () => jsonResponse({{
  databases: [{{
    database: db2Root,
    summary: {{ woundsTotal: 5, timeoutsTotal: 1, upgradeAbortsTotal: 0, writeBatchRetriesTotal: 0 }},
    contendedDocuments: [],
    totalContendedDocuments: 0,
    truncated: false,
    recentEvents: [],
  }}],
}});

const clearActiveP = app.clearContentionHistory();
const allBtn = findClickHandler(app.renderWorkspace(), "All Databases");
assert.ok(allBtn);
await allBtn();
assert.equal(app.state.contention.scope, "all");
assert.equal(app.state.contention.error, null);
assert.equal(app.state.contention.snapshot.databases[0].summary.woundsTotal, 5);

scopeFailDelDef.resolve(jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "stale active error" }} }}, 500));
await clearActiveP;
assert.equal(app.state.contention.error, null, "stale error from active scope must not overwrite all-scope state");
assert.equal(app.state.contention.clearing, false);

// Successful clear after scope switch must NOT fire post-clear refreshContention().
const scopeOkDelDef = deferred();
contentionDeleteHandler = () => scopeOkDelDef.promise;
const clearAllP = app.clearContentionHistory();
const activeBtn = findClickHandler(app.renderWorkspace(), "Active DB");
assert.ok(activeBtn);
await activeBtn();
const getsAfterScopeSwitch = contentionGetCount;
scopeOkDelDef.resolve(jsonResponse({{}}));
await clearAllP;
assert.equal(contentionGetCount, getsAfterScopeSwitch, "stale clear success after scope switch must not call refreshContention");

// 3. Switching view mid-clear (to Explorer, and Explorer -> Contention) blocks stale error and post-clear refresh.
const viewFailDelDef = deferred();
contentionDeleteHandler = () => viewFailDelDef.promise;
const clearViewFailP = app.clearContentionHistory();
await app.switchView("explorer");
viewFailDelDef.resolve(jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "stale view error" }} }}, 500));
await clearViewFailP;
assert.equal(app.state.view, "explorer");
assert.equal(app.state.contention.error, null, "failed clear after leaving contention view must not set error");
assert.equal(app.state.contention.clearing, false);

await app.switchView("contention");
const viewCycleDelDef = deferred();
contentionDeleteHandler = () => viewCycleDelDef.promise;
const clearCycleP = app.clearContentionHistory();
await app.switchView("explorer");
await app.switchView("contention");
const getsAfterReenter = contentionGetCount;
viewCycleDelDef.resolve(jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "stale cycle error" }} }}, 500));
await clearCycleP;
assert.equal(app.state.contention.error, null, "failed clear from prior view visit must not overwrite re-entered view error");
assert.equal(contentionGetCount, getsAfterReenter);

// 4. Switching DB mid-clear prevents stale error and stale toast on new DB.
const dbSwitchDelDef = deferred();
contentionDeleteHandler = () => dbSwitchDelDef.promise;
const clearDb1P = app.clearContentionHistory();
app.state.toasts = [];
await app.selectDatabase("p", "db2", [], true);
assert.equal(app.state.activeDatabaseId, "db2");
dbSwitchDelDef.resolve(jsonResponse({{ error: {{ code: 500, status: "INTERNAL", message: "stale db1 error" }} }}, 500));
await clearDb1P;
assert.equal(app.state.contention.error, null, "failed clear from db1 must not overwrite db2 contention error");
assert.equal(app.state.contention.clearing, false);
assert.equal(app.state.toasts.length, 0, "failed clear from db1 must not toast after switching to db2");

console.log("regression_contention_clear_history_loading_and_stale_error_guards passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_editor_key_value_errors_bytes_mode_error_and_narrow_query_overflow() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css = fs::read_to_string(repo_root.join("ui/styles.css")).expect("read ui/styles.css");

    // Verify `@media (max-width: 900px)` sets `.query-results-pane { overflow: visible; }`
    // strictly AFTER the base `.query-results-pane` rule so it wins the CSS cascade.
    let base_results_idx = css
        .find(".query-results-pane {")
        .expect("base .query-results-pane rule must exist");
    let media_900_idx = css
        .find("@media (max-width: 900px)")
        .expect("@media (max-width: 900px) block must exist");
    assert!(
        media_900_idx > base_results_idx,
        "@media (max-width: 900px) must appear after base .query-results-pane rule to override \
         overflow: hidden"
    );
    let slice_brace_block = |src: &str, start_idx: usize, label: &str| -> String {
        let tail = &src[start_idx..];
        let open_rel = tail
            .find('{')
            .unwrap_or_else(|| panic!("{label} must open a brace block"));
        let mut depth = 0usize;
        for (rel, ch) in tail[open_rel..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return tail[..=open_rel + rel].to_owned();
                    }
                }
                _ => {}
            }
        }
        panic!("unterminated brace block for {label}");
    };
    let media_block = slice_brace_block(&css, media_900_idx, "@media (max-width: 900px)");
    let nested_workspace_idx = media_block
        .find(".query-workspace {")
        .expect("@media (max-width: 900px) must include .query-workspace");
    let nested_workspace_block = slice_brace_block(
        &media_block,
        nested_workspace_idx,
        "@media (max-width: 900px) .query-workspace",
    );
    assert!(
        nested_workspace_block.contains("grid-template-columns: minmax(0, 1fr);"),
        "@media (max-width: 900px) .query-workspace must set minmax(0, 1fr), \
         got:\n{nested_workspace_block}"
    );
    let nested_results_idx = media_block
        .find(".query-results-pane {")
        .expect("@media (max-width: 900px) must include .query-results-pane");
    let nested_results_block = slice_brace_block(
        &media_block,
        nested_results_idx,
        "@media (max-width: 900px) .query-results-pane",
    );
    assert!(
        nested_results_block.contains("overflow: visible;")
            && nested_results_block.contains("min-width: 0;"),
        "@media (max-width: 900px) .query-results-pane must set min-width: 0 and overflow: \
         visible, got:\n{nested_results_block}"
    );

    let script = format!(
        r#"{NODE_BOOTSTRAP}
const {{
  createEditorSession,
  draftNodesToFields,
  extractSessionFields,
  renderHybridEditor,
  syncSessionWithDocument,
}} = await import("./ui/components/editor.js");

function findEventHandlers(node, attrPattern, results = []) {{
  if (!node || typeof node !== "object") return results;
  if (Array.isArray(node)) {{
    for (const item of node) findEventHandlers(item, attrPattern, results);
    return results;
  }}
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
    for (let i = 0; i < node.values.length; i++) {{
      const v = node.values[i];
      if (typeof v === "function" && attrPattern.test(node.strings[i])) {{
        let tagBefore = "";
        for (let k = i; k >= 0; k--) {{
          tagBefore = node.strings[k] + tagBefore;
          const lastLt = tagBefore.lastIndexOf("<");
          if (lastLt >= 0) {{
            tagBefore = tagBefore.slice(lastLt);
            break;
          }}
        }}
        results.push({{ tagBefore, handler: v }});
      }}
      findEventHandlers(v, attrPattern, results);
    }}
  }}
  return results;
}}

// 1. Key vs value validation errors are tracked independently and styled on the right inputs.
const docPath = "projects/p/databases/(default)/documents/items/d1";
const session = createEditorSession(docPath, "p", "(default)", {{
  name: docPath,
  fields: {{
    alpha: {{ integerValue: "10" }},
    beta: {{ integerValue: "20" }},
    gamma: {{ stringValue: "ok" }},
  }},
}});

// Duplicate key ("alpha") with VALID value on beta, plus EMPTY key with VALID value on gamma.
session.nodes[1].key = "alpha";
session.nodes[2].key = "";

let check = draftNodesToFields(session.nodes, "p", "(default)");
assert.equal(check.valid, false);
assert.equal(check.errors.get(session.nodes[0].id), undefined);
assert.deepEqual(check.errors.get(session.nodes[1].id), {{
  keyError: 'Duplicate field name "alpha"',
}});
assert.deepEqual(check.errors.get(session.nodes[2].id), {{
  keyError: "Field name is required",
}});

let extracted = extractSessionFields(session);
assert.equal(extracted.valid, false);
assert.equal(typeof extracted.error, "string");
assert.ok(extracted.error.includes('Duplicate field name "alpha"'));

let markup = renderTemplateMarkup(renderHybridEditor({{ session, onChange: () => {{}} }}));
// Row 2 (duplicate key "alpha"): key input has input-error, value input does NOT!
const rowMatches = markup.split('<div class="field-row">').slice(1);
assert.equal(rowMatches.length, 3);
assert.equal(rowMatches[0].includes("input-error"), false, "valid first field must have no input-error");
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

// Simultaneous duplicate key AND invalid integer value must keep BOTH errors without overwriting.
session.nodes[1].text = "not-an-int";
check = draftNodesToFields(session.nodes, "p", "(default)");
assert.equal(check.valid, false);
const combinedErr = check.errors.get(session.nodes[1].id);
assert.ok(combinedErr?.keyError?.includes('Duplicate field name "alpha"'));
assert.ok(combinedErr?.valueError, "valueError must be recorded alongside keyError");

markup = renderTemplateMarkup(renderHybridEditor({{ session, onChange: () => {{}} }}));
const bothRows = markup.split('<div class="field-row">').slice(1);
assert.ok(bothRows[1].includes('class="input field-key-input input-error"'));
assert.ok(bothRows[1].includes('class="input input-error"'));
assert.ok(bothRows[1].includes('Duplicate field name "alpha"'));
assert.ok(bothRows[1].includes(combinedErr.valueError));

// Pure value error on a valid unique key highlights ONLY the value input.
session.nodes[1].key = "beta";
session.nodes[2].key = "gamma";
markup = renderTemplateMarkup(renderHybridEditor({{ session, onChange: () => {{}} }}));
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

// 2. Rejected switchBytesMode("utf8") on non-UTF-8 bytes surfaces inline error without dirtying
//    a clean session, and clears on edit / type-change / mode-switch / editor mode switch / revert.
const binDoc = {{
  name: docPath,
  fields: {{
    raw: {{ bytesValue: "/+8=" }}, // [0xff, 0xef] — invalid UTF-8
  }},
}};
const binSession = createEditorSession(docPath, "p", "(default)", binDoc);
assert.equal(binSession.isDirty, false);
assert.equal(binSession.nodes[0].bytesMode, "base64");
assert.equal(binSession.nodes[0].bytesModeError, undefined);

let rerenderCount = 0;
const renderBin = () =>
  renderHybridEditor({{
    session: binSession,
    onChange: () => {{
      rerenderCount++;
    }},
  }});

let tpl = renderBin();
const findBytesModeSelectHandler = (t) =>
  findEventHandlers(t, /@change=\s*$/).find((h) =>
    h.tagBefore.includes('aria-label="Bytes encoding mode"')
  );
const findTypeSelectHandler = (t) =>
  findEventHandlers(t, /@change=\s*$/).find((h) =>
    h.tagBefore.includes("field-type-select")
  );
const findBytesInputHandler = (t) =>
  findEventHandlers(t, /@input=\s*$/).find((h) =>
    h.tagBefore.includes('aria-label="Bytes value"')
  );

const bytesSelect = findBytesModeSelectHandler(tpl);
assert.ok(bytesSelect, "bytes encoding mode select handler must exist");
const mockSelectTarget = {{ value: "utf8" }};
bytesSelect.handler({{ target: mockSelectTarget }});

assert.equal(binSession.nodes[0].bytesMode, "base64", "mode stays base64 when UTF-8 decode fails");
assert.equal(mockSelectTarget.value, "base64", "DOM select value snaps back to base64");
assert.ok(
  binSession.nodes[0].bytesModeError?.includes("UTF-8"),
  "bytesModeError must explain UTF-8 rejection"
);
assert.equal(binSession.isDirty, false, "rejected switchBytesMode must NOT dirty a clean session");
assert.equal(extractSessionFields(binSession).valid, true, "valid base64 bytes remain saveable");
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
const bytesSelect2 = findBytesModeSelectHandler(renderBin());
bytesSelect2.handler({{ target: {{ value: "hex" }} }});
assert.equal(binSession.nodes[0].bytesMode, "hex");
assert.equal(binSession.nodes[0].text, "ffef");
assert.equal(binSession.nodes[0].bytesModeError, undefined);
assert.equal(binSession.isDirty, false);

// Reject UTF-8 again from hex mode, then (b) editing value text clears bytesModeError.
findBytesModeSelectHandler(renderBin()).handler({{ target: {{ value: "utf8" }} }});
assert.ok(binSession.nodes[0].bytesModeError?.includes("UTF-8"));
findBytesInputHandler(renderBin()).handler({{ target: {{ value: "6869" }} }});
assert.equal(binSession.nodes[0].bytesModeError, undefined, "editing value clears bytesModeError");
assert.equal(binSession.isDirty, true);

// (c) Revert (syncSessionWithDocument) clears bytesModeError & restores clean state.
syncSessionWithDocument(binSession, binDoc);
assert.equal(binSession.isDirty, false);
assert.equal(binSession.nodes[0].bytesModeError, undefined);

// (d) Switching Structured -> Raw -> Structured clears bytesModeError even when rawText is untouched.
findBytesModeSelectHandler(renderBin()).handler({{ target: {{ value: "utf8" }} }});
assert.ok(binSession.nodes[0].bytesModeError?.includes("UTF-8"));
const toRaw = findClickHandler(renderBin(), "Raw ProtoJSON");
assert.ok(toRaw);
toRaw();
assert.equal(binSession.mode, "raw");
assert.equal(binSession.nodes[0].bytesModeError, undefined, "switching to Raw clears bytesModeError");
const toStructured = findClickHandler(renderBin(), "Structured Fields");
assert.ok(toStructured);
toStructured();
assert.equal(binSession.nodes[0].bytesModeError, undefined);
assert.equal(binSession.isDirty, false);

// (e) Changing field type clears bytesModeError.
findBytesModeSelectHandler(renderBin()).handler({{ target: {{ value: "utf8" }} }});
assert.ok(binSession.nodes[0].bytesModeError?.includes("UTF-8"));
const typeSelect = findTypeSelectHandler(renderBin());
assert.ok(typeSelect);
typeSelect.handler({{ target: {{ value: "stringValue" }} }});
assert.equal(binSession.nodes[0].type, "stringValue");
assert.equal(binSession.nodes[0].bytesModeError, undefined, "changing type clears bytesModeError");

// 3. Manual DraftNode literal omitting optional bytesModeError still validates cleanly.
const manualCheck = draftNodesToFields(
  [{{
    id: 999,
    key: "k",
    type: "bytesValue",
    text: "AAEC",
    boolVal: false,
    latText: "0",
    lngText: "0",
    bytesMode: "base64",
    children: [],
  }}],
  "p",
  "(default)"
);
assert.equal(manualCheck.valid, true);

console.log("regression_editor_key_value_errors_bytes_mode_error_and_narrow_query_overflow passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn regression_dark_theme_css_tokens_and_wcag_contrast() {
    use std::collections::{BTreeMap, BTreeSet};

    fn parse_css_tokens(block: &str) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for line in block.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("--")
                && let Some((name, val)) = rest.split_once(':')
            {
                let key = format!("--{}", name.trim());
                let value = val.trim().trim_end_matches(';').trim().to_owned();
                map.insert(key, value);
            }
        }
        map
    }

    fn extract_braced_body(src: &str, open_brace_idx: usize) -> Option<(usize, &str)> {
        let bytes = src.as_bytes();
        if bytes.get(open_brace_idx) != Some(&b'{') {
            return None;
        }
        let mut depth = 0usize;
        for (offset, &b) in bytes[open_brace_idx..].iter().enumerate() {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        let end = open_brace_idx + offset;
                        return Some((end + 1, &src[open_brace_idx + 1..end]));
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_hex_rgb(hex: &str) -> Option<(u8, u8, u8)> {
        let digits = hex.strip_prefix('#')?;
        if digits.len() != 6 {
            return None;
        }
        let r = u8::from_str_radix(&digits[0..2], 16).ok()?;
        let g = u8::from_str_radix(&digits[2..4], 16).ok()?;
        let b = u8::from_str_radix(&digits[4..6], 16).ok()?;
        Some((r, g, b))
    }

    fn relative_luminance(rgb: (u8, u8, u8)) -> f64 {
        fn channel(c: u8) -> f64 {
            let s = f64::from(c) / 255.0;
            if s <= 0.03928 {
                s / 12.92
            } else {
                ((s + 0.055) / 1.055).powf(2.4)
            }
        }
        0.2126 * channel(rgb.0) + 0.7152 * channel(rgb.1) + 0.0722 * channel(rgb.2)
    }

    fn contrast_ratio(fg: &str, bg: &str) -> f64 {
        let fg_rgb = parse_hex_rgb(fg).unwrap_or_else(|| panic!("invalid hex fg: {fg}"));
        let bg_rgb = parse_hex_rgb(bg).unwrap_or_else(|| panic!("invalid hex bg: {bg}"));
        let l1 = relative_luminance(fg_rgb);
        let l2 = relative_luminance(bg_rgb);
        let (hi, lo) = if l1 >= l2 { (l1, l2) } else { (l2, l1) };
        (hi + 0.05) / (lo + 0.05)
    }

    fn scan_stray_color_literals(
        src_path: &str,
        content: &str,
        is_css: bool,
        out: &mut Vec<String>,
    ) {
        let mut in_block_comment = false;
        let mut in_html_comment = false;
        for (idx, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if in_block_comment {
                if trimmed.contains("*/") {
                    in_block_comment = false;
                }
                continue;
            }
            if in_html_comment {
                if trimmed.contains("-->") {
                    in_html_comment = false;
                }
                continue;
            }
            if trimmed.starts_with("/*") {
                if !trimmed.contains("*/") {
                    in_block_comment = true;
                }
                continue;
            }
            if trimmed.starts_with("<!--") {
                if !trimmed.contains("-->") {
                    in_html_comment = true;
                }
                continue;
            }
            if trimmed.starts_with('*') || trimmed.starts_with("//") {
                continue;
            }

            // Check for `#RGB` / `#RRGGBB` / `#RRGGBBAA` hex color literals (excluding CSS ID
            // selectors like `#topbar`, URL fragments like `https://.../#deadbeef`, and JS
            // template strings like `#${id}`).
            let bytes = trimmed.as_bytes();
            for (i, &b) in bytes.iter().enumerate() {
                if b == b'#' {
                    let prev_ok = i == 0
                        || matches!(
                            bytes[i - 1],
                            b' ' | b'\t' | b':' | b',' | b'(' | b'"' | b'\'' | b'`' | b'='
                        );
                    let mut j = i + 1;
                    while j < bytes.len()
                        && (bytes[j].is_ascii_alphanumeric()
                            || bytes[j] == b'_'
                            || bytes[j] == b'$')
                    {
                        j += 1;
                    }
                    let token = &trimmed[i + 1..j];
                    let is_hex_len = matches!(token.len(), 3 | 4 | 6 | 8);
                    let prefix = &trimmed[..i];
                    let in_style_context = if is_css {
                        prefix.contains(':')
                    } else {
                        prefix.contains(':') || prefix.contains('=')
                    };
                    if prev_ok
                        && in_style_context
                        && is_hex_len
                        && token.bytes().all(|c| c.is_ascii_hexdigit())
                    {
                        out.push(format!("{src_path}:{}: {trimmed}", idx + 1));
                    }
                }
            }

            for func in ["rgb(", "rgba(", "hsl(", "hsla("] {
                let mut search_from = 0usize;
                while let Some(rel) = trimmed[search_from..].find(func) {
                    let pos = search_from + rel;
                    let word_boundary = pos == 0
                        || !(bytes[pos - 1].is_ascii_alphanumeric() || bytes[pos - 1] == b'_');
                    if word_boundary {
                        out.push(format!("{src_path}:{}: {trimmed}", idx + 1));
                        break;
                    }
                    search_from = pos + func.len();
                }
            }
        }
    }

    const UI_SOURCE_FILES: &[&str] = &[
        "ui/index.html",
        "ui/styles.css",
        "ui/app.js",
        "ui/components/combobox.js",
        "ui/components/editor.js",
        "ui/components/explorer.js",
        "ui/components/modals.js",
        "ui/components/query.js",
        "ui/components/query-suggestions.js",
        "ui/components/contention.js",
    ];

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let index_html =
        fs::read_to_string(repo_root.join("ui/index.html")).expect("read ui/index.html");
    let head_start = index_html.find("<head>").expect("ui/index.html <head>");
    let head_end = index_html[head_start..]
        .find("</head>")
        .expect("ui/index.html </head>");
    let head_html = &index_html[head_start..head_start + head_end];
    assert!(
        head_html.contains(r#"<meta name="color-scheme" content="light dark" />"#),
        "ui/index.html <head> must declare `<meta name=\"color-scheme\" content=\"light dark\" \
         />` to prevent dark-mode FOUC"
    );

    let css = fs::read_to_string(repo_root.join("ui/styles.css")).expect("read ui/styles.css");

    // 1. Locate top-level `:root { ... }` and `@media (prefers-color-scheme: dark) { :root { ... }
    //    }`, plus semantic rule wiring for `.sse-dot.error` and semantic toasts.
    let light_root_start = css
        .find(":root {")
        .expect("top-level :root block must exist");
    let light_brace = light_root_start + ":root ".len();
    let (light_root_end, light_root_body) =
        extract_braced_body(&css, light_brace).expect("top-level :root must be balanced");
    assert!(
        light_root_body.contains("color-scheme: light dark;"),
        ":root must declare `color-scheme: light dark;`"
    );

    let dark_media_start = css
        .find("@media (prefers-color-scheme: dark)")
        .expect("@media (prefers-color-scheme: dark) block must exist");
    let dark_media_brace = dark_media_start
        + css[dark_media_start..]
            .find('{')
            .expect("@media dark opening brace");
    let (dark_media_end, dark_media_body) =
        extract_braced_body(&css, dark_media_brace).expect("@media dark must be balanced");
    let dark_root_rel = dark_media_body
        .find(":root {")
        .expect("@media (prefers-color-scheme: dark) must contain a :root block");
    let (_, dark_root_body) = extract_braced_body(dark_media_body, dark_root_rel + ":root ".len())
        .expect("dark :root must be balanced");

    let sse_error_start = css
        .find(".sse-dot.error {")
        .expect(".sse-dot.error rule must exist");
    let (_, sse_error_body) = extract_braced_body(&css, sse_error_start + ".sse-dot.error ".len())
        .expect(".sse-dot.error rule must be balanced");
    assert!(
        sse_error_body.contains("background: var(--danger-fg);"),
        ".sse-dot.error must use var(--danger-fg) for dark-mode non-text contrast"
    );

    for (selector, token) in [
        (".toast.error", "--toast-error-border"),
        (".toast.success", "--toast-success-border"),
    ] {
        let rule_sig = format!("{selector} {{");
        let start = css
            .find(&rule_sig)
            .unwrap_or_else(|| panic!("{selector} rule must exist"));
        let (_, body) = extract_braced_body(&css, start + selector.len() + 1)
            .unwrap_or_else(|| panic!("{selector} rule must be balanced"));
        assert!(
            body.contains(&format!("border-color: var({token});")),
            "{selector} must override `border-color: var({token});` so semantic toasts do not \
             inherit --toast-border"
        );
    }

    // 2. Verify zero stray hardcoded hex or rgb/rgba/hsl/hsla color literals outside `:root` token
    //    blocks across CSS, HTML, and Web UI JS modules.
    let mut non_token_css = String::with_capacity(css.len());
    let mut ranges = [
        (light_root_start, light_root_end),
        (dark_media_start, dark_media_end),
    ];
    ranges.sort_by_key(|r| r.0);
    let mut cursor = 0usize;
    for (start, end) in ranges {
        non_token_css.push_str(&css[cursor..start]);
        cursor = end;
    }
    non_token_css.push_str(&css[cursor..]);

    let mut stray_literals = Vec::new();
    for &src_path in UI_SOURCE_FILES {
        if src_path == "ui/styles.css" {
            scan_stray_color_literals(src_path, &non_token_css, true, &mut stray_literals);
        } else {
            let text = fs::read_to_string(repo_root.join(src_path))
                .unwrap_or_else(|e| panic!("failed reading {src_path}: {e}"));
            scan_stray_color_literals(src_path, &text, false, &mut stray_literals);
        }
    }
    assert!(
        stray_literals.is_empty(),
        "found hardcoded color literals outside :root token blocks:\n{}",
        stray_literals.join("\n")
    );

    // 3. Every color/shadow token in light `:root` must have an explicit dark `:root` entry (and
    //    vice versa), and every `var(--...)` referenced across UI files must be defined in `:root`.
    let light_tokens = parse_css_tokens(light_root_body);
    let dark_tokens = parse_css_tokens(dark_root_body);

    for (name, val) in &light_tokens {
        let is_theme_token = val.starts_with('#')
            || val.contains("rgba(")
            || val.contains("rgb(")
            || name.contains("border")
            || name.contains("bg")
            || name.contains("text")
            || name.contains("shadow");
        if is_theme_token && name != "--radius-sm" && name != "--radius-md" && name != "--radius-lg"
        {
            assert!(
                dark_tokens.contains_key(name),
                "theme token {name} (light={val}) is missing from @media (prefers-color-scheme: \
                 dark) :root"
            );
        }
    }
    for (name, val) in &dark_tokens {
        assert!(
            light_tokens.contains_key(name),
            "dark theme token {name} (dark={val}) is missing from top-level :root"
        );
    }

    let mut referenced_vars = BTreeSet::new();
    for &src_path in UI_SOURCE_FILES {
        let text = fs::read_to_string(repo_root.join(src_path))
            .unwrap_or_else(|e| panic!("failed reading {src_path}: {e}"));
        let mut rest = text.as_str();
        while let Some(pos) = rest.find("var(--") {
            let after = &rest[pos + "var(".len()..];
            if let Some(end) = after.find([')', ',', ' ']) {
                referenced_vars.insert(after[..end].trim().to_owned());
            }
            rest = after;
        }
    }
    for var_name in &referenced_vars {
        assert!(
            light_tokens.contains_key(var_name),
            "referenced CSS custom property {var_name} is not defined in :root"
        );
    }

    // 4. Verify WCAG AA (>= 4.5:1) text contrast and WCAG 1.4.11 (>= 3.0:1) non-text UI indicator
    //    contrast for all dark-mode semantic token pairs (including surfaces, buttons, links,
    //    errors, banners, pills, toasts, all 11 type badges, and Contention tab elements).
    let dark_pairs = [
        ("--text-main", "--bg-app"),
        ("--text-main", "--bg-surface"),
        ("--text-main", "--bg-surface-alt"),
        ("--text-main", "--bg-card-subtle"),
        ("--text-main", "--bg-subtle"),
        ("--text-main", "--bg-hover"),
        ("--text-main", "--input-error-bg"),
        ("--text-muted", "--bg-app"),
        ("--text-muted", "--bg-surface"),
        ("--text-muted", "--bg-surface-alt"),
        ("--text-muted", "--bg-subtle"),
        ("--text-muted", "--bg-card-subtle"),
        ("--text-faint", "--bg-surface"),
        ("--text-faint", "--bg-surface-alt"),
        ("--text-faint", "--bg-subtle"),
        ("--text-faint", "--bg-card-subtle"),
        ("--text-on-solid", "--primary"),
        ("--text-on-solid", "--primary-hover"),
        ("--text-on-solid", "--accent"),
        ("--text-on-solid", "--accent-hover"),
        ("--text-on-solid", "--danger"),
        ("--text-on-solid", "--danger-hover"),
        ("--primary-link", "--bg-surface"),
        ("--primary-link", "--bg-selected"),
        ("--primary-link", "--bg-card-subtle"),
        ("--danger-fg", "--bg-surface"),
        ("--danger-fg", "--bg-card-subtle"),
        ("--danger-text", "--bg-surface"),
        ("--warning", "--bg-surface"),
        ("--warning", "--bg-card-subtle"),
        ("--primary-text", "--primary-soft"),
        ("--accent-text", "--accent-soft"),
        ("--danger-text", "--danger-soft"),
        ("--warning-text", "--warning-soft"),
        ("--toast-text", "--toast-bg"),
        ("--toast-text", "--toast-error-bg"),
        ("--toast-text", "--toast-success-bg"),
        ("--badge-int-text", "--badge-int-bg"),
        ("--badge-double-text", "--badge-double-bg"),
        ("--badge-string-text", "--badge-string-bg"),
        ("--badge-bool-text", "--badge-bool-bg"),
        ("--badge-null-text", "--badge-null-bg"),
        ("--badge-ts-text", "--badge-ts-bg"),
        ("--badge-ref-text", "--badge-ref-bg"),
        ("--badge-bytes-text", "--badge-bytes-bg"),
        ("--badge-geo-text", "--badge-geo-bg"),
        ("--badge-struct-text", "--badge-struct-bg"),
        ("--badge-vector-text", "--badge-vector-bg"),
    ];

    for (fg_tok, bg_tok) in dark_pairs {
        let fg = dark_tokens
            .get(fg_tok)
            .unwrap_or_else(|| panic!("missing dark token {fg_tok}"));
        let bg = dark_tokens
            .get(bg_tok)
            .unwrap_or_else(|| panic!("missing dark token {bg_tok}"));
        let ratio = contrast_ratio(fg, bg);
        assert!(
            ratio >= 4.5,
            "dark theme contrast for ({fg_tok}={fg} on {bg_tok}={bg}) is {ratio:.2}:1, below WCAG \
             AA 4.5:1"
        );
    }

    let dark_non_text_pairs = [
        ("--success", "--bg-subtle"),
        ("--warning", "--bg-subtle"),
        ("--danger-fg", "--bg-subtle"),
        ("--sse-dot-idle", "--bg-subtle"),
    ];

    for (fg_tok, bg_tok) in dark_non_text_pairs {
        let fg = dark_tokens
            .get(fg_tok)
            .unwrap_or_else(|| panic!("missing dark token {fg_tok}"));
        let bg = dark_tokens
            .get(bg_tok)
            .unwrap_or_else(|| panic!("missing dark token {bg_tok}"));
        let ratio = contrast_ratio(fg, bg);
        assert!(
            ratio >= 3.0,
            "dark theme non-text contrast for ({fg_tok}={fg} on {bg_tok}={bg}) is {ratio:.2}:1, \
             below WCAG 1.4.11 3.0:1"
        );
    }
}

#[test]
fn slice1_values_and_aggregation_builder_validation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const values = await import("./ui/values.js");
const query = await import("./ui/components/query.js");

// 1. findNonFiniteNumber and isReservedName exports.
assert.equal(values.findNonFiniteNumber({{ a: 1, b: [-0, "Infinity", "NaN", {{ c: 42 }}] }}), null);
assert.equal(values.findNonFiniteNumber(Infinity), "$");
assert.equal(values.findNonFiniteNumber({{ a: {{ b: [1, -Infinity] }} }}), "$.a.b[1]");
assert.equal(values.findNonFiniteNumber([1, NaN], "tags"), "tags[1]");
assert.equal(values.findNonFiniteNumber({{ "foo bar": Infinity }}), '$["foo bar"]');

for (const reserved of ["__a__", "__name__", "__custom__", "__proto__"]) {{
  assert.equal(values.isReservedName(reserved), true, "expected reserved: " + reserved);
}}
for (const allowed of ["____", "___", "_a_", "__a", "a__", "", "normal"]) {{
  assert.equal(values.isReservedName(allowed), false, "expected non-reserved: " + allowed);
}}

// 2. buildAggregationQueryFromVisual: default COUNT, SUM/AVG, empty field, 1..=5 cap.
{{
  const qState = query.createQueryRunnerState("orders");
  const built = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(built.valid, true, built.error);
  assert.deepEqual(built.query, {{
    structuredQuery: {{
      from: [{{ collectionId: "orders" }}],
      limit: 50,
    }},
    aggregations: [{{ count: {{}} }}],
  }});

  qState.aggregations = [
    {{ id: 1, op: "COUNT", fieldPath: "", alias: "total" }},
    {{ id: 2, op: "SUM", fieldPath: "amount", alias: "" }},
    {{ id: 3, op: "AVG", fieldPath: "score", alias: "" }},
  ];
  const mixed = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(mixed.valid, true, mixed.error);
  assert.deepEqual(mixed.query.aggregations, [
    {{ count: {{}}, alias: "total" }},
    {{ sum: {{ field: {{ fieldPath: "amount" }} }} }},
    {{ avg: {{ field: {{ fieldPath: "score" }} }} }},
  ]);

  // Empty SUM/AVG field path rejected.
  qState.aggregations = [{{ id: 1, op: "SUM", fieldPath: "  ", alias: "" }}];
  const emptySum = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(emptySum.valid, false);
  assert.ok(emptySum.error.includes("SUM"));

  qState.aggregations = [{{ id: 1, op: "AVG", fieldPath: "", alias: "" }}];
  const emptyAvg = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(emptyAvg.valid, false);
  assert.ok(emptyAvg.error.includes("AVG"));

  // 0 and >5 aggregations rejected; 5 accepted.
  qState.aggregations = [];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, false);
  qState.aggregations = Array.from({{ length: 5 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: "",
  }}));
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, true);
  qState.aggregations.push({{ id: 6, op: "COUNT", fieldPath: "", alias: "" }});
  const overCap = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(overCap.valid, false);
  assert.ok(overCap.error.includes("5"));
}}

// 3. Alias collision (explicit field_1 vs omitted), reserved, >1500 UTF-8 bytes, and prototype keys.
{{
  const qState = query.createQueryRunnerState("orders");
  qState.aggregations = [
    {{ id: 1, op: "COUNT", fieldPath: "", alias: "field_1" }},
    {{ id: 2, op: "COUNT", fieldPath: "", alias: "" }},
  ];
  const collision = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(collision.valid, false);
  assert.ok(collision.error.includes("field_1"));

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "__custom__" }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, false);

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "____" }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, true);

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "a".repeat(1500) }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, true);

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "€".repeat(501) }}];
  const tooLong = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(tooLong.valid, false);
  assert.ok(tooLong.error.includes("1500"));

  // Prototype keys: Object.prototype keys allowed once, duplicates rejected safely; __proto__ rejected on Standard, allowed on Enterprise.
  qState.aggregations = [
    {{ id: 1, op: "COUNT", fieldPath: "", alias: "constructor" }},
    {{ id: 2, op: "COUNT", fieldPath: "", alias: "toString" }},
    {{ id: 3, op: "COUNT", fieldPath: "", alias: "hasOwnProperty" }},
  ];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, true);
  qState.aggregations.push({{ id: 4, op: "COUNT", fieldPath: "", alias: "constructor" }});
  const protoDup = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(protoDup.valid, false);
  assert.ok(protoDup.error.includes("constructor"));

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "__name__" }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, false);
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: false }}).valid, false);
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }}).valid, true);

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "__proto__" }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, false);
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }}).valid, true);
  qState.aggregations.push({{ id: 2, op: "COUNT", fieldPath: "", alias: "__proto__" }});
  const entProtoDup = query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }});
  assert.equal(entProtoDup.valid, false);
  assert.ok(entProtoDup.error.includes("__proto__"));

  // Enterprise row cap: 6 and 64 allowed, 65 rejected; 1501-byte alias still rejected on Enterprise.
  qState.aggregations = Array.from({{ length: 6 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: `agg_${{idx}}`,
  }}));
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid, false);
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }}).valid, true);

  qState.aggregations = Array.from({{ length: 64 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: "",
  }}));
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }}).valid, true);
  qState.aggregations.push({{ id: 65, op: "COUNT", fieldPath: "", alias: "" }});
  const entOverCap = query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }});
  assert.equal(entOverCap.valid, false);
  assert.ok(entOverCap.error.includes("64"));

  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "€".repeat(501) }}];
  assert.equal(query.buildAggregationQueryFromVisual(qState, "p", "(default)", {{ enterprise: true }}).valid, false);
}}

// 4. UI row controls: sequential placeholders, COUNT clears/disables fieldPath, cap/delete disabled state.
{{
  const qState = query.createQueryRunnerState("orders");
  qState.queryKind = "aggregations";
  qState.aggregations = [
    {{ id: 1, op: "SUM", fieldPath: "amount", alias: "total" }},
    {{ id: 2, op: "AVG", fieldPath: "score", alias: "" }},
    {{ id: 3, op: "COUNT", fieldPath: "", alias: "" }},
  ];
  const props = {{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["orders"],
    state: qState,
    onRerender: () => {{}},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }};
  const markup = renderTemplateMarkup(query.renderQueryRunner(props));
  assert.ok(markup.includes('placeholder="field_1"'));
  assert.ok(markup.includes('placeholder="field_2"'));
  assert.equal(markup.includes('placeholder="field_3"'), false);

  // Changing SUM row op to COUNT clears row.fieldPath and disables its field input.
  const findAggSelect = (node) => {{
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) {{
      for (const item of node) {{
        const hit = findAggSelect(item);
        if (hit) return hit;
      }}
      return null;
    }}
    if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
      for (let i = 0; i < node.values.length; i++) {{
        if (typeof node.values[i] === "function" && /@change=\s*$/.test(node.strings[i])) {{
          let before = "";
          for (let k = i; k >= 0; k--) {{
            before = node.strings[k] + before;
            if (before.includes("<select")) break;
          }}
          if (before.includes("<select") && node.values[i - 2] === "Aggregation 1 operator") {{
            return node.values[i];
          }}
        }}
        const nested = findAggSelect(node.values[i]);
        if (nested) return nested;
      }}
    }}
    return null;
  }};
  const tpl = query.renderQueryRunner(props);
  const onOpChange = findAggSelect(tpl);
  assert.ok(onOpChange, "expected Aggregation 1 operator select handler");
  onOpChange({{ target: {{ value: "COUNT" }} }});
  assert.equal(qState.aggregations[0].op, "COUNT");
  assert.equal(qState.aggregations[0].fieldPath, "");
  const afterCountMarkup = renderTemplateMarkup(query.renderQueryRunner(props)).replace(/\s+/g, " ");
  assert.ok(afterCountMarkup.includes('aria-label="Aggregation 1 field path" placeholder="—" disabled'));

  // 1-row delete disabled; 5-row add disabled on Standard, enabled on Enterprise until 64 rows.
  qState.aggregations = [{{ id: 1, op: "COUNT", fieldPath: "", alias: "" }}];
  const oneRowMarkup = renderTemplateMarkup(query.renderQueryRunner(props)).replace(/\s+/g, " ");
  assert.ok(oneRowMarkup.includes('disabled title="Remove aggregation" aria-label="Remove aggregation 1"'));
  qState.aggregations = Array.from({{ length: 5 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: "",
  }}));
  const fiveRowMarkup = renderTemplateMarkup(query.renderQueryRunner(props)).replace(/\s+/g, " ");
  assert.ok(fiveRowMarkup.includes('disabled title="Maximum 5 aggregations per query"'));
  const entFiveRowMarkup = renderTemplateMarkup(query.renderQueryRunner({{ ...props, enterprise: true }})).replace(/\s+/g, " ");
  assert.equal(entFiveRowMarkup.includes("Maximum 5 aggregations per query"), false);
  assert.ok(entFiveRowMarkup.includes('title="Add aggregation"'));
  qState.aggregations = Array.from({{ length: 64 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: "",
  }}));
  const entSixtyFourMarkup = renderTemplateMarkup(query.renderQueryRunner({{ ...props, enterprise: true }})).replace(/\s+/g, " ");
  assert.ok(entSixtyFourMarkup.includes('disabled title="Maximum 64 aggregations per query"'));
}}

console.log("slice1_values_and_aggregation_builder_validation passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn slice1_modes_limit_raw_validation_and_execution_isolation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const app = await import("./ui/app.js");
const query = await import("./ui/components/query.js");
const stateMod = await import("./ui/state.js");

const makeProps = (qState) => ({{
  projectId: "p",
  databaseId: "(default)",
  knownCollections: ["orders"],
  state: qState,
  onRerender: () => {{}},
  onExecuteQuery: () => {{}},
  onOpenResultDocument: () => {{}},
}});

// 1. limitUserModified toggling ("50" <-> "" when untouched; preserved once modified) & inline "!" warning.
{{
  const qState = query.createQueryRunnerState("orders");
  assert.equal(qState.limitText, "50");
  assert.equal(qState.limitUserModified, false);
  const docMarkup = renderTemplateMarkup(query.renderQueryRunner(makeProps(qState)));
  assert.equal(docMarkup.includes("query-limit-warning"), false);
  assert.ok(docMarkup.includes('min="0"'));

  findClickHandler(query.renderQueryRunner(makeProps(qState)), "Aggregations")();
  assert.equal(qState.queryKind, "aggregations");
  assert.equal(qState.limitText, "");
  assert.equal(renderTemplateMarkup(query.renderQueryRunner(makeProps(qState))).includes("query-limit-warning"), false);

  findClickHandler(query.renderQueryRunner(makeProps(qState)), "Documents")();
  assert.equal(qState.queryKind, "documents");
  assert.equal(qState.limitText, "50");

  // User modifies limit to "0" -> preserved across switches and shows inline "!" warning in Aggregations.
  const limitInput = findInputHandler(query.renderQueryRunner(makeProps(qState)), 'placeholder="No limit"');
  assert.ok(limitInput);
  limitInput({{ target: {{ value: "0" }} }});
  assert.equal(qState.limitUserModified, true);
  assert.equal(qState.limitText, "0");

  findClickHandler(query.renderQueryRunner(makeProps(qState)), "Aggregations")();
  assert.equal(qState.limitText, "0");
  const aggLimitMarkup = renderTemplateMarkup(query.renderQueryRunner(makeProps(qState)));
  assert.ok(aggLimitMarkup.includes('class="query-limit-warning"'));
  assert.ok(aggLimitMarkup.includes('title="Aggregation runs over the limited query window"'));
  assert.ok(aggLimitMarkup.includes('aria-label="Aggregation runs over the limited query window"'));

  const builtLimitZero = query.buildAggregationQueryFromVisual(qState, "p", "(default)");
  assert.equal(builtLimitZero.valid, true);
  assert.equal(builtLimitZero.query.structuredQuery.limit, 0);
}}

// 2. Visual -> Raw transitions: pristine pass-through, valid compile, invalid blocking, & Raw queryKind buffer swap.
{{
  const pristine = query.createQueryRunnerState();
  pristine.rawQueryText = '{{"from":[{{"collectionId":"custom"}}]}}';
  pristine.rawAggregationText = '{{"structuredQuery":{{"from":[{{"collectionId":"custom"}}]}},"aggregations":[{{"count":{{}}}}]}}';

  findClickHandler(query.renderQueryRunner(makeProps(pristine)), "Raw JSON")();
  assert.equal(pristine.mode, "raw");
  assert.equal(pristine.error, null);
  assert.equal(pristine.rawQueryText, '{{"from":[{{"collectionId":"custom"}}]}}');

  // Direct queryKind swap in Raw mode preserves both buffers and switches Run button label.
  const rawAggBtn = findClickHandler(query.renderQueryRunner(makeProps(pristine)), "Aggregations");
  rawAggBtn();
  assert.equal(pristine.mode, "raw");
  assert.equal(pristine.queryKind, "aggregations");
  assert.equal(pristine.rawQueryText, '{{"from":[{{"collectionId":"custom"}}]}}');
  assert.equal(pristine.rawAggregationText, '{{"structuredQuery":{{"from":[{{"collectionId":"custom"}}]}},"aggregations":[{{"count":{{}}}}]}}');
  assert.ok(renderTemplateMarkup(query.renderQueryRunner(makeProps(pristine))).includes("▶ Run Aggregation"));

  // Pristine Aggregations Visual -> Raw pass-through preserves rawAggregationText.
  findClickHandler(query.renderQueryRunner(makeProps(pristine)), "Visual")();
  pristine.rawAggregationText = '{{"structuredQuery":{{"from":[{{"collectionId":"kept"}}]}},"aggregations":[{{"count":{{"upTo":"5"}}}}]}}';
  findClickHandler(query.renderQueryRunner(makeProps(pristine)), "Raw JSON")();
  assert.equal(pristine.mode, "raw");
  assert.equal(pristine.error, null);
  assert.ok(pristine.rawAggregationText.includes('"upTo":"5"'));

  // Non-pristine valid Visual -> Raw compiles into active raw buffer.
  const validNonPristine = query.createQueryRunnerState("orders");
  findClickHandler(query.renderQueryRunner(makeProps(validNonPristine)), "Raw JSON")();
  assert.equal(validNonPristine.mode, "raw");
  assert.ok(validNonPristine.rawQueryText.includes('"collectionId": "orders"'));

  // Invalid user-entered Visual -> Raw blocks and stays in visual with state.error.
  const invalidLimit = query.createQueryRunnerState("orders");
  invalidLimit.limitText = "-1";
  invalidLimit.limitUserModified = true;
  findClickHandler(query.renderQueryRunner(makeProps(invalidLimit)), "Raw JSON")();
  assert.equal(invalidLimit.mode, "visual");
  assert.ok(invalidLimit.error.includes("non-negative integer"));

  const invalidSum = query.createQueryRunnerState("orders");
  invalidSum.queryKind = "aggregations";
  invalidSum.aggregations = [{{ id: 1, op: "SUM", fieldPath: "", alias: "" }}];
  findClickHandler(query.renderQueryRunner(makeProps(invalidSum)), "Raw JSON")();
  assert.equal(invalidSum.mode, "visual");
  assert.ok(invalidSum.error.includes("SUM aggregation"));

  const invalidSubpathOnly = query.createQueryRunnerState("", "rooms/r1");
  findClickHandler(query.renderQueryRunner(makeProps(invalidSubpathOnly)), "Raw JSON")();
  assert.equal(invalidSubpathOnly.mode, "visual");
  assert.ok(invalidSubpathOnly.error.includes("Collection ID is required"));
}}

// 3. Raw JSON client-side validation & bare vs optional-wrapper execution.
{{
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "(default)";

  const assertRawError = async (queryKind, rawText, expectedSubstr) => {{
    const qState = query.createQueryRunnerState("orders");
    qState.queryKind = queryKind;
    qState.mode = "raw";
    if (queryKind === "aggregations") qState.rawAggregationText = rawText;
    else qState.rawQueryText = rawText;
    app.state.queryState = qState;
    let called = false;
    fetchImpl = async () => {{
      called = true;
      throw new Error("fetch should not be called");
    }};
    await app.executeCurrentQuery();
    assert.equal(called, false, "fetch must not run for invalid raw payload: " + rawText);
    assert.ok(
      qState.error && qState.error.includes(expectedSubstr),
      `expected error containing ${{JSON.stringify(expectedSubstr)}}, got ${{qState.error}}`
    );
  }};

  for (const nonObj of ["[]", "123", '"abc"', "null", "true"]) {{
    await assertRawError("documents", nonObj, "Raw JSON must be a JSON object.");
    await assertRawError("aggregations", nonObj, "Raw JSON must be a JSON object.");
  }}
  await assertRawError("documents", '{{"from":[{{"collectionId":"orders"}}],"limit":9007199254740993}}', "9007199254740993");
  await assertRawError("aggregations", '{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}},"aggregations":[{{"count":{{"upTo":9007199254740993}}}}]}}', "9007199254740993");
  await assertRawError("documents", '{{"from":[{{"collectionId":"orders"}}],"limit":1e309}}', "Raw JSON contains a non-finite number at $.limit (number overflow).");
  await assertRawError("aggregations", '{{"structuredAggregationQuery":{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}],"limit":-1e309}},"aggregations":[{{"count":{{}}}}]}}}}', "Raw JSON contains a non-finite number at $.structuredAggregationQuery.structuredQuery.limit (number overflow).");

  // Envelope / unsupported keys rejected in both bare and wrapped payloads.
  for (const badKey of ["readTime", "transaction", "newTransaction", "explainOptions", "requestOptions"]) {{
    await assertRawError("documents", `{{"from":[{{"collectionId":"orders"}}], "${{badKey}}": null}}`, "Unsupported top-level field(s) in Raw JSON");
    await assertRawError("documents", `{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}}, "${{badKey}}": null}}`, "Unsupported top-level field(s) in Raw JSON alongside structuredQuery");
    await assertRawError("aggregations", `{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}}, "aggregations":[{{"count":{{}}}}], "${{badKey}}": null}}`, "Unsupported top-level field(s) in Raw JSON");
    await assertRawError("aggregations", `{{"structuredAggregationQuery":{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}}, "aggregations":[{{"count":{{}}}}]}}, "${{badKey}}": null}}`, "Unsupported top-level field(s) in Raw JSON alongside structuredAggregationQuery");
  }}
  await assertRawError("aggregations", '{{"structuredAggregationQuery":{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}},"aggregations":[{{"count":{{}}}}]}},"structuredQuery":{{}}}}', "alongside structuredAggregationQuery");
  await assertRawError("documents", '{{"structuredQuery": null}}', '"structuredQuery" in Raw JSON must be a JSON object.');

  // Bare and single-wrapper payloads execute cleanly and preserve separate Documents vs Aggregations slots.
  const requests = [];
  fetchImpl = async (url, init) => {{
    requests.push({{ url, body: JSON.parse(init.body), hasSignal: Boolean(init.signal) }});
    if (url.endsWith(":runQuery")) {{
      return jsonResponse([{{
        document: {{ name: "projects/p/databases/(default)/documents/orders/o1", fields: {{}} }},
        readTime: "2026-01-01T00:00:01Z",
      }}]);
    }}
    if (url.endsWith(":runAggregationQuery")) {{
      return jsonResponse(
        '[{{"result":{{"aggregateFields":{{"big_count":{{"integerValue":"9007199254740993"}},"neg_zero":{{"doubleValue":-0.0}},"empty_avg":{{"nullValue":null}}}}}},"readTime":"2026-01-01T00:00:02Z"}}]'
      );
    }}
    throw new Error("unexpected url: " + url);
  }};

  const shared = query.createQueryRunnerState("orders");
  shared.mode = "raw";
  shared.rawQueryText = '{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}],"limit":5}}}}';
  shared.rawAggregationText = '{{"structuredAggregationQuery":{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}],"orderBy":[{{"field":{{"fieldPath":"age"}},"direction":"ASCENDING"}}],"limit":0}},"aggregations":[{{"count":{{}},"alias":"big_count"}}]}}}}';
  app.state.queryState = shared;
  await app.executeCurrentQuery();
  assert.equal(shared.results.length, 1);
  assert.equal(shared.readTime, "2026-01-01T00:00:01Z");
  assert.equal(typeof shared.elapsedMs, "number");
  assert.equal(shared.aggResults, null);

  shared.queryKind = "aggregations";
  await app.executeCurrentQuery();
  assert.equal(shared.results.length, 1, "Documents results must not be clobbered");
  assert.equal(shared.readTime, "2026-01-01T00:00:01Z");
  assert.equal(shared.aggReadTime, "2026-01-01T00:00:02Z");
  assert.equal(typeof shared.aggElapsedMs, "number");
  assert.deepEqual(shared.aggProvenance, {{ limit: 0, hasOrderBy: true }});
  assert.equal(requests[0].hasSignal, true);
  assert.equal(requests[1].hasSignal, true);
  assert.deepEqual(requests[1].body, {{
    structuredAggregationQuery: {{
      structuredQuery: {{
        from: [{{ collectionId: "orders" }}],
        orderBy: [{{ field: {{ fieldPath: "age" }}, direction: "ASCENDING" }}],
        limit: 0,
      }},
      aggregations: [{{ count: {{}}, alias: "big_count" }}],
    }},
  }});

  // Lossless typed aggregation result markup + provenance note variants.
  const aggMarkup = renderTemplateMarkup(query.renderQueryRunner(makeProps(shared))).replace(/\s+/g, " ");
  assert.ok(aggMarkup.includes("Constrained by orderBy + limit: 0"));
  assert.ok(aggMarkup.includes("9007199254740993"));
  assert.ok(aggMarkup.includes('<span class="type-badge integerValue"> int64 </span>'));
  assert.ok(aggMarkup.includes("-0.0"));
  assert.ok(aggMarkup.includes('<span class="type-badge doubleValue"> double </span>'));
  assert.ok(aggMarkup.includes('<span class="type-badge nullValue"> null </span>'));

  shared.aggProvenance = {{ limit: 50, hasOrderBy: false }};
  assert.ok(renderTemplateMarkup(query.renderQueryRunner(makeProps(shared))).includes("Constrained by limit: 50"));
  shared.aggProvenance = {{ limit: null, hasOrderBy: true }};
  assert.ok(renderTemplateMarkup(query.renderQueryRunner(makeProps(shared))).includes("Constrained by orderBy"));
  shared.aggProvenance = null;
  assert.equal(renderTemplateMarkup(query.renderQueryRunner(makeProps(shared))).includes("agg-provenance-note"), false);

  // Prototype-key aliases render safely via JSON.parse + Object.entries.
  shared.aggResults = JSON.parse('{{"__proto__":{{"integerValue":"7"}},"constructor":{{"doubleValue":1.5}}}}');
  const protoMarkup = renderTemplateMarkup(query.renderQueryRunner(makeProps(shared)));
  assert.ok(protoMarkup.includes("__proto__"));
  assert.ok(protoMarkup.includes("constructor"));

  // Bare (unwrapped) Documents and Aggregations Raw JSON payloads execute cleanly too.
  shared.queryKind = "documents";
  shared.rawQueryText = '{{"from":[{{"collectionId":"orders"}}],"limit":3}}';
  await app.executeCurrentQuery();
  assert.deepEqual(requests[2].body, {{
    structuredQuery: {{ from: [{{ collectionId: "orders" }}], limit: 3 }},
  }});
  shared.queryKind = "aggregations";
  shared.rawAggregationText = '{{"structuredQuery":{{"from":[{{"collectionId":"orders"}}]}},"aggregations":[{{"count":{{}}}}]}}';
  await app.executeCurrentQuery();
  assert.deepEqual(requests[3].body, {{
    structuredAggregationQuery: {{
      structuredQuery: {{ from: [{{ collectionId: "orders" }}] }},
      aggregations: [{{ count: {{}} }}],
    }},
  }});
  assert.equal(shared.aggProvenance, null);
}}

// 4. Execution cancellation, stale-response dropping, ApiError formatting, and reset/onOpenQueryCollection aborts.
{{
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "(default)";

  // 4A. Superseded query: run1 aborted when run2 starts; run1 finishing must not clear run2.running.
  const run1Def = deferred();
  const run2Def = deferred();
  const signals = [];
  let callIdx = 0;
  fetchImpl = async (_url, init) => {{
    signals.push(init.signal);
    const idx = callIdx++;
    if (idx === 0) return run1Def.promise;
    return run2Def.promise;
  }};

  const qState = query.createQueryRunnerState("orders");
  qState.queryKind = "aggregations";
  app.state.queryState = qState;
  const pRun1 = app.executeCurrentQuery();
  assert.equal(qState.running, true);
  const pRun2 = app.executeCurrentQuery();
  assert.equal(signals[0].aborted, true, "prior query signal must abort when superseded");
  assert.equal(signals[1].aborted, false);

  const abortErr = new Error("Aborted");
  abortErr.name = "AbortError";
  run1Def.reject(abortErr);
  await pRun1;
  assert.equal(qState.running, true, "stale aborted run1 must not reset run2 running flag");
  assert.equal(qState.error, null);

  run2Def.resolve(jsonResponse([{{
    result: {{ aggregateFields: {{ field_1: {{ integerValue: "12" }} }} }},
    readTime: "2026-01-01T00:00:03Z",
  }}]));
  await pRun2;
  assert.equal(qState.running, false);
  assert.equal(qState.aggResults.field_1.integerValue, "12");

  // 4B. resetDatabaseCaches() aborts active query and prevents late write / running clobber.
  const staleDef = deferred();
  let staleSignal = null;
  fetchImpl = async (_url, init) => {{
    staleSignal = init.signal;
    return staleDef.promise;
  }};
  const pStale = app.executeCurrentQuery();
  assert.equal(staleSignal.aborted, false);
  stateMod.bumpDbEpoch();
  stateMod.resetDatabaseCaches();
  assert.equal(staleSignal.aborted, true, "resetDatabaseCaches must abort active query run");
  staleDef.resolve(jsonResponse([{{ result: {{ aggregateFields: {{ field_1: {{ integerValue: "99" }} }} }} }}]));
  await pStale;
  assert.equal(app.state.queryState.aggResults, null);

  // 4C. onOpenQueryCollection() calls abortActiveQueryRun() before replacing state.queryState.
  const openDef = deferred();
  let openSignal = null;
  fetchImpl = async (_url, init) => {{
    openSignal = init.signal;
    return openDef.promise;
  }};
  app.state.queryState = query.createQueryRunnerState("orders");
  const pOpen = app.executeCurrentQuery();
  assert.equal(openSignal.aborted, false);
  const openBtn = findClickHandler(app.renderWorkspace(), "Open in Explorer") ?? (() => {{}});
  assert.ok(openBtn);
  // Trigger Explorer's onOpenQueryCollection via renderWorkspace in explorer view.
  app.state.view = "explorer";
  app.state.rootCollections = ["orders"];
  app.state.segments = ["orders"];
  app.state.collectionColumns = new Map([[
    "projects/p/databases/(default)/documents/orders",
    {{
      collectionPath: "projects/p/databases/(default)/documents/orders",
      collectionId: "orders",
      parentPath: "projects/p/databases/(default)/documents",
      documents: [],
      nextPageToken: "",
      filterText: "",
      loading: false,
    }},
  ]]);
  const explorerQueryBtn = findClickHandler(app.renderWorkspace(), "Query");
  assert.ok(explorerQueryBtn, "expected Explorer Query button");
  explorerQueryBtn();
  assert.equal(openSignal.aborted, true, "onOpenQueryCollection must abort active query run");
  openDef.resolve(jsonResponse([{{ document: {{ name: "projects/p/databases/(default)/documents/orders/o1" }}, readTime: "t" }}]));
  await pOpen;
  assert.equal(app.state.queryState.results, null);

  // 4D. ApiError banner formatting on :runAggregationQuery + Enterprise visual execution/renderWorkspace wiring.
  app.state.activeDbMeta = {{
    name: "projects/p/databases/(default)",
    uid: "u1",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "ENTERPRISE",
    concurrencyMode: "PESSIMISTIC",
  }};
  app.state.view = "query";
  app.state.queryState = query.createQueryRunnerState("orders");
  app.state.queryState.queryKind = "aggregations";
  app.state.queryState.aggregations = Array.from({{ length: 6 }}, (_, idx) => ({{
    id: idx + 1,
    op: "COUNT",
    fieldPath: "",
    alias: idx === 0 ? "__name__" : `c_${{idx}}`,
  }}));
  const wsEntMarkup = renderTemplateMarkup(app.renderWorkspace()).replace(/\s+/g, " ");
  assert.ok(wsEntMarkup.includes('title="Add aggregation"'));
  let entBody = null;
  fetchImpl = async (url, init) => {{
    assert.ok(url.endsWith(":runAggregationQuery"));
    entBody = JSON.parse(init.body);
    return jsonResponse([{{ result: {{ aggregateFields: {{ __name__: {{ integerValue: "3" }} }} }}, readTime: "t" }}]);
  }};
  await app.executeCurrentQuery();
  assert.equal(app.state.queryState.error, null);
  assert.equal(entBody.structuredAggregationQuery.aggregations.length, 6);
  assert.equal(entBody.structuredAggregationQuery.aggregations[0].alias, "__name__");

  app.state.activeDbMeta = null;
  app.state.queryState = query.createQueryRunnerState("orders");
  app.state.queryState.queryKind = "aggregations";
  fetchImpl = async (url) => {{
    assert.ok(url.endsWith(":runAggregationQuery"));
    return jsonResponse(
      {{ error: {{ code: 400, status: "INVALID_ARGUMENT", message: "bad agg" }} }},
      400
    );
  }};
  await app.executeCurrentQuery();
  assert.equal(app.state.queryState.error, "[400 INVALID_ARGUMENT] bad agg");
}}

console.log("slice1_modes_limit_raw_validation_and_execution_isolation passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn slice2_discovery_suggestions() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const query = await import("./ui/components/query.js");
const qsugg = await import("./ui/components/query-suggestions.js");

const rootPath = "projects/p/databases/(default)/documents";

const findComboboxInputHandlers = (node, id) => {{
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {{
    for (const item of node) {{
      const hit = findComboboxInputHandlers(item, id);
      if (hit) return hit;
    }}
    return null;
  }}
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
    for (let i = 0; i < node.values.length; i++) {{
      if (node.values[i] === id && /\bid=\s*$/.test(node.strings[i])) {{
        const handlers = {{}};
        for (let k = i + 1; k < node.values.length; k++) {{
          const s = node.strings[k];
          if (s.includes(">")) break;
          const m = /@([a-z]+)=\s*$/.exec(s);
          if (m && typeof node.values[k] === "function") {{
            handlers[m[1]] = node.values[k];
          }}
        }}
        return handlers;
      }}
    }}
    for (const v of node.values) {{
      const hit = findComboboxInputHandlers(v, id);
      if (hit) return hit;
    }}
  }}
  return null;
}};

const findOptionByLabel = (node, labelText) => {{
  const click = findClickHandler(node, labelText);
  return click ? {{ click }} : null;
}};

// 1. Key-only collection-group :runQuery (limit 500), phantom ancestors, uniform direct child seeding,
//    Partial footer, empty [{{readTime}}] doc count 0, and odd-segment/invalid parentSubpath skip.
{{
  const store = qsugg.createQuerySuggestionStore();
  const requests = [];
  let rerenders = 0;
  const ctx = {{
    rootPath,
    rootCollections: ["empty_root", "orgs"],
    onRerender: () => {{ rerenders++; }},
  }};

  let mode = "root_500";
  fetchImpl = async (url, init) => {{
    requests.push({{ url, method: init.method ?? "GET", body: init.body ? JSON.parse(init.body) : null, hasSignal: Boolean(init.signal) }});
    if (url === `/v1/${{rootPath}}:runQuery` && mode === "root_500") {{
      const rows = Array.from({{ length: 499 }}, (_, i) => ({{
        document: {{ name: `${{rootPath}}/orgs/org-${{i}}/projects/p-${{i}}/tasks/t-${{i}}` }},
        readTime: "2026-01-01T00:00:00Z",
      }}));
      rows.push({{
        document: {{ name: `${{rootPath}}/orgs/acme/audit/a1` }},
        readTime: "2026-01-01T00:00:00Z",
      }});
      return jsonResponse(rows);
    }}
    if (url === `/v1/${{rootPath}}/orgs/acme:listCollectionIds`) {{
      return jsonResponse({{ collectionIds: ["empty_subcol", "projects"] }});
    }}
    if (url === `/v1/${{rootPath}}/orgs/acme:runQuery`) {{
      return jsonResponse([{{ readTime: "2026-01-01T00:00:00Z" }}]);
    }}
    throw new Error("unexpected fetch in 1: " + url);
  }};

  // Odd-segment and invalid parentSubpaths must skip network calls.
  for (const badParent of ["orgs", "orgs/acme/projects", "__bad__/doc", "./a", "../a", "a..b"]) {{
    const skipped = await qsugg.ensureCollectionGroups(store, ctx, badParent);
    assert.deepEqual(skipped.items, []);
  }}
  assert.equal(requests.length, 0);

  // Root collection group discovery: 1 key-only :runQuery (limit 500), seeded with rootCollections.
  const rootEntry = await qsugg.ensureCollectionGroups(store, ctx, "");
  assert.equal(requests.length, 1);
  assert.equal(requests[0].hasSignal, true);
  assert.deepEqual(requests[0].body, {{
    structuredQuery: {{
      from: [{{ collectionId: "", allDescendants: true }}],
      select: {{ fields: [{{ fieldPath: "__name__" }}] }},
      limit: 500,
    }},
  }});
  assert.deepEqual(
    rootEntry.items.map((i) => i.value),
    ["audit", "empty_root", "orgs", "projects", "tasks"]
  );
  assert.equal(rootEntry.partial, true);
  assert.equal(qsugg.formatSuggestionFooter(rootEntry), "Partial");
  assert.ok(store.collectionGroupsByParent.has(""));
  assert.ok(rerenders >= 2);

  // Non-root parentSubpath with empty [{{readTime}}] result: doc count 0, partial false, uniform subcol seeding!
  mode = "sub_empty";
  const subEntry = await qsugg.ensureCollectionGroups(store, ctx, "/orgs/acme/");
  assert.equal(subEntry.partial, false);
  assert.equal(qsugg.formatSuggestionFooter(subEntry), "");
  assert.deepEqual(
    subEntry.items.map((i) => i.value),
    ["empty_subcol", "projects"]
  );
  assert.ok(store.collectionGroupsByParent.has("orgs/acme"));
  assert.ok(store.subcollectionsByParent.has("orgs/acme"));
}}

// 2. Slash-triggered parentSubpath alternating listDocuments(showMissing=true) and :listCollectionIds,
//    dynamic keepOpenOnSelect (col/ stays open + fetches children + resets activeIndex; col/doc closes),
//    and zero Explorer cache dependency.
{{
  const store = qsugg.createQuerySuggestionStore();
  const qState = query.createQueryRunnerState();
  const requests = [];
  let rerenderCount = 0;

  fetchImpl = async (url, init) => {{
    requests.push({{ url, method: init.method ?? "GET" }});
    if (url.startsWith(`/v1/${{rootPath}}/orgs?`)) {{
      assert.ok(url.includes("showMissing=true"));
      assert.ok(url.includes("mask.fieldPaths=__name__"));
      assert.ok(url.includes("pageSize=100"));
      return jsonResponse({{
        documents: [
          {{ name: `${{rootPath}}/orgs/acme` }},
          {{ name: `${{rootPath}}/orgs/beta` }},
        ],
        nextPageToken: "tok-2",
      }});
    }}
    if (url === `/v1/${{rootPath}}/orgs/acme:listCollectionIds`) {{
      return jsonResponse({{ collectionIds: ["projects"] }});
    }}
    if (url.startsWith(`/v1/${{rootPath}}/orgs/acme/projects?`)) {{
      return jsonResponse({{
        documents: [{{ name: `${{rootPath}}/orgs/acme/projects/p1` }}],
      }});
    }}
    throw new Error("unexpected fetch in 2: " + url);
  }};

  const renderFn = () => query.renderQueryRunner({{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["orgs", "users"],
    suggestions: store,
    state: qState,
    onRerender: () => {{ rerenderCount++; }},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }});

  // Invalid prefixes skip network calls.
  for (const invalidPrefix of ["/orgs/", "orgs//acme/", "./", "../", "__res__/"]) {{
    const res = await qsugg.ensureParentSubpathSuggestions(
      store,
      {{ rootPath, rootCollections: ["orgs"], onRerender: () => {{}} }},
      invalidPrefix
    );
    assert.deepEqual(res.items, []);
  }}
  assert.equal(requests.length, 0);

  // Root Collection ID (!allDescendants, parentSubpath "") serves knownCollections with 0 fetches.
  const tpl0 = renderFn();
  const colHandlers = findComboboxInputHandlers(tpl0, "query-collection-id");
  assert.ok(colHandlers);
  colHandlers.focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  assert.equal(store.comboboxUi.openId, "query-collection-id");
  assert.equal(requests.length, 0, "root Collection ID must use knownCollections with 0 HTTP calls");
  const colMarkup = renderTemplateMarkup(renderFn());
  assert.ok(colMarkup.includes(">orgs<") || colMarkup.includes("orgs"));

  // Focus parentSubpath (no slash yet -> synchronous rootCollections/, 0 HTTP calls).
  const parentHandlers = findComboboxInputHandlers(renderFn(), "query-parent-subpath");
  assert.ok(parentHandlers);
  parentHandlers.focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 50, bottom: 80, left: 10, width: 200 }}) }} }});
  assert.equal(store.comboboxUi.openId, "query-parent-subpath");
  assert.equal(requests.length, 0);

  // Select "orgs/" via Enter with ArrowDown -> keepOpenOnSelect keeps popup open, resets activeIndex,
  // and immediately fetches listDocuments for "orgs".
  findComboboxInputHandlers(renderFn(), "query-parent-subpath").keydown({{
    key: "ArrowDown",
    preventDefault() {{}},
    currentTarget: {{ getBoundingClientRect: () => ({{ top: 50, bottom: 80, left: 10, width: 200 }}) }},
  }});
  assert.equal(store.comboboxUi.activeIndex, 0);
  const parentHandlersOpen = findComboboxInputHandlers(renderFn(), "query-parent-subpath");
  parentHandlersOpen.keydown({{
    key: "Enter",
    preventDefault() {{}},
    currentTarget: {{ getBoundingClientRect: () => ({{ top: 50, bottom: 80, left: 10, width: 200 }}) }},
  }});
  assert.equal(qState.parentSubpath, "orgs/");
  assert.equal(store.comboboxUi.openId, "query-parent-subpath", "col/ selection must keep popup open");
  assert.equal(store.comboboxUi.activeIndex, -1, "keepOpenOnSelect must reset activeIndex to -1");

  await new Promise((r) => setTimeout(r, 0));
  assert.equal(requests.length, 1);
  const docEntry = store.docIdsByCollection.get("orgs");
  assert.equal(docEntry.partial, true, "nextPageToken must mark docIdsByCollection entry partial");
  assert.deepEqual(docEntry.items.map((i) => i.value), ["orgs/acme", "orgs/beta"]);

  const orgsTpl = renderFn();
  const orgsMarkup = renderTemplateMarkup(orgsTpl);
  assert.ok(orgsMarkup.includes("Partial"));
  assert.ok(orgsMarkup.includes("acme"));

  // Select "orgs/acme" (no trailing slash) -> closes popup and sets parentSubpath = "orgs/acme".
  const acmeOpt = findOptionByLabel(orgsTpl, "acme");
  assert.ok(acmeOpt);
  acmeOpt.click();
  assert.equal(qState.parentSubpath, "orgs/acme");
  assert.equal(store.comboboxUi.openId, null, "col/doc selection must close popup");

  // Typing "/" after "orgs/acme" triggers :listCollectionIds via ensureSubcollections cache.
  const parentHandlers2 = findComboboxInputHandlers(renderFn(), "query-parent-subpath");
  parentHandlers2.input({{
    target: {{ value: "orgs/acme/" }},
    currentTarget: {{ getBoundingClientRect: () => ({{ top: 50, bottom: 80, left: 10, width: 200 }}) }},
  }});
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(requests.length, 2);
  assert.deepEqual(store.subcollectionsByParent.get("orgs/acme").items.map((i) => i.value), ["projects"]);

  // Direct subcollection Collection ID reuses subcollectionsByParent with 0 extra fetches!
  qState.parentSubpath = "orgs/acme";
  const subColTpl = renderFn();
  const subColHandlers = findComboboxInputHandlers(subColTpl, "query-collection-id");
  subColHandlers.focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(requests.length, 2, "direct subcollection Collection ID shares subcollectionsByParent cache");
  assert.ok(renderTemplateMarkup(renderFn()).includes("projects"));
}}

console.log("slice2_discovery_suggestions passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn slice2_field_sampling_and_auto_type() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const query = await import("./ui/components/query.js");
const qsugg = await import("./ui/components/query-suggestions.js");

const rootPath = "projects/p/databases/(default)/documents";

const findComboboxInputHandlers = (node, id) => {{
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {{
    for (const item of node) {{
      const hit = findComboboxInputHandlers(item, id);
      if (hit) return hit;
    }}
    return null;
  }}
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
    for (let i = 0; i < node.values.length; i++) {{
      if (node.values[i] === id && /\bid=\s*$/.test(node.strings[i])) {{
        const handlers = {{}};
        for (let k = i + 1; k < node.values.length; k++) {{
          const s = node.strings[k];
          if (s.includes(">")) break;
          const m = /@([a-z]+)=\s*$/.exec(s);
          if (m && typeof node.values[k] === "function") {{
            handlers[m[1]] = node.values[k];
          }}
        }}
        return handlers;
      }}
    }}
    for (const v of node.values) {{
      const hit = findComboboxInputHandlers(v, id);
      if (hit) return hit;
    }}
  }}
  return null;
}};

const findSelectChangeByAriaLabel = (node, ariaLabel) => {{
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {{
    for (const item of node) {{
      const hit = findSelectChangeByAriaLabel(item, ariaLabel);
      if (hit) return hit;
    }}
    return null;
  }}
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
    for (let i = 0; i < node.strings.length; i++) {{
      const matchedStatic = node.strings[i].includes(`aria-label="${{ariaLabel}}"`);
      const matchedDynamic =
        i < node.values.length &&
        node.values[i] === ariaLabel &&
        /\baria-label=\s*$/.test(node.strings[i]);
      if (matchedStatic || matchedDynamic) {{
        for (let k = i; k < node.values.length; k++) {{
          const s = node.strings[k];
          if (k > i && s.includes(">")) break;
          if (/@change=\s*$/.test(s) && typeof node.values[k] === "function") {{
            return node.values[k];
          }}
        }}
      }}
    }}
    for (const v of node.values) {{
      const hit = findSelectChangeByAriaLabel(v, ariaLabel);
      if (hit) return hit;
    }}
  }}
  return null;
}};

const findOptionByValueMatch = (node, textSubstr) => {{
  const click = findClickHandler(node, textSubstr);
  return click ? {{ click }} : null;
}};

// 1. Field sampling depth 5, vector stop, 32-item array cap, escaping, Map prototype safety,
//    nullable/mixed/null badges, reserved newline names, __name__ placement, empty collection skip.
{{
  const store = qsugg.createQuerySuggestionStore();
  const requests = [];
  const ctx = {{ rootPath, rootCollections: ["products"], onRerender: () => {{}} }};

  // Array with 32 integers followed by 40 strings -> 32-item cap must pick integerValue!
  const cappedArray = [
    ...Array.from({{ length: 32 }}, () => ({{ integerValue: "1" }})),
    ...Array.from({{ length: 40 }}, () => ({{ stringValue: "later" }})),
  ];

  // Tie-breaking array (1 stringValue, 1 integerValue) -> stringValue wins by SCALAR_FILTER_TYPES order.
  const tiedArray = [{{ integerValue: "5" }}, {{ stringValue: "a" }}];

  const protoDocFields = JSON.parse('{{}}');
  protoDocFields["__proto__"] = {{ stringValue: "ignored-reserved" }};
  protoDocFields["__a\\nb__"] = {{ stringValue: "ignored-newline-reserved" }};
  protoDocFields["____"] = {{ stringValue: "allowed-4-underscores" }};
  protoDocFields["constructor"] = {{ stringValue: "safe-key" }};
  protoDocFields["price"] = {{ doubleValue: 19.99 }};
  protoDocFields["nullableInt"] = {{ nullValue: null }};
  protoDocFields["allNull"] = {{ nullValue: null }};
  protoDocFields["mixedVal"] = {{ integerValue: "1" }};
  protoDocFields["active"] = {{ booleanValue: true }};
  protoDocFields["tiedScalar"] = {{ integerValue: "1" }};
  protoDocFields["cappedArr"] = {{ arrayValue: {{ values: cappedArray }} }};
  protoDocFields["tiedArr"] = {{ arrayValue: {{ values: tiedArray }} }};
  protoDocFields["embedding"] = {{
    mapValue: {{
      fields: {{
        __type__: {{ stringValue: "__vector__" }},
        value: {{ arrayValue: {{ values: [{{ doubleValue: 0.5 }}] }} }},
      }},
    }},
  }};
  protoDocFields["meta"] = {{
    mapValue: {{
      fields: {{
        "display.name": {{ stringValue: "Widget" }},
        d2: {{
          mapValue: {{
            fields: {{
              d3: {{
                mapValue: {{
                  fields: {{
                    d4: {{
                      mapValue: {{
                        fields: {{
                          d5: {{
                            mapValue: {{
                              fields: {{
                                d6: {{ stringValue: "too-deep" }},
                              }},
                            }},
                          }},
                        }},
                      }},
                    }},
                  }},
                }},
              }},
            }},
          }},
        }},
      }},
    }},
  }};

  const doc2Fields = {{
    nullableInt: {{ integerValue: "42" }},
    allNull: {{ nullValue: null }},
    mixedVal: {{ stringValue: "two" }},
    tiedScalar: {{ stringValue: "wins-tie" }},
    mixedScalarAndArr: {{ booleanValue: true }},
  }};

  const doc3Fields = {{
    mixedScalarAndArr: {{ arrayValue: {{ values: [{{ geoPointValue: {{ latitude: 1, longitude: 2 }} }}] }} }},
  }};

  let nextMockMode = "three";
  fetchImpl = async (url, init) => {{
    requests.push({{ url, body: JSON.parse(init.body) }});
    if (nextMockMode === "empty") {{
      return jsonResponse([{{ readTime: "2026-01-01T00:00:00Z" }}]);
    }}
    if (nextMockMode === "one") {{
      return jsonResponse([{{ document: {{ name: `${{rootPath}}/one/d1`, fields: {{ a: {{ integerValue: "1" }} }} }} }}]);
    }}
    if (nextMockMode === "twenty_five") {{
      return jsonResponse(
        Array.from({{ length: 25 }}, (_, i) => ({{
          document: {{ name: `${{rootPath}}/full/d${{i}}`, fields: {{ k: {{ integerValue: String(i) }} }} }},
        }}))
      );
    }}
    return jsonResponse([
      {{ document: {{ name: `${{rootPath}}/products/p1`, fields: protoDocFields }} }},
      {{ document: {{ name: `${{rootPath}}/products/p2`, fields: doc2Fields }} }},
      {{ document: {{ name: `${{rootPath}}/products/p3`, fields: doc3Fields }} }},
    ]);
  }};

  // Empty collectionId and invalid parentSubpath skip HTTP calls.
  await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "", collectionId: "  ", allDescendants: false }});
  await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "orgs", collectionId: "products", allDescendants: false }});
  assert.equal(requests.length, 0);

  const entry = await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "", collectionId: "products", allDescendants: false }});
  assert.equal(requests.length, 1);
  assert.deepEqual(requests[0].body, {{
    structuredQuery: {{
      from: [{{ collectionId: "products", allDescendants: false }}],
      limit: 25,
    }},
  }});
  assert.equal(entry.sampledDocs, 3);
  assert.equal(entry.partial, false);
  assert.equal(qsugg.formatSuggestionFooter(entry), "Sampled 3 docs");

  const values = entry.items.map((i) => i.value);
  assert.equal(values.includes("__proto__"), false);
  assert.equal(values.includes("__a\\nb__"), false);
  assert.equal(values.includes("____"), true);
  assert.equal(values.includes("constructor"), true);
  assert.equal(values.includes("embedding"), true);
  assert.equal(values.includes("embedding.__type__"), false, "vector map must not expand");
  assert.equal(values.includes("embedding.value"), false, "vector map must not expand");
  assert.equal(values.includes("meta"), true);
  assert.equal(values.includes("meta.`display.name`"), true);
  assert.equal(values.includes("meta.d2.d3.d4.d5"), true, "depth 5 included");
  assert.equal(values.includes("meta.d2.d3.d4.d5.d6"), false, "depth 6 excluded");
  assert.equal(values[values.length - 1], "__name__", "__name__ pinned last in items");
  assert.equal(entry.aggItems.some((i) => i.value === "__name__"), false, "__name__ excluded from aggItems");
  assert.equal(entry.aggItems[0].value, "mixedVal", "numeric priority in aggItems (mixedVal has integerValue)");
  assert.equal(entry.aggItems[1].value, "nullableInt", "numeric priority in aggItems");
  assert.equal(entry.aggItems[2].value, "price", "numeric priority in aggItems");
  assert.equal(entry.fieldHints.get("__proto__"), undefined, "Map prototype safety");

  // Badges: nullable int -> int64, 2+ non-null -> mixed, all null -> null, vector -> vector.
  const byVal = new Map(entry.items.map((i) => [i.value, i]));
  assert.equal(byVal.get("nullableInt").badge, "int64");
  assert.equal(byVal.get("mixedVal").badge, "mixed");
  assert.equal(byVal.get("allNull").badge, "null");
  assert.equal(byVal.get("embedding").badge, "vector");
  assert.equal(byVal.get("meta.`display.name`").matchText, "meta.display.name");

  // Array 32-item cap and tie-breaking checks on hints.
  assert.equal(entry.fieldHints.get("cappedArr").arrayElementType, "integerValue");
  assert.equal(entry.fieldHints.get("tiedArr").arrayElementType, "stringValue");
  assert.equal(entry.fieldHints.get("tiedScalar").scalarType, "stringValue");

  // Footer formatting across 0, 1, 25 docs and Stale.
  nextMockMode = "empty";
  const emptyEntry = await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "", collectionId: "empty", allDescendants: false }});
  assert.equal(emptyEntry.sampledDocs, 0);
  assert.equal(qsugg.formatSuggestionFooter(emptyEntry), "Sampled 0 docs");

  nextMockMode = "one";
  const oneEntry = await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "", collectionId: "one", allDescendants: false }});
  assert.equal(qsugg.formatSuggestionFooter(oneEntry), "Sampled 1 doc");

  nextMockMode = "twenty_five";
  const fullEntry = await qsugg.ensureFieldSuggestions(store, ctx, {{ parentSubpath: "", collectionId: "full", allDescendants: false }});
  assert.equal(fullEntry.partial, true);
  assert.equal(qsugg.formatSuggestionFooter(fullEntry), "Sampled 25 docs · Partial");
  fullEntry.stale = true;
  assert.equal(qsugg.formatSuggestionFooter(fullEntry), "Sampled 25 docs · Partial · Stale");

  // 2. Non-clobbering Where filter auto-type detection & booleanValue normalization in renderQueryRunner.
  const qState = query.createQueryRunnerState("products");
  const f = query.createFilterRow();
  assert.equal(f.valueTypeManuallySet, false);
  assert.equal(f.valueUserEdited, false);
  qState.filters.push(f);

  const renderFn = () => query.renderQueryRunner({{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["products"],
    suggestions: store,
    state: qState,
    onRerender: () => {{}},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }});

  // Open filter field combobox and filter both raw "display.name" and backtick-escaped "`display.name`" -> matches "meta.`display.name`".
  const filterId = `query-filter-field-${{f.id}}`;
  findComboboxInputHandlers(renderFn(), filterId).focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "display.name" }} }});
  assert.ok(renderTemplateMarkup(renderFn()).includes("meta.`display.name`"));
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "`display.name`" }} }});
  assert.ok(renderTemplateMarkup(renderFn()).includes("meta.`display.name`"));

  // Clear filter input and select "active" (booleanValue) -> sets booleanValue and valueText = "true" without valueUserEdited.
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "active" }} }});
  findOptionByValueMatch(renderFn(), "active").click();
  assert.equal(f.fieldPath, "active");
  assert.equal(f.valueType, "booleanValue");
  assert.equal(f.valueText, "true");
  assert.equal(f.valueUserEdited, false);

  // Switch operator EQUAL -> IN (valueText -> "[]"), then IN -> EQUAL -> normalizes valueText back to "true" without setting valueUserEdited!
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "IN" }} }});
  assert.equal(f.valueText, "[]");
  assert.equal(f.valueUserEdited, false);
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "EQUAL" }} }});
  assert.equal(f.valueType, "booleanValue");
  assert.equal(f.valueText, "true");
  assert.equal(f.valueUserEdited, false);

  // Same for NOT_IN -> EQUAL and ARRAY_CONTAINS_ANY -> EQUAL even when valueTypeManuallySet is true.
  f.valueTypeManuallySet = true;
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "ARRAY_CONTAINS_ANY" }} }});
  assert.equal(f.valueText, "[]");
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "EQUAL" }} }});
  assert.equal(f.valueText, "true");
  f.valueTypeManuallySet = false;

  // Selecting "price" while !valueUserEdited auto-types to doubleValue and clears unedited boolean text "true" -> "".
  findComboboxInputHandlers(renderFn(), filterId).focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "price" }} }});
  findOptionByValueMatch(renderFn(), "price").click();
  assert.equal(f.valueType, "doubleValue");
  assert.equal(f.valueText, "");

  // Switching to ARRAY_CONTAINS on mixedScalarAndArr picks arrayElementType ("geoPointValue"), while IN picks scalarType ("booleanValue"),
  // and IS_NULL / IS_NAN leave valueType unchanged!
  findComboboxInputHandlers(renderFn(), filterId).focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "mixedScalarAndArr" }} }});
  findOptionByValueMatch(renderFn(), "mixedScalarAndArr").click();
  assert.equal(f.valueType, "booleanValue");
  assert.equal(f.valueText, "true");

  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "ARRAY_CONTAINS" }} }});
  assert.equal(f.valueType, "geoPointValue");
  assert.equal(f.valueText, "");

  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "ARRAY_CONTAINS_ANY" }} }});
  assert.equal(f.valueType, "geoPointValue");
  assert.equal(f.valueText, "[]");

  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "IS_NULL" }} }});
  assert.equal(f.valueType, "geoPointValue", "unary IS_NULL preserves valueType");
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "IS_NAN" }} }});
  assert.equal(f.valueType, "geoPointValue", "unary IS_NAN preserves valueType");

  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "NOT_IN" }} }});
  assert.equal(f.valueType, "booleanValue");
  assert.equal(f.valueText, "[]");

  // Switching back to EQUAL -> booleanValue normalizes to "true"; editing boolean select sets valueUserEdited = true and blocks further auto-typing!
  findSelectChangeByAriaLabel(renderFn(), "Filter operator")({{ target: {{ value: "EQUAL" }} }});
  assert.equal(f.valueText, "true");
  findSelectChangeByAriaLabel(renderFn(), "Filter boolean value")({{ target: {{ value: "false" }} }});
  assert.equal(f.valueUserEdited, true);
  assert.equal(f.valueText, "false");

  findComboboxInputHandlers(renderFn(), filterId).focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  findComboboxInputHandlers(renderFn(), filterId).input({{ target: {{ value: "price" }} }});
  findOptionByValueMatch(renderFn(), "price").click();
  assert.equal(f.valueType, "booleanValue", "valueUserEdited must prevent auto-type clobbering");
  assert.equal(f.valueText, "false");

  // Second filter row: manual valueType select sets valueTypeManuallySet = true; lat/lng sets valueUserEdited = true.
  const f2 = query.createFilterRow();
  qState.filters.push(f2);
  const f2Id = `query-filter-field-${{f2.id}}`;
  const row2TypeSelects = [];
  const collectSelects = (node) => {{
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) {{ node.forEach(collectSelects); return; }}
    if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
      for (let i = 0; i < node.strings.length; i++) {{
        if (node.strings[i].includes('aria-label="Filter value type"')) {{
          for (let k = i; k < node.values.length; k++) {{
            if (k > i && node.strings[k].includes(">")) break;
            if (/@change=\s*$/.test(node.strings[k])) row2TypeSelects.push(node.values[k]);
          }}
        }}
      }}
      node.values.forEach(collectSelects);
    }}
  }};
  collectSelects(renderFn());
  row2TypeSelects[1]({{ target: {{ value: "geoPointValue" }} }});
  assert.equal(f2.valueTypeManuallySet, true);
  findComboboxInputHandlers(renderFn(), f2Id).focus({{ currentTarget: {{ getBoundingClientRect: () => ({{ top: 100, bottom: 130, left: 10, width: 200 }}) }} }});
  findComboboxInputHandlers(renderFn(), f2Id).input({{ target: {{ value: "price" }} }});
  findOptionByValueMatch(renderFn(), "price").click();
  assert.equal(f2.valueType, "geoPointValue", "valueTypeManuallySet must prevent auto-type clobbering");
  const latInput = findInputHandler(renderFn(), 'aria-label="Filter latitude"');
  assert.ok(latInput);
  latInput({{ target: {{ value: "52.3" }} }});
  assert.equal(f2.valueUserEdited, true);
}}

console.log("slice2_field_sampling_and_auto_type passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn slice2_combobox_aria_geometry_and_reconciliation() {
    let script = format!(
        r#"{NODE_BOOTSTRAP}
const combobox = await import("./ui/components/combobox.js");
const query = await import("./ui/components/query.js");
const qsugg = await import("./ui/components/query-suggestions.js");
const app = await import("./ui/app.js");

const findHandlersByTag = (node, tagNeedle) => {{
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {{
    for (const item of node) {{
      const hit = findHandlersByTag(item, tagNeedle);
      if (hit) return hit;
    }}
    return null;
  }}
  if (Array.isArray(node.strings) && Array.isArray(node.values)) {{
    for (let i = 0; i < node.strings.length; i++) {{
      if (node.strings[i].includes(tagNeedle)) {{
        const handlers = {{}};
        for (let k = i; k < node.values.length; k++) {{
          if (k > i && node.strings[k].includes(">")) break;
          const m = /@([a-z]+)=\s*$/.exec(node.strings[k]);
          if (m && typeof node.values[k] === "function") {{
            handlers[m[1]] = node.values[k];
          }}
        }}
        if (Object.keys(handlers).length > 0) return handlers;
      }}
    }}
    for (const v of node.values) {{
      const hit = findHandlersByTag(v, tagNeedle);
      if (hit) return hit;
    }}
  }}
  return null;
}};

// 1. WAI-ARIA 1.2 bindings, listbox child purity, filtered-empty Loading vs No matching,
//    keyboard navigation (ArrowDown/Up/Home/End/Enter/Escape/Tab), mousedown preventDefault, blur guard,
//    flipUp threshold/positioning, capture-phase scroll/resize repositioning & clip-bounds dismissal.
{{
  const winListeners = new Map();
  let mockRect = {{ top: 100, bottom: 130, left: 24, width: 300, right: 324 }};
  let mockTopbarRect = {{ top: 0, bottom: 52, left: 0, width: 1200 }};
  let mockPaneRect = {{ top: 52, bottom: 780, left: 0, width: 450 }};
  let anchorConnected = true;
  let scrollIntoViewCalls = 0;

  globalThis.window.innerHeight = 800;
  globalThis.window.innerWidth = 1200;
  globalThis.window.addEventListener = (type, fn, opts) => {{
    const list = winListeners.get(type) ?? [];
    list.push({{ fn, opts }});
    winListeners.set(type, list);
  }};
  globalThis.window.removeEventListener = (type, fn, opts) => {{
    const list = winListeners.get(type) ?? [];
    const idx = list.findIndex((x) => x.fn === fn && Boolean(x.opts?.capture) === Boolean(opts?.capture));
    if (idx >= 0) list.splice(idx, 1);
  }};
  globalThis.document.getElementById = (id) => {{
    if (id === "cb-test") {{
      return {{
        isConnected: anchorConnected,
        getBoundingClientRect: () => mockRect,
      }};
    }}
    if (id === "topbar") {{
      return {{ getBoundingClientRect: () => mockTopbarRect }};
    }}
    if (id === "cb-test-listbox-opt-0" || id === "cb-test-listbox-opt-1") {{
      return {{
        scrollIntoView: () => {{ scrollIntoViewCalls++; }},
      }};
    }}
    return null;
  }};
  globalThis.document.querySelector = (sel) => {{
    if (sel === ".query-builder-pane") {{
      return {{ getBoundingClientRect: () => mockPaneRect }};
    }}
    return null;
  }};
  globalThis.requestAnimationFrame = (fn) => fn();

  const uiState = combobox.createComboboxUiState();
  let val = "";
  let loading = true;
  let items = [];
  let opened = 0;
  let refreshed = 0;
  let selected = [];
  let rerenders = 0;

  const renderBox = () => combobox.renderCombobox({{
    id: "cb-test",
    value: val,
    placeholder: "Pick field",
    ariaLabel: "Field path",
    items,
    uiState,
    footerText: "Sampled 2 docs",
    loading,
    onInput: (next) => {{ val = next; }},
    onSelect: (item) => {{ val = item.value; selected.push(item.value); }},
    onOpen: () => {{ opened++; }},
    onRefresh: () => {{ refreshed++; }},
    onRerender: () => {{ rerenders++; }},
  }});

  // Closed state: aria-expanded="false", NO .combobox-popup, NO aria-controls, NO aria-activedescendant.
  const closedMarkup = renderTemplateMarkup(renderBox());
  assert.ok(closedMarkup.includes('role="combobox"'));
  assert.ok(closedMarkup.includes('aria-autocomplete="list"'));
  assert.ok(closedMarkup.includes('aria-expanded="false"'));
  assert.equal(closedMarkup.includes("combobox-popup"), false);
  assert.equal(closedMarkup.includes("aria-controls"), false);
  assert.equal(closedMarkup.includes("aria-activedescendant"), false);

  // Open while loading && items.length === 0 -> renders sibling .combobox-empty "Loading..." outside <ul>.
  findHandlersByTag(renderBox(), 'role="combobox"').focus({{
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  assert.equal(opened, 1);
  assert.equal(uiState.openId, "cb-test");
  assert.equal(uiState.activeIndex, -1);
  assert.equal((winListeners.get("scroll") ?? []).length, 1);
  assert.equal(winListeners.get("scroll")[0].opts.capture, true);
  assert.equal((winListeners.get("resize") ?? []).length, 1);

  const loadingMarkup = renderTemplateMarkup(renderBox()).replace(/\s+/g, " ");
  assert.ok(loadingMarkup.includes('aria-expanded="true"'));
  assert.ok(loadingMarkup.includes('aria-controls="cb-test-listbox"'));
  assert.ok(loadingMarkup.includes('<ul id="cb-test-listbox" class="combobox-listbox" role="listbox"> </ul>'));
  assert.ok(loadingMarkup.includes('<div class="combobox-empty" role="status">Loading...</div>'));
  assert.ok(loadingMarkup.includes('class="combobox-refresh-btn" tabindex="-1"'));

  // Filtered-empty state when items.length > 0 and loading === true: must show "No matching suggestions" (NOT "Loading...").
  items = [
    {{ value: "alpha", label: "alpha", badge: "string", badgeClass: "stringValue" }},
    {{ value: "beta", label: "beta", badge: "int64", badgeClass: "integerValue" }},
  ];
  val = "nomatch";
  const filteredEmptyMarkup = renderTemplateMarkup(renderBox());
  assert.ok(filteredEmptyMarkup.includes("No matching suggestions"));
  assert.equal(filteredEmptyMarkup.includes("Loading..."), false);

  // Valid <ul role="listbox"> containing strictly <li role="option"> children only.
  loading = false;
  val = "";
  const openTpl = renderBox();
  const openMarkup = renderTemplateMarkup(openTpl).replace(/\s+/g, " ");
  const ulMatch = /<ul [^>]*role="listbox">(.*?)<\/ul>/.exec(openMarkup);
  assert.ok(ulMatch, "expected <ul role=listbox>");
  assert.equal(ulMatch[1].includes("<div"), false, "ul must contain only <li role=option> children");
  assert.ok(ulMatch[1].includes('role="option"'));

  // Keyboard navigation: ArrowDown, End, Home, ArrowUp, Enter free-form vs Enter active.
  let prevented = 0;
  let stopped = 0;
  const key = (k) => findHandlersByTag(renderBox(), 'role="combobox"').keydown({{
    key: k,
    preventDefault: () => {{ prevented++; }},
    stopPropagation: () => {{ stopped++; }},
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  key("ArrowDown");
  assert.equal(uiState.activeIndex, 0);
  assert.ok(scrollIntoViewCalls >= 1);
  assert.ok(renderTemplateMarkup(renderBox()).includes('aria-activedescendant="cb-test-listbox-opt-0"'));
  key("End");
  assert.equal(uiState.activeIndex, 1);
  key("Home");
  assert.equal(uiState.activeIndex, 0);
  key("ArrowUp");
  assert.equal(uiState.activeIndex, 1);

  // Enter when activeIndex === -1 closes popup without calling onSelect and preserves typed text!
  val = "custom-typed";
  uiState.activeIndex = -1;
  key("Enter");
  assert.equal(uiState.openId, null);
  assert.equal(val, "custom-typed");
  assert.equal(selected.length, 0);

  // ArrowUp when closed opens and sets activeIndex = items.length - 1; Tab closes without preventDefault!
  val = "";
  key("ArrowUp");
  assert.equal(uiState.openId, "cb-test");
  assert.equal(uiState.activeIndex, 1);
  const prevBeforeTab = prevented;
  key("Tab");
  assert.equal(uiState.openId, null);
  assert.equal(prevented, prevBeforeTab, "Tab must not call preventDefault");

  // Re-open, Escape calls preventDefault & stopPropagation.
  key("ArrowDown");
  assert.equal(uiState.openId, "cb-test");
  const prevBeforeEsc = prevented;
  key("Escape");
  assert.equal(uiState.openId, null);
  assert.equal(prevented, prevBeforeEsc + 1);
  assert.equal(stopped, 1);

  // mousedown preventDefault on option and Refresh button; Refresh calls onRefresh without closing.
  key("ArrowDown");
  const optHandlers = findHandlersByTag(renderBox(), 'class="combobox-option');
  const refreshHandlers = findHandlersByTag(renderBox(), 'class="combobox-refresh-btn"');
  let mdPrevented = 0;
  optHandlers.mousedown({{ preventDefault: () => {{ mdPrevented++; }} }});
  refreshHandlers.mousedown({{ preventDefault: () => {{ mdPrevented++; }} }});
  assert.equal(mdPrevented, 2);
  refreshHandlers.click();
  assert.equal(refreshed, 1);
  assert.equal(uiState.openId, "cb-test");

  // @blur active-ID guard: blur when openId !== id does nothing; when openId === id closes & removes listeners!
  uiState.openId = "other-id";
  findHandlersByTag(renderBox(), 'role="combobox"').blur();
  assert.equal(uiState.openId, "other-id");
  uiState.openId = "cb-test";
  findHandlersByTag(renderBox(), 'role="combobox"').blur();
  assert.equal(uiState.openId, null);
  assert.equal((winListeners.get("scroll") ?? []).length, 0, "capture scroll listener must be removed");
  assert.equal((winListeners.get("resize") ?? []).length, 0);

  // flipUp threshold/positioning: availableBelow < 280 && availableAbove > availableBelow.
  mockRect = {{ top: 600, bottom: 630, left: 24, width: 150, right: 174 }};
  findHandlersByTag(renderBox(), 'role="combobox"').focus({{
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  assert.equal(uiState.rect.flipUp, true);
  const flippedMarkup = renderTemplateMarkup(renderBox());
  assert.ok(flippedMarkup.includes("width: 220px; bottom: 202px; top: auto;"));

  // Scroll inside .combobox-listbox is ignored; external scroll repositions; clipped past #topbar or pane dismisses!
  const scrollFn = winListeners.get("scroll")[0].fn;
  const resizeFn = winListeners.get("resize")[0].fn;
  scrollFn({{ target: {{ classList: {{ contains: (c) => c === "combobox-listbox" }} }} }});
  scrollFn({{ target: {{ closest: (s) => (s === ".combobox-listbox" ? {{}} : null) }} }});
  assert.equal(uiState.openId, "cb-test");

  mockRect = {{ top: 150, bottom: 180, left: 30, width: 260, right: 290 }};
  scrollFn({{ target: {{}} }});
  assert.equal(uiState.rect.flipUp, false);
  assert.equal(uiState.rect.top, 150);

  // Clipped behind #topbar (rect.top < topbar.bottom)
  mockRect = {{ top: 40, bottom: 70, left: 30, width: 260, right: 290 }};
  scrollFn({{ target: {{}} }});
  assert.equal(uiState.openId, null, "clipped behind #topbar must dismiss popup");

  // Reopen, clipped past .query-builder-pane bottom via resize -> dismisses!
  mockRect = {{ top: 150, bottom: 180, left: 30, width: 260, right: 290 }};
  findHandlersByTag(renderBox(), 'role="combobox"').focus({{
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  mockRect = {{ top: 785, bottom: 815, left: 30, width: 260, right: 290 }};
  resizeFn();
  assert.equal(uiState.openId, null);

  // Detached anchor dismisses.
  mockRect = {{ top: 150, bottom: 180, left: 30, width: 260, right: 290 }};
  findHandlersByTag(renderBox(), 'role="combobox"').focus({{
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  anchorConnected = false;
  winListeners.get("scroll")[0].fn({{ target: {{}} }});
  assert.equal(uiState.openId, null);

  // Per-member Node guards when window/document members are stripped.
  delete globalThis.window.innerHeight;
  delete globalThis.document.getElementById;
  delete globalThis.document.querySelector;
  delete globalThis.requestAnimationFrame;
  findHandlersByTag(renderBox(), 'role="combobox"').focus({{
    currentTarget: {{ getBoundingClientRect: () => mockRect }},
  }});
  key("ArrowDown");
  assert.equal(uiState.openId, "cb-test");
  combobox.closeActiveCombobox(uiState);
}}

// 2. Stable row.id keying, reconcileComboboxUi dismissal across row removal, COUNT op, raw-mode renderQueryRunner,
//    and renderApp (view switch / modal open).
{{
  const store = qsugg.createQuerySuggestionStore();
  const qState = query.createQueryRunnerState("orders");
  const r1 = query.createFilterRow();
  const r2 = query.createFilterRow();
  qState.filters.push(r1, r2);

  const renderQ = () => query.renderQueryRunner({{
    projectId: "p",
    databaseId: "(default)",
    knownCollections: ["orders"],
    suggestions: store,
    state: qState,
    onRerender: () => {{}},
    onExecuteQuery: () => {{}},
    onOpenResultDocument: () => {{}},
  }});

  let cleaned = 0;
  store.comboboxUi.openId = `query-filter-field-${{r2.id}}`;
  store.comboboxUi.cleanup = () => {{ cleaned++; }};

  // Removing r1 keeps r2 open (stable row.id keying)!
  qState.filters.splice(0, 1);
  renderQ();
  assert.equal(store.comboboxUi.openId, `query-filter-field-${{r2.id}}`);
  assert.equal(cleaned, 0);

  // Removing r2 closes popup and runs cleanup via reconcileComboboxUi!
  qState.filters.splice(0, 1);
  renderQ();
  assert.equal(store.comboboxUi.openId, null);
  assert.equal(cleaned, 1);

  // Switching agg row from SUM to COUNT closes agg combobox via reconcileComboboxUi.
  qState.queryKind = "aggregations";
  qState.aggregations[0].op = "SUM";
  store.comboboxUi.openId = `query-agg-field-${{qState.aggregations[0].id}}`;
  renderQ();
  assert.equal(store.comboboxUi.openId, `query-agg-field-${{qState.aggregations[0].id}}`);
  qState.aggregations[0].op = "COUNT";
  renderQ();
  assert.equal(store.comboboxUi.openId, null);

  // Rendering renderQueryRunner in "raw" mode closes open combobox (even parentSubpath).
  qState.mode = "visual";
  store.comboboxUi.openId = "query-parent-subpath";
  renderQ();
  assert.equal(store.comboboxUi.openId, "query-parent-subpath");
  qState.mode = "raw";
  renderQ();
  assert.equal(store.comboboxUi.openId, null);

  // renderApp closes open combobox on view switch and modal open.
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "(default)";
  app.state.view = "query";
  app.state.queryState = query.createQueryRunnerState("orders");
  app.state.querySuggestions.comboboxUi.openId = "query-collection-id";
  await app.switchView("explorer");
  assert.equal(app.state.querySuggestions.comboboxUi.openId, null);

  app.state.view = "query";
  app.state.querySuggestions.comboboxUi.openId = "query-collection-id";
  app.openCreateDocumentModal("projects/p/databases/(default)/documents", "orders");
  assert.equal(app.state.querySuggestions.comboboxUi.openId, null);
  app.state.modal = null;
}}

console.log("slice2_combobox_aria_geometry_and_reconciliation passed");
"#
    );

    run_node_ui_test(&script);
}

#[test]
fn slice2_cache_and_sse_lifecycle() {
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

const api = await import("./ui/api.js");
const app = await import("./ui/app.js");
const qsugg = await import("./ui/components/query-suggestions.js");
const stateMod = await import("./ui/state.js");

const rootPath = "projects/p/databases/db1/documents";

// 1. connectEventStream: immediate FIFO flush of database_cleared and resync, plus MAX_WAIT_MS = 1000 ceiling.
{{
  const origSetTimeout = globalThis.setTimeout;
  const origClearTimeout = globalThis.clearTimeout;
  let nextTimerId = 1;
  const timers = new Map();
  const batches = [];

  globalThis.setTimeout = (fn, ms) => {{
    const id = nextTimerId++;
    timers.set(id, {{ fn, ms }});
    return id;
  }};
  globalThis.clearTimeout = (id) => {{
    timers.delete(id);
  }};

  try {{
    const esHandle = api.connectEventStream((b) => batches.push(b), () => {{}});

    // Buffer documents_changed, then database_cleared -> immediately flushes [documents_changed, database_cleared] FIFO!
    lastEs.emit("change", {{ data: JSON.stringify({{ type: "documents_changed", database: rootPath, paths: ["a"] }}) }});
    assert.equal(batches.length, 0);
    assert.equal(timers.size, 2, "debounce (150ms) and maxWait (1000ms) timers must both be armed");
    lastEs.emit("change", {{ data: JSON.stringify({{ type: "database_cleared", database: rootPath }}) }});
    assert.equal(batches.length, 1);
    assert.deepEqual(batches[0].map((e) => e.type), ["documents_changed", "database_cleared"]);
    assert.equal(timers.size, 0, "all timers cleared on urgent flush");

    // Buffer documents_changed, then resync -> immediately flushes [documents_changed, resync] FIFO!
    lastEs.emit("change", {{ data: JSON.stringify({{ type: "documents_changed", database: rootPath, paths: ["b"] }}) }});
    lastEs.emit("resync", {{ data: JSON.stringify({{ type: "resync" }}) }});
    assert.equal(batches.length, 2);
    assert.deepEqual(batches[1].map((e) => e.type), ["documents_changed", "resync"]);
    assert.equal(timers.size, 0);

    // Sustained documents_changed stream resets 150ms debounce repeatedly, while 1000ms maxWait timer fires!
    lastEs.emit("change", {{ data: JSON.stringify({{ type: "documents_changed", database: rootPath, paths: ["c1"] }}) }});
    const maxEntry = Array.from(timers.entries()).find(([, t]) => t.ms === 1000);
    assert.ok(maxEntry, "expected 1000ms maxWait timer");
    lastEs.emit("change", {{ data: JSON.stringify({{ type: "documents_changed", database: rootPath, paths: ["c2"] }}) }});
    assert.equal(timers.has(maxEntry[0]), true, "maxWait timer must NOT be reset on subsequent events");
    maxEntry[1].fn();
    assert.equal(batches.length, 3);
    assert.deepEqual(batches[2].map((e) => e.paths[0]), ["c1", "c2"]);
    assert.equal(timers.size, 0);
    esHandle.close();
  }} finally {{
    globalThis.setTimeout = origSetTimeout;
    globalThis.clearTimeout = origClearTimeout;
  }}
}}

// 2. applySyncSseBatch preempts in-flight applyAsyncSseRefresh & active Explorer reads via getDbSignal()
//    with zero post-notification /v1 calls while preserving state.ssePaused.
{{
  const fetched = [];
  const abortedUrls = [];
  const slowDb1Meta = deferred();
  const slowDb1Cols = deferred();

  fetchImpl = async (url, init) => {{
    fetched.push(url);
    if (init.signal) {{
      init.signal.addEventListener("abort", () => abortedUrls.push(url));
    }}
    if (url === "/emulator/v1") return jsonResponse([rootPath, "projects/p/databases/db2/documents"]);
    if (url === "/v1/projects/p/databases/db1") return slowDb1Meta.promise;
    if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") return slowDb1Cols.promise;
    if (url === "/v1/projects/p/databases/db2") {{
      return jsonResponse({{
        name: "projects/p/databases/db2",
        uid: "u2",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "STANDARD",
        concurrencyMode: "PESSIMISTIC",
      }});
    }}
    if (url === "/v1/projects/p/databases/db2/documents:listCollectionIds") {{
      return jsonResponse({{ collectionIds: [] }});
    }}
    throw new Error("unexpected url in 2A: " + url);
  }};

  // Switching DB aborts prior selectDatabase's getDatabase and listCollectionIds signals and suppresses AbortError.
  const pDb1 = app.selectDatabase("p", "db1", [], true);
  const pDb2 = app.selectDatabase("p", "db2", [], true);
  assert.ok(abortedUrls.includes("/v1/projects/p/databases/db1"));
  assert.ok(abortedUrls.includes("/v1/projects/p/databases/db1/documents:listCollectionIds"));
  const abortErrA = new Error("Aborted");
  abortErrA.name = "AbortError";
  slowDb1Meta.reject(abortErrA);
  slowDb1Cols.reject(abortErrA);
  await pDb1;
  await pDb2;
  assert.equal(app.state.toasts.some((t) => t.tone === "error"), false);

  // Reset cleanly for direct applySyncSseBatch / applyAsyncSseRefresh preemption test:
  fetched.length = 0;
  abortedUrls.length = 0;
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "db1";
  app.state.activeDbMeta = {{
    name: "projects/p/databases/db1",
    uid: "u1",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "STANDARD",
    concurrencyMode: "PESSIMISTIC",
  }};
  app.state.segments = ["users", "alice"];
  app.state.rootCollections = ["users"];
  app.state.querySuggestions.fieldsByScope.set("|users|0", {{
    items: [],
    aggItems: [],
    fieldHints: new Map(),
    scope: {{ parentSubpath: "", collectionId: "users", allDescendants: false }},
    loading: false,
    stale: false,
    partial: false,
    sampledDocs: 1,
  }});

  const inflightGetDoc = deferred();
  const inflightDocCols = deferred();
  const inflightSignals = [];
  fetchImpl = async (url, init) => {{
    fetched.push(url);
    if (init.signal) inflightSignals.push({{ url, signal: init.signal }});
    if (url === `/v1/${{rootPath}}:listCollectionIds`) {{
      return jsonResponse({{ collectionIds: ["users"] }});
    }}
    if (url.startsWith(`/v1/${{rootPath}}/users?`)) {{
      return jsonResponse({{ documents: [{{ name: `${{rootPath}}/users/alice` }}] }});
    }}
    if (url === `/v1/${{rootPath}}/users/alice`) {{
      return inflightGetDoc.promise;
    }}
    if (url === `/v1/${{rootPath}}/users/alice:listCollectionIds`) {{
      return inflightDocCols.promise;
    }}
    if (url === "/emulator/v1") return jsonResponse([]);
    throw new Error("unexpected url in preemption test: " + url);
  }};

  const plan1 = app.applySyncSseBatch([{{
    type: "documents_changed",
    database: rootPath,
    paths: [`${{rootPath}}/users/alice`],
  }}]);
  assert.ok(plan1);
  assert.equal(app.state.querySuggestions.fieldsByScope.get("|users|0").stale, true);

  const pRefresh1 = app.applyAsyncSseRefresh(plan1);
  for (let tick = 0; tick < 5 && !inflightSignals.some((s) => s.url.endsWith("/users/alice")); tick++) {{
    await new Promise((r) => setTimeout(r, 0));
  }}
  assert.ok(inflightSignals.some((s) => s.url.endsWith(":listCollectionIds")));
  assert.ok(inflightSignals.some((s) => s.url.includes("/users?")));
  assert.ok(inflightSignals.some((s) => s.url.endsWith("/users/alice")));

  const fetchedBeforeClear = fetched.length;
  // Synchronous database_cleared preemption while plan1 is awaiting getDocument:
  const plan2 = app.applySyncSseBatch([{{
    type: "database_cleared",
    database: rootPath,
  }}]);
  assert.ok(plan2);
  assert.equal(plan1.signal.aborted, true, "applySyncSseBatch must abort prior DB signal synchronously");
  assert.equal(app.state.activeProjectId, null);
  assert.equal(app.state.activeDatabaseId, null);
  assert.equal(app.state.querySuggestions.fieldsByScope.size, 0);

  // Reject aborted getDocument + listCollectionIds with AbortError; no error toast, no further /v1 calls!
  const abortErr = new Error("Aborted");
  abortErr.name = "AbortError";
  inflightGetDoc.reject(abortErr);
  inflightDocCols.reject(abortErr);
  await pRefresh1;
  await app.applyAsyncSseRefresh(plan2);

  const postClearV1Calls = fetched.slice(fetchedBeforeClear).filter((u) => u.startsWith("/v1/"));
  assert.deepEqual(postClearV1Calls, [], "zero post-notification /v1 calls allowed against cleared DB");
  assert.equal(app.state.toasts.some((t) => t.tone === "error"), false);

  // state.ssePaused preserved & blocks applySyncSseBatch.
  app.state.ssePaused = true;
  assert.equal(app.applySyncSseBatch([{{ type: "resync" }}]), null);
  assert.equal(app.state.ssePaused, true);
  app.state.ssePaused = false;
}}

// 3. resetQuerySuggestionStore (closes combobox, epoch stale-response drop) and markSuggestionsStale exact boundaries.
{{
  const store = qsugg.createQuerySuggestionStore();
  let cleaned = 0;
  store.comboboxUi.openId = "query-collection-id";
  store.comboboxUi.cleanup = () => {{ cleaned++; }};

  const slowRunQuery = deferred();
  let sugSignal = null;
  fetchImpl = async (_url, init) => {{
    sugSignal = init.signal;
    return slowRunQuery.promise;
  }};

  const pField = qsugg.ensureFieldSuggestions(
    store,
    {{ rootPath, rootCollections: [], onRerender: () => {{}} }},
    {{ parentSubpath: "", collectionId: "orders", allDescendants: false }}
  );
  assert.equal(sugSignal.aborted, false);
  const epochBefore = store.epoch;

  qsugg.resetQuerySuggestionStore(store);
  assert.equal(sugSignal.aborted, true);
  assert.equal(store.epoch, epochBefore + 1);
  assert.equal(store.fieldsByScope.size, 0);
  assert.equal(store.comboboxUi.openId, null);
  assert.equal(cleaned, 1);

  // Resolving stale suggestion fetch after reset must NOT repopulate cache.
  slowRunQuery.resolve(jsonResponse([{{ document: {{ name: `${{rootPath}}/orders/o1`, fields: {{ total: {{ integerValue: "1" }} }} }} }}]));
  await pField;
  assert.equal(store.fieldsByScope.size, 0);

  // Exact segment-boundary markSuggestionsStale across all 4 caches (including ancestor docIdsByCollection).
  const mkEntry = (scope) => ({{
    items: [],
    aggItems: [],
    fieldHints: new Map(),
    scope,
    loading: false,
    stale: false,
    partial: false,
    sampledDocs: 0,
  }});
  store.subcollectionsByParent.set("orgs/acme", {{ items: [], loading: false, stale: false, partial: false }});
  store.subcollectionsByParent.set("orgs/acme/projects/p1", {{ items: [], loading: false, stale: false, partial: false }});
  store.subcollectionsByParent.set("orgs/acme2", {{ items: [], loading: false, stale: false, partial: false }});

  store.docIdsByCollection.set("orgs/acme/projects/p1/tasks", {{ items: [], loading: false, stale: false, partial: false }});
  store.docIdsByCollection.set("orgs/acme/projects", {{ items: [], loading: false, stale: false, partial: false }});
  store.docIdsByCollection.set("orgs", {{ items: [], loading: false, stale: false, partial: false }});
  store.docIdsByCollection.set("orgs/acme/projects2", {{ items: [], loading: false, stale: false, partial: false }});

  store.collectionGroupsByParent.set("", {{ items: [], loading: false, stale: false, partial: false }});
  store.collectionGroupsByParent.set("orgs/acme", {{ items: [], loading: false, stale: false, partial: false }});
  store.collectionGroupsByParent.set("orgs/other", {{ items: [], loading: false, stale: false, partial: false }});

  store.fieldsByScope.set("orgs/acme/projects/p1|tasks|0", mkEntry({{ parentSubpath: "orgs/acme/projects/p1", collectionId: "tasks", allDescendants: false }}));
  store.fieldsByScope.set("orgs/acme|tasks|1", mkEntry({{ parentSubpath: "orgs/acme", collectionId: "tasks", allDescendants: true }}));
  store.fieldsByScope.set("|tasks|1", mkEntry({{ parentSubpath: "", collectionId: "tasks", allDescendants: true }}));
  store.fieldsByScope.set("orgs/other|tasks|1", mkEntry({{ parentSubpath: "orgs/other", collectionId: "tasks", allDescendants: true }}));
  store.fieldsByScope.set("orgs/acme/projects/p1|tasks_v2|0", mkEntry({{ parentSubpath: "orgs/acme/projects/p1", collectionId: "tasks_v2", allDescendants: false }}));

  qsugg.markSuggestionsStale(
    store,
    new Set([`${{rootPath}}/orgs/acme/projects/p1/tasks/t1`])
  );

  assert.equal(store.subcollectionsByParent.get("orgs/acme").stale, true);
  assert.equal(store.subcollectionsByParent.get("orgs/acme/projects/p1").stale, true);
  assert.equal(store.subcollectionsByParent.get("orgs/acme2").stale, false);

  assert.equal(store.docIdsByCollection.get("orgs/acme/projects/p1/tasks").stale, true);
  assert.equal(store.docIdsByCollection.get("orgs/acme/projects").stale, true);
  assert.equal(store.docIdsByCollection.get("orgs").stale, true);
  assert.equal(store.docIdsByCollection.get("orgs/acme/projects2").stale, false);

  assert.equal(store.collectionGroupsByParent.get("").stale, true);
  assert.equal(store.collectionGroupsByParent.get("orgs/acme").stale, true);
  assert.equal(store.collectionGroupsByParent.get("orgs/other").stale, false);

  assert.equal(store.fieldsByScope.get("orgs/acme/projects/p1|tasks|0").stale, true);
  assert.equal(store.fieldsByScope.get("orgs/acme|tasks|1").stale, true);
  assert.equal(store.fieldsByScope.get("|tasks|1").stale, true);
  assert.equal(store.fieldsByScope.get("orgs/other|tasks|1").stale, false);
  assert.equal(store.fieldsByScope.get("orgs/acme/projects/p1|tasks_v2|0").stale, false);
}}

// 4. toggleSsePaused unpausing marks suggestions stale, and onOpenQueryCollection preserves suggestion caches.
{{
  app.state.activeProjectId = "p";
  app.state.activeDatabaseId = "db1";
  app.state.activeDbMeta = {{
    name: "projects/p/databases/db1",
    uid: "u1",
    type: "FIRESTORE_NATIVE",
    databaseEdition: "STANDARD",
    concurrencyMode: "PESSIMISTIC",
  }};
  app.state.querySuggestions.fieldsByScope.set("|orders|0", {{
    items: [],
    aggItems: [],
    fieldHints: new Map(),
    scope: {{ parentSubpath: "", collectionId: "orders", allDescendants: false }},
    loading: false,
    stale: false,
    partial: false,
    sampledDocs: 2,
  }});
  app.state.ssePaused = true;
  fetchImpl = async (url) => {{
    if (url === "/emulator/v1") return jsonResponse([rootPath]);
    if (url === "/v1/projects/p/databases/db1") return jsonResponse(app.state.activeDbMeta);
    if (url === "/v1/projects/p/databases/db1/documents:listCollectionIds") return jsonResponse({{ collectionIds: ["orders"] }});
    throw new Error("unexpected url on unpause: " + url);
  }};
  await app.toggleSsePaused();
  assert.equal(app.state.querySuggestions.fieldsByScope.get("|orders|0").stale, true);

  // Explorer Query button (onOpenQueryCollection) replaces queryState while keeping querySuggestions!
  app.state.view = "explorer";
  app.state.rootCollections = ["orders"];
  app.state.segments = ["orders"];
  app.state.collectionColumns = new Map([[
    `${{rootPath}}/orders`,
    {{
      collectionPath: `${{rootPath}}/orders`,
      collectionId: "orders",
      parentPath: rootPath,
      documents: [],
      nextPageToken: "",
      filterText: "",
      loading: false,
    }},
  ]]);
  const queryBtn = findClickHandler(app.renderWorkspace(), "Query");
  assert.ok(queryBtn);
  queryBtn();
  assert.equal(app.state.view, "query");
  assert.equal(app.state.queryState.collectionId, "orders");
  assert.equal(app.state.querySuggestions.fieldsByScope.has("|orders|0"), true, "onOpenQueryCollection must preserve suggestion cache");
}}

console.log("slice2_cache_and_sse_lifecycle passed");
"#
    );

    run_node_ui_test(&script);
}
