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
