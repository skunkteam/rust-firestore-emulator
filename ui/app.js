// @ts-check

import { html, nothing, render } from "./lit-html.js";
import {
  ApiError,
  clearDatabase,
  connectEventStream,
  createDocument,
  deleteCollection,
  deleteDocument,
  getDatabase,
  getDocument,
  listCollectionIds,
  listDatabases,
  listDocuments,
  parseRootRef,
  patchDatabase,
  runQuery,
  updateDocument,
} from "./api.js";
import { generateAutoId } from "./values.js";
import {
  computeChangedFieldMask,
  createEditorSession,
  extractSessionFields,
  renderHybridEditor,
  syncSessionWithDocument,
} from "./components/editor.js";
import { renderExplorer } from "./components/explorer.js";
import {
  buildStructuredQueryFromVisual,
  createQueryRunnerState,
  renderQueryRunner,
} from "./components/query.js";

/** @typedef {import('./api.js').DatabaseMetadata} DatabaseMetadata */
/** @typedef {import('./api.js').EmulatorSseEvent} EmulatorSseEvent */
/** @typedef {import('./components/explorer.js').CollectionColumnState} CollectionColumnState */
/** @typedef {import('./components/explorer.js').DocumentLevelState} DocumentLevelState */
/** @typedef {import('./components/editor.js').EditorSession} EditorSession */
/** @typedef {import('./lit-html.js').TemplateResult} TemplateResult */

/**
 * @typedef {
 *   | { kind: 'open_db', projectId: string, databaseId: string, edition: 'STANDARD' | 'ENTERPRISE', busy: boolean, error: string | null }
 *   | { kind: 'create_doc', parentPath: string, collectionId: string, lockCollection: boolean, docId: string, lockDocId: boolean, session: EditorSession, busy: boolean, error: string | null }
 *   | { kind: 'delete_doc', docPath: string, recursive: boolean, hasSubcollections: boolean, busy: boolean, error: string | null }
 *   | { kind: 'delete_col', colPath: string, colId: string, busy: boolean, error: string | null }
 *   | { kind: 'clear_db', projectId: string, databaseId: string, busy: boolean, error: string | null }
 * } ActiveModal
 */

/** @type {HTMLElement} */
const topbarEl = /** @type {HTMLElement} */ (document.getElementById("topbar"));
/** @type {HTMLElement} */
const workspaceEl = /** @type {HTMLElement} */ (
  document.getElementById("workspace")
);
/** @type {HTMLElement} */
const modalRootEl = /** @type {HTMLElement} */ (
  document.getElementById("modal-root")
);
/** @type {HTMLElement} */
const toastRootEl = /** @type {HTMLElement} */ (
  document.getElementById("toast-root")
);

const emulatorVersion = String(
  /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (window))
    .EMULATOR_VERSION ?? "dev"
);

const state = {
  /** @type {Array<{ projectId: string, databaseId: string, rootPath: string }>} */
  databases: [],
  /** @type {string | null} */
  activeProjectId: null,
  /** @type {string | null} */
  activeDatabaseId: null,
  /** @type {DatabaseMetadata | null} */
  activeDbMeta: null,

  /** @type {'explorer' | 'query'} */
  view: "explorer",

  /** @type {string[]} */
  rootCollections: [],
  rootLoading: false,
  /** @type {string[]} */
  segments: [],

  /** @type {Map<string, CollectionColumnState>} */
  collectionColumns: new Map(),
  /** @type {Map<string, DocumentLevelState>} */
  documentLevels: new Map(),

  queryState: createQueryRunnerState(),

  /** @type {'live' | 'error'} */
  sseStatus: "live",
  ssePaused: false,

  /** @type {ActiveModal | null} */
  modal: null,

  /** @type {Array<{ id: number, text: string, tone: 'info' | 'success' | 'error' }>} */
  toasts: [],
};

let nextToastId = 1;

/**
 * @param {string} text
 * @param {'info' | 'success' | 'error'} [tone]
 */
function pushToast(text, tone = "info") {
  const id = nextToastId++;
  state.toasts.push({ id, text, tone });
  renderApp();
  setTimeout(() => {
    state.toasts = state.toasts.filter((t) => t.id !== id);
    renderApp();
  }, 4200);
}

/**
 * Parses `/projects/{p}/databases/{d}/documents[/...]` from a browser pathname.
 * @param {string} pathname
 * @returns {{ projectId: string, databaseId: string, segments: string[] } | null}
 */
function parseBrowserRoute(pathname) {
  const stripped = pathname.replace(/\/+$/, "");
  if (!stripped) return null;
  const m =
    /^\/projects\/([^/]+)\/databases\/([^/]+)\/documents(?:\/(.*))?$/.exec(
      stripped
    );
  if (!m) return null;
  const projectId = decodeURIComponent(m[1]);
  const databaseId = decodeURIComponent(m[2]);
  const tail = m[3] ? m[3].split("/").map((s) => decodeURIComponent(s)) : [];
  return { projectId, databaseId, segments: tail };
}

/**
 * Syncs the browser address bar with the current active database and segments.
 * @param {boolean} [replace]
 */
function syncBrowserUrl(replace = false) {
  let nextPath = "/";
  if (state.activeProjectId && state.activeDatabaseId) {
    const base = `/projects/${encodeURIComponent(state.activeProjectId)}/databases/${encodeURIComponent(state.activeDatabaseId)}/documents`;
    const tail = state.segments.map((s) => encodeURIComponent(s)).join("/");
    nextPath = tail ? `${base}/${tail}` : base;
  }
  if (window.location.pathname !== nextPath) {
    if (replace) {
      window.history.replaceState(null, "", nextPath);
    } else {
      window.history.pushState(null, "", nextPath);
    }
  }
}

/**
 * Refreshes the list of active databases from `GET /emulator/v1` without creating any database.
 */
async function refreshDatabaseList() {
  const rawList = await listDatabases();
  const parsed = rawList
    .map(parseRootRef)
    .filter((item) => item !== null)
    .sort((a, b) => a.rootPath.localeCompare(b.rootPath));
  state.databases = parsed;
}

/**
 * Switches to (or initializes) a target database and loads its metadata & root collections.
 * @param {string} projectId
 * @param {string} databaseId
 * @param {string[]} [initialSegments]
 * @param {boolean} [replaceHistory]
 */
async function selectDatabase(
  projectId,
  databaseId,
  initialSegments = [],
  replaceHistory = false
) {
  const dbChanged =
    state.activeProjectId !== projectId || state.activeDatabaseId !== databaseId;
  state.activeProjectId = projectId;
  state.activeDatabaseId = databaseId;
  state.segments = initialSegments;

  if (dbChanged) {
    state.rootCollections = [];
    state.collectionColumns.clear();
    state.documentLevels.clear();
    state.queryState = createQueryRunnerState();
  }

  syncBrowserUrl(replaceHistory);
  renderApp();

  try {
    state.rootLoading = true;
    renderApp();
    const [meta, cols] = await Promise.all([
      getDatabase(projectId, databaseId),
      listCollectionIds(
        `projects/${projectId}/databases/${databaseId}/documents`
      ),
    ]);
    state.activeDbMeta = meta;
    state.rootCollections = cols;
    await refreshDatabaseList();
  } catch (err) {
    pushToast(
      `Failed to load database: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    state.rootLoading = false;
    renderApp();
  }

  if (initialSegments.length > 0) {
    await hydrateVisiblePath();
  }
}

/**
 * Loads all collection columns and document levels along `state.segments`.
 */
async function hydrateVisiblePath() {
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;

  for (let i = 0; i < state.segments.length; i += 2) {
    const colId = state.segments[i];
    const parentSegments = state.segments.slice(0, i);
    const colSegments = state.segments.slice(0, i + 1);
    const parentPath =
      parentSegments.length === 0
        ? rootPath
        : `${rootPath}/${parentSegments.join("/")}`;
    const colPath = `${rootPath}/${colSegments.join("/")}`;

    await loadCollectionColumn(colPath, parentPath, colId, false);

    if (state.segments.length > i + 1) {
      const docId = state.segments[i + 1];
      const docPath = `${colPath}/${docId}`;
      await loadDocumentLevel(docPath, docId, false);
    }
  }
}

/**
 * @param {string} colPath
 * @param {string} parentPath
 * @param {string} colId
 * @param {boolean} appendPage
 */
async function loadCollectionColumn(colPath, parentPath, colId, appendPage) {
  let colState = state.collectionColumns.get(colPath);
  if (!colState) {
    colState = {
      collectionPath: colPath,
      collectionId: colId,
      parentPath,
      documents: [],
      nextPageToken: "",
      filterText: "",
      loading: true,
    };
    state.collectionColumns.set(colPath, colState);
  } else {
    colState.loading = true;
  }
  renderApp();

  try {
    const res = await listDocuments(parentPath, colId, {
      showMissing: true,
      maskFieldPaths: ["__name__"],
      pageSize: 200,
      pageToken: appendPage ? colState.nextPageToken : undefined,
    });
    const mapped = res.documents.map((doc) => {
      const fullName = doc.name ?? "";
      const id = fullName.slice(fullName.lastIndexOf("/") + 1);
      const phantom = !doc.createTime && !doc.updateTime;
      return { id, fullName, phantom };
    });
    colState.documents = appendPage
      ? [...colState.documents, ...mapped]
      : mapped;
    colState.nextPageToken = res.nextPageToken;
  } catch (err) {
    pushToast(
      `Failed to list ${colId}: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    colState.loading = false;
    renderApp();
  }
}

/**
 * Loads a document and its subcollections, respecting the dirty-state guard if `preserveDirty` is true.
 * @param {string} docPath
 * @param {string} docId
 * @param {boolean} preserveDirty
 */
async function loadDocumentLevel(docPath, docId, preserveDirty) {
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const projectId = state.activeProjectId;
  const databaseId = state.activeDatabaseId;

  let level = state.documentLevels.get(docPath);
  if (!level) {
    level = {
      docPath,
      docId,
      exists: false,
      loading: true,
      subcollections: [],
      editorSession: null,
    };
    state.documentLevels.set(docPath, level);
  } else {
    level.loading = true;
  }
  renderApp();

  try {
    const [docOutcome, subcols] = await Promise.all([
      getDocument(docPath).then(
        (doc) => ({ exists: true, doc }),
        (err) => {
          if (err instanceof ApiError && err.code === 404) {
            return { exists: false, doc: null };
          }
          throw err;
        }
      ),
      listCollectionIds(docPath),
    ]);

    level.subcollections = subcols;
    level.exists = docOutcome.exists;

    if (docOutcome.exists && docOutcome.doc) {
      if (
        preserveDirty &&
        level.editorSession &&
        level.editorSession.isDirty
      ) {
        if (
          level.editorSession.cleanDoc?.updateTime !==
          docOutcome.doc.updateTime
        ) {
          level.editorSession.externalDoc = docOutcome.doc;
        }
      } else if (level.editorSession) {
        syncSessionWithDocument(level.editorSession, docOutcome.doc);
      } else {
        level.editorSession = createEditorSession(
          docPath,
          projectId,
          databaseId,
          docOutcome.doc
        );
      }
    } else {
      if (!preserveDirty || !level.editorSession?.isDirty) {
        level.editorSession = null;
      }
    }
  } catch (err) {
    pushToast(
      `Failed to load document ${docId}: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    level.loading = false;
    renderApp();
  }
}

/**
 * Handles debounced SSE invalidation batches without clobbering dirty edits or resurrecting cleared databases.
 * @param {EmulatorSseEvent[]} events
 */
async function handleSseBatch(events) {
  if (state.ssePaused) return;

  let shouldRefreshDbList = false;
  let shouldRefreshMeta = false;
  let fullResyncActiveDb = false;
  /** @type {Set<string>} */
  const changedPaths = new Set();

  const activeRoot =
    state.activeProjectId && state.activeDatabaseId
      ? `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`
      : null;

  for (const ev of events) {
    if (ev.type === "resync") {
      shouldRefreshDbList = true;
      fullResyncActiveDb = true;
      continue;
    }
    if (ev.type === "database_created") {
      shouldRefreshDbList = true;
      continue;
    }
    if (ev.type === "database_cleared") {
      shouldRefreshDbList = true;
      if (ev.database && ev.database === activeRoot) {
        // Clear cached state WITHOUT calling /v1/projects/... so we never re-create the cleared database!
        state.activeProjectId = null;
        state.activeDatabaseId = null;
        state.activeDbMeta = null;
        state.rootCollections = [];
        state.segments = [];
        state.collectionColumns.clear();
        state.documentLevels.clear();
        syncBrowserUrl(true);
        pushToast(`Database ${ev.database} was cleared`, "info");
      }
      continue;
    }
    if (ev.type === "database_config_changed") {
      if (ev.database && ev.database === activeRoot) {
        shouldRefreshMeta = true;
      }
      continue;
    }
    if (ev.type === "documents_changed") {
      if (ev.database && ev.database === activeRoot) {
        for (const p of ev.paths ?? []) {
          changedPaths.add(p);
        }
      }
    }
  }

  if (shouldRefreshDbList) {
    await refreshDatabaseList();
    // If no active DB is selected and a DB now exists, auto-select the first one cleanly.
    if (!state.activeProjectId && state.databases.length > 0) {
      const first = state.databases[0];
      await selectDatabase(first.projectId, first.databaseId, [], true);
      return;
    }
    renderApp();
  }

  if (!state.activeProjectId || !state.activeDatabaseId || !activeRoot) {
    return;
  }

  if (shouldRefreshMeta || fullResyncActiveDb) {
    try {
      state.activeDbMeta = await getDatabase(
        state.activeProjectId,
        state.activeDatabaseId
      );
    } catch {
      // Ignore transient error
    }
  }

  if (fullResyncActiveDb || changedPaths.size > 0) {
    try {
      state.rootCollections = await listCollectionIds(activeRoot);
    } catch {
      // Ignore
    }

    for (let i = 0; i < state.segments.length; i += 2) {
      const colId = state.segments[i];
      const parentSegments = state.segments.slice(0, i);
      const colSegments = state.segments.slice(0, i + 1);
      const parentPath =
        parentSegments.length === 0
          ? activeRoot
          : `${activeRoot}/${parentSegments.join("/")}`;
      const colPath = `${activeRoot}/${colSegments.join("/")}`;

      const affectsCollection =
        fullResyncActiveDb ||
        Array.from(changedPaths).some((p) => p.startsWith(`${colPath}/`));

      if (affectsCollection) {
        await loadCollectionColumn(colPath, parentPath, colId, false);
      }

      if (state.segments.length > i + 1) {
        const docId = state.segments[i + 1];
        const docPath = `${colPath}/${docId}`;
        const affectsDoc =
          fullResyncActiveDb ||
          changedPaths.has(docPath) ||
          Array.from(changedPaths).some((p) => p.startsWith(`${docPath}/`));
        if (affectsDoc) {
          await loadDocumentLevel(docPath, docId, true);
        }
      }
    }
    renderApp();
  }
}

/**
 * Saves an edited document from its EditorSession.
 * @param {DocumentLevelState} docLevel
 */
async function saveDocumentFromInspector(docLevel) {
  const session = docLevel.editorSession;
  if (!session) return;

  const extracted = extractSessionFields(session);
  if (!extracted.valid || !extracted.fields) {
    session.saveError = extracted.error ?? "Invalid field values";
    renderApp();
    return;
  }

  session.saving = true;
  session.saveError = null;
  renderApp();

  try {
    const cleanFields = session.cleanDoc?.fields ?? {};
    const updateMask =
      session.mode === "structured"
        ? computeChangedFieldMask(cleanFields, extracted.fields)
        : null;

    if (updateMask !== null && updateMask.length === 0) {
      session.isDirty = false;
      session.saving = false;
      renderApp();
      return;
    }

    const updated = await updateDocument(
      docLevel.docPath,
      extracted.fields,
      updateMask,
      session.cleanDoc?.updateTime ?? null
    );
    syncSessionWithDocument(session, updated);
    pushToast(`Saved ${docLevel.docId}`, "success");
  } catch (err) {
    session.saving = false;
    session.saveError = err instanceof Error ? err.message : String(err);
  }
  renderApp();
}

/**
 * Executes the current query in QueryRunnerState.
 */
async function executeCurrentQuery() {
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const qState = state.queryState;
  qState.error = null;

  /** @type {Record<string, unknown>} */
  let structuredQuery;
  if (qState.mode === "visual") {
    const built = buildStructuredQueryFromVisual(
      qState,
      state.activeProjectId,
      state.activeDatabaseId
    );
    if (!built.valid || !built.query) {
      qState.error = built.error ?? "Invalid query";
      renderApp();
      return;
    }
    structuredQuery = built.query;
    qState.rawQueryText = JSON.stringify(structuredQuery, null, 2);
  } else {
    try {
      const parsed = JSON.parse(qState.rawQueryText);
      structuredQuery =
        parsed && typeof parsed === "object" && parsed.structuredQuery
          ? parsed.structuredQuery
          : parsed;
    } catch (err) {
      qState.error = `Invalid JSON: ${err instanceof Error ? err.message : String(err)}`;
      renderApp();
      return;
    }
  }

  const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
  const cleanParentSub = qState.parentSubpath.trim().replace(/^\/+|\/+$/g, "");
  const parentPath = cleanParentSub ? `${rootPath}/${cleanParentSub}` : rootPath;

  qState.running = true;
  renderApp();
  const t0 = performance.now();

  try {
    const rows = await runQuery(parentPath, structuredQuery);
    qState.elapsedMs = performance.now() - t0;
    qState.readTime = rows[0]?.readTime ?? null;
    qState.results = rows
      .map((r) => r.document)
      .filter((d) => d !== undefined);
  } catch (err) {
    qState.error = err instanceof Error ? err.message : String(err);
  } finally {
    qState.running = false;
    renderApp();
  }
}

/**
 * Toggles database edition or concurrency mode via PATCH /v1/projects/{p}/databases/{d}.
 * @param {'edition' | 'concurrency'} which
 */
async function toggleDatabaseSetting(which) {
  if (!state.activeProjectId || !state.activeDatabaseId || !state.activeDbMeta) {
    return;
  }
  try {
    const patch =
      which === "edition"
        ? {
            databaseEdition: /** @type {'STANDARD' | 'ENTERPRISE'} */ (
              state.activeDbMeta.databaseEdition === "ENTERPRISE"
                ? "STANDARD"
                : "ENTERPRISE"
            ),
          }
        : {
            concurrencyMode: /** @type {'PESSIMISTIC' | 'OPTIMISTIC'} */ (
              state.activeDbMeta.concurrencyMode === "OPTIMISTIC"
                ? "PESSIMISTIC"
                : "OPTIMISTIC"
            ),
          };
    state.activeDbMeta = await patchDatabase(
      state.activeProjectId,
      state.activeDatabaseId,
      patch
    );
    renderApp();
  } catch (err) {
    pushToast(
      `Failed to update database config: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  }
}

function openDatabaseModal() {
  state.modal = {
    kind: "open_db",
    projectId: state.activeProjectId ?? "demo-project",
    databaseId: state.activeDatabaseId ?? "(default)",
    edition: "STANDARD",
    busy: false,
    error: null,
  };
  renderApp();
}

/**
 * @param {string} parentPath
 * @param {string} [collectionId]
 * @param {string} [fixedDocId]
 */
function openCreateDocumentModal(
  parentPath,
  collectionId = "",
  fixedDocId = ""
) {
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const session = createEditorSession(
    "",
    state.activeProjectId,
    state.activeDatabaseId,
    null
  );
  state.modal = {
    kind: "create_doc",
    parentPath,
    collectionId,
    lockCollection: Boolean(collectionId),
    docId: fixedDocId || generateAutoId(),
    lockDocId: Boolean(fixedDocId),
    session,
    busy: false,
    error: null,
  };
  renderApp();
}

function renderTopbar() {
  const activeKey =
    state.activeProjectId && state.activeDatabaseId
      ? `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`
      : "";

  const dotClass = state.ssePaused
    ? "paused"
    : state.sseStatus === "live"
      ? "live"
      : "error";
  const sseLabel = state.ssePaused
    ? "Live Paused"
    : state.sseStatus === "live"
      ? "Live"
      : "Reconnecting";

  return html`
    <div class="topbar-inner">
      <div class="topbar-left">
        <a
          class="brand"
          href="/"
          @click=${(/** @type {MouseEvent} */ e) => {
            e.preventDefault();
            if (state.activeProjectId && state.activeDatabaseId) {
              state.view = "explorer";
              state.segments = [];
              syncBrowserUrl();
              renderApp();
            }
          }}
        >
          <span class="brand-dot"></span>
          <span>Firestore Emulator</span>
        </a>
        <span class="version-pill">v${emulatorVersion}</span>

        ${state.databases.length > 0
          ? html`
              <select
                class="select mono"
                .value=${activeKey}
                @change=${(/** @type {Event} */ e) => {
                  const val = /** @type {HTMLSelectElement} */ (e.target).value;
                  const parsed = parseRootRef(val);
                  if (parsed) {
                    void selectDatabase(parsed.projectId, parsed.databaseId, []);
                  }
                }}
              >
                ${!activeKey
                  ? html`<option value="">— Select database —</option>`
                  : nothing}
                ${state.databases.map(
                  (db) => html`
                    <option
                      value=${db.rootPath}
                      ?selected=${db.rootPath === activeKey}
                    >
                      ${db.projectId} / ${db.databaseId}
                    </option>
                  `
                )}
              </select>
            `
          : nothing}

        <button type="button" class="btn btn-sm" @click=${openDatabaseModal}>
          + Open / Create Database
        </button>

        ${state.activeDbMeta
          ? html`
              <button
                type="button"
                class="config-pill ${state.activeDbMeta.databaseEdition ===
                "ENTERPRISE"
                  ? "enterprise"
                  : ""}"
                title="Click to toggle STANDARD / ENTERPRISE edition"
                @click=${() => void toggleDatabaseSetting("edition")}
              >
                ${state.activeDbMeta.databaseEdition}
              </button>
              <button
                type="button"
                class="config-pill ${state.activeDbMeta.concurrencyMode ===
                "OPTIMISTIC"
                  ? "optimistic"
                  : ""}"
                title="Click to toggle PESSIMISTIC / OPTIMISTIC concurrency"
                @click=${() => void toggleDatabaseSetting("concurrency")}
              >
                ${state.activeDbMeta.concurrencyMode}
              </button>
            `
          : nothing}
      </div>

      <div class="topbar-right">
        ${state.activeProjectId && state.activeDatabaseId
          ? html`
              <div class="segmented">
                <button
                  type="button"
                  class=${state.view === "explorer" ? "active" : ""}
                  @click=${() => {
                    state.view = "explorer";
                    renderApp();
                  }}
                >
                  Explorer
                </button>
                <button
                  type="button"
                  class=${state.view === "query" ? "active" : ""}
                  @click=${() => {
                    state.view = "query";
                    renderApp();
                  }}
                >
                  Query Runner
                </button>
              </div>

              <button
                type="button"
                class="btn btn-sm btn-danger"
                @click=${() => {
                  if (state.activeProjectId && state.activeDatabaseId) {
                    state.modal = {
                      kind: "clear_db",
                      projectId: state.activeProjectId,
                      databaseId: state.activeDatabaseId,
                      busy: false,
                      error: null,
                    };
                    renderApp();
                  }
                }}
              >
                Clear DB
              </button>
            `
          : nothing}

        <button
          type="button"
          class="sse-badge"
          title="Click to pause or resume live SSE updates"
          @click=${() => {
            state.ssePaused = !state.ssePaused;
            renderApp();
          }}
        >
          <span class="sse-dot ${dotClass}"></span>
          <span>${sseLabel}</span>
        </button>
      </div>
    </div>
  `;
}

function renderWorkspace() {
  if (!state.activeProjectId || !state.activeDatabaseId) {
    return html`
      <div class="empty-hero">
        <h2>No Active Firestore Database</h2>
        <p>
          Connect your test suite to automatically populate databases, or create
          a database right now to start adding collections and documents.
        </p>
        <button
          type="button"
          class="btn btn-primary"
          @click=${openDatabaseModal}
        >
          + Open / Create Database
        </button>
      </div>
    `;
  }

  if (state.view === "query") {
    return renderQueryRunner({
      projectId: state.activeProjectId,
      databaseId: state.activeDatabaseId,
      knownCollections: state.rootCollections,
      state: state.queryState,
      onRerender: renderApp,
      onExecuteQuery: () => void executeCurrentQuery(),
      onOpenResultDocument: (docName) => {
        const m =
          /^projects\/[^/]+\/databases\/[^/]+\/documents\/(.+)$/.exec(docName);
        if (m) {
          state.view = "explorer";
          state.segments = m[1].split("/");
          syncBrowserUrl();
          renderApp();
          void hydrateVisiblePath();
        }
      },
    });
  }

  return renderExplorer({
    projectId: state.activeProjectId,
    databaseId: state.activeDatabaseId,
    rootCollections: state.rootCollections,
    rootLoading: state.rootLoading,
    segments: state.segments,
    collectionColumns: state.collectionColumns,
    documentLevels: state.documentLevels,
    onSelectSegments: (nextSegments) => {
      state.segments = nextSegments;
      syncBrowserUrl();
      renderApp();
      void hydrateVisiblePath();
    },
    onOpenStartCollection: (parentPath) => openCreateDocumentModal(parentPath),
    onOpenAddDocument: (parentPath, colId, fixedDocId) =>
      openCreateDocumentModal(parentPath, colId, fixedDocId),
    onOpenDeleteCollection: (colPath, colId) => {
      state.modal = {
        kind: "delete_col",
        colPath,
        colId,
        busy: false,
        error: null,
      };
      renderApp();
    },
    onOpenDeleteDocument: (docPath, hasSubcollections) => {
      state.modal = {
        kind: "delete_doc",
        docPath,
        recursive: false,
        hasSubcollections,
        busy: false,
        error: null,
      };
      renderApp();
    },
    onOpenQueryCollection: (parentPath, collectionId) => {
      const rootPrefix = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
      const parentSubpath = parentPath.startsWith(`${rootPrefix}/`)
        ? parentPath.slice(rootPrefix.length + 1)
        : "";
      state.queryState = createQueryRunnerState(collectionId, parentSubpath);
      state.view = "query";
      renderApp();
    },
    onLoadMoreDocuments: (colPath) => {
      const col = state.collectionColumns.get(colPath);
      if (col) {
        void loadCollectionColumn(
          col.collectionPath,
          col.parentPath,
          col.collectionId,
          true
        );
      }
    },
    onSaveDocument: (docLevel) => void saveDocumentFromInspector(docLevel),
    onFilterChange: (colPath, filterText) => {
      const col = state.collectionColumns.get(colPath);
      if (col) {
        col.filterText = filterText;
        renderApp();
      }
    },
    onRerender: renderApp,
  });
}

function renderModal() {
  const m = state.modal;
  if (!m) return nothing;

  const close = () => {
    if (m.busy) return;
    state.modal = null;
    renderApp();
  };

  if (m.kind === "open_db") {
    const submit = async () => {
      const p = m.projectId.trim();
      const d = m.databaseId.trim();
      if (!p || !d || p.includes("/") || d.includes("/")) {
        m.error = "Enter valid non-empty Project ID and Database ID (no slashes)";
        renderApp();
        return;
      }
      m.busy = true;
      m.error = null;
      renderApp();
      try {
        await patchDatabase(p, d, { databaseEdition: m.edition });
        state.modal = null;
        await selectDatabase(p, d, []);
        pushToast(`Opened database ${p}/${d}`, "success");
      } catch (err) {
        m.busy = false;
        m.error = err instanceof Error ? err.message : String(err);
        renderApp();
      }
    };

    return html`
      <div class="modal-backdrop" @click=${close}>
        <div
          class="modal-card modal-sm"
          @click=${(/** @type {Event} */ e) => e.stopPropagation()}
        >
          <div class="modal-header">
            <span>Open or Create Database</span>
            <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
              ✕
            </button>
          </div>
          <div class="modal-body">
            ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
            <div class="form-field">
              <label>Project ID</label>
              <input
                type="text"
                class="input mono"
                .value=${m.projectId}
                @input=${(/** @type {Event} */ e) => {
                  m.projectId = /** @type {HTMLInputElement} */ (e.target).value;
                }}
              />
            </div>
            <div class="form-field">
              <label>Database ID</label>
              <input
                type="text"
                class="input mono"
                .value=${m.databaseId}
                @input=${(/** @type {Event} */ e) => {
                  m.databaseId = /** @type {HTMLInputElement} */ (e.target).value;
                }}
              />
            </div>
            <div class="form-field">
              <label>Edition</label>
              <select
                class="select"
                .value=${m.edition}
                @change=${(/** @type {Event} */ e) => {
                  m.edition = /** @type {'STANDARD' | 'ENTERPRISE'} */ (
                    /** @type {HTMLSelectElement} */ (e.target).value
                  );
                }}
              >
                <option value="STANDARD">STANDARD (Pessimistic Concurrency)</option>
                <option value="ENTERPRISE">ENTERPRISE (Optimistic Concurrency)</option>
              </select>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
              Cancel
            </button>
            <button
              type="button"
              class="btn btn-primary"
              ?disabled=${m.busy}
              @click=${submit}
            >
              ${m.busy ? "Opening..." : "Open Database"}
            </button>
          </div>
        </div>
      </div>
    `;
  }

  if (m.kind === "create_doc") {
    const submit = async () => {
      const colId = m.collectionId.trim();
      const docId = m.docId.trim() || generateAutoId();
      if (!colId || colId.includes("/")) {
        m.error = "Collection ID must be a single non-empty segment";
        renderApp();
        return;
      }
      if (docId.includes("/")) {
        m.error = "Document ID cannot contain '/'";
        renderApp();
        return;
      }
      const extracted = extractSessionFields(m.session);
      if (!extracted.valid || !extracted.fields) {
        m.error = extracted.error ?? "Fix field validation errors";
        renderApp();
        return;
      }

      m.busy = true;
      m.error = null;
      renderApp();

      try {
        const created = await createDocument(
          m.parentPath,
          colId,
          docId,
          extracted.fields
        );
        state.modal = null;
        const fullName = created.name ?? "";
        const match =
          /^projects\/[^/]+\/databases\/[^/]+\/documents\/(.+)$/.exec(fullName);
        if (match && state.activeProjectId && state.activeDatabaseId) {
          state.rootCollections = await listCollectionIds(
            `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`
          );
          state.segments = match[1].split("/");
          syncBrowserUrl();
          await hydrateVisiblePath();
        }
        pushToast(`Created ${colId}/${docId}`, "success");
      } catch (err) {
        m.busy = false;
        m.error = err instanceof Error ? err.message : String(err);
      }
      renderApp();
    };

    return html`
      <div class="modal-backdrop" @click=${close}>
        <div
          class="modal-card"
          @click=${(/** @type {Event} */ e) => e.stopPropagation()}
        >
          <div class="modal-header">
            <span>
              ${m.lockCollection
                ? `Add Document to "${m.collectionId}"`
                : "Start Collection & Create First Document"}
            </span>
            <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
              ✕
            </button>
          </div>
          <div class="modal-body">
            ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
            ${!m.lockCollection
              ? html`
                  <div class="form-field">
                    <label>Collection ID</label>
                    <input
                      type="text"
                      class="input mono"
                      placeholder="e.g. users"
                      .value=${m.collectionId}
                      @input=${(/** @type {Event} */ e) => {
                        m.collectionId = /** @type {HTMLInputElement} */ (
                          e.target
                        ).value;
                      }}
                    />
                  </div>
                `
              : nothing}

            <div class="form-row">
              <div class="form-field" style="flex: 1 1 auto;">
                <label>Document ID</label>
                <input
                  type="text"
                  class="input mono"
                  ?disabled=${m.lockDocId}
                  placeholder="20-char auto-ID or custom ID"
                  .value=${m.docId}
                  @input=${(/** @type {Event} */ e) => {
                    m.docId = /** @type {HTMLInputElement} */ (e.target).value;
                  }}
                />
              </div>
              ${!m.lockDocId
                ? html`
                    <button
                      type="button"
                      class="btn"
                      @click=${() => {
                        m.docId = generateAutoId();
                        renderApp();
                      }}
                    >
                      Auto-ID
                    </button>
                  `
                : nothing}
            </div>

            <div style="border: 1px solid var(--border); border-radius: 8px; overflow: hidden;">
              ${renderHybridEditor({
                session: m.session,
                onChange: renderApp,
                embeddedInModal: true,
              })}
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
              Cancel
            </button>
            <button
              type="button"
              class="btn btn-primary"
              ?disabled=${m.busy}
              @click=${submit}
            >
              ${m.busy ? "Creating..." : "Save Document"}
            </button>
          </div>
        </div>
      </div>
    `;
  }

  if (m.kind === "delete_doc") {
    const confirmDelete = async () => {
      m.busy = true;
      m.error = null;
      renderApp();
      try {
        await deleteDocument(m.docPath, m.recursive);
        state.documentLevels.delete(m.docPath);
        state.modal = null;
        if (state.activeProjectId && state.activeDatabaseId) {
          const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
          if (m.recursive || !m.hasSubcollections) {
            state.segments = state.segments.slice(0, -1);
          }
          state.rootCollections = await listCollectionIds(rootPath);
          syncBrowserUrl();
          await hydrateVisiblePath();
        }
        pushToast("Document deleted", "success");
      } catch (err) {
        m.busy = false;
        m.error = err instanceof Error ? err.message : String(err);
      }
      renderApp();
    };

    return html`
      <div class="modal-backdrop" @click=${close}>
        <div
          class="modal-card modal-sm"
          @click=${(/** @type {Event} */ e) => e.stopPropagation()}
        >
          <div class="modal-header">
            <span>Delete Document</span>
            <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
              ✕
            </button>
          </div>
          <div class="modal-body">
            ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
            <p style="margin: 0;">
              Delete document <strong class="mono">${m.docPath}</strong>?
            </p>
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
              <input
                type="checkbox"
                .checked=${m.recursive}
                @change=${(/** @type {Event} */ e) => {
                  m.recursive = /** @type {HTMLInputElement} */ (
                    e.target
                  ).checked;
                  renderApp();
                }}
              />
              <span>Also delete all nested subcollections recursively</span>
            </label>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
              Cancel
            </button>
            <button
              type="button"
              class="btn btn-danger"
              ?disabled=${m.busy}
              @click=${confirmDelete}
            >
              ${m.busy ? "Deleting..." : "Delete"}
            </button>
          </div>
        </div>
      </div>
    `;
  }

  if (m.kind === "delete_col") {
    const confirmDelete = async () => {
      m.busy = true;
      m.error = null;
      renderApp();
      try {
        await deleteCollection(m.colPath);
        state.collectionColumns.delete(m.colPath);
        state.modal = null;
        if (state.activeProjectId && state.activeDatabaseId) {
          const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
          const colIdx = state.segments.indexOf(m.colId);
          if (colIdx >= 0) {
            state.segments = state.segments.slice(0, colIdx);
          }
          state.rootCollections = await listCollectionIds(rootPath);
          syncBrowserUrl();
          await hydrateVisiblePath();
        }
        pushToast(`Deleted collection ${m.colId}`, "success");
      } catch (err) {
        m.busy = false;
        m.error = err instanceof Error ? err.message : String(err);
      }
      renderApp();
    };

    return html`
      <div class="modal-backdrop" @click=${close}>
        <div
          class="modal-card modal-sm"
          @click=${(/** @type {Event} */ e) => e.stopPropagation()}
        >
          <div class="modal-header">
            <span>Delete Collection</span>
            <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
              ✕
            </button>
          </div>
          <div class="modal-body">
            ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
            <p style="margin: 0;">
              Recursively delete all documents and subcollections inside
              <strong class="mono">${m.colId}</strong>?
            </p>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
              Cancel
            </button>
            <button
              type="button"
              class="btn btn-danger"
              ?disabled=${m.busy}
              @click=${confirmDelete}
            >
              ${m.busy ? "Deleting..." : "Delete Collection"}
            </button>
          </div>
        </div>
      </div>
    `;
  }

  // clear_db
  const confirmClearDb = async () => {
    m.busy = true;
    m.error = null;
    renderApp();
    try {
      await clearDatabase(m.projectId, m.databaseId);
      state.modal = null;
      state.activeProjectId = null;
      state.activeDatabaseId = null;
      state.activeDbMeta = null;
      state.rootCollections = [];
      state.segments = [];
      state.collectionColumns.clear();
      state.documentLevels.clear();
      await refreshDatabaseList();
      if (state.databases.length > 0) {
        const next = state.databases[0];
        await selectDatabase(next.projectId, next.databaseId, [], true);
      } else {
        syncBrowserUrl(true);
      }
      pushToast(`Cleared database ${m.projectId}/${m.databaseId}`, "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    renderApp();
  };

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card modal-sm"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>Clear Entire Database</span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          <p style="margin: 0;">
            Wipe all collections and documents in
            <strong class="mono">${m.projectId}/${m.databaseId}</strong>? This
            cannot be undone.
          </p>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
            Cancel
          </button>
          <button
            type="button"
            class="btn btn-danger"
            ?disabled=${m.busy}
            @click=${confirmClearDb}
          >
            ${m.busy ? "Clearing..." : "Clear Database"}
          </button>
        </div>
      </div>
    </div>
  `;
}

function renderToasts() {
  return html`
    ${state.toasts.map(
      (t) => html`
        <div class="toast ${t.tone === "error" ? "error" : t.tone === "success" ? "success" : ""}">
          <span>${t.text}</span>
          <button
            type="button"
            class="btn btn-sm btn-ghost"
            style="color: inherit; padding: 0 4px;"
            @click=${() => {
              state.toasts = state.toasts.filter((x) => x.id !== t.id);
              renderApp();
            }}
          >
            ✕
          </button>
        </div>
      `
    )}
  `;
}

function renderApp() {
  render(renderTopbar(), topbarEl);
  render(renderWorkspace(), workspaceEl);
  render(renderModal(), modalRootEl);
  render(renderToasts(), toastRootEl);
}

async function init() {
  renderApp();

  connectEventStream(
    (batch) => {
      void handleSseBatch(batch);
    },
    (status) => {
      state.sseStatus = status;
      renderApp();
    }
  );

  window.addEventListener("popstate", () => {
    const route = parseBrowserRoute(window.location.pathname);
    if (route) {
      void selectDatabase(route.projectId, route.databaseId, route.segments, true);
    } else {
      state.segments = [];
      renderApp();
    }
  });

  try {
    await refreshDatabaseList();
    const initialRoute = parseBrowserRoute(window.location.pathname);
    if (initialRoute) {
      await selectDatabase(
        initialRoute.projectId,
        initialRoute.databaseId,
        initialRoute.segments,
        true
      );
    } else if (state.databases.length > 0) {
      const first = state.databases[0];
      await selectDatabase(first.projectId, first.databaseId, [], true);
    } else {
      renderApp();
    }
  } catch (err) {
    pushToast(
      `Failed to initialize UI: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  }
}

void init();
