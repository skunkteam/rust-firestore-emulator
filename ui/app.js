// @ts-check

import { html, nothing, render } from "./lit-html.js";
import {
  ApiError,
  clearContention,
  connectContentionEvents,
  connectEventStream,
  getContention,
  getDatabase,
  getDocument,
  listCollectionIds,
  listDatabases,
  listDocuments,
  parseRootRef,
  patchDatabase,
  runAggregationQuery,
  runQuery,
  updateDocument,
} from "./api.js";
import {
  generateAutoId,
  stringifyProtoJson,
} from "./values.js";
import {
  getDisplayedContentionDatabases,
  hasContentionWaiters,
  renderContention,
  updateWaiterElapsedDom,
} from "./components/contention.js";
import {
  computeChangedFieldMask,
  createEditorSession,
  extractSessionFields,
  syncSessionWithDocument,
} from "./components/editor.js";
import { renderExplorer } from "./components/explorer.js";
import { reconcileComboboxUi } from "./components/combobox.js";
import {
  getModalFocusableElements,
  handleModalKeydown,
  renderModal as renderModalView,
} from "./components/modals.js";
import {
  buildAggregationQueryFromVisual,
  buildStructuredQueryFromVisual,
  createQueryRunnerState,
  extractAggProvenance,
  parseRawQueryPayload,
} from "./components/query-model.js";
import { renderQueryRunner } from "./components/query.js";
import {
  markSuggestionsStale,
  resetQuerySuggestionStore,
} from "./components/query-suggestions.js";
import {
  getActiveRootPath,
  isValidPathSegment,
  parseBrowserRoute,
  syncBrowserUrl,
} from "./router.js";
import {
  abortActiveQueryRun,
  beginQueryRun,
  bumpDbEpoch,
  bumpNavEpoch,
  clearActiveDatabaseState,
  deleteCollectionColumnCache,
  deleteDocumentLevelCache,
  dismissToast,
  finishQueryRun,
  getDbEpoch,
  getDbSignal,
  getNavEpoch,
  isCurrentColSeq,
  isCurrentContentionSeq,
  isCurrentDbListSeq,
  isCurrentDocSeq,
  isCurrentQueryRunSeq,
  nextColSeq,
  nextContentionSeq,
  nextDbListSeq,
  nextDocSeq,
  pushToast,
  resetDatabaseCaches,
  state,
} from "./state.js";

/** @typedef {import('./api.js').ContentionSseEvent} ContentionSseEvent */
/** @typedef {import('./api.js').EmulatorSseEvent} EmulatorSseEvent */
/** @typedef {import('./components/explorer.js').DocumentLevelState} DocumentLevelState */
/** @typedef {import('./values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('./lit-html.js').TemplateResult} TemplateResult */

export { state, parseBrowserRoute };

/** @type {HTMLElement | null} */
const topbarEl = document.getElementById("topbar");
/** @type {HTMLElement | null} */
const workspaceEl = document.getElementById("workspace");
/** @type {HTMLElement | null} */
const modalRootEl = document.getElementById("modal-root");
/** @type {HTMLElement | null} */
const toastRootEl = document.getElementById("toast-root");

const emulatorVersion = String(
  /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (window))
    .EMULATOR_VERSION ?? "dev"
);

/** @type {Promise<void>} */
let sseQueue = Promise.resolve();
/** @type {{ close: () => void } | null} */
let activeEventStream = null;
/** @type {{ close: () => void } | null} */
let activeContentionEventStream = null;
/** @type {object | null} */
let activeContentionStreamToken = null;
/** @type {ReturnType<typeof setInterval> | null} */
let contentionTickerId = null;
let connectingContention = false;

const MAX_COLLECTION_COLUMN_PAGES = 1000;
const MAX_TRACKED_IMPORTS = 64;

/** @type {Map<number, 'local' | Set<string>>} */
const appliedImports = new Map();

/**
 * Records a locally applied importId or SSE-applied `(importId, databaseName)` entry
 * in FIFO insertion order capped at `MAX_TRACKED_IMPORTS` imports.
 * @param {number} importId
 * @param {string | null} databaseName Null marks the entire importId as locally applied.
 */
function recordTrackedImport(importId, databaseName) {
  const existing = appliedImports.get(importId);
  if (databaseName === null) {
    if (
      existing === undefined &&
      appliedImports.size >= MAX_TRACKED_IMPORTS
    ) {
      const oldest = appliedImports.keys().next();
      if (!oldest.done) {
        appliedImports.delete(oldest.value);
      }
    }
    appliedImports.set(importId, "local");
    return;
  }
  if (existing === "local") {
    return;
  }
  if (existing instanceof Set) {
    existing.add(databaseName);
    return;
  }
  if (appliedImports.size >= MAX_TRACKED_IMPORTS) {
    const oldest = appliedImports.keys().next();
    if (!oldest.done) {
      appliedImports.delete(oldest.value);
    }
  }
  appliedImports.set(importId, new Set([databaseName]));
}

/**
 * Formats an error for UI banners, preserving `[code status] message` for `ApiError`.
 * @param {unknown} err
 * @returns {string}
 */
function formatApiError(err) {
  return err instanceof ApiError
    ? `[${err.code} ${err.status}] ${err.message}`
    : err instanceof Error
      ? err.message
      : String(err);
}

/**
 * Returns true unless the document is currently hidden.
 * @returns {boolean}
 */
function isDocumentVisible() {
  return (
    typeof document === "undefined" ||
    !document ||
    document.visibilityState !== "hidden"
  );
}

/**
 * Returns true only when `/emulator/v1/contention/events` SSE should be open.
 * @returns {boolean}
 */
function shouldConnectContentionSse() {
  return Boolean(
    state.view === "contention" &&
      !state.ssePaused &&
      isDocumentVisible() &&
      state.activeProjectId &&
      state.activeDatabaseId
  );
}

/**
 * Returns true only when the 1000ms DOM-only waiter elapsed ticker should run.
 * @returns {boolean}
 */
function shouldRunContentionTicker() {
  if (
    state.view !== "contention" ||
    !isDocumentVisible() ||
    !state.activeProjectId ||
    !state.activeDatabaseId ||
    state.contention.lastFetchedAtMs === null
  ) {
    return false;
  }
  const activeRoot = getActiveRootPath(
    state.activeProjectId,
    state.activeDatabaseId
  );
  const displayed = getDisplayedContentionDatabases(
    state.contention,
    activeRoot
  );
  return hasContentionWaiters(displayed);
}

function stopContentionTicker() {
  if (contentionTickerId !== null) {
    clearInterval(contentionTickerId);
    contentionTickerId = null;
  }
}

function syncContentionTicker() {
  if (!shouldRunContentionTicker()) {
    stopContentionTicker();
    return;
  }
  if (contentionTickerId === null) {
    contentionTickerId = setInterval(() => {
      if (!shouldRunContentionTicker()) {
        stopContentionTicker();
        return;
      }
      const tickerRoot =
        workspaceEl ??
        (typeof document !== "undefined" &&
        typeof document?.getElementById === "function"
          ? document.getElementById("workspace")
          : null) ??
        (typeof document !== "undefined" ? document : null);
      updateWaiterElapsedDom(tickerRoot, state.contention, Date.now());
    }, 1000);
  }
}

function closeContentionEventStream() {
  activeContentionStreamToken = null;
  if (activeContentionEventStream) {
    activeContentionEventStream.close();
    activeContentionEventStream = null;
  }
  state.contentionSseStatus = "idle";
}

function syncContentionConnection() {
  if (!shouldConnectContentionSse()) {
    closeContentionEventStream();
    return;
  }
  if (
    activeContentionEventStream !== null ||
    connectingContention ||
    state.contentionSseStatus === "error"
  ) {
    return;
  }
  connectingContention = true;
  const token = {};
  activeContentionStreamToken = token;
  state.contentionSseStatus = "connecting";
  try {
    const stream = connectContentionEvents({
      onInvalidate(batch) {
        if (
          activeContentionStreamToken !== token ||
          !shouldConnectContentionSse()
        ) {
          return;
        }
        const activeRoot = getActiveRootPath(
          state.activeProjectId,
          state.activeDatabaseId
        );
        const relevant =
          state.contention.scope === "all" ||
          batch.some(
            (ev) =>
              ev &&
              typeof ev === "object" &&
              (ev.type === "resync" ||
                !ev.database ||
                ev.database === activeRoot)
          );
        if (relevant) {
          void refreshContention();
        }
      },
      onStatusChange(status) {
        if (activeContentionStreamToken !== token) return;
        const prevStatus = state.contentionSseStatus;
        state.contentionSseStatus = status;
        renderApp();
        if (
          prevStatus === "error" &&
          status === "live" &&
          activeContentionStreamToken === token &&
          shouldConnectContentionSse()
        ) {
          void refreshContention();
        }
      },
    });
    if (activeContentionStreamToken === token) {
      activeContentionEventStream = stream;
    } else {
      stream.close();
    }
  } catch {
    if (activeContentionStreamToken === token) {
      activeContentionStreamToken = null;
      activeContentionEventStream = null;
      state.contentionSseStatus = "error";
    }
  } finally {
    connectingContention = false;
  }
}

function syncContentionLifecycle() {
  syncContentionConnection();
  syncContentionTicker();
}

/**
 * Enqueues an SSE or local snapshot-refresh batch onto the serialized `sseQueue`
 * after running synchronous preemption (`applySyncSseBatch`).
 * @param {EmulatorSseEvent[]} batch
 * @param {{ ignorePaused?: boolean }} [options]
 * @returns {Promise<void>}
 */
function enqueueSseBatch(batch, options) {
  const plan = applySyncSseBatch(batch, options);
  if (!plan) return sseQueue;
  sseQueue = sseQueue
    .then(() => applyAsyncSseRefresh(plan))
    .catch(() => {});
  return sseQueue;
}

/**
 * Returns the tail promise of the serialized SSE processing queue.
 * @returns {Promise<void>}
 */
export function flushSseQueue() {
  return sseQueue;
}

/**
 * Fetches the current contention snapshot according to `state.contention.scope`,
 * discarding stale responses across rapid refreshes, view leaves, or DB changes.
 * @returns {Promise<void>}
 */
export async function refreshContention() {
  if (
    state.view !== "contention" ||
    !state.activeProjectId ||
    !state.activeDatabaseId
  ) {
    return;
  }
  const myDbEpoch = getDbEpoch();
  const mySeq = nextContentionSeq();
  const scope = state.contention.scope;
  const activeRoot = getActiveRootPath(
    state.activeProjectId,
    state.activeDatabaseId
  );
  const queryDb = scope === "active" && activeRoot ? activeRoot : undefined;

  state.contention.loading = true;
  state.contention.error = null;
  renderApp();

  try {
    const snapshot = await getContention(queryDb);
    if (
      myDbEpoch !== getDbEpoch() ||
      !isCurrentContentionSeq(mySeq) ||
      state.view !== "contention" ||
      state.contention.scope !== scope
    ) {
      return;
    }
    const nowMs = Date.now();
    state.contention.snapshot = snapshot;
    state.contention.lastFetchedAtMs = nowMs;
    state.contention.lastFetchedIso = new Date(nowMs).toISOString();
    state.contention.error = null;
  } catch (err) {
    if (
      myDbEpoch !== getDbEpoch() ||
      !isCurrentContentionSeq(mySeq) ||
      state.view !== "contention" ||
      state.contention.scope !== scope
    ) {
      return;
    }
    state.contention.error = formatApiError(err);
  } finally {
    if (myDbEpoch === getDbEpoch() && isCurrentContentionSeq(mySeq)) {
      state.contention.loading = false;
      syncContentionLifecycle();
      renderApp();
    }
  }
}

/**
 * Clears contention summary counters and recent history for the selected scope
 * without disturbing active locks/waiters, then refreshes the snapshot.
 * @returns {Promise<void>}
 */
export async function clearContentionHistory() {
  if (
    state.contention.clearing ||
    state.view !== "contention" ||
    !state.activeProjectId ||
    !state.activeDatabaseId
  ) {
    return;
  }
  const myDbEpoch = getDbEpoch();
  const scope = state.contention.scope;
  const targetProject = state.activeProjectId;
  const targetDb = state.activeDatabaseId;
  const activeRoot = getActiveRootPath(targetProject, targetDb);
  if (scope === "active" && !activeRoot) {
    return;
  }
  const queryDb = scope === "active" ? /** @type {string} */ (activeRoot) : undefined;

  const mySeq = nextContentionSeq();
  state.contention.loading = false;
  state.contention.clearing = true;
  state.contention.error = null;
  renderApp();

  const isCurrentClearContext = () =>
    myDbEpoch === getDbEpoch() &&
    isCurrentContentionSeq(mySeq) &&
    state.view === "contention" &&
    state.contention.scope === scope;

  try {
    await clearContention(queryDb);
    if (myDbEpoch !== getDbEpoch()) return;
    notifyToast(
      queryDb
        ? `Cleared contention history for ${targetProject}/${targetDb}`
        : "Cleared contention history for all databases",
      "success"
    );
    if (isCurrentClearContext()) {
      await refreshContention();
    }
  } catch (err) {
    if (myDbEpoch !== getDbEpoch()) return;
    const message = formatApiError(err);
    if (isCurrentClearContext()) {
      state.contention.error = message;
    }
    notifyToast(`Failed to clear contention history: ${message}`, "error");
  } finally {
    if (myDbEpoch === getDbEpoch() && state.contention.clearing) {
      state.contention.clearing = false;
      renderApp();
    }
  }
}

/**
 * Switches the active workspace view and synchronizes URL history and contention lifecycle.
 * @param {'explorer' | 'query' | 'contention'} nextView
 * @returns {Promise<void>}
 */
export async function switchView(nextView) {
  if (state.view === nextView) {
    if (
      nextView === "contention" &&
      state.activeProjectId &&
      state.activeDatabaseId
    ) {
      syncContentionLifecycle();
      if (!state.contention.snapshot && !state.contention.loading) {
        await refreshContention();
      }
    }
    return;
  }
  state.view = nextView;
  if (nextView !== "contention") {
    nextContentionSeq();
    state.contention.loading = false;
    syncContentionLifecycle();
    syncUrl();
    renderApp();
    return;
  }
  ensureVisibilityListener();
  syncUrl();
  syncContentionLifecycle();
  renderApp();
  if (state.activeProjectId && state.activeDatabaseId) {
    await refreshContention();
  }
}

/**
 * Switches to the Explorer view and navigates to/hydrates the given document resource path.
 * @param {string} docName
 * @returns {Promise<void>}
 */
export async function openDocumentFromName(docName) {
  const m = /^projects\/([^/]+)\/databases\/([^/]+)\/documents\/(.+)$/.exec(
    docName
  );
  if (!m) return;
  const [, targetProject, targetDatabase, relPath] = m;
  if (
    !isValidPathSegment(targetProject) ||
    !isValidPathSegment(targetDatabase)
  ) {
    return;
  }
  const nextSegments = relPath.split("/");
  if (
    nextSegments.length === 0 ||
    nextSegments.length % 2 !== 0 ||
    !nextSegments.every(isValidPathSegment)
  ) {
    return;
  }

  nextContentionSeq();
  state.contention.loading = false;
  state.view = "explorer";
  syncContentionLifecycle();

  if (
    targetProject === state.activeProjectId &&
    targetDatabase === state.activeDatabaseId
  ) {
    const myDbEpoch = getDbEpoch();
    const signal = getDbSignal();
    state.segments = nextSegments;
    const myNavEpoch = bumpNavEpoch();
    syncUrl();
    renderApp();
    const activeRoot = getActiveRootPath(targetProject, targetDatabase);
    if (activeRoot && !state.rootCollections.includes(nextSegments[0])) {
      try {
        const cols = await listCollectionIds(activeRoot, {
          signal,
          shouldAbort: () => myDbEpoch !== getDbEpoch() || signal.aborted,
        });
        if (
          myDbEpoch === getDbEpoch() &&
          !signal.aborted &&
          state.activeProjectId === targetProject &&
          state.activeDatabaseId === targetDatabase
        ) {
          state.rootCollections = cols;
          renderApp();
        }
      } catch {
        // Proceed with path hydration even if root collection refresh fails
      }
    }
    if (
      myDbEpoch === getDbEpoch() &&
      !signal.aborted &&
      myNavEpoch === getNavEpoch()
    ) {
      await hydrateVisiblePath(myDbEpoch, myNavEpoch, nextSegments);
    }
    return;
  }

  await selectDatabase(targetProject, targetDatabase, nextSegments);
}

/**
 * Handles browser tab `visibilitychange` transitions for contention SSE and elapsed wait ticks.
 * @returns {Promise<void>}
 */
export async function handleVisibilityChange() {
  if (!isDocumentVisible()) {
    syncContentionLifecycle();
    if (state.view === "contention") {
      renderApp();
    }
    return;
  }
  syncContentionLifecycle();
  if (
    !state.ssePaused &&
    state.view === "contention" &&
    state.activeProjectId &&
    state.activeDatabaseId
  ) {
    await refreshContention();
  }
}

/**
 * Toggles live SSE update pausing, triggering active-DB/contention resyncs when unpaused.
 * @returns {Promise<void>}
 */
export async function toggleSsePaused() {
  const wasPaused = state.ssePaused;
  state.ssePaused = !wasPaused;
  syncContentionLifecycle();
  renderApp();
  if (wasPaused && !state.ssePaused) {
    const epochBefore = getDbEpoch();
    await enqueueSseBatch([{ type: "resync" }]);
    if (
      !state.ssePaused &&
      state.view === "contention" &&
      state.activeProjectId &&
      state.activeDatabaseId
    ) {
      syncContentionLifecycle();
      if (epochBefore === getDbEpoch()) {
        await refreshContention();
      }
    }
  }
}

/**
 * @param {string} text
 * @param {'info' | 'success' | 'error'} [tone]
 */
function notifyToast(text, tone = "info") {
  pushToast(text, tone, renderApp);
}

/**
 * @param {boolean} [replace]
 */
function syncUrl(replace = false) {
  syncBrowserUrl(
    state.activeProjectId,
    state.activeDatabaseId,
    state.segments,
    replace,
    state.view
  );
}

/**
 * Refreshes the list of active databases from `GET /emulator/v1` without creating any database.
 * @returns {Promise<boolean>} True if this request applied its result (not superseded by a newer call).
 */
async function refreshDatabaseList() {
  const seq = nextDbListSeq();
  /** @type {string[]} */
  let rawList;
  try {
    rawList = await listDatabases();
  } catch (err) {
    if (!isCurrentDbListSeq(seq)) return false;
    throw err;
  }
  if (!isCurrentDbListSeq(seq)) return false;
  const parsed = rawList
    .map(parseRootRef)
    .filter((item) => item !== null)
    .sort((a, b) => a.rootPath.localeCompare(b.rootPath));
  state.databases = parsed;
  return true;
}

/**
 * Switches to (or initializes) a target database and loads its metadata & root collections.
 * @param {string} projectId
 * @param {string} databaseId
 * @param {string[]} [initialSegments]
 * @param {boolean} [replaceHistory]
 */
export async function selectDatabase(
  projectId,
  databaseId,
  initialSegments = [],
  replaceHistory = false
) {
  const dbChanged =
    state.activeProjectId !== projectId || state.activeDatabaseId !== databaseId;
  const myDbEpoch = dbChanged ? bumpDbEpoch() : getDbEpoch();
  const myNavEpoch = bumpNavEpoch();
  state.activeProjectId = projectId;
  state.activeDatabaseId = databaseId;
  state.segments = [...initialSegments];

  if (dbChanged) {
    resetDatabaseCaches();
    if (state.view === "contention") {
      state.contention.loading = true;
    }
  }
  const signal = getDbSignal();

  syncUrl(replaceHistory);
  syncContentionLifecycle();
  renderApp();

  try {
    state.rootLoading = true;
    renderApp();
    const [meta, cols] = await Promise.all([
      getDatabase(projectId, databaseId, { signal }),
      listCollectionIds(
        `projects/${projectId}/databases/${databaseId}/documents`,
        {
          signal,
          shouldAbort: () => myDbEpoch !== getDbEpoch() || signal.aborted,
        }
      ),
    ]);
    if (myDbEpoch !== getDbEpoch() || signal.aborted) return;
    state.activeDbMeta = meta;
    state.rootCollections = cols;
    await refreshDatabaseList();
    if (myDbEpoch !== getDbEpoch() || signal.aborted) return;
  } catch (err) {
    if (myDbEpoch !== getDbEpoch() || signal.aborted || isAbortError(err)) {
      return;
    }
    notifyToast(
      `Failed to load database: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    if (myDbEpoch === getDbEpoch() && !signal.aborted) {
      state.rootLoading = false;
      renderApp();
    }
  }

  if (
    myDbEpoch === getDbEpoch() &&
    !signal.aborted &&
    myNavEpoch === getNavEpoch() &&
    initialSegments.length > 0 &&
    state.view !== "contention"
  ) {
    await hydrateVisiblePath(myDbEpoch, myNavEpoch, initialSegments);
  }

  if (
    myDbEpoch === getDbEpoch() &&
    !signal.aborted &&
    state.view === "contention"
  ) {
    syncContentionLifecycle();
    await refreshContention();
  }
}

/**
 * Loads collection columns and document levels along `segmentsSnapshot`, optionally filtered
 * by `shouldLoadCol`/`shouldLoadDoc` predicates, returning whether hydration completed or
 * stopped due to a database or navigation epoch change.
 * @param {number} [expectedDbEpoch]
 * @param {number} [expectedNavEpoch]
 * @param {string[]} [segmentsSnapshot]
 * @param {{
 *   expectedActiveRoot?: string | null,
 *   signal?: AbortSignal,
 *   isPausedBlocked?: () => boolean,
 *   shouldLoadCol?: (colPath: string) => boolean,
 *   shouldLoadDoc?: (docPath: string) => boolean,
 * }} [options]
 * @returns {Promise<'ok' | 'stale_db' | 'stale_nav'>}
 */
export async function hydrateVisiblePath(
  expectedDbEpoch = getDbEpoch(),
  expectedNavEpoch = getNavEpoch(),
  segmentsSnapshot = [...state.segments],
  options = {}
) {
  const {
    expectedActiveRoot = getActiveRootPath(
      state.activeProjectId,
      state.activeDatabaseId
    ),
    signal,
    isPausedBlocked,
    shouldLoadCol,
    shouldLoadDoc,
  } = options;
  const isStaleDb = () =>
    Boolean(isPausedBlocked?.()) ||
    Boolean(signal?.aborted) ||
    expectedDbEpoch !== getDbEpoch() ||
    !state.activeProjectId ||
    !state.activeDatabaseId ||
    getActiveRootPath(state.activeProjectId, state.activeDatabaseId) !==
      expectedActiveRoot;
  const isStaleNav = () => expectedNavEpoch !== getNavEpoch();

  if (isStaleDb() || !expectedActiveRoot) return "stale_db";
  if (isStaleNav()) return "stale_nav";

  for (let i = 0; i < segmentsSnapshot.length; i += 2) {
    if (isStaleDb()) return "stale_db";
    if (isStaleNav()) return "stale_nav";

    const colId = segmentsSnapshot[i];
    const parentSegments = segmentsSnapshot.slice(0, i);
    const colSegments = segmentsSnapshot.slice(0, i + 1);
    const parentPath =
      parentSegments.length === 0
        ? expectedActiveRoot
        : `${expectedActiveRoot}/${parentSegments.join("/")}`;
    const colPath = `${expectedActiveRoot}/${colSegments.join("/")}`;

    if (!shouldLoadCol || shouldLoadCol(colPath)) {
      await loadCollectionColumn(
        colPath,
        parentPath,
        colId,
        false,
        expectedDbEpoch
      );
      if (isStaleDb()) return "stale_db";
      if (isStaleNav()) return "stale_nav";
    }

    if (segmentsSnapshot.length > i + 1) {
      const docId = segmentsSnapshot[i + 1];
      const docPath = `${colPath}/${docId}`;
      if (!shouldLoadDoc || shouldLoadDoc(docPath)) {
        await loadDocumentLevel(docPath, docId, true, expectedDbEpoch);
        if (isStaleDb()) return "stale_db";
        if (isStaleNav()) return "stale_nav";
      }
    }
  }
  return "ok";
}

/**
 * @param {FirestoreDocument[]} docs
 * @returns {Array<{ id: string, fullName: string, phantom: boolean }>}
 */
function mapColumnDocuments(docs) {
  return docs.map((doc) => {
    const fullName = doc.name ?? "";
    const id = fullName.slice(fullName.lastIndexOf("/") + 1);
    const phantom = !doc.createTime && !doc.updateTime;
    return { id, fullName, phantom };
  });
}

/**
 * @param {string} colPath
 * @param {string} parentPath
 * @param {string} colId
 * @param {boolean} appendPage
 * @param {number} [expectedDbEpoch]
 */
export async function loadCollectionColumn(
  colPath,
  parentPath,
  colId,
  appendPage,
  expectedDbEpoch = getDbEpoch()
) {
  const signal = getDbSignal();
  if (expectedDbEpoch !== getDbEpoch() || signal.aborted) return;
  const seq = nextColSeq(colPath);

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
    if (appendPage) {
      const res = await listDocuments(parentPath, colId, {
        showMissing: true,
        maskFieldPaths: ["__name__"],
        pageSize: 200,
        pageToken: colState.nextPageToken,
        signal,
      });
      if (
        expectedDbEpoch !== getDbEpoch() ||
        signal.aborted ||
        !isCurrentColSeq(colPath, seq)
      ) {
        return;
      }
      const mapped = mapColumnDocuments(res.documents);
      colState.documents = [...colState.documents, ...mapped];
      colState.nextPageToken = res.nextPageToken;
    } else {
      const priorCount = colState.documents.length;
      /** @type {Set<string>} */
      const seenTokens = new Set();
      /** @type {Array<{ id: string, fullName: string, phantom: boolean }>} */
      const collected = [];
      /** @type {string | undefined} */
      let pageToken = undefined;
      let nextToken = "";

      for (let page = 0; page < MAX_COLLECTION_COLUMN_PAGES; page++) {
        const pageSize =
          page === 0
            ? Math.max(200, priorCount)
            : Math.max(1, priorCount - collected.length);
        const res = await listDocuments(parentPath, colId, {
          showMissing: true,
          maskFieldPaths: ["__name__"],
          pageSize,
          pageToken,
          signal,
        });
        if (
          expectedDbEpoch !== getDbEpoch() ||
          signal.aborted ||
          !isCurrentColSeq(colPath, seq)
        ) {
          return;
        }
        const mapped = mapColumnDocuments(res.documents);
        for (const doc of mapped) {
          collected.push(doc);
        }
        nextToken = res.nextPageToken;
        if (!nextToken || seenTokens.has(nextToken) || mapped.length === 0) {
          nextToken = "";
          break;
        }
        if (collected.length >= priorCount) {
          break;
        }
        seenTokens.add(nextToken);
        pageToken = nextToken;
      }

      colState.documents = collected;
      colState.nextPageToken = nextToken;
    }
  } catch (err) {
    if (
      expectedDbEpoch !== getDbEpoch() ||
      signal.aborted ||
      !isCurrentColSeq(colPath, seq)
    ) {
      return;
    }
    notifyToast(
      `Failed to list ${colId}: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    if (
      expectedDbEpoch === getDbEpoch() &&
      !signal.aborted &&
      isCurrentColSeq(colPath, seq)
    ) {
      colState.loading = false;
      renderApp();
    }
  }
}

/**
 * Loads a document and its subcollections, respecting the dirty-state guard if `preserveDirty` is true.
 * @param {string} docPath
 * @param {string} docId
 * @param {boolean} preserveDirty
 * @param {number} [expectedDbEpoch]
 */
export async function loadDocumentLevel(
  docPath,
  docId,
  preserveDirty,
  expectedDbEpoch = getDbEpoch()
) {
  const signal = getDbSignal();
  if (expectedDbEpoch !== getDbEpoch() || signal.aborted) return;
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const projectId = state.activeProjectId;
  const databaseId = state.activeDatabaseId;
  const seq = nextDocSeq(docPath);
  const myNavEpoch = getNavEpoch();

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
      getDocument(docPath, { signal }).then(
        (doc) => ({ exists: true, doc }),
        (err) => {
          if (err instanceof ApiError && err.code === 404) {
            return { exists: false, doc: null };
          }
          throw err;
        }
      ),
      listCollectionIds(docPath, {
        signal,
        shouldAbort: () =>
          expectedDbEpoch !== getDbEpoch() ||
          signal.aborted ||
          !isCurrentDocSeq(docPath, seq) ||
          myNavEpoch !== getNavEpoch(),
      }),
    ]);
    if (
      expectedDbEpoch !== getDbEpoch() ||
      signal.aborted ||
      !isCurrentDocSeq(docPath, seq) ||
      myNavEpoch !== getNavEpoch()
    ) {
      return;
    }

    level.subcollections = subcols;
    level.exists = docOutcome.exists;

    if (docOutcome.exists && docOutcome.doc) {
      if (
        preserveDirty &&
        level.editorSession &&
        level.editorSession.isDirty
      ) {
        level.editorSession.deletedExternally = false;
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
      if (preserveDirty && level.editorSession?.isDirty) {
        level.editorSession.deletedExternally = true;
        level.editorSession.externalDoc = null;
      } else {
        level.editorSession = null;
      }
    }
  } catch (err) {
    if (
      expectedDbEpoch !== getDbEpoch() ||
      signal.aborted ||
      !isCurrentDocSeq(docPath, seq) ||
      myNavEpoch !== getNavEpoch()
    ) {
      return;
    }
    notifyToast(
      `Failed to load document ${docId}: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
  } finally {
    if (
      expectedDbEpoch === getDbEpoch() &&
      !signal.aborted &&
      isCurrentDocSeq(docPath, seq)
    ) {
      level.loading = false;
      renderApp();
    }
  }
}

/**
 * @typedef {{
 *   dbEpoch: number,
 *   signal: AbortSignal,
 *   activeRoot: string | null,
 *   shouldRefreshDbList: boolean,
 *   shouldRefreshMeta: boolean,
 *   fullResyncActiveDb: boolean,
 *   clearedActiveDb: boolean,
 *   replacedActiveDb: boolean,
 *   anyDatabaseReplaced: boolean,
 *   ignorePaused: boolean,
 *   changedPaths: Set<string>,
 * }} SseRefreshPlan
 */

/**
 * Returns true if any open document along `segments` under `activeRoot` has unsaved edits.
 * @param {string} activeRoot
 * @param {string[]} segments
 * @returns {boolean}
 */
function hasDirtySegmentAlongPath(activeRoot, segments) {
  for (let i = 1; i < segments.length; i += 2) {
    const docPath = `${activeRoot}/${segments.slice(0, i + 1).join("/")}`;
    if (state.documentLevels.get(docPath)?.editorSession?.isDirty) {
      return true;
    }
  }
  return false;
}

/**
 * Truncates clean stale explorer segments to the deepest surviving prefix after replacement.
 * Leaves `state.segments` untouched if any open document along the path has unsaved edits.
 * @param {string} activeRoot
 */
function truncateStaleSegmentsAfterReplace(activeRoot) {
  if (
    state.segments.length === 0 ||
    hasDirtySegmentAlongPath(activeRoot, state.segments)
  ) {
    return;
  }

  let survivingLen = 0;
  if (state.rootCollections.includes(state.segments[0])) {
    survivingLen = 1;
    for (let i = 1; i < state.segments.length; i += 2) {
      const docPath = `${activeRoot}/${state.segments.slice(0, i + 1).join("/")}`;
      const level = state.documentLevels.get(docPath);
      if (!level || (!level.exists && level.subcollections.length === 0)) {
        break;
      }
      survivingLen = i + 1;
      if (i + 1 < state.segments.length) {
        const subcolId = state.segments[i + 1];
        if (!level.subcollections.includes(subcolId)) {
          break;
        }
        survivingLen = i + 2;
      }
    }
  }

  if (survivingLen < state.segments.length) {
    state.segments = state.segments.slice(0, survivingLen);
    bumpNavEpoch();
    syncUrl(true);
  }
}

/**
 * Prunes non-visible collection columns and non-dirty non-visible document levels after replacement.
 * @param {string} activeRoot
 */
function pruneInvisibleCachesAfterReplace(activeRoot) {
  /** @type {Set<string>} */
  const visibleColPaths = new Set();
  /** @type {Set<string>} */
  const visibleDocPaths = new Set();
  for (let i = 0; i < state.segments.length; i += 2) {
    visibleColPaths.add(
      `${activeRoot}/${state.segments.slice(0, i + 1).join("/")}`
    );
    if (i + 1 < state.segments.length) {
      visibleDocPaths.add(
        `${activeRoot}/${state.segments.slice(0, i + 2).join("/")}`
      );
    }
  }

  for (const key of Array.from(state.collectionColumns.keys())) {
    if (!visibleColPaths.has(key)) {
      deleteCollectionColumnCache(key);
    }
  }
  for (const [key, level] of Array.from(state.documentLevels.entries())) {
    if (!visibleDocPaths.has(key) && !level.editorSession?.isDirty) {
      deleteDocumentLevelCache(key);
    }
  }
}

/**
 * Clears active database caches and URL without issuing metadata reads that would recreate a cleared DB.
 * @param {string} clearedRoot
 * @returns {number} New database epoch
 */
function clearActiveDatabaseFromSse(clearedRoot) {
  const nextEpoch = bumpDbEpoch();
  bumpNavEpoch();
  clearActiveDatabaseState(state);
  syncUrl(true);
  notifyToast(`Database ${clearedRoot} was cleared`, "info");
  return nextEpoch;
}

/**
 * Synchronously processes an SSE batch: preempts cleared or replaced databases
 * (rotating cancellation signals, clearing active caches/query state and suggestions),
 * updates URL/toasts, and marks matching suggestion caches stale without firing HTTP requests.
 * @param {EmulatorSseEvent[]} events
 * @param {{ ignorePaused?: boolean }} [options]
 * @returns {SseRefreshPlan | null}
 */
export function applySyncSseBatch(events, options = {}) {
  const ignorePaused = Boolean(options?.ignorePaused);
  if (state.ssePaused && !ignorePaused) return null;

  let shouldRefreshDbList = false;
  let shouldRefreshMeta = false;
  let fullResyncActiveDb = false;
  let clearedActiveDb = false;
  let replacedActiveDb = false;
  let anyDatabaseReplaced = false;
  /** @type {Set<string>} */
  const changedPaths = new Set();

  const activeRoot = getActiveRootPath(
    state.activeProjectId,
    state.activeDatabaseId
  );

  const markActiveDatabaseCleared = (/** @type {string} */ clearedRoot) => {
    clearActiveDatabaseFromSse(clearedRoot);
    clearedActiveDb = true;
    replacedActiveDb = false;
    fullResyncActiveDb = false;
    shouldRefreshMeta = false;
    changedPaths.clear();
  };

  for (const ev of events) {
    if (
      (ev.type === "database_replaced" || ev.type === "database_created") &&
      typeof ev.importId === "number" &&
      Number.isFinite(ev.importId)
    ) {
      if (appliedImports.get(ev.importId) === "local") {
        continue;
      }
      recordTrackedImport(ev.importId, ev.database ?? "");
    }
    if (ev.type === "resync") {
      shouldRefreshDbList = true;
      if (!clearedActiveDb) {
        fullResyncActiveDb = true;
      }
      continue;
    }
    if (ev.type === "database_created") {
      shouldRefreshDbList = true;
      continue;
    }
    if (ev.type === "database_cleared") {
      shouldRefreshDbList = true;
      // Invalidate any in-flight pre-clear GET /emulator/v1 so it cannot repopulate
      // state.databases or auto-select (lazily resurrect) the cleared database.
      nextDbListSeq();
      if (ev.database) {
        state.databases = state.databases.filter(
          (db) => db.rootPath !== ev.database
        );
      }
      if (ev.database && activeRoot !== null && ev.database === activeRoot) {
        markActiveDatabaseCleared(ev.database);
      }
      continue;
    }
    if (ev.type === "database_replaced") {
      shouldRefreshDbList = true;
      anyDatabaseReplaced = true;
      if (!clearedActiveDb && activeRoot !== null && ev.database === activeRoot) {
        fullResyncActiveDb = true;
        replacedActiveDb = true;
      }
      continue;
    }
    if (ev.type === "database_config_changed") {
      if (!clearedActiveDb && activeRoot !== null && ev.database === activeRoot) {
        shouldRefreshMeta = true;
      }
      continue;
    }
    if (ev.type === "documents_changed") {
      if (!clearedActiveDb && activeRoot !== null && ev.database === activeRoot) {
        for (const p of ev.paths ?? []) {
          changedPaths.add(p);
        }
      }
    }
  }

  if (replacedActiveDb && !clearedActiveDb) {
    abortActiveQueryRun();
    state.queryState = createQueryRunnerState();
    resetQuerySuggestionStore(state.querySuggestions);
  } else if (!clearedActiveDb) {
    if (fullResyncActiveDb) {
      markSuggestionsStale(state.querySuggestions, "all");
    } else if (changedPaths.size > 0) {
      markSuggestionsStale(state.querySuggestions, changedPaths);
    }
  }

  if (
    !shouldRefreshDbList &&
    !shouldRefreshMeta &&
    !fullResyncActiveDb &&
    !clearedActiveDb &&
    !replacedActiveDb &&
    !anyDatabaseReplaced &&
    changedPaths.size === 0
  ) {
    return null;
  }

  return {
    dbEpoch: getDbEpoch(),
    signal: getDbSignal(),
    activeRoot,
    shouldRefreshDbList,
    shouldRefreshMeta,
    fullResyncActiveDb,
    clearedActiveDb,
    replacedActiveDb,
    anyDatabaseReplaced,
    ignorePaused,
    changedPaths,
  };
}

/**
 * Executes asynchronous Explorer refreshes for a planned SSE batch, guarded by
 * `dbEpoch` and `AbortSignal` after every `await`.
 * @param {SseRefreshPlan | null} plan
 * @returns {Promise<void>}
 */
export async function applyAsyncSseRefresh(plan) {
  if (!plan) return;
  const isPausedBlocked = () => state.ssePaused && !plan.ignorePaused;
  if (isPausedBlocked()) return;
  const myDbEpoch = plan.dbEpoch ?? getDbEpoch();
  const signal = plan.signal ?? getDbSignal();
  const isStale = () => myDbEpoch !== getDbEpoch() || signal.aborted;

  let {
    shouldRefreshDbList,
    fullResyncActiveDb,
    clearedActiveDb,
    replacedActiveDb,
    shouldRefreshMeta,
  } = plan;
  const { anyDatabaseReplaced, changedPaths, activeRoot } = plan;

  if (shouldRefreshDbList) {
    let applied = false;
    try {
      applied = await refreshDatabaseList();
    } catch (err) {
      if (isPausedBlocked() || isStale() || isAbortError(err)) return;
      fullResyncActiveDb = false;
      notifyToast(
        `Failed to refresh database list: ${err instanceof Error ? err.message : String(err)}`,
        "error"
      );
    }
    if (isPausedBlocked()) return;
    if (
      applied &&
      !isStale() &&
      fullResyncActiveDb &&
      activeRoot &&
      !clearedActiveDb &&
      !state.rootLoading &&
      state.activeDbMeta !== null &&
      !state.databases.some((db) => db.rootPath === activeRoot)
    ) {
      // Clear cached state WITHOUT calling getDatabase(), which would lazily recreate the cleared DB!
      clearActiveDatabaseFromSse(activeRoot);
      clearedActiveDb = true;
      replacedActiveDb = false;
      fullResyncActiveDb = false;
      shouldRefreshMeta = false;
      changedPaths.clear();
    }
    // If no active DB is selected and a DB now exists, auto-select the first one cleanly.
    if (
      applied &&
      !state.activeProjectId &&
      state.databases.length > 0 &&
      !isPausedBlocked()
    ) {
      const first = state.databases[0];
      await selectDatabase(first.projectId, first.databaseId, [], true);
      return;
    }
    renderApp();
  }

  const isStaleDb = () =>
    isPausedBlocked() ||
    isStale() ||
    getActiveRootPath(state.activeProjectId, state.activeDatabaseId) !==
      activeRoot;

  if (
    clearedActiveDb ||
    state.rootLoading ||
    !state.activeProjectId ||
    !state.activeDatabaseId ||
    !activeRoot ||
    isStaleDb()
  ) {
    return;
  }

  let didUpdateState = replacedActiveDb;

  if (shouldRefreshMeta || fullResyncActiveDb) {
    try {
      const meta = await getDatabase(
        state.activeProjectId,
        state.activeDatabaseId,
        { signal }
      );
      if (isStaleDb()) {
        return;
      }
      state.activeDbMeta = meta;
      didUpdateState = true;
    } catch (err) {
      if (isStaleDb() || isAbortError(err)) return;
      // Ignore transient error
    }
  }

  if (isStaleDb()) return;

  if (anyDatabaseReplaced) {
    const affectsContention =
      replacedActiveDb || state.contention.scope === "all";
    if (affectsContention) {
      const needsFallbackRefresh =
        state.view === "contention" &&
        (!shouldConnectContentionSse() || state.contentionSseStatus !== "live");
      if (state.view !== "contention" || needsFallbackRefresh) {
        state.contention.snapshot = null;
        state.contention.lastFetchedAtMs = null;
        state.contention.lastFetchedIso = null;
        state.contention.error = null;
        didUpdateState = true;
      }
      if (needsFallbackRefresh) {
        await refreshContention();
        if (isStaleDb()) {
          return;
        }
        didUpdateState = false;
      }
    }
  }

  if (fullResyncActiveDb || changedPaths.size > 0) {
    const myNavEpoch = getNavEpoch();
    const segmentsSnapshot = [...state.segments];
    let rootColsLoaded = false;
    try {
      const cols = await listCollectionIds(activeRoot, {
        signal,
        shouldAbort: isStaleDb,
      });
      if (isStaleDb()) {
        return;
      }
      state.rootCollections = cols;
      rootColsLoaded = true;
    } catch (err) {
      if (isStaleDb() || isAbortError(err)) return;
      // Ignore transient error
    }
    if (isStaleDb()) {
      return;
    }

    const changedPathList = [...changedPaths];
    const skipVanishedReplacedPath =
      replacedActiveDb &&
      rootColsLoaded &&
      segmentsSnapshot.length > 0 &&
      !state.rootCollections.includes(segmentsSnapshot[0]) &&
      !hasDirtySegmentAlongPath(activeRoot, segmentsSnapshot);

    /** @type {'ok' | 'stale_db' | 'stale_nav'} */
    let hydrateOutcome = "ok";
    if (myNavEpoch !== getNavEpoch()) {
      hydrateOutcome = "stale_nav";
    } else if (!skipVanishedReplacedPath) {
      hydrateOutcome = await hydrateVisiblePath(
        myDbEpoch,
        myNavEpoch,
        segmentsSnapshot,
        {
          expectedActiveRoot: activeRoot,
          signal,
          isPausedBlocked,
          shouldLoadCol: (colPath) =>
            fullResyncActiveDb ||
            changedPathList.some((p) => p.startsWith(`${colPath}/`)),
          shouldLoadDoc: (docPath) =>
            fullResyncActiveDb ||
            changedPaths.has(docPath) ||
            changedPathList.some((p) => p.startsWith(`${docPath}/`)),
        }
      );
    }

    if (hydrateOutcome === "stale_db" || isStaleDb()) {
      return;
    }

    if (replacedActiveDb) {
      if (hydrateOutcome === "ok" && rootColsLoaded) {
        truncateStaleSegmentsAfterReplace(activeRoot);
      }
      pruneInvisibleCachesAfterReplace(activeRoot);
    }

    didUpdateState = true;
  }

  if (didUpdateState) {
    renderApp();
  }
}

/**
 * Handles debounced SSE invalidation batches without clobbering dirty edits or resurrecting cleared databases.
 * @param {EmulatorSseEvent[]} events
 * @param {{ ignorePaused?: boolean }} [options]
 */
export async function handleSseBatch(events, options = {}) {
  const plan = applySyncSseBatch(events, options);
  if (plan) {
    await applyAsyncSseRefresh(plan);
  }
}

/**
 * Saves an edited document from its EditorSession.
 * @param {DocumentLevelState} docLevel
 */
export async function saveDocumentFromInspector(docLevel) {
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

  const myDbEpoch = getDbEpoch();

  try {
    const cleanFields = session.cleanDoc?.fields ?? {};
    const updateMask =
      session.mode === "structured" && session.cleanDoc !== null
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
    if (myDbEpoch !== getDbEpoch()) return;
    nextDocSeq(docLevel.docPath);
    docLevel.loading = false;
    docLevel.exists = true;
    syncSessionWithDocument(session, updated);
    notifyToast(`Saved ${docLevel.docId}`, "success");
  } catch (err) {
    if (myDbEpoch !== getDbEpoch()) return;
    session.saving = false;
    session.saveError = err instanceof Error ? err.message : String(err);
  }
  renderApp();
}

/**
 * @param {unknown} err
 * @returns {boolean}
 */
function isAbortError(err) {
  return Boolean(
    err && typeof err === "object" && "name" in err && err.name === "AbortError"
  );
}

/**
 * Executes the current query in QueryRunnerState.
 */
export async function executeCurrentQuery() {
  if (!state.activeProjectId || !state.activeDatabaseId) return;
  const qState = state.queryState;
  const queryKind = qState.queryKind;
  qState.error = null;

  const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
  const cleanParentSub = qState.parentSubpath.trim().replace(/^\/+|\/+$/g, "");
  const parentPath = cleanParentSub ? `${rootPath}/${cleanParentSub}` : rootPath;

  /** @type {Record<string, unknown>} */
  let payload;
  if (qState.mode === "visual") {
    const enterprise = state.activeDbMeta?.databaseEdition === "ENTERPRISE";
    const built =
      queryKind === "aggregations"
        ? buildAggregationQueryFromVisual(
            qState,
            state.activeProjectId,
            state.activeDatabaseId,
            { enterprise }
          )
        : buildStructuredQueryFromVisual(
            qState,
            state.activeProjectId,
            state.activeDatabaseId
          );
    if (!built.valid || !built.query) {
      qState.error = built.error ?? "Invalid query";
      renderApp();
      return;
    }
    payload = built.query;
    if (queryKind === "aggregations") {
      qState.rawAggregationText = stringifyProtoJson(payload, 2);
    } else {
      qState.rawQueryText = stringifyProtoJson(payload, 2);
    }
  } else {
    const rawText =
      queryKind === "aggregations"
        ? qState.rawAggregationText
        : qState.rawQueryText;
    const rawRes = parseRawQueryPayload(rawText, queryKind, parentPath);
    if (!rawRes.valid || !rawRes.payload) {
      qState.error = rawRes.error ?? "Invalid Raw JSON";
      renderApp();
      return;
    }
    payload = rawRes.payload;
  }

  const myDbEpoch = getDbEpoch();
  const { seq: mySeq, controller } = beginQueryRun();
  const { signal } = controller;
  const isCurrentRun = () =>
    myDbEpoch === getDbEpoch() &&
    isCurrentQueryRunSeq(mySeq) &&
    !signal.aborted &&
    qState.queryKind === queryKind;

  qState.running = true;
  renderApp();
  const t0 = performance.now();

  try {
    if (queryKind === "aggregations") {
      const provenance = extractAggProvenance(payload);
      const rows = await runAggregationQuery(parentPath, payload, { signal });
      if (!isCurrentRun()) return;
      qState.aggElapsedMs = performance.now() - t0;
      qState.aggReadTime = rows[0]?.readTime ?? null;
      qState.aggResults = rows[0]?.result?.aggregateFields ?? {};
      qState.aggProvenance = provenance;
    } else {
      const rows = await runQuery(parentPath, payload, { signal });
      if (!isCurrentRun()) return;
      qState.elapsedMs = performance.now() - t0;
      qState.readTime = rows[0]?.readTime ?? null;
      qState.results = rows
        .map((r) => r.document)
        .filter((d) => d !== undefined);
    }
  } catch (err) {
    if (!isCurrentRun() || isAbortError(err)) {
      return;
    }
    qState.error = formatApiError(err);
  } finally {
    finishQueryRun(controller);
    if (myDbEpoch === getDbEpoch() && isCurrentQueryRunSeq(mySeq)) {
      qState.running = false;
      renderApp();
    }
  }
}

/**
 * Toggles database edition or concurrency mode via PATCH /v1/projects/{p}/databases/{d}.
 * @param {'edition' | 'concurrency'} which
 */
export async function toggleDatabaseSetting(which) {
  if (!state.activeProjectId || !state.activeDatabaseId || !state.activeDbMeta) {
    return;
  }
  const targetProject = state.activeProjectId;
  const targetDb = state.activeDatabaseId;
  const myDbEpoch = getDbEpoch();
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
    const updatedMeta = await patchDatabase(targetProject, targetDb, patch);
    if (
      myDbEpoch !== getDbEpoch() ||
      state.activeProjectId !== targetProject ||
      state.activeDatabaseId !== targetDb
    ) {
      return;
    }
    state.activeDbMeta = updatedMeta;
    renderApp();
  } catch (err) {
    if (myDbEpoch !== getDbEpoch()) return;
    notifyToast(
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

function openExportModal() {
  state.modal = {
    kind: "export_snapshot",
    busy: false,
    busyScope: null,
    error: null,
  };
  renderApp();
}

function openImportModal() {
  state.modal = {
    kind: "import_snapshot",
    file: null,
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
export function openCreateDocumentModal(
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

/**
 * Renders the top navigation bar.
 * @returns {TemplateResult}
 */
export function renderTopbar() {
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
              nextContentionSeq();
              state.contention.loading = false;
              state.view = "explorer";
              state.segments = [];
              bumpNavEpoch();
              syncContentionLifecycle();
              syncUrl();
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
                aria-label="Select database"
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
              <div class="segmented" role="group" aria-label="Workspace view">
                <button
                  type="button"
                  class=${state.view === "explorer" ? "active" : ""}
                  aria-pressed=${String(state.view === "explorer")}
                  @click=${() => switchView("explorer")}
                >
                  Explorer
                </button>
                <button
                  type="button"
                  class=${state.view === "query" ? "active" : ""}
                  aria-pressed=${String(state.view === "query")}
                  @click=${() => switchView("query")}
                >
                  Query Runner
                </button>
                <button
                  type="button"
                  class=${state.view === "contention" ? "active" : ""}
                  aria-pressed=${String(state.view === "contention")}
                  @click=${() => switchView("contention")}
                >
                  Contention
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
          class="btn btn-sm"
          title="Import a .fsemu snapshot"
          @click=${openImportModal}
        >
          Import
        </button>

        ${state.databases.length > 0
          ? html`
              <button
                type="button"
                class="btn btn-sm"
                title="Export databases to a .fsemu snapshot"
                @click=${openExportModal}
              >
                Export
              </button>
            `
          : nothing}

        <button
          type="button"
          class="sse-badge"
          title="Click to pause or resume live SSE updates"
          @click=${toggleSsePaused}
        >
          <span class="sse-dot ${dotClass}"></span>
          <span>${sseLabel}</span>
        </button>
      </div>
    </div>
  `;
}

/**
 * Renders the active workspace view (empty hero, Query Runner, Contention Visualizer, or Explorer).
 * @returns {TemplateResult}
 */
export function renderWorkspace() {
  if (!state.activeProjectId || !state.activeDatabaseId) {
    return html`
      <div class="empty-hero">
        <h2>No Active Firestore Database</h2>
        <p>
          Connect your test suite to automatically populate databases, create a database right now, or import a saved .fsemu snapshot.
        </p>
        <div class="empty-hero-actions">
          <button
            type="button"
            class="btn btn-primary"
            @click=${openDatabaseModal}
          >
            + Open / Create Database
          </button>
          <button type="button" class="btn" @click=${openImportModal}>
            Import Snapshot
          </button>
        </div>
      </div>
    `;
  }

  if (state.view === "contention") {
    return renderContention({
      projectId: state.activeProjectId,
      databaseId: state.activeDatabaseId,
      state: state.contention,
      sseStatus: state.contentionSseStatus,
      ssePaused: state.ssePaused,
      onChangeScope: (nextScope) => {
        if (state.contention.scope === nextScope) return Promise.resolve();
        state.contention.scope = nextScope;
        state.contention.snapshot = null;
        state.contention.lastFetchedAtMs = null;
        state.contention.lastFetchedIso = null;
        state.contention.error = null;
        syncContentionLifecycle();
        return refreshContention();
      },
      onRefresh: () => refreshContention(),
      onClearHistory: () => clearContentionHistory(),
      onOpenDocument: (docName) => openDocumentFromName(docName),
    });
  }

  if (state.view === "query") {
    return renderQueryRunner({
      projectId: state.activeProjectId,
      databaseId: state.activeDatabaseId,
      knownCollections: state.rootCollections,
      enterprise: state.activeDbMeta?.databaseEdition === "ENTERPRISE",
      suggestions: state.querySuggestions,
      state: state.queryState,
      onRerender: renderApp,
      onExecuteQuery: () => void executeCurrentQuery(),
      onAbortQueryRun: () => {
        abortActiveQueryRun();
        state.queryState.running = false;
      },
      onOpenResultDocument: (docName) => void openDocumentFromName(docName),
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
      bumpNavEpoch();
      syncUrl();
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
      abortActiveQueryRun();
      state.queryState = createQueryRunnerState(collectionId, parentSubpath);
      void switchView("query");
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

/**
 * Renders the currently active modal dialog, or `nothing` when no modal is open.
 * @returns {TemplateResult | symbol}
 */
export function renderModal() {
  return renderModalView(state, {
    onRerender: renderApp,
    onSelectDatabase: selectDatabase,
    onHydrateVisiblePath: hydrateVisiblePath,
    onRefreshDatabaseList: refreshDatabaseList,
    onToast: notifyToast,
    onImportSuccess: async (summary) => {
      const importId =
        typeof summary.importId === "number" &&
        Number.isFinite(summary.importId)
          ? summary.importId
          : null;
      const tracked =
        importId !== null ? appliedImports.get(importId) : undefined;
      const databasesToApply =
        tracked === "local"
          ? []
          : tracked instanceof Set
            ? summary.databases.filter((d) => !tracked.has(d.name))
            : summary.databases;
      if (importId !== null) {
        recordTrackedImport(importId, null);
      }
      if (databasesToApply.length === 0) {
        return;
      }
      const filteredEvents = databasesToApply.map((d) => ({
        type: /** @type {'database_replaced' | 'database_created'} */ (
          d.replaced ? "database_replaced" : "database_created"
        ),
        database: d.name,
      }));
      await enqueueSseBatch(filteredEvents, { ignorePaused: true });
    },
  });
}

/**
 * Renders active toast notifications.
 * @returns {TemplateResult}
 */
export function renderToasts() {
  return html`
    ${state.toasts.map(
      (t) => html`
        <div class="toast ${t.tone === "error" ? "error" : t.tone === "success" ? "success" : ""}">
          <span>${t.text}</span>
          <button
            type="button"
            class="btn btn-sm btn-ghost"
            style="color: inherit; padding: 0 4px;"
            aria-label="Dismiss notification"
            @click=${() => {
              dismissToast(t.id);
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

let wasModalOpen = false;
/** @type {Element | null} */
let previousModalFocus = null;
/** @type {((e: KeyboardEvent) => void) | null} */
let modalKeydownListener = null;

function closeActiveModal() {
  if (!state.modal || state.modal.busy) return;
  state.modal = null;
  renderApp();
}

/**
 * Manages modal Escape/Tab keyboard listener, initial focus, and focus restoration.
 * @param {boolean} wasOpenBefore
 * @param {Element | null} prevActiveBeforeRender
 */
function syncModalAccessibility(wasOpenBefore, prevActiveBeforeRender) {
  const isOpenNow = state.modal !== null;
  if (!wasOpenBefore && isOpenNow) {
    previousModalFocus = prevActiveBeforeRender;
    if (
      typeof document !== "undefined" &&
      typeof document?.addEventListener === "function" &&
      !modalKeydownListener
    ) {
      modalKeydownListener = (e) => {
        handleModalKeydown(e, state, modalRootEl, closeActiveModal);
      };
      document.addEventListener("keydown", modalKeydownListener);
    }
    const focusEntry = () => {
      if (!state.modal) return;
      const dialogEl =
        typeof modalRootEl?.querySelector === "function"
          ? modalRootEl.querySelector('[role="dialog"]')
          : null;
      if (!dialogEl) return;
      const query =
        typeof dialogEl.querySelector === "function"
          ? (/** @type {string} */ sel) => dialogEl.querySelector(sel)
          : null;
      const initialField = query
        ? (query("[data-autofocus]:not([disabled])") ??
          query(
            'input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled])'
          ))
        : null;
      const fallback = getModalFocusableElements(dialogEl)[0] ?? dialogEl;
      const target = /** @type {Partial<HTMLElement>} */ (
        initialField ?? fallback
      );
      if (typeof target?.focus === "function") {
        target.focus();
      }
    };
    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(focusEntry);
    } else {
      focusEntry();
    }
  } else if (wasOpenBefore && !isOpenNow) {
    if (
      typeof document !== "undefined" &&
      typeof document?.removeEventListener === "function" &&
      modalKeydownListener
    ) {
      document.removeEventListener("keydown", modalKeydownListener);
      modalKeydownListener = null;
    }
    const toRestore = /** @type {(Element & Partial<HTMLElement>) | null} */ (
      previousModalFocus
    );
    previousModalFocus = null;
    if (
      toRestore &&
      typeof toRestore.focus === "function" &&
      toRestore.isConnected !== false
    ) {
      toRestore.focus();
    }
  }
  wasModalOpen = isOpenNow;
}

function renderApp() {
  if (
    state.view !== "query" ||
    state.modal !== null ||
    state.queryState.mode !== "visual"
  ) {
    reconcileComboboxUi(state.querySuggestions.comboboxUi, new Set());
  }
  const prevActive =
    !wasModalOpen &&
    state.modal !== null &&
    typeof document !== "undefined" &&
    document
      ? (document.activeElement ?? null)
      : null;
  if (topbarEl) render(renderTopbar(), topbarEl);
  if (workspaceEl) {
    render(renderWorkspace(), workspaceEl);
    if (state.view === "contention") {
      updateWaiterElapsedDom(workspaceEl, state.contention, Date.now());
    }
  }
  if (modalRootEl) render(renderModal(), modalRootEl);
  if (toastRootEl) render(renderToasts(), toastRootEl);
  syncModalAccessibility(wasModalOpen, prevActive);
  syncContentionLifecycle();
}

let popstateBound = false;
let visibilityBound = false;

function ensureVisibilityListener() {
  if (
    !visibilityBound &&
    typeof document !== "undefined" &&
    typeof document?.addEventListener === "function"
  ) {
    visibilityBound = true;
    document.addEventListener("visibilitychange", () => {
      void handleVisibilityChange();
    });
  }
}

/**
 * Initializes event listeners, SSE streaming, and initial database/route hydration.
 */
export async function init() {
  appliedImports.clear();
  state.sseStatus = "live";
  nextContentionSeq();
  closeContentionEventStream();
  stopContentionTicker();
  const initialRoute = parseBrowserRoute(window.location.pathname);
  state.view = initialRoute?.view === "contention" ? "contention" : "explorer";
  renderApp();

  ensureVisibilityListener();

  activeEventStream?.close();
  activeEventStream = connectEventStream(
    (batch) => {
      void enqueueSseBatch(batch);
    },
    (status) => {
      const prevStatus = state.sseStatus;
      state.sseStatus = status;
      renderApp();
      if (prevStatus === "error" && status === "live") {
        void enqueueSseBatch([{ type: "resync" }]);
      }
    }
  );

  if (!popstateBound) {
    popstateBound = true;
    window.addEventListener("popstate", () => {
      const route = parseBrowserRoute(window.location.pathname);
      if (route) {
        const nextView = route.view === "contention" ? "contention" : "explorer";
        state.view = nextView;
        if (nextView !== "contention") {
          nextContentionSeq();
          state.contention.loading = false;
        }
        if (
          route.projectId === state.activeProjectId &&
          route.databaseId === state.activeDatabaseId
        ) {
          state.segments = [...route.segments];
          bumpNavEpoch();
          syncContentionLifecycle();
          renderApp();
          if (nextView === "contention") {
            void refreshContention();
          } else if (state.segments.length > 0) {
            void hydrateVisiblePath();
          }
        } else {
          void selectDatabase(
            route.projectId,
            route.databaseId,
            route.segments,
            true
          );
        }
      } else {
        if (state.view === "contention") {
          nextContentionSeq();
          state.contention.loading = false;
          state.view = "explorer";
        }
        state.segments = [];
        bumpNavEpoch();
        syncContentionLifecycle();
        renderApp();
      }
    });
  }

  try {
    await refreshDatabaseList();
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
    notifyToast(
      `Failed to initialize UI: ${err instanceof Error ? err.message : String(err)}`,
      "error"
    );
    renderApp();
  }
}

if (topbarEl && workspaceEl && modalRootEl && toastRootEl) {
  void init();
}
