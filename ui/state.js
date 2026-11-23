// @ts-check

import { createContentionState } from "./components/contention.js";
import { createQueryRunnerState } from "./components/query.js";

/** @typedef {import('./api.js').DatabaseMetadata} DatabaseMetadata */
/** @typedef {import('./components/contention.js').ContentionState} ContentionState */
/** @typedef {import('./components/explorer.js').CollectionColumnState} CollectionColumnState */
/** @typedef {import('./components/explorer.js').DocumentLevelState} DocumentLevelState */
/** @typedef {import('./components/editor.js').EditorSession} EditorSession */
/** @typedef {import('./components/query.js').QueryRunnerState} QueryRunnerState */

/**
 * @typedef {
 *   | { kind: 'open_db', projectId: string, databaseId: string, edition: 'STANDARD' | 'ENTERPRISE', busy: boolean, error: string | null }
 *   | { kind: 'create_doc', parentPath: string, collectionId: string, lockCollection: boolean, docId: string, lockDocId: boolean, session: EditorSession, busy: boolean, error: string | null }
 *   | { kind: 'delete_doc', docPath: string, recursive: boolean, hasSubcollections: boolean, busy: boolean, error: string | null }
 *   | { kind: 'delete_col', colPath: string, colId: string, busy: boolean, error: string | null }
 *   | { kind: 'clear_db', projectId: string, databaseId: string, busy: boolean, error: string | null }
 * } ActiveModal
 */

/**
 * @typedef {{
 *   databases: Array<{ projectId: string, databaseId: string, rootPath: string }>,
 *   activeProjectId: string | null,
 *   activeDatabaseId: string | null,
 *   activeDbMeta: DatabaseMetadata | null,
 *   view: 'explorer' | 'query' | 'contention',
 *   rootCollections: string[],
 *   rootLoading: boolean,
 *   segments: string[],
 *   collectionColumns: Map<string, CollectionColumnState>,
 *   documentLevels: Map<string, DocumentLevelState>,
 *   queryState: QueryRunnerState,
 *   contention: ContentionState,
 *   sseStatus: 'live' | 'error',
 *   contentionSseStatus: 'idle' | 'connecting' | 'live' | 'error',
 *   ssePaused: boolean,
 *   modal: ActiveModal | null,
 *   toasts: Array<{ id: number, text: string, tone: 'info' | 'success' | 'error' }>,
 * }} AppState
 */

/** @type {AppState} */
export const state = {
  databases: [],
  activeProjectId: null,
  activeDatabaseId: null,
  activeDbMeta: null,

  view: "explorer",

  rootCollections: [],
  rootLoading: false,
  segments: [],

  collectionColumns: new Map(),
  documentLevels: new Map(),

  queryState: createQueryRunnerState(),
  contention: createContentionState(),

  sseStatus: "live",
  contentionSseStatus: "idle",
  ssePaused: false,

  modal: null,

  toasts: [],
};

let nextToastId = 1;
let dbEpoch = 0;
let navEpoch = 0;
let dbListSeq = 0;
let contentionSeq = 0;
/** @type {Map<string, number>} */
const colRequestSeq = new Map();
/** @type {Map<string, number>} */
const docRequestSeq = new Map();

/**
 * Returns the current database epoch counter.
 * @returns {number}
 */
export function getDbEpoch() {
  return dbEpoch;
}

/**
 * Increments and returns the database epoch counter.
 * @returns {number}
 */
export function bumpDbEpoch() {
  return ++dbEpoch;
}

/**
 * Returns the current navigation epoch counter.
 * @returns {number}
 */
export function getNavEpoch() {
  return navEpoch;
}

/**
 * Increments and returns the navigation epoch counter.
 * @returns {number}
 */
export function bumpNavEpoch() {
  return ++navEpoch;
}

/**
 * Increments and returns the database list request sequence number.
 * @returns {number}
 */
export function nextDbListSeq() {
  return ++dbListSeq;
}

/**
 * Checks whether a database list sequence number is still the latest.
 * @param {number} seq
 * @returns {boolean}
 */
export function isCurrentDbListSeq(seq) {
  return seq === dbListSeq;
}

/**
 * Increments and records the request sequence number for a collection column.
 * @param {string} colPath
 * @returns {number}
 */
export function nextColSeq(colPath) {
  const seq = (colRequestSeq.get(colPath) ?? 0) + 1;
  colRequestSeq.set(colPath, seq);
  return seq;
}

/**
 * Checks whether a collection request sequence number is still the latest for `colPath`.
 * @param {string} colPath
 * @param {number} seq
 * @returns {boolean}
 */
export function isCurrentColSeq(colPath, seq) {
  return colRequestSeq.get(colPath) === seq;
}

/**
 * Increments and records the request sequence number for a document level.
 * @param {string} docPath
 * @returns {number}
 */
export function nextDocSeq(docPath) {
  const seq = (docRequestSeq.get(docPath) ?? 0) + 1;
  docRequestSeq.set(docPath, seq);
  return seq;
}

/**
 * Checks whether a document request sequence number is still the latest for `docPath`.
 * @param {string} docPath
 * @param {number} seq
 * @returns {boolean}
 */
export function isCurrentDocSeq(docPath, seq) {
  return docRequestSeq.get(docPath) === seq;
}

/**
 * Increments and returns the contention snapshot request sequence number.
 * @returns {number}
 */
export function nextContentionSeq() {
  return ++contentionSeq;
}

/**
 * Checks whether a contention snapshot request sequence number is still the latest.
 * @param {number} seq
 * @returns {boolean}
 */
export function isCurrentContentionSeq(seq) {
  return seq === contentionSeq;
}

/**
 * Removes cached document level state (and its request sequence when targeting the global state) for `docPath`.
 * @param {string} docPath
 * @param {AppState} [targetState]
 */
export function deleteDocumentLevelCache(docPath, targetState = state) {
  targetState.documentLevels.delete(docPath);
  if (targetState === state) {
    docRequestSeq.delete(docPath);
  }
}

/**
 * Clears cached collection/document levels, query runner state, and contention snapshot data
 * for the active database (preserving the selected contention scope).
 * @param {AppState} [targetState]
 */
export function resetDatabaseCaches(targetState = state) {
  targetState.activeDbMeta = null;
  targetState.rootCollections = [];
  targetState.collectionColumns.clear();
  targetState.documentLevels.clear();
  if (targetState === state) {
    colRequestSeq.clear();
    docRequestSeq.clear();
    nextContentionSeq();
  }
  targetState.queryState = createQueryRunnerState();
  targetState.contention.snapshot = null;
  targetState.contention.loading = false;
  targetState.contention.clearing = false;
  targetState.contention.error = null;
  targetState.contention.lastFetchedAtMs = null;
  targetState.contention.lastFetchedIso = null;
}

/**
 * Clears the active database selection, segments, and all associated caches.
 * @param {AppState} [targetState]
 */
export function clearActiveDatabaseState(targetState = state) {
  targetState.activeProjectId = null;
  targetState.activeDatabaseId = null;
  targetState.segments = [];
  resetDatabaseCaches(targetState);
}

/**
 * Removes cached collection/document state (and request sequences when targeting the global state) under `prefix`.
 * @param {string} prefix
 * @param {AppState} [targetState]
 */
export function pruneCachesWithPrefix(prefix, targetState = state) {
  const slashPrefix = `${prefix}/`;
  for (const key of targetState.collectionColumns.keys()) {
    if (key === prefix || key.startsWith(slashPrefix)) {
      targetState.collectionColumns.delete(key);
      if (targetState === state) {
        colRequestSeq.delete(key);
      }
    }
  }
  for (const key of targetState.documentLevels.keys()) {
    if (key === prefix || key.startsWith(slashPrefix)) {
      targetState.documentLevels.delete(key);
      if (targetState === state) {
        docRequestSeq.delete(key);
      }
    }
  }
}

/**
 * Removes a toast notification by id and returns whether a toast was removed.
 * @param {number} id
 * @returns {boolean}
 */
export function dismissToast(id) {
  const prevLen = state.toasts.length;
  state.toasts = state.toasts.filter((t) => t.id !== id);
  return state.toasts.length !== prevLen;
}

/**
 * Appends a transient toast notification and schedules automatic dismissal.
 * @param {string} text
 * @param {'info' | 'success' | 'error'} [tone]
 * @param {() => void} [onChange]
 */
export function pushToast(text, tone = "info", onChange) {
  const id = nextToastId++;
  state.toasts.push({ id, text, tone });
  onChange?.();
  setTimeout(() => {
    if (dismissToast(id)) onChange?.();
  }, 4200);
}
