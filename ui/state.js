// @ts-check

import { createQueryRunnerState } from "./components/query.js";

/** @typedef {import('./api.js').DatabaseMetadata} DatabaseMetadata */
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
 *   view: 'explorer' | 'query',
 *   rootCollections: string[],
 *   rootLoading: boolean,
 *   segments: string[],
 *   collectionColumns: Map<string, CollectionColumnState>,
 *   documentLevels: Map<string, DocumentLevelState>,
 *   queryState: QueryRunnerState,
 *   sseStatus: 'live' | 'error',
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

  sseStatus: "live",
  ssePaused: false,

  modal: null,

  toasts: [],
};

let nextToastId = 1;
let dbEpoch = 0;
let navEpoch = 0;
let dbListSeq = 0;
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
 * Removes cached document level state and its request sequence for `docPath`.
 * @param {string} docPath
 */
export function deleteDocumentLevelCache(docPath) {
  state.documentLevels.delete(docPath);
  docRequestSeq.delete(docPath);
}

/**
 * Clears cached collection/document levels and query runner state for the active database.
 */
export function resetDatabaseCaches() {
  state.activeDbMeta = null;
  state.rootCollections = [];
  state.collectionColumns.clear();
  state.documentLevels.clear();
  colRequestSeq.clear();
  docRequestSeq.clear();
  state.queryState = createQueryRunnerState();
}

/**
 * Clears the active database selection, segments, and all associated caches.
 */
export function clearActiveDatabaseState() {
  state.activeProjectId = null;
  state.activeDatabaseId = null;
  state.segments = [];
  resetDatabaseCaches();
}

/**
 * Removes cached collection/document state and request sequences under `prefix`.
 * @param {string} prefix
 */
export function pruneCachesWithPrefix(prefix) {
  const slashPrefix = `${prefix}/`;
  for (const key of state.collectionColumns.keys()) {
    if (key === prefix || key.startsWith(slashPrefix)) {
      state.collectionColumns.delete(key);
      colRequestSeq.delete(key);
    }
  }
  for (const key of state.documentLevels.keys()) {
    if (key === prefix || key.startsWith(slashPrefix)) {
      state.documentLevels.delete(key);
      docRequestSeq.delete(key);
    }
  }
}

/**
 * Removes a toast notification by id.
 * @param {number} id
 */
export function dismissToast(id) {
  state.toasts = state.toasts.filter((t) => t.id !== id);
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
    dismissToast(id);
    onChange?.();
  }, 4200);
}
