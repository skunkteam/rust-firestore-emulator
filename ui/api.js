// @ts-check

import { stringifyProtoJson } from "./values.js";

/** @typedef {import('./values.js').FirestoreValue} FirestoreValue */
/** @typedef {import('./values.js').FirestoreDocument} FirestoreDocument */

/**
 * @typedef {{
 *   name: string,
 *   uid: string,
 *   type: string,
 *   databaseEdition: 'STANDARD' | 'ENTERPRISE',
 *   concurrencyMode: 'PESSIMISTIC' | 'OPTIMISTIC',
 * }} DatabaseMetadata
 */

/**
 * @typedef {{
 *   name: string,
 *   projectId: string,
 *   databaseId: string,
 *   databaseEdition: 'STANDARD' | 'ENTERPRISE',
 *   concurrencyMode: 'PESSIMISTIC' | 'OPTIMISTIC',
 *   documentCount: number,
 *   replaced: boolean,
 * }} ImportedDatabaseSummary
 */

/**
 * @typedef {{
 *   databases: ImportedDatabaseSummary[],
 *   totalDatabases: number,
 *   totalDocuments: number,
 * }} ImportSummary
 */

/**
 * @typedef {{
 *   type: 'database_created' | 'database_cleared' | 'database_replaced' | 'database_config_changed' | 'documents_changed' | 'resync',
 *   database?: string,
 *   paths?: string[],
 *   updateTime?: string,
 *   skipped?: number,
 * }} EmulatorSseEvent
 */

/** @typedef {'PESSIMISTIC_TXN' | 'OPTIMISTIC_TXN' | 'WRITE_BATCH' | 'SINGLE_WRITE' | 'POINT_READ'} SessionKind */
/** @typedef {'READ' | 'WRITE'} ContentionLockMode */
/** @typedef {'ACTIVE' | 'COMMITTING'} ContentionSessionState */
/** @typedef {'WOUNDED' | 'READ_TIMEOUT' | 'WRITE_TIMEOUT' | 'UPGRADE_ABORT' | 'WRITE_BATCH_RETRY' | 'TRANSACTION_EXPIRED'} ContentionEventKind */

/**
 * @typedef {{
 *   woundsTotal: number,
 *   timeoutsTotal: number,
 *   upgradeAbortsTotal: number,
 *   writeBatchRetriesTotal: number,
 * }} ContentionSummary
 */

/**
 * @typedef {{
 *   sessionId: number,
 *   txnId: string | null,
 *   kind: SessionKind,
 *   seq: number | null,
 *   state: ContentionSessionState,
 *   woundable: boolean,
 * }} WriterSnapshot
 */

/**
 * @typedef {{
 *   sessionId: number,
 *   txnId: string | null,
 *   kind: SessionKind,
 *   seq: number | null,
 *   state: ContentionSessionState,
 *   woundable: boolean,
 *   retained: boolean,
 *   activeLeases: number,
 * }} ReaderSnapshot
 */

/**
 * @typedef {{
 *   sessionId: number,
 *   txnId: string | null,
 *   kind: SessionKind,
 *   seq: number,
 *   mode: ContentionLockMode,
 *   waitedMs: number,
 * }} WaiterSnapshot
 */

/**
 * @typedef {{
 *   document: string,
 *   writer: WriterSnapshot | null,
 *   readers: ReaderSnapshot[],
 *   readerCount: number,
 *   waiters: WaiterSnapshot[],
 *   waiterCount: number,
 * }} ContendedDocumentSnapshot
 */

/**
 * @typedef {{
 *   id: number,
 *   kind: ContentionEventKind,
 *   timestamp: string,
 *   docName: string | null,
 *   sessionId: number,
 *   txnId: string | null,
 *   sessionKind: SessionKind,
 *   seq: number | null,
 *   wounderSessionId?: number,
 *   wounderTxnId?: string,
 *   wounderKind?: SessionKind,
 *   wounderSeq?: number,
 *   waitMs?: number,
 *   attempt?: number,
 * }} ContentionEvent
 */

/**
 * @typedef {{
 *   database: string,
 *   summary: ContentionSummary,
 *   contendedDocuments: ContendedDocumentSnapshot[],
 *   totalContendedDocuments: number,
 *   truncated: boolean,
 *   recentEvents: ContentionEvent[],
 * }} DatabaseContentionSnapshot
 */

/**
 * @typedef {{
 *   databases: DatabaseContentionSnapshot[],
 * }} ContentionSnapshot
 */

/**
 * @typedef {{
 *   type: 'contention_changed' | 'database_cleared' | 'resync',
 *   database?: string,
 *   skipped?: number,
 * }} ContentionSseEvent
 */

export class ApiError extends Error {
  /**
   * @param {number} code
   * @param {string} status
   * @param {string} message
   */
  constructor(code, status, message) {
    super(message);
    this.name = "ApiError";
    this.code = code;
    this.status = status;
  }
}

/**
 * Encodes each segment of a slash-delimited Firestore resource path.
 * @param {string} resourcePath
 * @returns {string}
 */
export function encodeResourcePath(resourcePath) {
  return resourcePath
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

/**
 * Parses a RootRef string (`projects/{p}/databases/{d}/documents` or `projects/{p}/databases/{d}`).
 * @param {string} rootStr
 * @returns {{ projectId: string, databaseId: string, rootPath: string } | null}
 */
export function parseRootRef(rootStr) {
  const m =
    /^projects\/([^/]+)\/databases\/([^/]+)(?:\/documents)?$/.exec(rootStr);
  if (!m) return null;
  return {
    projectId: m[1],
    databaseId: m[2],
    rootPath: `projects/${m[1]}/databases/${m[2]}/documents`,
  };
}

/**
 * Parses a failed HTTP response body into an ApiError.
 * @param {Response} res
 * @param {string} text
 * @returns {ApiError}
 */
function parseApiError(res, text) {
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && parsed.error) {
      return new ApiError(
        Number(parsed.error.code ?? res.status),
        String(parsed.error.status ?? "UNKNOWN"),
        String(parsed.error.message ?? res.statusText)
      );
    }
  } catch {
    // Fall back to raw text / statusText below
  }
  return new ApiError(res.status, "HTTP_ERROR", text || res.statusText);
}

/**
 * @template T
 * @param {string} url
 * @param {RequestInit} [init]
 * @returns {Promise<T>}
 */
async function requestJson(url, init = {}) {
  const res = await fetch(url, init);
  const text = await res.text();
  if (!res.ok) {
    throw parseApiError(res, text);
  }
  if (!text) {
    return /** @type {T} */ ({});
  }
  return /** @type {T} */ (JSON.parse(text));
}

/**
 * Lists active database RootRef strings (`["projects/p/databases/d/documents"]`).
 * Does NOT lazily instantiate any database.
 * @returns {Promise<string[]>}
 */
export async function listDatabases() {
  return requestJson("/emulator/v1");
}

/**
 * Fetches database edition & concurrency metadata (creates the database if absent).
 * @param {string} projectId
 * @param {string} databaseId
 * @param {{ signal?: AbortSignal }} [opts]
 * @returns {Promise<DatabaseMetadata>}
 */
export async function getDatabase(projectId, databaseId, opts = {}) {
  const p = encodeURIComponent(projectId);
  const d = encodeURIComponent(databaseId);
  return requestJson(
    `/v1/projects/${p}/databases/${d}`,
    opts.signal ? { signal: opts.signal } : undefined
  );
}

/**
 * Updates database edition and/or concurrency mode (creates the database if absent).
 * @param {string} projectId
 * @param {string} databaseId
 * @param {{ databaseEdition?: 'STANDARD' | 'ENTERPRISE', concurrencyMode?: 'PESSIMISTIC' | 'OPTIMISTIC' }} patch
 * @returns {Promise<DatabaseMetadata>}
 */
export async function patchDatabase(projectId, databaseId, patch) {
  const p = encodeURIComponent(projectId);
  const d = encodeURIComponent(databaseId);
  return requestJson(`/v1/projects/${p}/databases/${d}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
}

/**
 * Clears all documents and collections in the specified database.
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {Promise<void>}
 */
export async function clearDatabase(projectId, databaseId) {
  const rootPath = `projects/${projectId}/databases/${databaseId}/documents`;
  await requestJson(`/emulator/v1/${encodeResourcePath(rootPath)}`, {
    method: "DELETE",
  });
}

const MAX_LIST_COLLECTION_PAGES = 1000;

/**
 * Lists direct child collection IDs under a database root or document path.
 * @param {string} parentPath
 * @param {{ signal?: AbortSignal, shouldAbort?: () => boolean }} [opts]
 * @returns {Promise<string[]>}
 */
export async function listCollectionIds(parentPath, opts = {}) {
  const { signal, shouldAbort } = opts;
  const isAborted = () => Boolean(signal?.aborted || shouldAbort?.());
  const url = `/v1/${encodeResourcePath(parentPath)}:listCollectionIds`;
  /** @type {string[]} */
  const collectionIds = [];
  /** @type {Set<string>} */
  const seenTokens = new Set();
  let pageToken = "";

  for (let page = 0; page < MAX_LIST_COLLECTION_PAGES; page++) {
    if (isAborted()) {
      break;
    }
    /** @type {{ collectionIds?: string[], nextPageToken?: string }} */
    const res = await requestJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(pageToken ? { pageToken } : {}),
      ...(signal ? { signal } : {}),
    });
    if (isAborted()) {
      break;
    }
    if (Array.isArray(res.collectionIds)) {
      for (const id of res.collectionIds) {
        collectionIds.push(id);
      }
    }
    const nextToken = res.nextPageToken ?? "";
    if (!nextToken) {
      break;
    }
    if (seenTokens.has(nextToken)) {
      console.warn(
        `listCollectionIds(${parentPath}): duplicate nextPageToken "${nextToken}"; stopping pagination early.`
      );
      break;
    }
    seenTokens.add(nextToken);
    pageToken = nextToken;
    if (page === MAX_LIST_COLLECTION_PAGES - 1) {
      console.warn(
        `listCollectionIds(${parentPath}): reached max page cap (${MAX_LIST_COLLECTION_PAGES}) with remaining nextPageToken; stopping pagination early.`
      );
    }
  }

  return collectionIds;
}

/**
 * Lists documents in a collection.
 * @param {string} parentPath
 * @param {string} collectionId
 * @param {{ showMissing?: boolean, maskFieldPaths?: string[], pageSize?: number, pageToken?: string, signal?: AbortSignal }} [opts]
 * @returns {Promise<{ documents: FirestoreDocument[], nextPageToken: string }>}
 */
export async function listDocuments(parentPath, collectionId, opts = {}) {
  const params = new URLSearchParams();
  if (opts.showMissing !== undefined) {
    params.set("showMissing", String(opts.showMissing));
  }
  if (opts.pageSize !== undefined) {
    params.set("pageSize", String(opts.pageSize));
  }
  if (opts.pageToken) {
    params.set("pageToken", opts.pageToken);
  }
  for (const fp of opts.maskFieldPaths ?? []) {
    params.append("mask.fieldPaths", fp);
  }
  const qs = params.toString();
  const url = `/v1/${encodeResourcePath(parentPath)}/${encodeURIComponent(collectionId)}${qs ? `?${qs}` : ""}`;
  /** @type {{ documents?: FirestoreDocument[], nextPageToken?: string }} */
  const res = await requestJson(
    url,
    opts.signal ? { signal: opts.signal } : undefined
  );
  return {
    documents: res.documents ?? [],
    nextPageToken: res.nextPageToken ?? "",
  };
}

/**
 * Fetches a single document by full resource path.
 * @param {string} docPath
 * @param {{ signal?: AbortSignal }} [opts]
 * @returns {Promise<FirestoreDocument>}
 */
export async function getDocument(docPath, opts = {}) {
  return requestJson(
    `/v1/${encodeResourcePath(docPath)}`,
    opts.signal ? { signal: opts.signal } : undefined
  );
}

/**
 * Creates a new document with a client-supplied documentId.
 * @param {string} parentPath
 * @param {string} collectionId
 * @param {string} docId
 * @param {Record<string, FirestoreValue>} fields
 * @returns {Promise<FirestoreDocument>}
 */
export async function createDocument(parentPath, collectionId, docId, fields) {
  const qs = new URLSearchParams({ documentId: docId }).toString();
  const url = `/v1/${encodeResourcePath(parentPath)}/${encodeURIComponent(collectionId)}?${qs}`;
  return requestJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: stringifyProtoJson({ fields }),
  });
}

/**
 * Updates an existing document (or upserts) with optional field mask and updateTime precondition.
 * @param {string} docPath
 * @param {Record<string, FirestoreValue>} fields
 * @param {string[] | null} [updateMask]
 * @param {string | null} [updateTime]
 * @returns {Promise<FirestoreDocument>}
 */
export async function updateDocument(
  docPath,
  fields,
  updateMask = null,
  updateTime = null
) {
  const params = new URLSearchParams();
  if (updateMask !== null) {
    if (updateMask.length === 0) {
      params.append("updateMask.fieldPaths", "");
    } else {
      for (const fp of updateMask) {
        params.append("updateMask.fieldPaths", fp);
      }
    }
  }
  if (updateTime) {
    params.set("currentDocument.updateTime", updateTime);
  }
  const qs = params.toString();
  const url = `/v1/${encodeResourcePath(docPath)}${qs ? `?${qs}` : ""}`;
  return requestJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: stringifyProtoJson({ fields }),
  });
}

/**
 * Deletes a document (either just the document via v1 REST or recursively with subcollections).
 * @param {string} docPath
 * @param {boolean} recursive
 * @returns {Promise<void>}
 */
export async function deleteDocument(docPath, recursive) {
  const prefix = recursive ? "/emulator/v1" : "/v1";
  await requestJson(`${prefix}/${encodeResourcePath(docPath)}`, {
    method: "DELETE",
  });
}

/**
 * Recursively deletes all documents in a collection and its nested subcollections.
 * @param {string} colPath
 * @returns {Promise<void>}
 */
export async function deleteCollection(colPath) {
  await requestJson(`/emulator/v1/${encodeResourcePath(colPath)}`, {
    method: "DELETE",
  });
}

/**
 * Executes a StructuredQuery against parentPath.
 * @param {string} parentPath
 * @param {Record<string, unknown>} structuredQuery
 * @param {{ signal?: AbortSignal }} [opts]
 * @returns {Promise<Array<{ document?: FirestoreDocument, readTime?: string }>>}
 */
export async function runQuery(parentPath, structuredQuery, opts = {}) {
  return requestJson(`/v1/${encodeResourcePath(parentPath)}:runQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: stringifyProtoJson({ structuredQuery }),
    ...(opts.signal ? { signal: opts.signal } : {}),
  });
}

const DEFAULT_EXPORT_FILENAME = "firestore-emulator-snapshot.fsemu";

/**
 * Extracts a safe basename from a Content-Disposition header value.
 * @param {string | null | undefined} headerValue
 * @returns {string}
 */
function parseContentDispositionFilename(headerValue) {
  if (!headerValue) return DEFAULT_EXPORT_FILENAME;
  const match = /(?:^|;)\s*filename\s*=\s*(?:"([^"]*)"|([^;]+))/i.exec(
    headerValue
  );
  const raw = (match?.[1] ?? match?.[2] ?? "").trim();
  const base = raw.split(/[\\/]/).pop() ?? "";
  return base && base !== "." && base !== ".."
    ? base
    : DEFAULT_EXPORT_FILENAME;
}

/**
 * Exports selected or all emulator databases as a binary `.fsemu` snapshot blob.
 * @param {string[]} [databases]
 * @returns {Promise<{ blob: Blob, filename: string }>}
 */
export async function exportSnapshot(databases = []) {
  const params = new URLSearchParams();
  for (const db of databases) {
    params.append("database", db);
  }
  const qs = params.toString();
  const url = `/emulator/v1/export${qs ? `?${qs}` : ""}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw parseApiError(res, text);
  }
  const blob = await res.blob();
  const filename = parseContentDispositionFilename(
    res.headers?.get("content-disposition")
  );
  return { blob, filename };
}

/**
 * Imports a binary `.fsemu` snapshot file into the running emulator.
 * @param {File | Blob} file
 * @returns {Promise<ImportSummary>}
 */
export async function importSnapshot(file) {
  return requestJson("/emulator/v1/import", {
    method: "POST",
    headers: { "Content-Type": "application/octet-stream" },
    body: file,
  });
}

/**
 * Executes a StructuredAggregationQuery against parentPath.
 * @param {string} parentPath
 * @param {Record<string, unknown>} structuredAggregationQuery
 * @param {{ signal?: AbortSignal }} [opts]
 * @returns {Promise<Array<{ result?: { aggregateFields?: Record<string, FirestoreValue> }, readTime?: string }>>}
 */
export async function runAggregationQuery(
  parentPath,
  structuredAggregationQuery,
  opts = {}
) {
  return requestJson(
    `/v1/${encodeResourcePath(parentPath)}:runAggregationQuery`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: stringifyProtoJson({ structuredAggregationQuery }),
      ...(opts.signal ? { signal: opts.signal } : {}),
    }
  );
}

/**
 * Fetches contention snapshots and recent events, optionally scoped to a database RootRef.
 * @param {string} [database]
 * @returns {Promise<ContentionSnapshot>}
 */
export async function getContention(database) {
  const qs = database
    ? `?${new URLSearchParams({ database }).toString()}`
    : "";
  return requestJson(`/emulator/v1/contention${qs}`);
}

/**
 * Clears contention summary counters and recent event history without disturbing active locks.
 * @param {string} [database]
 * @returns {Promise<void>}
 */
export async function clearContention(database) {
  const qs = database
    ? `?${new URLSearchParams({ database }).toString()}`
    : "";
  await requestJson(`/emulator/v1/contention${qs}`, {
    method: "DELETE",
  });
}

/**
 * @template T
 * @param {{
 *   url: string,
 *   primaryEvent: string,
 *   debounceMs: number,
 *   maxWaitMs?: number,
 *   flushOnResync?: boolean,
 *   isUrgent?: (ev: T) => boolean,
 *   onInvalidate?: (events: T[]) => void,
 *   onStatusChange?: (status: 'live' | 'error') => void,
 *   makeResyncFallback: () => T,
 * }} opts
 * @returns {{ close: () => void }}
 */
function createDebouncedEventStream(opts) {
  const es = new EventSource(opts.url);
  /** @type {T[]} */
  let pending = [];
  /** @type {ReturnType<typeof setTimeout> | null} */
  let timer = null;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let maxTimer = null;
  let closed = false;

  const flush = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
    if (maxTimer !== null) {
      clearTimeout(maxTimer);
      maxTimer = null;
    }
    const batch = pending;
    pending = [];
    if (!closed && batch.length > 0) {
      opts.onInvalidate?.(batch);
    }
  };

  const enqueue = (/** @type {T} */ ev, urgent = false) => {
    if (closed) return;
    pending.push(ev);
    if (urgent || opts.isUrgent?.(ev)) {
      flush();
      return;
    }
    if (timer !== null) {
      clearTimeout(timer);
    }
    timer = setTimeout(flush, opts.debounceMs);
    if (opts.maxWaitMs !== undefined && maxTimer === null) {
      maxTimer = setTimeout(flush, opts.maxWaitMs);
    }
  };

  es.addEventListener("open", () => {
    if (!closed) {
      opts.onStatusChange?.("live");
    }
  });

  es.addEventListener("error", () => {
    if (!closed) {
      opts.onStatusChange?.("error");
    }
  });

  es.addEventListener(opts.primaryEvent, (msg) => {
    try {
      const ev = /** @type {unknown} */ (
        JSON.parse(/** @type {MessageEvent} */ (msg).data)
      );
      if (ev && typeof ev === "object") {
        enqueue(/** @type {T} */ (ev));
      }
    } catch {
      // Ignore malformed frame
    }
  });

  es.addEventListener("resync", (msg) => {
    const flushNow = Boolean(opts.flushOnResync);
    try {
      const ev = /** @type {unknown} */ (
        JSON.parse(/** @type {MessageEvent} */ (msg).data)
      );
      if (ev && typeof ev === "object") {
        enqueue(/** @type {T} */ (ev), flushNow);
      } else {
        enqueue(opts.makeResyncFallback(), flushNow);
      }
    } catch {
      enqueue(opts.makeResyncFallback(), flushNow);
    }
  });

  return {
    close() {
      closed = true;
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
      if (maxTimer !== null) {
        clearTimeout(maxTimer);
        maxTimer = null;
      }
      pending = [];
      es.close();
    },
  };
}

/**
 * Connects to `/emulator/v1/events` SSE stream and coalesces events with a 150ms trailing debounce,
 * a 1000ms maximum wait ceiling to prevent event starvation, and immediate FIFO flush on
 * `database_cleared` and `resync` events.
 * @param {(events: EmulatorSseEvent[]) => void} onInvalidate
 * @param {(status: 'live' | 'error') => void} onStatusChange
 * @returns {{ close: () => void }}
 */
export function connectEventStream(onInvalidate, onStatusChange) {
  const MAX_WAIT_MS = 1000;
  return createDebouncedEventStream({
    url: "/emulator/v1/events",
    primaryEvent: "change",
    debounceMs: 150,
    maxWaitMs: MAX_WAIT_MS,
    flushOnResync: true,
    isUrgent: (ev) => ev.type === "database_cleared" || ev.type === "resync",
    onInvalidate,
    onStatusChange,
    makeResyncFallback: () => /** @type {EmulatorSseEvent} */ ({ type: "resync" }),
  });
}

/**
 * Connects to `/emulator/v1/contention/events` SSE stream and coalesces
 * `contention` and `resync` events over a 200ms trailing debounce window
 * with a 1000ms maximum wait ceiling to prevent >5Hz starvation.
 * @param {{
 *   onInvalidate?: (events: ContentionSseEvent[]) => void,
 *   onStatusChange?: (status: 'live' | 'error') => void,
 * }} handlers
 * @returns {{ close: () => void }}
 */
export function connectContentionEvents(handlers) {
  return createDebouncedEventStream({
    url: "/emulator/v1/contention/events",
    primaryEvent: "contention",
    debounceMs: 200,
    maxWaitMs: 1000,
    onInvalidate: handlers.onInvalidate,
    onStatusChange: handlers.onStatusChange,
    makeResyncFallback: () =>
      /** @type {ContentionSseEvent} */ ({ type: "resync" }),
  });
}
