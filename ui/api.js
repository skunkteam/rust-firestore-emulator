// @ts-check

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
 *   type: 'database_created' | 'database_cleared' | 'database_config_changed' | 'documents_changed' | 'resync',
 *   database?: string,
 *   paths?: string[],
 *   updateTime?: string,
 *   skipped?: number,
 * }} EmulatorSseEvent
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
 * @template T
 * @param {string} url
 * @param {RequestInit} [init]
 * @returns {Promise<T>}
 */
async function requestJson(url, init = {}) {
  const res = await fetch(url, init);
  const text = await res.text();
  if (!res.ok) {
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === "object" && parsed.error) {
        throw new ApiError(
          Number(parsed.error.code ?? res.status),
          String(parsed.error.status ?? "UNKNOWN"),
          String(parsed.error.message ?? res.statusText)
        );
      }
    } catch (err) {
      if (err instanceof ApiError) throw err;
    }
    throw new ApiError(res.status, "HTTP_ERROR", text || res.statusText);
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
 * @returns {Promise<DatabaseMetadata>}
 */
export async function getDatabase(projectId, databaseId) {
  const p = encodeURIComponent(projectId);
  const d = encodeURIComponent(databaseId);
  return requestJson(`/v1/projects/${p}/databases/${d}`);
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

/**
 * Lists direct child collection IDs under a database root or document path.
 * @param {string} parentPath
 * @returns {Promise<string[]>}
 */
export async function listCollectionIds(parentPath) {
  /** @type {{ collectionIds?: string[] }} */
  const res = await requestJson(
    `/v1/${encodeResourcePath(parentPath)}:listCollectionIds`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    }
  );
  return res.collectionIds ?? [];
}

/**
 * Lists documents in a collection.
 * @param {string} parentPath
 * @param {string} collectionId
 * @param {{ showMissing?: boolean, maskFieldPaths?: string[], pageSize?: number, pageToken?: string }} [opts]
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
  const res = await requestJson(url);
  return {
    documents: res.documents ?? [],
    nextPageToken: res.nextPageToken ?? "",
  };
}

/**
 * Fetches a single document by full resource path.
 * @param {string} docPath
 * @returns {Promise<FirestoreDocument>}
 */
export async function getDocument(docPath) {
  return requestJson(`/v1/${encodeResourcePath(docPath)}`);
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
    body: JSON.stringify({ fields }),
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
    body: JSON.stringify({ fields }),
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
 * @returns {Promise<Array<{ document?: FirestoreDocument, readTime?: string }>>}
 */
export async function runQuery(parentPath, structuredQuery) {
  return requestJson(`/v1/${encodeResourcePath(parentPath)}:runQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ structuredQuery }),
  });
}

/**
 * Connects to `/emulator/v1/events` SSE stream and coalesces events with a 150ms trailing debounce.
 * @param {(events: EmulatorSseEvent[]) => void} onInvalidate
 * @param {(status: 'live' | 'error') => void} onStatusChange
 * @returns {{ close: () => void }}
 */
export function connectEventStream(onInvalidate, onStatusChange) {
  const es = new EventSource("/emulator/v1/events");
  /** @type {EmulatorSseEvent[]} */
  let pending = [];
  /** @type {ReturnType<typeof setTimeout> | null} */
  let timer = null;

  const enqueue = (/** @type {EmulatorSseEvent} */ ev) => {
    pending.push(ev);
    if (timer !== null) {
      clearTimeout(timer);
    }
    timer = setTimeout(() => {
      const batch = pending;
      pending = [];
      timer = null;
      onInvalidate(batch);
    }, 150);
  };

  es.addEventListener("open", () => {
    onStatusChange("live");
  });

  es.addEventListener("error", () => {
    onStatusChange("error");
  });

  es.addEventListener("change", (msg) => {
    try {
      const ev = /** @type {EmulatorSseEvent} */ (
        JSON.parse(/** @type {MessageEvent} */ (msg).data)
      );
      enqueue(ev);
    } catch {
      // Ignore malformed frame
    }
  });

  es.addEventListener("resync", (msg) => {
    try {
      const ev = /** @type {EmulatorSseEvent} */ (
        JSON.parse(/** @type {MessageEvent} */ (msg).data)
      );
      enqueue(ev);
    } catch {
      enqueue({ type: "resync" });
    }
  });

  return {
    close() {
      if (timer !== null) {
        clearTimeout(timer);
      }
      es.close();
    },
  };
}
