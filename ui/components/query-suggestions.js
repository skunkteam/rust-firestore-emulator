// @ts-check

import { listCollectionIds, listDocuments, runQuery } from "../api.js";
import { isValidPathSegment } from "../router.js";
import {
  VALUE_TYPES,
  escapeFieldPathSegment,
  getValueType,
  isReservedName,
  isVectorMapValue,
} from "../values.js";
import { closeActiveCombobox, createComboboxUiState } from "./combobox.js";

/** @typedef {import('../values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('../values.js').FirestoreValue} FirestoreValue */
/** @typedef {import('../values.js').ValueTypeKey} ValueTypeKey */
/** @typedef {import('./combobox.js').ComboboxItem} ComboboxItem */
/** @typedef {import('./combobox.js').ComboboxUiState} ComboboxUiState */

/**
 * @typedef {{
 *   rootPath: string,
 *   rootCollections: string[],
 *   onRerender: () => void,
 * }} SuggestionContext
 */

/**
 * @typedef {{
 *   items: ComboboxItem[],
 *   loading: boolean,
 *   stale: boolean,
 *   partial: boolean,
 * }} SuggestionCacheEntry
 */

/**
 * @typedef {{
 *   scalarType: ValueTypeKey | null,
 *   arrayElementType: ValueTypeKey | null,
 *   isNumeric: boolean,
 * }} FieldHint
 */

/**
 * @typedef {{
 *   items: ComboboxItem[],
 *   aggItems: ComboboxItem[],
 *   fieldHints: Map<string, FieldHint>,
 *   scope: { parentSubpath: string, collectionId: string, allDescendants: boolean },
 *   loading: boolean,
 *   stale: boolean,
 *   partial: boolean,
 *   sampledDocs: number,
 * }} FieldScopeEntry
 */

/**
 * @typedef {{
 *   epoch: number,
 *   invalidationSeq: number,
 *   controllers: Map<string, AbortController>,
 *   inflight: Map<string, Promise<unknown>>,
 *   subcollectionsByParent: Map<string, SuggestionCacheEntry>,
 *   docIdsByCollection: Map<string, SuggestionCacheEntry>,
 *   collectionGroupsByParent: Map<string, SuggestionCacheEntry>,
 *   fieldsByScope: Map<string, FieldScopeEntry>,
 *   comboboxUi: ComboboxUiState,
 * }} QuerySuggestionStore
 */

/**
 * @typedef {{
 *   escapedPath: string,
 *   rawPath: string,
 *   observedNonNullTypes: Set<ValueTypeKey | 'vector'>,
 *   sawNull: boolean,
 *   scalarCounts: Map<ValueTypeKey, number>,
 *   arrayElemCounts: Map<ValueTypeKey, number>,
 * }} FieldStat
 */

const COLLECTION_GROUP_DISCOVERY_LIMIT = 500;
const FIELD_SAMPLE_LIMIT = 25;
const DOCUMENT_LIST_PAGE_SIZE = 100;
const MAX_FIELD_DEPTH = 5;
const MAX_SAMPLED_FIELD_PATHS = 200;
const MAX_ARRAY_SAMPLE_ITEMS = 32;

const SCALAR_FILTER_TYPE_KEYS = VALUE_TYPES.filter(
  (t) => t.key !== "arrayValue" && t.key !== "mapValue" && t.key !== "nullValue"
).map((t) => t.key);
const SCALAR_FILTER_TYPE_SET = new Set(SCALAR_FILTER_TYPE_KEYS);
const VALUE_LABEL_BY_KEY = new Map(VALUE_TYPES.map((t) => [t.key, t.label]));

/**
 * @param {string} seg
 * @returns {boolean}
 */
function isSafeResourceSegment(seg) {
  return isValidPathSegment(seg) && !isReservedName(seg);
}

/**
 * Normalizes a parent document subpath (trims whitespace and leading/trailing slashes)
 * and validates that it is empty or an even-segment path without empty, dot, or reserved segments.
 * @param {string} rawParentSubpath
 * @returns {{ valid: boolean, normParent: string, segments: string[] }}
 */
export function parseNormalizedParentSubpath(rawParentSubpath) {
  const trimmed = rawParentSubpath.trim().replace(/^\/+|\/+$/g, "");
  if (trimmed === "") {
    return { valid: true, normParent: "", segments: [] };
  }
  const segments = trimmed.split("/");
  if (segments.length % 2 !== 0 || segments.some((s) => !isSafeResourceSegment(s))) {
    return { valid: false, normParent: "", segments: [] };
  }
  return { valid: true, normParent: segments.join("/"), segments };
}

/**
 * Builds an unambiguous JSON-tuple cache key for `fieldsByScope`.
 * @param {string} normParent
 * @param {string} colId
 * @param {boolean} allDescendants
 * @returns {string}
 */
function makeFieldScopeKey(normParent, colId, allDescendants) {
  return JSON.stringify([normParent, colId, Boolean(allDescendants)]);
}

/**
 * @returns {SuggestionCacheEntry}
 */
function createEmptyCacheEntry() {
  return {
    items: [],
    loading: false,
    stale: false,
    partial: false,
  };
}

/**
 * @param {string} parentSubpath
 * @param {string} collectionId
 * @param {boolean} allDescendants
 * @returns {FieldScopeEntry}
 */
function createEmptyFieldScopeEntry(parentSubpath, collectionId, allDescendants) {
  return {
    items: [],
    aggItems: [],
    fieldHints: new Map(),
    scope: { parentSubpath, collectionId, allDescendants },
    loading: false,
    stale: false,
    partial: false,
    sampledDocs: 0,
  };
}

/**
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
function compareCodepoints(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Formats cache provenance status text for a combobox footer.
 * @param {{ sampledDocs?: number, partial?: boolean, stale?: boolean } | null | undefined} entry
 * @returns {string}
 */
export function formatSuggestionFooter(entry) {
  if (!entry) {
    return "";
  }
  const sampleText =
    typeof entry.sampledDocs === "number"
      ? entry.sampledDocs === 1
        ? "Sampled 1 doc"
        : `Sampled ${entry.sampledDocs} docs`
      : "";
  return [
    sampleText,
    entry.partial ? "Partial" : "",
    entry.stale ? "Stale" : "",
  ]
    .filter(Boolean)
    .join(" · ");
}

/**
 * Creates a DB-scoped query suggestion store with empty caches and combobox UI state.
 * @returns {QuerySuggestionStore}
 */
export function createQuerySuggestionStore() {
  return {
    epoch: 0,
    invalidationSeq: 0,
    controllers: new Map(),
    inflight: new Map(),
    subcollectionsByParent: new Map(),
    docIdsByCollection: new Map(),
    collectionGroupsByParent: new Map(),
    fieldsByScope: new Map(),
    comboboxUi: createComboboxUiState(),
  };
}

/**
 * Aborts all in-flight suggestion requests, increments `store.epoch` and `store.invalidationSeq`,
 * clears all four cache maps and in-flight promises, and closes active combobox UI state.
 * @param {QuerySuggestionStore} store
 */
export function resetQuerySuggestionStore(store) {
  for (const controller of store.controllers.values()) {
    controller.abort();
  }
  store.controllers.clear();
  store.inflight.clear();
  store.epoch++;
  store.invalidationSeq++;
  store.subcollectionsByParent.clear();
  store.docIdsByCollection.clear();
  store.collectionGroupsByParent.clear();
  store.fieldsByScope.clear();
  closeActiveCombobox(store.comboboxUi);
}

/**
 * Parses a `fieldsByScope` JSON 3-tuple cache key when `entry.scope` metadata is unavailable.
 * @param {string} scopeKey
 * @param {FieldScopeEntry} entry
 * @returns {{ parentSubpath: string, collectionId: string, allDescendants: boolean } | null}
 */
function resolveFieldEntryScope(scopeKey, entry) {
  if (entry.scope) {
    return entry.scope;
  }
  try {
    const parsed = /** @type {unknown} */ (JSON.parse(scopeKey));
    if (
      Array.isArray(parsed) &&
      parsed.length === 3 &&
      typeof parsed[0] === "string" &&
      typeof parsed[1] === "string" &&
      typeof parsed[2] === "boolean"
    ) {
      return {
        parentSubpath: parsed[0],
        collectionId: parsed[1],
        allDescendants: parsed[2],
      };
    }
  } catch {
    // Ignore malformed key
  }
  return null;
}

/**
 * Marks matching cached suggestion entries as `stale = true` synchronously without firing HTTP requests.
 * @param {QuerySuggestionStore} store
 * @param {Iterable<string> | 'all'} changedPathsOrAll
 */
export function markSuggestionsStale(store, changedPathsOrAll) {
  if (changedPathsOrAll === "all") {
    store.invalidationSeq++;
    for (const entry of store.subcollectionsByParent.values()) {
      entry.stale = true;
    }
    for (const entry of store.docIdsByCollection.values()) {
      entry.stale = true;
    }
    for (const entry of store.collectionGroupsByParent.values()) {
      entry.stale = true;
    }
    for (const entry of store.fieldsByScope.values()) {
      entry.stale = true;
    }
    return;
  }

  let markedAny = false;
  for (const rawPath of changedPathsOrAll) {
    const relDocPath = rawPath
      .replace(/^projects\/[^/]+\/databases\/[^/]+\/documents\/?/, "")
      .replace(/\/+$/, "");
    if (!relDocPath) {
      continue;
    }
    const segs = relDocPath.split("/");
    if (segs.length < 2 || segs.length % 2 !== 0 || segs.some((s) => !s)) {
      continue;
    }

    const parentDoc = segs.slice(0, segs.length - 2).join("/");
    const colId = segs[segs.length - 2];

    for (let k = 2; k <= segs.length - 2; k += 2) {
      const ancestorDoc = segs.slice(0, k).join("/");
      const subEntry = store.subcollectionsByParent.get(ancestorDoc);
      if (subEntry) {
        subEntry.stale = true;
        markedAny = true;
      }
      const cgEntry = store.collectionGroupsByParent.get(ancestorDoc);
      if (cgEntry) {
        cgEntry.stale = true;
        markedAny = true;
      }
    }

    const rootCgEntry = store.collectionGroupsByParent.get("");
    if (rootCgEntry) {
      rootCgEntry.stale = true;
      markedAny = true;
    }

    const changedCol = segs.slice(0, segs.length - 1).join("/");
    const colEntry = store.docIdsByCollection.get(changedCol);
    if (colEntry) {
      colEntry.stale = true;
      markedAny = true;
    }
    for (let k = 1; k <= segs.length - 3; k += 2) {
      const ancestorCol = segs.slice(0, k).join("/");
      const ancEntry = store.docIdsByCollection.get(ancestorCol);
      if (ancEntry) {
        ancEntry.stale = true;
        markedAny = true;
      }
    }

    for (const [scopeKey, entry] of store.fieldsByScope.entries()) {
      const scope = resolveFieldEntryScope(scopeKey, entry);
      if (!scope) continue;
      if (!scope.allDescendants) {
        if (scope.parentSubpath === parentDoc && scope.collectionId === colId) {
          entry.stale = true;
          markedAny = true;
        }
      } else if (
        scope.collectionId === colId &&
        (scope.parentSubpath === "" ||
          parentDoc === scope.parentSubpath ||
          parentDoc.startsWith(`${scope.parentSubpath}/`))
      ) {
        entry.stale = true;
        markedAny = true;
      }
    }
  }
  if (markedAny) {
    store.invalidationSeq++;
  }
}

/**
 * Tracks cache placeholders inserted for an initial (never-settled) fetch so that
 * a transient failure — even after a `force: true` supersede — evicts the placeholder
 * instead of retaining an unpopulated entry.
 * @type {WeakSet<object>}
 */
const unpopulatedPlaceholders = new WeakSet();

/**
 * Deduplicates concurrent requests via `store.inflight`, aborts prior controllers when `force` is true,
 * chains same-epoch aborted callers to any replacement in-flight promise, preserves mid-flight
 * invalidations via `store.invalidationSeq`, checks `!signal.aborted && reqEpoch === store.epoch`
 * before cache writes, and evicts unpopulated placeholders (or marks existing entries stale) on error.
 * @template {{ loading: boolean, stale: boolean }} T
 * @param {QuerySuggestionStore} store
 * @param {SuggestionContext} ctx
 * @param {Map<string, T>} cacheMap
 * @param {string} cacheKey
 * @param {string} ctrlKey
 * @param {boolean} force
 * @param {() => T} createInitial
 * @param {(signal: AbortSignal) => Promise<T>} fetcher
 * @returns {Promise<T>}
 */
function runCachedFetch(
  store,
  ctx,
  cacheMap,
  cacheKey,
  ctrlKey,
  force,
  createInitial,
  fetcher
) {
  const existing = cacheMap.get(cacheKey);
  if (existing && existing.loading && !force) {
    const inflightPromise = store.inflight.get(ctrlKey);
    if (inflightPromise) {
      return /** @type {Promise<T>} */ (inflightPromise);
    }
    return Promise.resolve(existing);
  }
  if (existing && !existing.stale && !force) {
    return Promise.resolve(existing);
  }

  const hadExisting =
    existing !== undefined && !unpopulatedPlaceholders.has(existing);
  const entry = existing ?? createInitial();
  if (!hadExisting) {
    unpopulatedPlaceholders.add(entry);
  }
  entry.loading = true;
  cacheMap.set(cacheKey, entry);

  const prev = store.controllers.get(ctrlKey);
  if (prev) {
    store.controllers.delete(ctrlKey);
    store.inflight.delete(ctrlKey);
    prev.abort();
  }
  const controller = new AbortController();
  store.controllers.set(ctrlKey, controller);
  const { signal } = controller;
  const reqEpoch = store.epoch;
  const invalidationBefore = store.invalidationSeq;

  let settled = false;
  /** @type {Promise<T> | null} */
  let promise = null;
  /**
   * Resolves an aborted or epoch-superseded fetch by chaining to any same-epoch
   * replacement in-flight promise, or falling back to the current cache entry.
   * @returns {Promise<T> | T}
   */
  const resolveAbortedOrStale = () => {
    if (signal.aborted && reqEpoch === store.epoch) {
      const replacement = store.inflight.get(ctrlKey);
      if (replacement && replacement !== promise) {
        return /** @type {Promise<T>} */ (replacement);
      }
      return cacheMap.get(cacheKey) ?? entry;
    }
    return entry;
  };

  promise = (async () => {
    try {
      const nextEntry = await fetcher(signal);
      if (signal.aborted || reqEpoch !== store.epoch) {
        return resolveAbortedOrStale();
      }
      if (store.invalidationSeq !== invalidationBefore) {
        nextEntry.stale = true;
      }
      unpopulatedPlaceholders.delete(nextEntry);
      cacheMap.set(cacheKey, nextEntry);
      return nextEntry;
    } catch {
      if (signal.aborted || reqEpoch !== store.epoch) {
        return resolveAbortedOrStale();
      }
      entry.loading = false;
      entry.stale = true;
      if (!hadExisting && cacheMap.get(cacheKey) === entry) {
        cacheMap.delete(cacheKey);
      }
      return entry;
    } finally {
      settled = true;
      if (store.controllers.get(ctrlKey) === controller) {
        store.controllers.delete(ctrlKey);
      }
      if (promise !== null && store.inflight.get(ctrlKey) === promise) {
        store.inflight.delete(ctrlKey);
      }
      if (!signal.aborted && reqEpoch === store.epoch) {
        ctx.onRerender();
      }
    }
  })();

  if (!settled && store.controllers.get(ctrlKey) === controller) {
    store.inflight.set(ctrlKey, promise);
  }
  if (!settled && !signal.aborted && reqEpoch === store.epoch) {
    ctx.onRerender();
  }
  return promise;
}

/**
 * Reads cached `parentSubpath` combobox suggestions, loading state, and footer text.
 * @param {QuerySuggestionStore} store
 * @param {string[]} knownCollections
 * @param {string} rootPath
 * @param {string} rawInput
 * @returns {{ items: ComboboxItem[], loading: boolean, footerText: string }}
 */
export function getParentSubpathSuggestions(
  store,
  knownCollections,
  rootPath,
  rawInput
) {
  const lastSlash = rawInput.lastIndexOf("/");
  if (lastSlash === -1) {
    return {
      items: knownCollections.map((col) => ({
        value: `${col}/`,
        label: `${col}/`,
      })),
      loading: false,
      footerText: "",
    };
  }
  if (!rootPath) {
    return { items: [], loading: false, footerText: "" };
  }
  const prefix = rawInput.slice(0, lastSlash + 1);
  const rawPrefix = rawInput.slice(0, lastSlash);
  const segments = rawPrefix.split("/");
  if (segments.length === 0 || segments.some((s) => !isSafeResourceSegment(s))) {
    return { items: [], loading: false, footerText: "" };
  }
  if (segments.length % 2 === 0) {
    const normParent = segments.join("/");
    const subEntry = store.subcollectionsByParent.get(normParent);
    if (!subEntry) {
      return { items: [], loading: false, footerText: "" };
    }
    return {
      items: subEntry.items.map((item) => ({
        ...item,
        value: `${prefix}${item.value}/`,
        label: `${item.label}/`,
      })),
      loading: subEntry.loading,
      footerText: formatSuggestionFooter(subEntry),
    };
  }
  const normColPath = segments.join("/");
  const docEntry = store.docIdsByCollection.get(normColPath);
  return {
    items: docEntry?.items ?? [],
    loading: docEntry?.loading ?? false,
    footerText: formatSuggestionFooter(docEntry),
  };
}

/**
 * Reads cached `collectionId` combobox suggestions, loading state, and footer text.
 * @param {QuerySuggestionStore} store
 * @param {string[]} knownCollections
 * @param {string} rawParentSubpath
 * @param {boolean} allDescendants
 * @returns {{ items: ComboboxItem[], loading: boolean, footerText: string }}
 */
export function getCollectionIdSuggestions(
  store,
  knownCollections,
  rawParentSubpath,
  allDescendants
) {
  const parsedParent = parseNormalizedParentSubpath(rawParentSubpath);
  if (!parsedParent.valid) {
    return { items: [], loading: false, footerText: "" };
  }
  if (allDescendants) {
    const cgEntry = store.collectionGroupsByParent.get(parsedParent.normParent);
    return {
      items: cgEntry?.items ?? [],
      loading: cgEntry?.loading ?? false,
      footerText: formatSuggestionFooter(cgEntry),
    };
  }
  if (parsedParent.normParent === "") {
    return {
      items: knownCollections.map((c) => ({ value: c, label: c })),
      loading: false,
      footerText: "",
    };
  }
  const subEntry = store.subcollectionsByParent.get(parsedParent.normParent);
  return {
    items: subEntry?.items ?? [],
    loading: subEntry?.loading ?? false,
    footerText: formatSuggestionFooter(subEntry),
  };
}

/**
 * Looks up the cached `FieldScopeEntry` for `(rawParentSubpath, collectionId, allDescendants)`.
 * @param {QuerySuggestionStore | null | undefined} store
 * @param {string} rawParentSubpath
 * @param {string} collectionId
 * @param {boolean} allDescendants
 * @returns {FieldScopeEntry | null}
 */
export function getFieldScopeEntry(
  store,
  rawParentSubpath,
  collectionId,
  allDescendants
) {
  const parsedParent = parseNormalizedParentSubpath(rawParentSubpath);
  if (!store || !parsedParent.valid) {
    return null;
  }
  const colId = collectionId.trim();
  if (!colId || !isSafeResourceSegment(colId)) {
    return null;
  }
  const scopeKey = makeFieldScopeKey(
    parsedParent.normParent,
    colId,
    Boolean(allDescendants)
  );
  return store.fieldsByScope.get(scopeKey) ?? null;
}

/**
 * Lazily discovers direct subcollection IDs under an even-segment `parentSubpath`.
 * Skips root `""` and invalid/odd/reserved paths without issuing requests.
 * @param {QuerySuggestionStore} store
 * @param {SuggestionContext} ctx
 * @param {string} parentSubpath
 * @param {boolean} [force]
 * @returns {Promise<SuggestionCacheEntry>}
 */
export async function ensureSubcollections(store, ctx, parentSubpath, force = false) {
  const parsed = parseNormalizedParentSubpath(parentSubpath);
  if (!ctx.rootPath || !parsed.valid || parsed.segments.length < 2) {
    return createEmptyCacheEntry();
  }

  const normParent = parsed.normParent;
  return runCachedFetch(
    store,
    ctx,
    store.subcollectionsByParent,
    normParent,
    `sub:${normParent}`,
    force,
    createEmptyCacheEntry,
    async (signal) => {
      const colIds = await listCollectionIds(`${ctx.rootPath}/${normParent}`, {
        signal,
      });
      return {
        items: colIds.map((colId) => ({ value: colId, label: colId })),
        loading: false,
        stale: false,
        partial: false,
      };
    }
  );
}

/**
 * Provides `parentSubpath` suggestions: root collections synchronously before `/`, document IDs
 * on odd slash prefixes (`col/`), and direct subcollections on even slash prefixes (`col/doc/`).
 * @param {QuerySuggestionStore} store
 * @param {SuggestionContext} ctx
 * @param {string} rawInput
 * @param {boolean} [force]
 * @returns {Promise<SuggestionCacheEntry>}
 */
export async function ensureParentSubpathSuggestions(
  store,
  ctx,
  rawInput,
  force = false
) {
  const lastSlash = rawInput.lastIndexOf("/");
  if (lastSlash === -1) {
    return {
      items: ctx.rootCollections.map((col) => ({
        value: `${col}/`,
        label: `${col}/`,
      })),
      loading: false,
      stale: false,
      partial: false,
    };
  }

  if (!ctx.rootPath) {
    return createEmptyCacheEntry();
  }

  const prefix = rawInput.slice(0, lastSlash + 1);
  const rawPrefix = rawInput.slice(0, lastSlash);
  const segments = rawPrefix.split("/");
  if (segments.length === 0 || segments.some((s) => !isSafeResourceSegment(s))) {
    return createEmptyCacheEntry();
  }

  if (segments.length % 2 === 0) {
    const normParent = segments.join("/");
    const subEntry = await ensureSubcollections(store, ctx, normParent, force);
    return {
      items: subEntry.items.map((item) => ({
        ...item,
        value: `${prefix}${item.value}/`,
        label: `${item.label}/`,
      })),
      loading: subEntry.loading,
      stale: subEntry.stale,
      partial: subEntry.partial,
    };
  }

  const normColPath = segments.join("/");
  return runCachedFetch(
    store,
    ctx,
    store.docIdsByCollection,
    normColPath,
    `docs:${normColPath}`,
    force,
    createEmptyCacheEntry,
    async (signal) => {
      const parentRel = segments.slice(0, -1).join("/");
      const parentPath = parentRel ? `${ctx.rootPath}/${parentRel}` : ctx.rootPath;
      const colId = segments[segments.length - 1];
      const res = await listDocuments(parentPath, colId, {
        showMissing: true,
        maskFieldPaths: ["__name__"],
        pageSize: DOCUMENT_LIST_PAGE_SIZE,
        signal,
      });
      /** @type {ComboboxItem[]} */
      const items = [];
      for (const doc of res.documents) {
        const docName = doc.name ?? "";
        const slashIdx = docName.lastIndexOf("/");
        const docId = slashIdx >= 0 ? docName.slice(slashIdx + 1) : docName;
        if (docId) {
          items.push({ value: `${prefix}${docId}`, label: docId });
        }
      }
      return {
        items,
        loading: false,
        stale: false,
        partial: Boolean(res.nextPageToken),
      };
    }
  );
}

/**
 * Discovers descendant collection group IDs under `parentSubpath` using direct child
 * collection seeding plus a single lock-free key-only `allDescendants` query (`limit: 500`).
 * @param {QuerySuggestionStore} store
 * @param {SuggestionContext} ctx
 * @param {string} parentSubpath
 * @param {boolean} [force]
 * @returns {Promise<SuggestionCacheEntry>}
 */
export async function ensureCollectionGroups(store, ctx, parentSubpath, force = false) {
  const parsed = parseNormalizedParentSubpath(parentSubpath);
  if (!ctx.rootPath || !parsed.valid) {
    return createEmptyCacheEntry();
  }

  const normParent = parsed.normParent;
  return runCachedFetch(
    store,
    ctx,
    store.collectionGroupsByParent,
    normParent,
    `cg:${normParent}`,
    force,
    createEmptyCacheEntry,
    async (signal) => {
      const parentPath = normParent ? `${ctx.rootPath}/${normParent}` : ctx.rootPath;
      const subPromise =
        normParent !== ""
          ? ensureSubcollections(store, ctx, normParent, force)
          : Promise.resolve(null);
      const queryPromise = runQuery(
        parentPath,
        {
          from: [{ collectionId: "", allDescendants: true }],
          select: { fields: [{ fieldPath: "__name__" }] },
          limit: COLLECTION_GROUP_DISCOVERY_LIMIT,
        },
        { signal }
      );

      const [subEntry, rows] = await Promise.all([subPromise, queryPromise]);
      /** @type {Set<string>} */
      const colIds = new Set();
      if (normParent === "") {
        for (const col of ctx.rootCollections) {
          if (isSafeResourceSegment(col)) {
            colIds.add(col);
          }
        }
      } else if (subEntry) {
        for (const item of subEntry.items) {
          if (isSafeResourceSegment(item.value)) {
            colIds.add(item.value);
          }
        }
      }

      const docRows = Array.isArray(rows) ? rows.filter((r) => Boolean(r.document)) : [];
      const pathPrefix = `${parentPath}/`;
      for (const row of docRows) {
        const docName = row.document?.name ?? "";
        if (!docName.startsWith(pathPrefix)) {
          continue;
        }
        const rel = docName.slice(pathPrefix.length);
        const segs = rel.split("/");
        for (let i = 0; i + 1 < segs.length; i += 2) {
          const seg = segs[i];
          if (isSafeResourceSegment(seg)) {
            colIds.add(seg);
          }
        }
      }

      const items = Array.from(colIds)
        .sort(compareCodepoints)
        .map((colId) => ({ value: colId, label: colId }));
      return {
        items,
        loading: false,
        stale: Boolean(subEntry?.stale),
        partial: docRows.length >= COLLECTION_GROUP_DISCOVERY_LIMIT,
      };
    }
  );
}

/**
 * Picks the highest-frequency `ValueTypeKey` in `counts`, breaking ties by `SCALAR_FILTER_TYPE_KEYS` order.
 * @param {Map<ValueTypeKey, number>} counts
 * @returns {ValueTypeKey | null}
 */
function pickDominantScalarType(counts) {
  /** @type {ValueTypeKey | null} */
  let bestKey = null;
  let bestCount = 0;
  for (const key of SCALAR_FILTER_TYPE_KEYS) {
    const count = counts.get(key) ?? 0;
    if (count > bestCount) {
      bestCount = count;
      bestKey = key;
    }
  }
  return bestKey;
}

/**
 * Resolves the UI type badge text and CSS class for a sampled field.
 * @param {FieldStat} stat
 * @returns {{ badge: string, badgeClass: string }}
 */
function resolveFieldBadge(stat) {
  if (stat.observedNonNullTypes.size === 0) {
    return {
      badge: VALUE_LABEL_BY_KEY.get("nullValue") ?? "null",
      badgeClass: "nullValue",
    };
  }
  if (stat.observedNonNullTypes.size === 1) {
    const [onlyType] = stat.observedNonNullTypes;
    if (onlyType === "vector") {
      return { badge: "vector", badgeClass: "vector" };
    }
    return {
      badge: VALUE_LABEL_BY_KEY.get(onlyType) ?? onlyType,
      badgeClass: onlyType,
    };
  }
  return { badge: "mixed", badgeClass: "mixed" };
}

/**
 * Recursively folds a `fields` map into `stats` up to 1-based depth `MAX_FIELD_DEPTH` and
 * `MAX_SAMPLED_FIELD_PATHS` unique paths.
 * @param {Record<string, FirestoreValue>} fields
 * @param {number} depth
 * @param {string} escapedPrefix
 * @param {string} rawPrefix
 * @param {Map<string, FieldStat>} stats
 */
function foldFieldsMap(fields, depth, escapedPrefix, rawPrefix, stats) {
  for (const [seg, val] of Object.entries(fields)) {
    if (!seg || isReservedName(seg) || !val || typeof val !== "object") {
      continue;
    }
    const escapedSeg = escapeFieldPathSegment(seg);
    const escapedPath = escapedPrefix ? `${escapedPrefix}.${escapedSeg}` : escapedSeg;
    const rawPath = rawPrefix ? `${rawPrefix}.${seg}` : seg;

    let stat = stats.get(escapedPath);
    if (!stat) {
      if (stats.size >= MAX_SAMPLED_FIELD_PATHS) {
        continue;
      }
      stat = {
        escapedPath,
        rawPath,
        observedNonNullTypes: new Set(),
        sawNull: false,
        scalarCounts: new Map(),
        arrayElemCounts: new Map(),
      };
      stats.set(escapedPath, stat);
    }

    if (isVectorMapValue(val)) {
      stat.observedNonNullTypes.add("vector");
      continue;
    }

    const typeKey = getValueType(val);
    if (typeKey === "nullValue") {
      stat.sawNull = true;
      continue;
    }

    stat.observedNonNullTypes.add(typeKey);
    if (SCALAR_FILTER_TYPE_SET.has(typeKey)) {
      stat.scalarCounts.set(typeKey, (stat.scalarCounts.get(typeKey) ?? 0) + 1);
    }

    if (typeKey === "arrayValue") {
      const arrValues = val.arrayValue?.values;
      if (Array.isArray(arrValues)) {
        const scanCount = Math.min(arrValues.length, MAX_ARRAY_SAMPLE_ITEMS);
        for (let i = 0; i < scanCount; i++) {
          const elem = arrValues[i];
          if (!elem || typeof elem !== "object" || isVectorMapValue(elem)) {
            continue;
          }
          const elemType = getValueType(elem);
          if (SCALAR_FILTER_TYPE_SET.has(elemType)) {
            stat.arrayElemCounts.set(
              elemType,
              (stat.arrayElemCounts.get(elemType) ?? 0) + 1
            );
          }
        }
      }
      continue;
    }

    if (typeKey === "mapValue" && depth < MAX_FIELD_DEPTH) {
      const nestedFields = val.mapValue?.fields;
      if (nestedFields && typeof nestedFields === "object") {
        foldFieldsMap(nestedFields, depth + 1, escapedPath, rawPath, stats);
      }
    }
  }
}

/**
 * Folds sampled documents into `FieldScopeEntry` lists and per-field auto-type hints.
 * @param {FirestoreDocument[]} docs
 * @param {{ parentSubpath: string, collectionId: string, allDescendants: boolean }} scope
 * @returns {FieldScopeEntry}
 */
function buildFieldScopeEntry(docs, scope) {
  /** @type {Map<string, FieldStat>} */
  const stats = new Map();
  for (const doc of docs) {
    if (doc.fields && typeof doc.fields === "object") {
      foldFieldsMap(doc.fields, 1, "", "", stats);
    }
  }

  const sortedStats = Array.from(stats.values()).sort((a, b) =>
    compareCodepoints(a.escapedPath, b.escapedPath)
  );

  /** @type {ComboboxItem[]} */
  const userItems = [];
  /** @type {ComboboxItem[]} */
  const numericAggItems = [];
  /** @type {ComboboxItem[]} */
  const otherAggItems = [];
  /** @type {Map<string, FieldHint>} */
  const fieldHints = new Map();

  for (const stat of sortedStats) {
    const { badge, badgeClass } = resolveFieldBadge(stat);
    /** @type {ComboboxItem} */
    const item = {
      value: stat.escapedPath,
      label: stat.escapedPath,
      ...(stat.rawPath !== stat.escapedPath ? { matchText: stat.rawPath } : {}),
      badge,
      badgeClass,
    };
    userItems.push(item);

    const isNumeric =
      stat.observedNonNullTypes.has("integerValue") ||
      stat.observedNonNullTypes.has("doubleValue");
    if (isNumeric) {
      numericAggItems.push(item);
    } else {
      otherAggItems.push(item);
    }

    fieldHints.set(stat.escapedPath, {
      scalarType: pickDominantScalarType(stat.scalarCounts),
      arrayElementType: pickDominantScalarType(stat.arrayElemCounts),
      isNumeric,
    });
  }

  for (const stat of sortedStats) {
    if (stat.rawPath !== stat.escapedPath && !fieldHints.has(stat.rawPath)) {
      const hint = fieldHints.get(stat.escapedPath);
      if (hint) {
        fieldHints.set(stat.rawPath, hint);
      }
    }
  }

  fieldHints.set("__name__", {
    scalarType: "referenceValue",
    arrayElementType: null,
    isNumeric: false,
  });

  const nameItem = /** @type {ComboboxItem} */ ({
    value: "__name__",
    label: "__name__",
    badge: VALUE_LABEL_BY_KEY.get("referenceValue") ?? "reference",
    badgeClass: "referenceValue",
  });

  return {
    items: [...userItems, nameItem],
    aggItems: [...numericAggItems, ...otherAggItems],
    fieldHints,
    scope,
    loading: false,
    stale: false,
    partial: docs.length >= FIELD_SAMPLE_LIMIT,
    sampledDocs: docs.length,
  };
}

/**
 * Lazily samples up to 25 documents in the target `(parentSubpath, collectionId, allDescendants)`
 * scope to suggest field paths and dominant value types.
 * @param {QuerySuggestionStore} store
 * @param {SuggestionContext} ctx
 * @param {{ parentSubpath: string, collectionId: string, allDescendants: boolean }} scope
 * @param {boolean} [force]
 * @returns {Promise<FieldScopeEntry>}
 */
export async function ensureFieldSuggestions(store, ctx, scope, force = false) {
  const colId = scope.collectionId.trim();
  const parsed = parseNormalizedParentSubpath(scope.parentSubpath);
  const allDescendants = Boolean(scope.allDescendants);
  if (!ctx.rootPath || !colId || !isSafeResourceSegment(colId) || !parsed.valid) {
    return createEmptyFieldScopeEntry(parsed.normParent, colId, allDescendants);
  }

  const normParent = parsed.normParent;
  const normalizedScope = {
    parentSubpath: normParent,
    collectionId: colId,
    allDescendants,
  };
  const scopeKey = makeFieldScopeKey(normParent, colId, allDescendants);
  return runCachedFetch(
    store,
    ctx,
    store.fieldsByScope,
    scopeKey,
    `fields:${scopeKey}`,
    force,
    () => createEmptyFieldScopeEntry(normParent, colId, allDescendants),
    async (signal) => {
      const parentPath = normParent ? `${ctx.rootPath}/${normParent}` : ctx.rootPath;
      const rows = await runQuery(
        parentPath,
        {
          from: [{ collectionId: colId, allDescendants }],
          limit: FIELD_SAMPLE_LIMIT,
        },
        { signal }
      );
      /** @type {FirestoreDocument[]} */
      const docs = [];
      if (Array.isArray(rows)) {
        for (const row of rows) {
          if (row.document) {
            docs.push(row.document);
          }
        }
      }
      return buildFieldScopeEntry(docs, normalizedScope);
    }
  );
}
