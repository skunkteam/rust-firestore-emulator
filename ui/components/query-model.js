// @ts-check

import {
  VALUE_TYPES,
  encodeBytes,
  findNonFiniteNumber,
  findUnsafeUnquotedInteger,
  isReservedName,
  normalizeProtoValue,
  normalizeReference,
  stringifyProtoJson,
  validateDouble,
  validateGeoPoint,
  validateInteger64,
  validateTimestamp,
} from "../values.js";

/** @typedef {import('../values.js').ValueTypeKey} ValueTypeKey */
/** @typedef {import('../values.js').FirestoreValue} FirestoreValue */
/** @typedef {import('../values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('./query-suggestions.js').FieldScopeEntry} FieldScopeEntry */

/**
 * Supported StructuredQuery field and unary operators matching
 * crates/emulator-database/src/database/query/filter.rs.
 * @type {ReadonlyArray<{ op: string, label: string, kind: 'field' | 'array' | 'unary' }>}
 */
export const FILTER_OPERATORS = [
  { op: "EQUAL", label: "== (EQUAL)", kind: "field" },
  { op: "NOT_EQUAL", label: "!= (NOT_EQUAL)", kind: "field" },
  { op: "LESS_THAN", label: "< (LESS_THAN)", kind: "field" },
  { op: "LESS_THAN_OR_EQUAL", label: "<= (LESS_THAN_OR_EQUAL)", kind: "field" },
  { op: "GREATER_THAN", label: "> (GREATER_THAN)", kind: "field" },
  {
    op: "GREATER_THAN_OR_EQUAL",
    label: ">= (GREATER_THAN_OR_EQUAL)",
    kind: "field",
  },
  { op: "ARRAY_CONTAINS", label: "array-contains", kind: "field" },
  { op: "IN", label: "in (JSON array)", kind: "array" },
  {
    op: "ARRAY_CONTAINS_ANY",
    label: "array-contains-any (JSON array)",
    kind: "array",
  },
  { op: "NOT_IN", label: "not-in (JSON array)", kind: "array" },
  { op: "IS_NULL", label: "is null", kind: "unary" },
  { op: "IS_NOT_NULL", label: "is not null", kind: "unary" },
  { op: "IS_NAN", label: "is NaN", kind: "unary" },
  { op: "IS_NOT_NAN", label: "is not NaN", kind: "unary" },
];

export const SCALAR_FILTER_TYPES = VALUE_TYPES.filter(
  (t) => t.key !== "arrayValue" && t.key !== "mapValue" && t.key !== "nullValue"
);

export const SCALAR_FILTER_TYPE_SET = new Set(
  SCALAR_FILTER_TYPES.map((t) => t.key)
);

export const UNSUPPORTED_QUERY_ENVELOPE_KEYS = [
  "readTime",
  "transaction",
  "newTransaction",
  "explainOptions",
  "requestOptions",
];

const UNSUPPORTED_QUERY_ENVELOPE_KEY_SET = new Set(
  UNSUPPORTED_QUERY_ENVELOPE_KEYS
);

export const STANDARD_MAX_AGGREGATIONS = 5;
export const ENTERPRISE_MAX_AGGREGATIONS = 64;
const MAX_AGGREGATION_ALIAS_BYTES = 1500;
const utf8Encoder = new TextEncoder();

/**
 * @typedef {{
 *   id: number,
 *   fieldPath: string,
 *   op: string,
 *   valueType: ValueTypeKey,
 *   valueText: string,
 *   latText: string,
 *   lngText: string,
 *   valueTypeManuallySet: boolean,
 *   valueUserEdited: boolean,
 * }} QueryFilterRow
 */

/**
 * @typedef {{
 *   id: number,
 *   fieldPath: string,
 *   direction: 'ASCENDING' | 'DESCENDING',
 * }} QueryOrderRow
 */

/**
 * @typedef {{
 *   id: number,
 *   op: 'COUNT' | 'SUM' | 'AVG',
 *   fieldPath: string,
 *   alias: string,
 * }} QueryAggregationRow
 */

/**
 * @typedef {{
 *   queryKind: 'documents' | 'aggregations',
 *   mode: 'visual' | 'raw',
 *   parentSubpath: string,
 *   collectionId: string,
 *   allDescendants: boolean,
 *   compositeOp: 'AND' | 'OR',
 *   filters: QueryFilterRow[],
 *   orders: QueryOrderRow[],
 *   limitText: string,
 *   limitUserModified: boolean,
 *   aggregations: QueryAggregationRow[],
 *   rawQueryText: string,
 *   rawAggregationText: string,
 *   error: string | null,
 *   running: boolean,
 *   elapsedMs: number | null,
 *   readTime: string | null,
 *   results: FirestoreDocument[] | null,
 *   aggResults: Record<string, FirestoreValue> | null,
 *   aggReadTime: string | null,
 *   aggElapsedMs: number | null,
 *   aggProvenance: { limit: number | null, hasOrderBy: boolean } | null,
 * }} QueryRunnerState
 */

let nextRowId = 1;

/**
 * Creates a new default visual Where filter row.
 * @returns {QueryFilterRow}
 */
export function createFilterRow() {
  return {
    id: nextRowId++,
    fieldPath: "",
    op: "EQUAL",
    valueType: "stringValue",
    valueText: "",
    latText: "0",
    lngText: "0",
    valueTypeManuallySet: false,
    valueUserEdited: false,
  };
}

/**
 * Creates a new default visual Order By row.
 * @returns {QueryOrderRow}
 */
export function createOrderRow() {
  return {
    id: nextRowId++,
    fieldPath: "",
    direction: "ASCENDING",
  };
}

/**
 * Creates a new default visual Aggregation row.
 * @returns {QueryAggregationRow}
 */
export function createAggregationRow() {
  return {
    id: nextRowId++,
    op: "COUNT",
    fieldPath: "",
    alias: "",
  };
}

/**
 * Creates initial state for the Structured Query Runner.
 * @param {string} [collectionId]
 * @param {string} [parentSubpath]
 * @returns {QueryRunnerState}
 */
export function createQueryRunnerState(collectionId = "", parentSubpath = "") {
  const targetCollection = collectionId || "users";
  const initialQuery = {
    from: [{ collectionId: targetCollection, allDescendants: false }],
    limit: 50,
  };
  const initialAggregationQuery = {
    structuredQuery: {
      from: [{ collectionId: targetCollection, allDescendants: false }],
    },
    aggregations: [{ count: {} }],
  };
  return {
    queryKind: "documents",
    mode: "visual",
    parentSubpath,
    collectionId,
    allDescendants: false,
    compositeOp: "AND",
    filters: [],
    orders: [],
    limitText: "50",
    limitUserModified: false,
    aggregations: [createAggregationRow()],
    rawQueryText: stringifyProtoJson(initialQuery, 2),
    rawAggregationText: stringifyProtoJson(initialAggregationQuery, 2),
    error: null,
    running: false,
    elapsedMs: null,
    readTime: null,
    results: null,
    aggResults: null,
    aggReadTime: null,
    aggElapsedMs: null,
    aggProvenance: null,
  };
}

/**
 * Returns true when visual controls remain in their untouched default state.
 * @param {QueryRunnerState} state
 * @returns {boolean}
 */
export function isUntouchedDefaultQueryState(state) {
  const basePristine =
    !state.collectionId.trim() &&
    !state.parentSubpath.trim() &&
    !state.allDescendants &&
    !state.limitUserModified &&
    state.filters.length === 0 &&
    state.orders.length === 0;
  if (!basePristine) return false;
  if (state.queryKind === "aggregations") {
    return (
      state.aggregations.length === 1 &&
      state.aggregations[0].op === "COUNT" &&
      !state.aggregations[0].alias.trim() &&
      !state.aggregations[0].fieldPath.trim()
    );
  }
  return true;
}

/**
 * Applies non-clobbering field value type detection from sampled `fieldHints`
 * when neither `valueTypeManuallySet` nor `valueUserEdited` is set.
 * @param {QueryFilterRow} f
 * @param {FieldScopeEntry | null | undefined} fieldEntry
 */
export function applyFilterAutoType(f, fieldEntry) {
  if (f.valueTypeManuallySet || f.valueUserEdited || !fieldEntry) {
    return;
  }
  const opMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
  if (!opMeta || opMeta.kind === "unary") {
    return;
  }
  const trimmedPath = f.fieldPath.trim();
  const hint =
    fieldEntry.fieldHints.get(f.fieldPath) ??
    (trimmedPath ? fieldEntry.fieldHints.get(trimmedPath) : undefined);
  if (!hint) {
    return;
  }
  const detectedType =
    f.op === "ARRAY_CONTAINS" || f.op === "ARRAY_CONTAINS_ANY"
      ? hint.arrayElementType
      : hint.scalarType;
  if (!detectedType || !SCALAR_FILTER_TYPE_SET.has(detectedType)) {
    return;
  }
  const prevType = f.valueType;
  f.valueType = detectedType;
  if (opMeta.kind === "field") {
    if (detectedType === "booleanValue") {
      if (f.valueText !== "true" && f.valueText !== "false") {
        f.valueText = "true";
      }
    } else if (prevType === "booleanValue" && !f.valueUserEdited) {
      f.valueText = "";
    }
  }
}

/**
 * Transitions a filter row's operator, normalizing array placeholders, auto-typed
 * scalar defaults, and boolean `<select>` values cleanly.
 * @param {QueryFilterRow} f
 * @param {string} nextOp
 * @param {FieldScopeEntry | null} [fieldEntry]
 */
export function transitionFilterOperator(f, nextOp, fieldEntry = null) {
  f.op = nextOp;
  const nextMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
  if (nextMeta?.kind === "array" && !f.valueText.trim().startsWith("[")) {
    f.valueText = "[]";
  }
  if (
    nextMeta?.kind === "field" &&
    !f.valueUserEdited &&
    f.valueText === "[]"
  ) {
    f.valueText = "";
  }
  applyFilterAutoType(f, fieldEntry);
  if (
    nextMeta?.kind === "field" &&
    f.valueType === "booleanValue" &&
    f.valueText !== "true" &&
    f.valueText !== "false"
  ) {
    f.valueText = "true";
  }
}

/**
 * Builds and validates a scalar `FirestoreValue` for a visual filter row.
 * @param {QueryFilterRow} f
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {{ valid: boolean, value?: FirestoreValue, error?: string }}
 */
export function buildScalarFilterValue(f, projectId, databaseId) {
  switch (f.valueType) {
    case "stringValue":
      return { valid: true, value: { stringValue: f.valueText } };
    case "booleanValue":
      return {
        valid: true,
        value: { booleanValue: f.valueText.trim() !== "false" },
      };
    case "integerValue": {
      const c = validateInteger64(f.valueText);
      return c.valid && c.canonical
        ? { valid: true, value: { integerValue: c.canonical } }
        : { valid: false, error: c.error };
    }
    case "doubleValue": {
      const c = validateDouble(f.valueText);
      return c.valid && c.value !== undefined
        ? { valid: true, value: { doubleValue: c.value } }
        : { valid: false, error: c.error };
    }
    case "timestampValue": {
      const c = validateTimestamp(f.valueText);
      return c.valid
        ? { valid: true, value: { timestampValue: f.valueText.trim() } }
        : { valid: false, error: c.error };
    }
    case "referenceValue": {
      const c = normalizeReference(f.valueText, projectId, databaseId);
      return c.valid && c.fullPath
        ? { valid: true, value: { referenceValue: c.fullPath } }
        : { valid: false, error: c.error };
    }
    case "bytesValue": {
      const c = encodeBytes(f.valueText, "base64");
      return c.valid && c.base64 !== undefined
        ? { valid: true, value: { bytesValue: c.base64 } }
        : { valid: false, error: c.error };
    }
    case "geoPointValue": {
      const c = validateGeoPoint(f.latText, f.lngText);
      return c.valid
        ? {
            valid: true,
            value: {
              geoPointValue: { latitude: c.latitude, longitude: c.longitude },
            },
          }
        : { valid: false, error: c.error };
    }
    default:
      return { valid: true, value: { stringValue: f.valueText } };
  }
}

/**
 * Builds a canonical StructuredQuery JSON object from visual builder rows.
 * @param {QueryRunnerState} state
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {{ valid: boolean, query?: Record<string, unknown>, error?: string }}
 */
export function buildStructuredQueryFromVisual(state, projectId, databaseId) {
  const colId = state.collectionId.trim();
  if (!colId && !state.allDescendants) {
    return {
      valid: false,
      error:
        "Collection ID is required (or enable Collection Group / allDescendants)",
    };
  }

  /** @type {Record<string, unknown>} */
  const query = {
    from: [
      {
        collectionId: colId,
        ...(state.allDescendants ? { allDescendants: true } : {}),
      },
    ],
  };

  /** @type {Record<string, unknown>[]} */
  const builtFilters = [];
  for (const f of state.filters) {
    const fp = f.fieldPath.trim();
    if (!fp) {
      return { valid: false, error: "Every filter row must specify a field path" };
    }
    const opMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
    if (!opMeta) {
      return { valid: false, error: `Unsupported operator: ${f.op}` };
    }
    if (opMeta.kind === "unary") {
      builtFilters.push({
        unaryFilter: {
          op: f.op,
          field: { fieldPath: fp },
        },
      });
    } else if (opMeta.kind === "array") {
      const unsafeLiteral = findUnsafeUnquotedInteger(f.valueText);
      if (unsafeLiteral) {
        return {
          valid: false,
          error: `Unquoted 64-bit integer ${unsafeLiteral} in ${f.op} filter for "${fp}" loses precision in JSON.parse; wrap in quotes inside ProtoJSON integerValue`,
        };
      }
      /** @type {unknown} */
      let parsedArr;
      try {
        parsedArr = JSON.parse(f.valueText);
      } catch {
        return {
          valid: false,
          error: `Filter "${fp} ${f.op}": enter a JSON array of ProtoJSON values or primitives, e.g. ["alice", "bob"]`,
        };
      }
      if (!Array.isArray(parsedArr)) {
        return {
          valid: false,
          error: `Filter "${fp} ${f.op}" requires a JSON array`,
        };
      }
      const nonFinitePath = findNonFiniteNumber(parsedArr, fp);
      if (nonFinitePath !== null) {
        return {
          valid: false,
          error: `Numeric literal at ${nonFinitePath} in ${f.op} filter for "${fp}" overflows IEEE-754 double; use {"doubleValue": "Infinity"} or "-Infinity" instead`,
        };
      }
      /** @type {FirestoreValue[]} */
      const values = [];
      for (let i = 0; i < parsedArr.length; i++) {
        const item = parsedArr[i];
        if (item === null) values.push({ nullValue: null });
        else if (typeof item === "boolean") values.push({ booleanValue: item });
        else if (typeof item === "string") values.push({ stringValue: item });
        else if (typeof item === "number") {
          if (Number.isSafeInteger(item) && !Object.is(item, -0)) {
            values.push({ integerValue: String(item) });
          } else {
            values.push({ doubleValue: item });
          }
        } else if (Array.isArray(item)) {
          return {
            valid: false,
            error:
              f.op === "ARRAY_CONTAINS_ANY"
                ? `Nested arrays are not supported in ${f.op} filter for "${fp}"`
                : `Array elements in ${f.op} filter for "${fp}" must use explicit ProtoJSON {"arrayValue": {"values": [...]}}`,
          };
        } else if (item && typeof item === "object") {
          const res = normalizeProtoValue(
            item,
            `${fp}[${i}]`,
            f.op === "ARRAY_CONTAINS_ANY"
          );
          if (!res.valid || !res.value) {
            return { valid: false, error: res.error };
          }
          values.push(res.value);
        } else {
          return {
            valid: false,
            error: `Invalid item in ${f.op} array for "${fp}"`,
          };
        }
      }
      builtFilters.push({
        fieldFilter: {
          field: { fieldPath: fp },
          op: f.op,
          value: { arrayValue: { values } },
        },
      });
    } else {
      const valRes = buildScalarFilterValue(f, projectId, databaseId);
      if (!valRes.valid || !valRes.value) {
        return { valid: false, error: `Filter "${fp}": ${valRes.error}` };
      }
      builtFilters.push({
        fieldFilter: {
          field: { fieldPath: fp },
          op: f.op,
          value: valRes.value,
        },
      });
    }
  }

  if (builtFilters.length === 1) {
    query.where = builtFilters[0];
  } else if (builtFilters.length > 1) {
    query.where = {
      compositeFilter: {
        op: state.compositeOp,
        filters: builtFilters,
      },
    };
  }

  if (state.orders.length > 0) {
    /** @type {Record<string, unknown>[]} */
    const orderBy = [];
    for (const o of state.orders) {
      const fp = o.fieldPath.trim();
      if (!fp) {
        return {
          valid: false,
          error: "Every orderBy row must specify a field path",
        };
      }
      orderBy.push({
        field: { fieldPath: fp },
        direction: o.direction,
      });
    }
    query.orderBy = orderBy;
  }

  const trimmedLimit = state.limitText.trim();
  if (trimmedLimit) {
    const n = Number(trimmedLimit);
    if (!Number.isInteger(n) || n < 0) {
      return { valid: false, error: "Limit must be a non-negative integer" };
    }
    query.limit = n;
  }

  return { valid: true, query };
}

/**
 * Builds a canonical StructuredAggregationQuery JSON object from visual builder rows.
 * @param {QueryRunnerState} state
 * @param {string} projectId
 * @param {string} databaseId
 * @param {{ enterprise?: boolean }} [opts]
 * @returns {{ valid: boolean, query?: Record<string, unknown>, error?: string }}
 */
export function buildAggregationQueryFromVisual(
  state,
  projectId,
  databaseId,
  opts = {}
) {
  const base = buildStructuredQueryFromVisual(state, projectId, databaseId);
  if (!base.valid || !base.query) {
    return base;
  }

  const enterprise = Boolean(opts.enterprise);
  const maxAggregations = enterprise
    ? ENTERPRISE_MAX_AGGREGATIONS
    : STANDARD_MAX_AGGREGATIONS;

  if (!Array.isArray(state.aggregations) || state.aggregations.length === 0) {
    return { valid: false, error: "At least one aggregation is required" };
  }
  if (state.aggregations.length > maxAggregations) {
    return {
      valid: false,
      error: `Maximum ${maxAggregations} aggregations per query`,
    };
  }

  let nextAutoAlias = 1;
  /** @type {Set<string>} */
  const seenAliases = new Set();
  /** @type {Record<string, unknown>[]} */
  const builtAggs = [];

  for (const row of state.aggregations) {
    /** @type {Record<string, unknown>} */
    let opObj;
    if (row.op === "COUNT") {
      opObj = { count: {} };
    } else if (row.op === "SUM" || row.op === "AVG") {
      const fp = row.fieldPath.trim();
      if (!fp) {
        return {
          valid: false,
          error: `${row.op} aggregation must specify a field path`,
        };
      }
      opObj =
        row.op === "SUM"
          ? { sum: { field: { fieldPath: fp } } }
          : { avg: { field: { fieldPath: fp } } };
    } else {
      return {
        valid: false,
        error: `Unsupported aggregation operator: ${String(row.op)}`,
      };
    }

    const rawAlias = row.alias.trim();
    /** @type {string} */
    let effectiveAlias;
    if (rawAlias !== "") {
      if (!enterprise && isReservedName(rawAlias)) {
        return {
          valid: false,
          error: `Aggregation alias "${rawAlias}" cannot use reserved __...__ format`,
        };
      }
      if (utf8Encoder.encode(rawAlias).length > MAX_AGGREGATION_ALIAS_BYTES) {
        return {
          valid: false,
          error: `Aggregation alias "${rawAlias}" exceeds ${MAX_AGGREGATION_ALIAS_BYTES} UTF-8 bytes`,
        };
      }
      effectiveAlias = rawAlias;
    } else {
      effectiveAlias = `field_${nextAutoAlias++}`;
    }

    if (seenAliases.has(effectiveAlias)) {
      return {
        valid: false,
        error: `Duplicate aggregation alias "${effectiveAlias}"`,
      };
    }
    seenAliases.add(effectiveAlias);

    builtAggs.push({
      ...opObj,
      ...(rawAlias ? { alias: rawAlias } : {}),
    });
  }

  return {
    valid: true,
    query: {
      structuredQuery: base.query,
      aggregations: builtAggs,
    },
  };
}

/**
 * Formats an optional aggregation provenance constraint label.
 * @param {{ limit: number | null, hasOrderBy: boolean } | null} provenance
 * @returns {string | null}
 */
export function formatAggProvenanceNote(provenance) {
  if (!provenance) return null;
  const hasLimit = provenance.limit !== null;
  const hasOrder = provenance.hasOrderBy;
  if (hasOrder && hasLimit) {
    return `Constrained by orderBy + limit: ${provenance.limit}`;
  }
  if (hasLimit) {
    return `Constrained by limit: ${provenance.limit}`;
  }
  if (hasOrder) {
    return "Constrained by orderBy";
  }
  return null;
}

/**
 * Validates and unwraps Raw JSON query text for `documents` or `aggregations`.
 * @param {string} rawText
 * @param {'documents' | 'aggregations'} queryKind
 * @param {string} [expectedParentPath]
 * @returns {{ valid: boolean, payload?: Record<string, unknown>, error?: string }}
 */
export function parseRawQueryPayload(rawText, queryKind, expectedParentPath) {
  const unsafeLiteral = findUnsafeUnquotedInteger(rawText);
  if (unsafeLiteral) {
    return {
      valid: false,
      error: `Unquoted 64-bit integer ${unsafeLiteral} loses precision in JSON.parse; wrap in quotes: "${unsafeLiteral}"`,
    };
  }

  /** @type {unknown} */
  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch (err) {
    return {
      valid: false,
      error: `Invalid JSON: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return {
      valid: false,
      error: "Raw JSON must be a JSON object.",
    };
  }

  const badPath = findNonFiniteNumber(parsed);
  if (badPath !== null) {
    return {
      valid: false,
      error: `Raw JSON contains a non-finite number at ${badPath} (number overflow).`,
    };
  }

  const obj = /** @type {Record<string, unknown>} */ (parsed);
  const wrapperKey =
    queryKind === "aggregations"
      ? "structuredAggregationQuery"
      : "structuredQuery";

  if (Object.hasOwn(obj, wrapperKey)) {
    /** @type {string[]} */
    const badKeys = [];
    for (const k of Object.keys(obj)) {
      if (k === wrapperKey || k === "parent") {
        continue;
      }
      if (UNSUPPORTED_QUERY_ENVELOPE_KEY_SET.has(k)) {
        if (obj[k] !== null) {
          badKeys.push(k);
        }
        continue;
      }
      badKeys.push(k);
    }
    if (badKeys.length > 0) {
      return {
        valid: false,
        error: `Unsupported top-level field(s) in Raw JSON alongside ${wrapperKey}: ${badKeys.join(", ")}`,
      };
    }

    if (Object.hasOwn(obj, "parent")) {
      const parentVal = obj.parent;
      if (parentVal !== null && typeof parentVal !== "string") {
        return {
          valid: false,
          error: `"parent" in Raw JSON alongside ${wrapperKey} must be a string or null.`,
        };
      }
      if (
        typeof parentVal === "string" &&
        parentVal !== "" &&
        expectedParentPath &&
        parentVal !== expectedParentPath
      ) {
        return {
          valid: false,
          error: `"parent" in Raw JSON (${parentVal}) does not match target path (${expectedParentPath}).`,
        };
      }
    }

    const inner = obj[wrapperKey];
    if (!inner || typeof inner !== "object" || Array.isArray(inner)) {
      return {
        valid: false,
        error: `"${wrapperKey}" in Raw JSON must be a JSON object.`,
      };
    }
    return {
      valid: true,
      payload: /** @type {Record<string, unknown>} */ (inner),
    };
  }

  const unsupported = UNSUPPORTED_QUERY_ENVELOPE_KEYS.filter(
    (k) => Object.hasOwn(obj, k) && obj[k] !== null
  );
  if (unsupported.length > 0) {
    return {
      valid: false,
      error: `Unsupported top-level field(s) in Raw JSON: ${unsupported.join(", ")}`,
    };
  }

  const hasNullEnvelopeKey = UNSUPPORTED_QUERY_ENVELOPE_KEYS.some(
    (k) => Object.hasOwn(obj, k) && obj[k] === null
  );
  if (hasNullEnvelopeKey) {
    const cleaned = { ...obj };
    for (const k of UNSUPPORTED_QUERY_ENVELOPE_KEYS) {
      if (Object.hasOwn(cleaned, k) && cleaned[k] === null) {
        delete cleaned[k];
      }
    }
    return { valid: true, payload: cleaned };
  }

  return { valid: true, payload: obj };
}

/**
 * Extracts limit/orderBy provenance from a StructuredAggregationQuery payload.
 * @param {Record<string, unknown>} payload
 * @returns {{ limit: number | null, hasOrderBy: boolean } | null}
 */
export function extractAggProvenance(payload) {
  const rawSq = payload.structuredQuery;
  if (!rawSq || typeof rawSq !== "object" || Array.isArray(rawSq)) {
    return null;
  }
  const sq = /** @type {Record<string, unknown>} */ (rawSq);
  /** @type {number | null} */
  let limit = null;
  if (sq.limit !== undefined && sq.limit !== null) {
    if (typeof sq.limit === "number" && Number.isFinite(sq.limit)) {
      limit = sq.limit;
    } else if (
      typeof sq.limit === "string" &&
      /^-?\d+$/.test(sq.limit.trim())
    ) {
      const parsed = Number(sq.limit.trim());
      if (Number.isFinite(parsed)) {
        limit = parsed;
      }
    }
  }
  const hasOrderBy = Array.isArray(sq.orderBy) && sq.orderBy.length > 0;
  if (limit === null && !hasOrderBy) {
    return null;
  }
  return { limit, hasOrderBy };
}
