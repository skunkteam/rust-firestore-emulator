// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  VALUE_TYPES,
  findNonFiniteNumber,
  findUnsafeUnquotedInteger,
  formatValuePreview,
  getValueType,
  isReservedName,
  normalizeProtoValue,
  normalizeReference,
  stringifyProtoJson,
  validateDouble,
  validateGeoPoint,
  validateInteger64,
  validateTimestamp,
  encodeBytes,
} from "../values.js";

/** @typedef {import('../values.js').ValueTypeKey} ValueTypeKey */
/** @typedef {import('../values.js').FirestoreValue} FirestoreValue */
/** @typedef {import('../values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */

/**
 * Supported StructuredQuery field and unary operators matching
 * crates/emulator-database/src/database/query/filter.rs.
 */
const FILTER_OPERATORS = [
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
  { op: "ARRAY_CONTAINS_ANY", label: "array-contains-any (JSON array)", kind: "array" },
  { op: "NOT_IN", label: "not-in (JSON array)", kind: "array" },
  { op: "IS_NULL", label: "is null", kind: "unary" },
  { op: "IS_NOT_NULL", label: "is not null", kind: "unary" },
  { op: "IS_NAN", label: "is NaN", kind: "unary" },
  { op: "IS_NOT_NAN", label: "is not NaN", kind: "unary" },
];

const SCALAR_FILTER_TYPES = VALUE_TYPES.filter(
  (t) => t.key !== "arrayValue" && t.key !== "mapValue" && t.key !== "nullValue"
);

/**
 * @typedef {{
 *   id: number,
 *   fieldPath: string,
 *   op: string,
 *   valueType: ValueTypeKey,
 *   valueText: string,
 *   latText: string,
 *   lngText: string,
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

const utf8Encoder = new TextEncoder();
const MAX_AGGREGATION_ALIAS_BYTES = 1500;
const STANDARD_MAX_AGGREGATIONS = 5;
const ENTERPRISE_MAX_AGGREGATIONS = 64;

let nextRowId = 1;

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
    aggregations: [
      { id: nextRowId++, op: "COUNT", fieldPath: "", alias: "" },
    ],
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
      error: "Collection ID is required (or enable Collection Group / allDescendants)",
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
        return { valid: false, error: `Filter "${fp} ${f.op}" requires a JSON array` };
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
          return { valid: false, error: `Invalid item in ${f.op} array for "${fp}"` };
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
        return { valid: false, error: "Every orderBy row must specify a field path" };
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
 * Returns true when visual controls remain in their untouched default state.
 * @param {QueryRunnerState} state
 * @returns {boolean}
 */
function isPristineVisual(state) {
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
 * @param {QueryFilterRow} f
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {{ valid: boolean, value?: FirestoreValue, error?: string }}
 */
function buildScalarFilterValue(f, projectId, databaseId) {
  switch (f.valueType) {
    case "stringValue":
      return { valid: true, value: { stringValue: f.valueText } };
    case "booleanValue":
      return {
        valid: true,
        value: { booleanValue: f.valueText.trim() === "true" },
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
 * Renders the Structured Query Runner workspace.
 *
 * @param {{
 *   projectId: string,
 *   databaseId: string,
 *   knownCollections: string[],
 *   enterprise?: boolean,
 *   state: QueryRunnerState,
 *   onRerender: () => void,
 *   onExecuteQuery: () => void,
 *   onOpenResultDocument: (docName: string) => void,
 * }} props
 * @returns {TemplateResult}
 */
export function renderQueryRunner(props) {
  const {
    projectId,
    databaseId,
    knownCollections,
    enterprise = false,
    state,
    onRerender,
    onExecuteQuery,
    onOpenResultDocument,
  } = props;
  const maxAggregations = enterprise
    ? ENTERPRISE_MAX_AGGREGATIONS
    : STANDARD_MAX_AGGREGATIONS;

  const switchQueryKind = (
    /** @type {'documents' | 'aggregations'} */ nextKind
  ) => {
    if (state.queryKind === nextKind) return;
    state.queryKind = nextKind;
    state.error = null;
    if (!state.limitUserModified) {
      state.limitText = nextKind === "aggregations" ? "" : "50";
    }
    onRerender();
  };

  const switchTab = (/** @type {'visual' | 'raw'} */ nextMode) => {
    if (state.mode === nextMode) return;
    if (nextMode === "raw") {
      if (isPristineVisual(state)) {
        state.error = null;
      } else {
        const built =
          state.queryKind === "aggregations"
            ? buildAggregationQueryFromVisual(state, projectId, databaseId, {
                enterprise,
              })
            : buildStructuredQueryFromVisual(state, projectId, databaseId);
        if (!built.valid || !built.query) {
          state.error = built.error ?? "Invalid query";
          onRerender();
          return;
        }
        if (state.queryKind === "aggregations") {
          state.rawAggregationText = stringifyProtoJson(built.query, 2);
        } else {
          state.rawQueryText = stringifyProtoJson(built.query, 2);
        }
        state.error = null;
      }
    } else {
      state.error = null;
    }
    state.mode = nextMode;
    onRerender();
  };

  const addAggregation = () => {
    if (state.aggregations.length >= maxAggregations) return;
    state.aggregations.push({
      id: nextRowId++,
      op: "COUNT",
      fieldPath: "",
      alias: "",
    });
    onRerender();
  };

  const addFilter = () => {
    state.filters.push({
      id: nextRowId++,
      fieldPath: "",
      op: "EQUAL",
      valueType: "stringValue",
      valueText: "",
      latText: "0",
      lngText: "0",
    });
    onRerender();
  };

  const addOrder = () => {
    state.orders.push({
      id: nextRowId++,
      fieldPath: "",
      direction: "ASCENDING",
    });
    onRerender();
  };

  const isAgg = state.queryKind === "aggregations";
  const rawBufferValue = isAgg ? state.rawAggregationText : state.rawQueryText;

  return html`
    <div class="query-workspace">
      <aside class="query-builder-pane">
        <div class="query-mode-bar">
          <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
            <div class="segmented" role="group" aria-label="Query kind">
              <button
                type="button"
                class=${state.queryKind === "documents" ? "active" : ""}
                aria-pressed=${String(state.queryKind === "documents")}
                @click=${() => switchQueryKind("documents")}
              >
                Documents
              </button>
              <button
                type="button"
                class=${state.queryKind === "aggregations" ? "active" : ""}
                aria-pressed=${String(state.queryKind === "aggregations")}
                @click=${() => switchQueryKind("aggregations")}
              >
                Aggregations
              </button>
            </div>

            <div class="segmented" role="group" aria-label="Editor mode">
              <button
                type="button"
                class=${state.mode === "visual" ? "active" : ""}
                aria-pressed=${String(state.mode === "visual")}
                @click=${() => switchTab("visual")}
              >
                Visual
              </button>
              <button
                type="button"
                class=${state.mode === "raw" ? "active" : ""}
                aria-pressed=${String(state.mode === "raw")}
                @click=${() => switchTab("raw")}
              >
                Raw JSON
              </button>
            </div>
          </div>

          <button
            type="button"
            class="btn btn-primary"
            ?disabled=${state.running}
            @click=${onExecuteQuery}
          >
            ${state.running
              ? "Running..."
              : isAgg
                ? "▶ Run Aggregation"
                : "▶ Run Query"}
          </button>
        </div>

        <div class="form-field">
          <label for="query-parent-subpath">Parent Document Path (optional — leave empty for database root)</label>
          <input
            id="query-parent-subpath"
            type="text"
            class="input mono"
            placeholder="e.g. rooms/room-1"
            .value=${state.parentSubpath}
            @input=${(/** @type {Event} */ e) => {
              state.parentSubpath = /** @type {HTMLInputElement} */ (
                e.target
              ).value;
              onRerender();
            }}
          />
        </div>

        ${state.error ? html`<div class="error-banner">${state.error}</div>` : nothing}

        ${state.mode === "visual"
          ? html`
              <div class="query-section">
                <div class="query-section-title">
                  <label for="query-collection-id">Collection Source</label>
                </div>
                <div class="query-row">
                  <input
                    id="query-collection-id"
                    type="text"
                    list="known-collections-list"
                    class="input mono"
                    style="flex: 1 1 180px;"
                    placeholder="Collection ID (e.g. users)"
                    .value=${state.collectionId}
                    @input=${(/** @type {Event} */ e) => {
                      state.collectionId = /** @type {HTMLInputElement} */ (
                        e.target
                      ).value;
                      onRerender();
                    }}
                  />
                  <datalist id="known-collections-list">
                    ${knownCollections.map(
                      (c) => html`<option value=${c}></option>`
                    )}
                  </datalist>
                </div>
                <label for="query-all-descendants" style="display: flex; align-items: center; gap: 6px; font-size: 12.5px;">
                  <input
                    id="query-all-descendants"
                    type="checkbox"
                    .checked=${state.allDescendants}
                    @change=${(/** @type {Event} */ e) => {
                      state.allDescendants = /** @type {HTMLInputElement} */ (
                        e.target
                      ).checked;
                      onRerender();
                    }}
                  />
                  Collection group query (<code>allDescendants: true</code>)
                </label>
              </div>

              ${isAgg
                ? renderAggregationsSection(
                    state,
                    maxAggregations,
                    addAggregation,
                    onRerender
                  )
                : nothing}

              <div class="query-section">
                <div class="query-section-title">
                  <span>Where Filters</span>
                  <div style="display: flex; align-items: center; gap: 6px;">
                    ${state.filters.length > 1
                      ? html`
                          <select
                            class="select"
                            aria-label="Combine filters"
                            style="padding: 2px 6px; font-size: 11.5px;"
                            .value=${state.compositeOp}
                            @change=${(/** @type {Event} */ e) => {
                              state.compositeOp = /** @type {'AND' | 'OR'} */ (
                                /** @type {HTMLSelectElement} */ (e.target).value
                              );
                              onRerender();
                            }}
                          >
                            <option value="AND">Match ALL (AND)</option>
                            <option value="OR">Match ANY (OR)</option>
                          </select>
                        `
                      : nothing}
                    <button type="button" class="btn btn-sm" @click=${addFilter}>
                      + Add Filter
                    </button>
                  </div>
                </div>

                ${state.filters.length === 0
                  ? html`<div style="font-size: 12px; color: var(--text-faint);">
                      No filters — returns all documents in collection.
                    </div>`
                  : state.filters.map((f, idx) =>
                      renderFilterRow(f, idx, state, onRerender)
                    )}
              </div>

              <div class="query-section">
                <div class="query-section-title">
                  <span>Order By</span>
                  <button type="button" class="btn btn-sm" @click=${addOrder}>
                    + Add Sort
                  </button>
                </div>
                ${state.orders.length === 0
                  ? html`<div style="font-size: 12px; color: var(--text-faint);">
                      Default ordering (<code>__name__ ASC</code>).
                    </div>`
                  : state.orders.map(
                      (o, idx) => html`
                        <div class="query-row">
                          <input
                            type="text"
                            class="input mono"
                            style="flex: 1 1 150px;"
                            aria-label=${`Sort ${idx + 1} field path`}
                            placeholder="field path (e.g. age)"
                            .value=${o.fieldPath}
                            @input=${(/** @type {Event} */ e) => {
                              o.fieldPath = /** @type {HTMLInputElement} */ (
                                e.target
                              ).value;
                              onRerender();
                            }}
                          />
                          <select
                            class="select"
                            aria-label=${`Sort ${idx + 1} direction`}
                            .value=${o.direction}
                            @change=${(/** @type {Event} */ e) => {
                              o.direction =
                                /** @type {'ASCENDING' | 'DESCENDING'} */ (
                                  /** @type {HTMLSelectElement} */ (e.target)
                                    .value
                                );
                              onRerender();
                            }}
                          >
                            <option value="ASCENDING">ASCENDING</option>
                            <option value="DESCENDING">DESCENDING</option>
                          </select>
                          <button
                            type="button"
                            class="btn btn-sm btn-ghost"
                            title="Remove sort order"
                            aria-label=${`Remove sort order ${idx + 1}`}
                            @click=${() => {
                              state.orders.splice(idx, 1);
                              onRerender();
                            }}
                          >
                            ✕
                          </button>
                        </div>
                      `
                    )}
              </div>

              <div class="query-section">
                <div class="query-section-title">
                  <label for="query-limit">Limit</label>
                </div>
                <div class="query-row">
                  <input
                    id="query-limit"
                    type="number"
                    min="0"
                    class="input mono"
                    style="max-width: 140px;"
                    placeholder="No limit"
                    .value=${state.limitText}
                    @input=${(/** @type {Event} */ e) => {
                      state.limitText = /** @type {HTMLInputElement} */ (
                        e.target
                      ).value;
                      state.limitUserModified = true;
                      onRerender();
                    }}
                  />
                  ${isAgg && state.limitText.trim() !== ""
                    ? html`
                        <span
                          class="query-limit-warning"
                          title="Aggregation runs over the limited query window"
                          aria-label="Aggregation runs over the limited query window"
                        >
                          !
                        </span>
                      `
                    : nothing}
                </div>
              </div>
            `
          : html`
              <div class="raw-json-wrap">
                <textarea
                  class="textarea raw-json-textarea"
                  aria-label=${isAgg
                    ? "Raw StructuredAggregationQuery JSON"
                    : "Raw StructuredQuery JSON"}
                  spellcheck="false"
                  .value=${rawBufferValue}
                  @input=${(/** @type {Event} */ e) => {
                    const nextVal = /** @type {HTMLTextAreaElement} */ (
                      e.target
                    ).value;
                    if (isAgg) {
                      state.rawAggregationText = nextVal;
                    } else {
                      state.rawQueryText = nextVal;
                    }
                    onRerender();
                  }}
                ></textarea>
              </div>
            `}
      </aside>

      ${renderResultsPane(state, onOpenResultDocument)}
    </div>
  `;
}

/**
 * @param {QueryRunnerState} state
 * @param {number} maxAggregations
 * @param {() => void} addAggregation
 * @param {() => void} onRerender
 * @returns {TemplateResult}
 */
function renderAggregationsSection(
  state,
  maxAggregations,
  addAggregation,
  onRerender
) {
  const atCap = state.aggregations.length >= maxAggregations;
  let nextAutoAlias = 1;
  const rowPlaceholders = state.aggregations.map((row) =>
    row.alias.trim() === "" ? `field_${nextAutoAlias++}` : "alias (optional)"
  );

  return html`
    <div class="query-section">
      <div class="query-section-title">
        <span>Aggregations</span>
        <button
          type="button"
          class="btn btn-sm"
          ?disabled=${atCap}
          title=${atCap
            ? `Maximum ${maxAggregations} aggregations per query`
            : "Add aggregation"}
          @click=${addAggregation}
        >
          + Add Aggregation
        </button>
      </div>

      ${state.aggregations.map((row, idx) => {
        const isCount = row.op === "COUNT";
        return html`
          <div class="query-agg-row">
            <select
              class="select"
              aria-label=${`Aggregation ${idx + 1} operator`}
              .value=${row.op}
              @change=${(/** @type {Event} */ e) => {
                const nextOp = /** @type {'COUNT' | 'SUM' | 'AVG'} */ (
                  /** @type {HTMLSelectElement} */ (e.target).value
                );
                row.op = nextOp;
                if (nextOp === "COUNT") {
                  row.fieldPath = "";
                }
                onRerender();
              }}
            >
              <option value="COUNT" ?selected=${row.op === "COUNT"}>COUNT</option>
              <option value="SUM" ?selected=${row.op === "SUM"}>SUM</option>
              <option value="AVG" ?selected=${row.op === "AVG"}>AVG</option>
            </select>

            <input
              type="text"
              class="input mono"
              style="flex: 1 1 120px;"
              aria-label=${`Aggregation ${idx + 1} field path`}
              placeholder=${isCount ? "—" : "field path (e.g. total)"}
              ?disabled=${isCount}
              .value=${row.fieldPath}
              @input=${(/** @type {Event} */ e) => {
                row.fieldPath = /** @type {HTMLInputElement} */ (
                  e.target
                ).value;
                onRerender();
              }}
            />

            <input
              type="text"
              class="input mono"
              style="flex: 0 1 120px;"
              aria-label=${`Aggregation ${idx + 1} alias`}
              placeholder=${rowPlaceholders[idx]}
              .value=${row.alias}
              @input=${(/** @type {Event} */ e) => {
                row.alias = /** @type {HTMLInputElement} */ (e.target).value;
                onRerender();
              }}
            />

            <button
              type="button"
              class="btn btn-sm btn-ghost"
              ?disabled=${state.aggregations.length <= 1}
              title="Remove aggregation"
              aria-label=${`Remove aggregation ${idx + 1}`}
              @click=${() => {
                if (state.aggregations.length <= 1) return;
                state.aggregations.splice(idx, 1);
                onRerender();
              }}
            >
              ✕
            </button>
          </div>
        `;
      })}
    </div>
  `;
}

/**
 * @param {{ limit: number | null, hasOrderBy: boolean } | null} provenance
 * @returns {string | null}
 */
function formatAggProvenanceNote(provenance) {
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
 * @param {QueryRunnerState} state
 * @param {(docName: string) => void} onOpenResultDocument
 * @returns {TemplateResult}
 */
function renderResultsPane(state, onOpenResultDocument) {
  if (state.queryKind === "aggregations") {
    const aggResults = state.aggResults;
    const aggEntries = aggResults
      ? Object.entries(aggResults).filter(([k]) => Object.hasOwn(aggResults, k))
      : [];
    const provenanceNote = formatAggProvenanceNote(state.aggProvenance);

    return html`
      <section class="query-results-pane">
        <div class="query-results-header">
          <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
            <span>
              ${aggResults === null
                ? "Run an aggregation query to inspect results."
                : html`<strong>${aggEntries.length}</strong>
                    ${aggEntries.length === 1 ? "aggregation" : "aggregations"}`}
            </span>
            ${provenanceNote
              ? html`<span class="agg-provenance-note">${provenanceNote}</span>`
              : nothing}
          </div>
          ${state.aggReadTime
            ? html`
                <span class="mono" style="font-size: 11.5px; color: var(--text-muted);">
                  ${state.aggElapsedMs !== null
                    ? `${state.aggElapsedMs.toFixed(1)} ms · `
                    : ""}readTime: ${state.aggReadTime}
                </span>
              `
            : nothing}
        </div>

        <div class="query-results-list">
          ${aggResults === null
            ? html`<div class="col-empty">No aggregation query executed yet.</div>`
            : aggEntries.length === 0
              ? html`<div class="col-empty">No aggregation fields returned.</div>`
              : aggEntries.map(([alias, val]) => {
                  const valType = getValueType(val);
                  const typeLabel =
                    VALUE_TYPES.find((t) => t.key === valType)?.label ??
                    valType;
                  return html`
                    <div class="agg-result-card">
                      <div class="query-result-top" style="margin-bottom: 0;">
                        <span class="agg-result-alias mono">${alias}</span>
                        <div style="display: flex; align-items: center; gap: 8px;">
                          <span class="agg-result-value mono">
                            ${formatValuePreview(val)}
                          </span>
                          <span class="type-badge ${valType}">
                            ${typeLabel}
                          </span>
                        </div>
                      </div>
                    </div>
                  `;
                })}
        </div>
      </section>
    `;
  }

  return html`
    <section class="query-results-pane">
      <div class="query-results-header">
        <span>
          ${state.results === null
            ? "Run a query to inspect matching documents."
            : html`<strong>${state.results.length}</strong> matching
                ${state.results.length === 1 ? "document" : "documents"}`}
        </span>
        ${state.readTime
          ? html`
              <span class="mono" style="font-size: 11.5px; color: var(--text-muted);">
                ${state.elapsedMs !== null
                  ? `${state.elapsedMs.toFixed(1)} ms · `
                  : ""}readTime: ${state.readTime}
              </span>
            `
          : nothing}
      </div>

      <div class="query-results-list">
        ${state.results === null
          ? html`<div class="col-empty">No query executed yet.</div>`
          : state.results.length === 0
            ? html`<div class="col-empty">
                0 documents matched your query.
              </div>`
            : state.results.map((doc) => {
                const docName = doc.name ?? "";
                const shortPath = docName.replace(
                  /^projects\/[^/]+\/databases\/[^/]+\/documents\//,
                  ""
                );
                const entries = Object.entries(doc.fields ?? {});
                return html`
                  <div class="query-result-card">
                    <div class="query-result-top">
                      <span class="mono" style="font-weight: 600;">
                        ${shortPath || docName}
                      </span>
                      <button
                        type="button"
                        class="btn btn-sm btn-primary"
                        @click=${() => onOpenResultDocument(docName)}
                      >
                        Open in Explorer ›
                      </button>
                    </div>
                    <div class="query-result-fields">
                      ${entries.length === 0
                        ? html`<span style="font-size: 12px; color: var(--text-faint);">
                            (empty document)
                          </span>`
                        : entries.map(
                            ([k, v]) => html`
                              <span class="field-chip">
                                <strong>${k}</strong>: ${formatValuePreview(v)}
                              </span>
                            `
                          )}
                    </div>
                  </div>
                `;
              })}
      </div>
    </section>
  `;
}

/**
 * @param {QueryFilterRow} f
 * @param {number} idx
 * @param {QueryRunnerState} state
 * @param {() => void} onRerender
 * @returns {TemplateResult}
 */
function renderFilterRow(f, idx, state, onRerender) {
  const opMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
  const kind = opMeta?.kind ?? "field";

  return html`
    <div
      style="border: 1px solid var(--border); border-radius: 6px; padding: 8px; background: var(--bg-surface); display: flex; flex-direction: column; gap: 6px;"
    >
      <div class="query-row">
        <input
          type="text"
          class="input mono"
          style="flex: 1 1 130px;"
          aria-label=${`Filter ${idx + 1} field path`}
          placeholder="field path (e.g. active)"
          .value=${f.fieldPath}
          @input=${(/** @type {Event} */ e) => {
            f.fieldPath = /** @type {HTMLInputElement} */ (e.target).value;
            onRerender();
          }}
        />
        <select
          class="select"
          aria-label="Filter operator"
          .value=${f.op}
          @change=${(/** @type {Event} */ e) => {
            f.op = /** @type {HTMLSelectElement} */ (e.target).value;
            const nextMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
            if (nextMeta?.kind === "array" && !f.valueText.trim().startsWith("[")) {
              f.valueText = "[]";
            }
            onRerender();
          }}
        >
          ${FILTER_OPERATORS.map(
            (o) =>
              html`<option value=${o.op} ?selected=${f.op === o.op}>
                ${o.label}
              </option>`
          )}
        </select>
        <button
          type="button"
          class="btn btn-sm btn-ghost"
          title="Remove filter"
          aria-label=${`Remove filter ${idx + 1}`}
          @click=${() => {
            state.filters.splice(idx, 1);
            onRerender();
          }}
        >
          ✕
        </button>
      </div>

      ${kind === "unary"
        ? nothing
        : kind === "array"
          ? html`
              <input
                type="text"
                class="input mono"
                aria-label="Filter array values"
                placeholder='JSON array, e.g. ["alice", "bob"] or [{"integerValue": "1"}]'
                .value=${f.valueText}
                @input=${(/** @type {Event} */ e) => {
                  f.valueText = /** @type {HTMLInputElement} */ (
                    e.target
                  ).value;
                  onRerender();
                }}
              />
            `
          : html`
              <div class="query-row">
                <select
                  class="select"
                  aria-label="Filter value type"
                  .value=${f.valueType}
                  @change=${(/** @type {Event} */ e) => {
                    f.valueType = /** @type {ValueTypeKey} */ (
                      /** @type {HTMLSelectElement} */ (e.target).value
                    );
                    if (f.valueType === "booleanValue") {
                      f.valueText = "true";
                    }
                    onRerender();
                  }}
                >
                  ${SCALAR_FILTER_TYPES.map(
                    (t) =>
                      html`<option
                        value=${t.key}
                        ?selected=${f.valueType === t.key}
                      >
                        ${t.label}
                      </option>`
                  )}
                </select>

                ${f.valueType === "booleanValue"
                  ? html`
                      <select
                        class="select"
                        aria-label="Filter boolean value"
                        .value=${f.valueText === "false" ? "false" : "true"}
                        @change=${(/** @type {Event} */ e) => {
                          f.valueText = /** @type {HTMLSelectElement} */ (
                            e.target
                          ).value;
                          onRerender();
                        }}
                      >
                        <option value="true">true</option>
                        <option value="false">false</option>
                      </select>
                    `
                  : f.valueType === "geoPointValue"
                    ? html`
                        <input
                          type="text"
                          class="input mono"
                          style="max-width: 110px;"
                          aria-label="Filter latitude"
                          placeholder="lat"
                          .value=${f.latText}
                          @input=${(/** @type {Event} */ e) => {
                            f.latText = /** @type {HTMLInputElement} */ (
                              e.target
                            ).value;
                            onRerender();
                          }}
                        />
                        <input
                          type="text"
                          class="input mono"
                          style="max-width: 110px;"
                          aria-label="Filter longitude"
                          placeholder="lng"
                          .value=${f.lngText}
                          @input=${(/** @type {Event} */ e) => {
                            f.lngText = /** @type {HTMLInputElement} */ (
                              e.target
                            ).value;
                            onRerender();
                          }}
                        />
                      `
                    : html`
                        <input
                          type="text"
                          class="input mono"
                          style="flex: 1 1 140px;"
                          aria-label="Filter comparison value"
                          placeholder="comparison value"
                          .value=${f.valueText}
                          @input=${(/** @type {Event} */ e) => {
                            f.valueText = /** @type {HTMLInputElement} */ (
                              e.target
                            ).value;
                            onRerender();
                          }}
                        />
                      `}
              </div>
            `}
    </div>
  `;
}
