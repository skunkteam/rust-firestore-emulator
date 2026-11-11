// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  VALUE_TYPES,
  formatValuePreview,
  normalizeReference,
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
 * Supported operators matching crates/emulator-database/src/database/query/filter.rs.
 * (Omits ARRAY_CONTAINS_ANY and NOT_IN which are unimplemented in filter.rs.)
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
  { op: "IN", label: "in (JSON array)", kind: "in" },
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
 *   mode: 'visual' | 'raw',
 *   parentSubpath: string,
 *   collectionId: string,
 *   allDescendants: boolean,
 *   compositeOp: 'AND' | 'OR',
 *   filters: QueryFilterRow[],
 *   orders: QueryOrderRow[],
 *   limitText: string,
 *   rawQueryText: string,
 *   error: string | null,
 *   running: boolean,
 *   elapsedMs: number | null,
 *   readTime: string | null,
 *   results: FirestoreDocument[] | null,
 * }} QueryRunnerState
 */

let nextRowId = 1;

/**
 * Creates initial state for the Structured Query Runner.
 * @param {string} [collectionId]
 * @param {string} [parentSubpath]
 * @returns {QueryRunnerState}
 */
export function createQueryRunnerState(collectionId = "", parentSubpath = "") {
  const initialQuery = {
    from: [{ collectionId: collectionId || "users", allDescendants: false }],
    limit: 50,
  };
  return {
    mode: "visual",
    parentSubpath,
    collectionId,
    allDescendants: false,
    compositeOp: "AND",
    filters: [],
    orders: [],
    limitText: "50",
    rawQueryText: JSON.stringify(initialQuery, null, 2),
    error: null,
    running: false,
    elapsedMs: null,
    readTime: null,
    results: null,
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
    } else if (opMeta.kind === "in") {
      /** @type {unknown} */
      let parsedArr;
      try {
        parsedArr = JSON.parse(f.valueText);
      } catch {
        return {
          valid: false,
          error: `Filter "${fp} IN": enter a JSON array of ProtoJSON values or primitives, e.g. ["alice", "bob"]`,
        };
      }
      if (!Array.isArray(parsedArr)) {
        return { valid: false, error: `Filter "${fp} IN" requires a JSON array` };
      }
      /** @type {FirestoreValue[]} */
      const values = [];
      for (const item of parsedArr) {
        if (item === null) values.push({ nullValue: null });
        else if (typeof item === "boolean") values.push({ booleanValue: item });
        else if (typeof item === "string") values.push({ stringValue: item });
        else if (typeof item === "number") {
          if (Number.isInteger(item)) {
            values.push({ integerValue: String(item) });
          } else {
            values.push({ doubleValue: item });
          }
        } else if (item && typeof item === "object") {
          values.push(/** @type {FirestoreValue} */ (item));
        } else {
          return { valid: false, error: `Invalid item in IN array for "${fp}"` };
        }
      }
      builtFilters.push({
        fieldFilter: {
          field: { fieldPath: fp },
          op: "IN",
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
    state,
    onRerender,
    onExecuteQuery,
    onOpenResultDocument,
  } = props;

  const switchTab = (/** @type {'visual' | 'raw'} */ nextMode) => {
    if (state.mode === nextMode) return;
    if (nextMode === "raw") {
      const built = buildStructuredQueryFromVisual(state, projectId, databaseId);
      if (built.valid && built.query) {
        state.rawQueryText = JSON.stringify(built.query, null, 2);
        state.error = null;
      }
    }
    state.mode = nextMode;
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

  return html`
    <div class="query-workspace">
      <aside class="query-builder-pane">
        <div style="display: flex; align-items: center; justify-content: space-between; gap: 8px;">
          <div class="segmented">
            <button
              type="button"
              class=${state.mode === "visual" ? "active" : ""}
              @click=${() => switchTab("visual")}
            >
              Visual Builder
            </button>
            <button
              type="button"
              class=${state.mode === "raw" ? "active" : ""}
              @click=${() => switchTab("raw")}
            >
              Raw StructuredQuery JSON
            </button>
          </div>

          <button
            type="button"
            class="btn btn-primary"
            ?disabled=${state.running}
            @click=${onExecuteQuery}
          >
            ${state.running ? "Running..." : "▶ Run Query"}
          </button>
        </div>

        <div class="form-field">
          <label>Parent Document Path (optional — leave empty for database root)</label>
          <input
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
                  <span>Collection Source</span>
                </div>
                <div class="query-row">
                  <input
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
                <label style="display: flex; align-items: center; gap: 6px; font-size: 12.5px;">
                  <input
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

              <div class="query-section">
                <div class="query-section-title">
                  <span>Where Filters</span>
                  <div style="display: flex; align-items: center; gap: 6px;">
                    ${state.filters.length > 1
                      ? html`
                          <select
                            class="select"
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
                <div class="query-section-title"><span>Limit</span></div>
                <input
                  type="number"
                  min="1"
                  class="input mono"
                  style="max-width: 140px;"
                  placeholder="No limit"
                  .value=${state.limitText}
                  @input=${(/** @type {Event} */ e) => {
                    state.limitText = /** @type {HTMLInputElement} */ (
                      e.target
                    ).value;
                    onRerender();
                  }}
                />
              </div>
            `
          : html`
              <div class="raw-json-wrap">
                <textarea
                  class="textarea raw-json-textarea"
                  spellcheck="false"
                  .value=${state.rawQueryText}
                  @input=${(/** @type {Event} */ e) => {
                    state.rawQueryText = /** @type {HTMLTextAreaElement} */ (
                      e.target
                    ).value;
                    onRerender();
                  }}
                ></textarea>
              </div>
            `}
      </aside>

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
    </div>
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
          placeholder="field path (e.g. active)"
          .value=${f.fieldPath}
          @input=${(/** @type {Event} */ e) => {
            f.fieldPath = /** @type {HTMLInputElement} */ (e.target).value;
            onRerender();
          }}
        />
        <select
          class="select"
          .value=${f.op}
          @change=${(/** @type {Event} */ e) => {
            f.op = /** @type {HTMLSelectElement} */ (e.target).value;
            if (f.op === "IN" && !f.valueText.trim().startsWith("[")) {
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
        : kind === "in"
          ? html`
              <input
                type="text"
                class="input mono"
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
