// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  VALUE_TYPES,
  formatValuePreview,
  getValueType,
  stringifyProtoJson,
} from "../values.js";
import { reconcileComboboxUi, renderCombobox } from "./combobox.js";
import {
  ENTERPRISE_MAX_AGGREGATIONS,
  FILTER_OPERATORS,
  SCALAR_FILTER_TYPES,
  STANDARD_MAX_AGGREGATIONS,
  applyFilterAutoType,
  buildAggregationQueryFromVisual,
  buildStructuredQueryFromVisual,
  createAggregationRow,
  createFilterRow,
  createOrderRow,
  createQueryRunnerState,
  extractAggProvenance,
  formatAggProvenanceNote,
  isUntouchedDefaultQueryState,
  parseRawQueryPayload,
  transitionFilterOperator,
} from "./query-model.js";
import {
  ensureCollectionGroups,
  ensureFieldSuggestions,
  ensureParentSubpathSuggestions,
  ensureSubcollections,
  formatSuggestionFooter,
  getCollectionIdSuggestions,
  getFieldScopeEntry,
  getParentSubpathSuggestions,
  parseNormalizedParentSubpath,
} from "./query-suggestions.js";

/** @typedef {import('../values.js').ValueTypeKey} ValueTypeKey */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */
/** @typedef {import('./combobox.js').ComboboxItem} ComboboxItem */
/** @typedef {import('./query-model.js').QueryFilterRow} QueryFilterRow */
/** @typedef {import('./query-model.js').QueryOrderRow} QueryOrderRow */
/** @typedef {import('./query-model.js').QueryAggregationRow} QueryAggregationRow */
/** @typedef {import('./query-model.js').QueryRunnerState} QueryRunnerState */
/** @typedef {import('./query-suggestions.js').FieldScopeEntry} FieldScopeEntry */
/** @typedef {import('./query-suggestions.js').QuerySuggestionStore} QuerySuggestionStore */
/** @typedef {import('./query-suggestions.js').SuggestionContext} SuggestionContext */

export {
  buildAggregationQueryFromVisual,
  buildStructuredQueryFromVisual,
  createFilterRow,
  createQueryRunnerState,
  extractAggProvenance,
  parseRawQueryPayload,
  transitionFilterOperator,
};

/**
 * @typedef {{
 *   store: QuerySuggestionStore,
 *   ctx: SuggestionContext,
 *   fieldScope: { parentSubpath: string, collectionId: string, allDescendants: boolean },
 *   fieldEntry: FieldScopeEntry | null,
 *   renderedIds: Set<string>,
 * }} QueryComboboxContext
 */

/**
 * Renders either a plain `<input>` or an autosuggestion-backed field combobox.
 * @param {{
 *   id: string,
 *   value: string,
 *   placeholder: string,
 *   ariaLabel: string,
 *   inputStyle: string,
 *   disabled?: boolean,
 *   useAggItems?: boolean,
 *   comboCtx: QueryComboboxContext | null,
 *   onChangeValue: (val: string) => void,
 *   onSelectItem?: (item: ComboboxItem) => void,
 *   onRerender: () => void,
 * }} opts
 * @returns {TemplateResult}
 */
function renderFieldCombobox(opts) {
  const {
    id,
    value,
    placeholder,
    ariaLabel,
    inputStyle,
    disabled = false,
    useAggItems = false,
    comboCtx,
    onChangeValue,
    onSelectItem,
    onRerender,
  } = opts;
  if (disabled || !comboCtx) {
    return html`
      <input
        type="text"
        class="input mono"
        style=${inputStyle}
        aria-label=${ariaLabel}
        placeholder=${placeholder}
        ?disabled=${disabled}
        .value=${value}
        @input=${(/** @type {Event} */ e) => {
          onChangeValue(/** @type {HTMLInputElement} */ (e.target).value);
          onRerender();
        }}
      />
    `;
  }
  comboCtx.renderedIds.add(id);
  return renderCombobox({
    id,
    value,
    placeholder,
    ariaLabel,
    inputStyle,
    items: useAggItems
      ? comboCtx.fieldEntry?.aggItems ?? []
      : comboCtx.fieldEntry?.items ?? [],
    uiState: comboCtx.store.comboboxUi,
    footerText: formatSuggestionFooter(comboCtx.fieldEntry),
    loading: comboCtx.fieldEntry?.loading ?? false,
    topObstructionSelector: "#topbar",
    clipContainerSelector: ".query-builder-pane",
    onInput: onChangeValue,
    onSelect: (item) => {
      if (onSelectItem) {
        onSelectItem(item);
      } else {
        onChangeValue(item.value);
      }
    },
    onOpen: () => {
      void ensureFieldSuggestions(
        comboCtx.store,
        comboCtx.ctx,
        comboCtx.fieldScope
      );
    },
    onRefresh: () => {
      void ensureFieldSuggestions(
        comboCtx.store,
        comboCtx.ctx,
        comboCtx.fieldScope,
        true
      );
    },
    onRerender,
  });
}

/**
 * Renders the Structured Query Runner workspace.
 *
 * @param {{
 *   projectId: string,
 *   databaseId: string,
 *   knownCollections: string[],
 *   enterprise?: boolean,
 *   suggestions?: QuerySuggestionStore,
 *   state: QueryRunnerState,
 *   onRerender: () => void,
 *   onExecuteQuery: () => void,
 *   onAbortQueryRun?: () => void,
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
    suggestions,
    state,
    onRerender,
    onExecuteQuery,
    onAbortQueryRun,
    onOpenResultDocument,
  } = props;
  const maxAggregations = enterprise
    ? ENTERPRISE_MAX_AGGREGATIONS
    : STANDARD_MAX_AGGREGATIONS;
  const useCombobox = Boolean(suggestions) && state.mode === "visual";
  /** @type {Set<string>} */
  const renderedIds = new Set();
  const rootPath =
    projectId && databaseId
      ? `projects/${projectId}/databases/${databaseId}/documents`
      : "";
  /** @type {SuggestionContext} */
  const suggestionCtx = {
    rootPath,
    rootCollections: knownCollections,
    onRerender,
  };
  const parsedParent = parseNormalizedParentSubpath(state.parentSubpath);
  const fieldScope = {
    parentSubpath: state.parentSubpath,
    collectionId: state.collectionId,
    allDescendants: state.allDescendants,
  };
  const fieldEntry =
    useCombobox && suggestions
      ? getFieldScopeEntry(
          suggestions,
          state.parentSubpath,
          state.collectionId,
          state.allDescendants
        )
      : null;
  /** @type {QueryComboboxContext | null} */
  const comboCtx =
    useCombobox && suggestions
      ? {
          store: suggestions,
          ctx: suggestionCtx,
          fieldScope,
          fieldEntry,
          renderedIds,
        }
      : null;

  const switchQueryKind = (
    /** @type {'documents' | 'aggregations'} */ nextKind
  ) => {
    if (state.queryKind === nextKind) return;
    onAbortQueryRun?.();
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
      if (isUntouchedDefaultQueryState(state)) {
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
    state.aggregations.push(createAggregationRow());
    onRerender();
  };

  const addFilter = () => {
    state.filters.push(createFilterRow());
    onRerender();
  };

  const addOrder = () => {
    state.orders.push(createOrderRow());
    onRerender();
  };

  const isAgg = state.queryKind === "aggregations";
  const rawBufferValue = isAgg ? state.rawAggregationText : state.rawQueryText;

  const parentSubpathControl = (() => {
    if (!comboCtx) {
      return html`
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
      `;
    }
    comboCtx.renderedIds.add("query-parent-subpath");
    const parentState = getParentSubpathSuggestions(
      comboCtx.store,
      knownCollections,
      rootPath,
      state.parentSubpath
    );
    return renderCombobox({
      id: "query-parent-subpath",
      value: state.parentSubpath,
      placeholder: "e.g. rooms/room-1",
      ariaLabel: "Parent Document Path",
      items: parentState.items,
      uiState: comboCtx.store.comboboxUi,
      footerText: parentState.footerText,
      loading: parentState.loading,
      keepOpenOnSelect: (item) => item.value.endsWith("/"),
      topObstructionSelector: "#topbar",
      clipContainerSelector: ".query-builder-pane",
      onInput: (val) => {
        state.parentSubpath = val;
        if (val.includes("/")) {
          void ensureParentSubpathSuggestions(
            comboCtx.store,
            comboCtx.ctx,
            val
          );
        }
      },
      onSelect: (item) => {
        state.parentSubpath = item.value;
        if (item.value.endsWith("/")) {
          void ensureParentSubpathSuggestions(
            comboCtx.store,
            comboCtx.ctx,
            item.value
          );
        }
      },
      onOpen: () => {
        void ensureParentSubpathSuggestions(
          comboCtx.store,
          comboCtx.ctx,
          state.parentSubpath
        );
      },
      onRefresh: () => {
        void ensureParentSubpathSuggestions(
          comboCtx.store,
          comboCtx.ctx,
          state.parentSubpath,
          true
        );
      },
      onRerender,
    });
  })();

  const collectionIdControl = (() => {
    if (!comboCtx) {
      return html`
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
      `;
    }
    comboCtx.renderedIds.add("query-collection-id");
    const colState = getCollectionIdSuggestions(
      comboCtx.store,
      knownCollections,
      state.parentSubpath,
      state.allDescendants
    );
    return renderCombobox({
      id: "query-collection-id",
      value: state.collectionId,
      placeholder: "Collection ID (e.g. users)",
      ariaLabel: "Collection ID",
      inputStyle: "flex: 1 1 180px;",
      items: colState.items,
      uiState: comboCtx.store.comboboxUi,
      footerText: colState.footerText,
      loading: colState.loading,
      topObstructionSelector: "#topbar",
      clipContainerSelector: ".query-builder-pane",
      onInput: (val) => {
        state.collectionId = val;
      },
      onSelect: (item) => {
        state.collectionId = item.value;
      },
      onOpen: () => {
        if (state.allDescendants) {
          void ensureCollectionGroups(
            comboCtx.store,
            comboCtx.ctx,
            state.parentSubpath
          );
        } else if (parsedParent.valid && parsedParent.normParent !== "") {
          void ensureSubcollections(
            comboCtx.store,
            comboCtx.ctx,
            state.parentSubpath
          );
        }
      },
      onRefresh: () => {
        if (state.allDescendants) {
          void ensureCollectionGroups(
            comboCtx.store,
            comboCtx.ctx,
            state.parentSubpath,
            true
          );
        } else if (parsedParent.valid && parsedParent.normParent !== "") {
          void ensureSubcollections(
            comboCtx.store,
            comboCtx.ctx,
            state.parentSubpath,
            true
          );
        }
      },
      onRerender,
    });
  })();

  const viewTemplate = html`
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
          ${parentSubpathControl}
        </div>

        ${state.error ? html`<div class="error-banner">${state.error}</div>` : nothing}

        ${state.mode === "visual"
          ? html`
              <div class="query-section">
                <div class="query-section-title">
                  <label for="query-collection-id">Collection Source</label>
                </div>
                <div class="query-row">
                  ${collectionIdControl}
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
                    onRerender,
                    comboCtx
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
                      renderFilterRow(f, idx, state, onRerender, comboCtx)
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
                  : state.orders.map((o, idx) => {
                      const orderFieldControl = renderFieldCombobox({
                        id: `query-order-field-${o.id}`,
                        value: o.fieldPath,
                        placeholder: "field path (e.g. age)",
                        ariaLabel: `Sort ${idx + 1} field path`,
                        inputStyle: "flex: 1 1 150px;",
                        comboCtx,
                        onChangeValue: (val) => {
                          o.fieldPath = val;
                        },
                        onRerender,
                      });
                      return html`
                        <div class="query-row">
                          ${orderFieldControl}
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
                      `;
                    })}
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

  if (suggestions) {
    reconcileComboboxUi(suggestions.comboboxUi, renderedIds);
  }
  return viewTemplate;
}

/**
 * @param {QueryRunnerState} state
 * @param {number} maxAggregations
 * @param {() => void} addAggregation
 * @param {() => void} onRerender
 * @param {QueryComboboxContext | null} comboCtx
 * @returns {TemplateResult}
 */
function renderAggregationsSection(
  state,
  maxAggregations,
  addAggregation,
  onRerender,
  comboCtx
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
        const aggFieldControl = renderFieldCombobox({
          id: `query-agg-field-${row.id}`,
          value: row.fieldPath,
          placeholder: isCount ? "—" : "field path (e.g. total)",
          ariaLabel: `Aggregation ${idx + 1} field path`,
          inputStyle: "flex: 1 1 120px;",
          disabled: isCount,
          useAggItems: true,
          comboCtx,
          onChangeValue: (val) => {
            row.fieldPath = val;
          },
          onRerender,
        });
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

            ${aggFieldControl}

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
 * @param {QueryComboboxContext | null} comboCtx
 * @returns {TemplateResult}
 */
function renderFilterRow(f, idx, state, onRerender, comboCtx) {
  const opMeta = FILTER_OPERATORS.find((o) => o.op === f.op);
  const kind = opMeta?.kind ?? "field";

  const fieldControl = renderFieldCombobox({
    id: `query-filter-field-${f.id}`,
    value: f.fieldPath,
    placeholder: "field path (e.g. active)",
    ariaLabel: `Filter ${idx + 1} field path`,
    inputStyle: "flex: 1 1 130px;",
    comboCtx,
    onChangeValue: (val) => {
      f.fieldPath = val;
    },
    onSelectItem: (item) => {
      f.fieldPath = item.value;
      applyFilterAutoType(f, comboCtx?.fieldEntry ?? null);
    },
    onRerender,
  });

  return html`
    <div
      style="border: 1px solid var(--border); border-radius: 6px; padding: 8px; background: var(--bg-surface); display: flex; flex-direction: column; gap: 6px;"
    >
      <div class="query-row">
        ${fieldControl}
        <select
          class="select"
          aria-label="Filter operator"
          .value=${f.op}
          @change=${(/** @type {Event} */ e) => {
            const nextOp = /** @type {HTMLSelectElement} */ (e.target).value;
            transitionFilterOperator(f, nextOp, comboCtx?.fieldEntry ?? null);
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
                  f.valueUserEdited = true;
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
                    f.valueTypeManuallySet = true;
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
                          f.valueUserEdited = true;
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
                            f.valueUserEdited = true;
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
                            f.valueUserEdited = true;
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
                            f.valueUserEdited = true;
                            onRerender();
                          }}
                        />
                      `}
              </div>
            `}
    </div>
  `;
}
