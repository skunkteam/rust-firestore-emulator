// @ts-check

import { html, nothing } from "../lit-html.js";

/** @typedef {import('../api.js').ContendedDocumentSnapshot} ContendedDocumentSnapshot */
/** @typedef {import('../api.js').ContentionEvent} ContentionEvent */
/** @typedef {import('../api.js').ContentionSnapshot} ContentionSnapshot */
/** @typedef {import('../api.js').ContentionSummary} ContentionSummary */
/** @typedef {import('../api.js').DatabaseContentionSnapshot} DatabaseContentionSnapshot */
/** @typedef {import('../api.js').ReaderSnapshot} ReaderSnapshot */
/** @typedef {import('../api.js').WaiterSnapshot} WaiterSnapshot */
/** @typedef {import('../api.js').WriterSnapshot} WriterSnapshot */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */

/**
 * @typedef {{
 *   scope: 'active' | 'all',
 *   snapshot: ContentionSnapshot | null,
 *   loading: boolean,
 *   clearing: boolean,
 *   error: string | null,
 *   lastFetchedAtMs: number | null,
 *   lastFetchedIso: string | null,
 * }} ContentionState
 */

/**
 * Creates a fresh Contention visualizer UI state object.
 * @returns {ContentionState}
 */
export function createContentionState() {
  return {
    scope: "active",
    snapshot: null,
    loading: false,
    clearing: false,
    error: null,
    lastFetchedAtMs: null,
    lastFetchedIso: null,
  };
}

/**
 * Formats millisecond durations concisely for wait queue and timeout display.
 * @param {number} ms
 * @returns {string}
 */
export function formatWaitDuration(ms) {
  const safe = Math.max(0, Number.isFinite(ms) ? ms : 0);
  if (safe < 1000) {
    return `${Math.round(safe)}ms`;
  }
  if (safe < 60_000) {
    return `${(safe / 1000).toFixed(1)}s`;
  }
  const totalSec = Math.floor(safe / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return `${mins}m ${secs}s`;
}

/**
 * Returns the database contention snapshots visible for the selected scope.
 * @param {ContentionState} contention
 * @param {string | null} activeRoot
 * @returns {DatabaseContentionSnapshot[]}
 */
export function getDisplayedContentionDatabases(contention, activeRoot) {
  const dbs = contention.snapshot?.databases ?? [];
  if (contention.scope === "active" && activeRoot) {
    return dbs.filter((d) => d.database === activeRoot);
  }
  return dbs;
}

/**
 * Returns true if any displayed contended document currently has queued waiters.
 * @param {DatabaseContentionSnapshot[]} databases
 * @returns {boolean}
 */
export function hasContentionWaiters(databases) {
  return databases.some((db) =>
    db.contendedDocuments.some((doc) => doc.waiters.length > 0)
  );
}

/**
 * Updates only `[data-wait-base-ms]` DOM nodes with elapsed wait duration since snapshot fetch
 * without triggering a full `lit-html` template re-render.
 * Supports empty `.wait-elapsed` elements (no `lit-html` `ChildPart` comment anchors inside).
 * @param {ParentNode | null | undefined} container
 * @param {ContentionState | number | null} fetchedOrState
 * @param {number} [nowMs]
 */
export function updateWaiterElapsedDom(
  container,
  fetchedOrState,
  nowMs = Date.now()
) {
  if (!container || typeof container.querySelectorAll !== "function") return;
  const fetchedAtMs =
    typeof fetchedOrState === "number" || fetchedOrState === null
      ? fetchedOrState
      : fetchedOrState.lastFetchedAtMs;
  if (fetchedAtMs === null) return;
  const elapsed = Math.max(0, nowMs - fetchedAtMs);
  const nodes = container.querySelectorAll("[data-wait-base-ms]");
  for (const el of nodes) {
    const baseRaw = el.getAttribute?.("data-wait-base-ms");
    if (baseRaw === null || baseRaw === undefined) continue;
    const baseMs = Number(baseRaw);
    if (!Number.isFinite(baseMs)) continue;
    const totalMs = baseMs + elapsed;
    const label = formatWaitDuration(totalMs);
    el.setAttribute?.("data-wait-ms", String(totalMs));
    el.setAttribute?.("data-wait-label", label);
    el.textContent = label;
  }
}

/**
 * Aggregates summary counters across a list of database snapshots.
 * @param {DatabaseContentionSnapshot[]} databases
 * @returns {ContentionSummary}
 */
function aggregateSummary(databases) {
  const summary = {
    woundsTotal: 0,
    timeoutsTotal: 0,
    upgradeAbortsTotal: 0,
    writeBatchRetriesTotal: 0,
  };
  for (const db of databases) {
    summary.woundsTotal += db.summary.woundsTotal;
    summary.timeoutsTotal += db.summary.timeoutsTotal;
    summary.upgradeAbortsTotal += db.summary.upgradeAbortsTotal;
    summary.writeBatchRetriesTotal += db.summary.writeBatchRetriesTotal;
  }
  return summary;
}

/**
 * Renders the Contention Visualizer workspace view.
 *
 * @param {{
 *   projectId: string,
 *   databaseId: string,
 *   state: ContentionState,
 *   sseStatus: 'idle' | 'connecting' | 'live' | 'error',
 *   ssePaused: boolean,
 *   onChangeScope: (scope: 'active' | 'all') => unknown,
 *   onRefresh: () => unknown,
 *   onClearHistory: () => unknown,
 *   onOpenDocument: (docName: string) => unknown,
 * }} props
 * @returns {TemplateResult}
 */
export function renderContention(props) {
  const {
    projectId,
    databaseId,
    state,
    sseStatus,
    ssePaused,
    onChangeScope,
    onRefresh,
    onClearHistory,
    onOpenDocument,
  } = props;

  const activeRoot = `projects/${projectId}/databases/${databaseId}/documents`;
  const displayedDbs = getDisplayedContentionDatabases(state, activeRoot);
  const totals = aggregateSummary(displayedDbs);
  const elapsedSinceFetch =
    state.lastFetchedAtMs !== null
      ? Math.max(0, Date.now() - state.lastFetchedAtMs)
      : 0;

  const scopeLabel =
    state.scope === "active"
      ? `${projectId}/${databaseId}`
      : `All databases (${displayedDbs.length})`;

  const clearAriaLabel =
    state.scope === "active"
      ? `Clear history (${projectId}/${databaseId})`
      : "Clear history (all databases)";

  const clearButtonTitle =
    state.scope === "active"
      ? `Clear contention counters and event history for ${activeRoot} (preserves active locks)`
      : "Clear contention counters and event history for all databases (preserves active locks)";

  const streamLabel = ssePaused
    ? "SSE paused"
    : sseStatus === "live"
      ? "Stream live"
      : sseStatus === "connecting"
        ? "Stream connecting"
        : sseStatus === "error"
          ? "Stream reconnecting"
          : "Stream idle";

  const streamDotClass = ssePaused
    ? "paused"
    : sseStatus === "live"
      ? "live"
      : sseStatus === "error"
        ? "error"
        : "idle";

  return html`
    <div class="contention-workspace">
      <div class="contention-toolbar">
        <div class="contention-toolbar-left">
          <div class="segmented" role="group" aria-label="Contention scope">
            <button
              type="button"
              class=${state.scope === "active" ? "active" : ""}
              aria-pressed=${String(state.scope === "active")}
              @click=${() => onChangeScope("active")}
            >
              Active DB
            </button>
            <button
              type="button"
              class=${state.scope === "all" ? "active" : ""}
              aria-pressed=${String(state.scope === "all")}
              @click=${() => onChangeScope("all")}
            >
              All Databases
            </button>
          </div>
          <span class="contention-scope-label mono" title=${state.scope === "active" ? activeRoot : "All active emulator databases"}>
            Scope: ${scopeLabel}
          </span>
          <span class="contention-stream-pill">
            <span class="sse-dot ${streamDotClass}"></span>
            <span>${streamLabel}</span>
          </span>
          ${state.lastFetchedIso
            ? html`<span class="contention-updated mono" title=${state.lastFetchedIso}>
                Updated ${state.lastFetchedIso}
              </span>`
            : nothing}
        </div>

        <div class="contention-toolbar-right">
          <button
            type="button"
            class="btn btn-sm"
            ?disabled=${state.loading}
            @click=${onRefresh}
          >
            ${state.loading ? "Refreshing..." : "Refresh"}
          </button>
          <button
            type="button"
            class="btn btn-sm btn-danger"
            ?disabled=${state.clearing}
            title=${clearButtonTitle}
            aria-label=${clearAriaLabel}
            @click=${onClearHistory}
          >
            ${state.clearing ? "Clearing..." : "Clear history"}
          </button>
        </div>
      </div>

      ${state.error
        ? html`<div class="error-banner" role="alert">${state.error}</div>`
        : nothing}

      <div class="contention-summary-grid" aria-label="Contention summary">
        ${renderSummaryCard("Wounds", totals.woundsTotal, "wound")}
        ${renderSummaryCard("Timeouts", totals.timeoutsTotal, "timeout")}
        ${renderSummaryCard("Upgrade Aborts", totals.upgradeAbortsTotal, "upgrade")}
        ${renderSummaryCard(
          "WriteBatch Retries",
          totals.writeBatchRetriesTotal,
          "retry"
        )}
      </div>

      <div class="contention-body">
        ${state.loading && state.snapshot === null
          ? html`<div class="contention-empty-panel">
              Loading contention snapshot...
            </div>`
          : displayedDbs.length === 0
            ? renderEmptyDatabaseSnapshot(state.scope, activeRoot)
            : displayedDbs.map((db) =>
                renderDatabaseSection(
                  db,
                  state.scope === "all",
                  elapsedSinceFetch,
                  onOpenDocument
                )
              )}
      </div>
    </div>
  `;
}

/**
 * @param {string} label
 * @param {number} value
 * @param {string} tone
 * @returns {TemplateResult}
 */
function renderSummaryCard(label, value, tone) {
  return html`
    <div class="contention-stat-card tone-${tone}">
      <span class="contention-stat-label">${label}</span>
      <strong class="contention-stat-value mono">${value}</strong>
    </div>
  `;
}

/**
 * @param {'active' | 'all'} scope
 * @param {string} activeRoot
 * @returns {TemplateResult}
 */
function renderEmptyDatabaseSnapshot(scope, activeRoot) {
  return html`
    <div class="contention-panels">
      <section class="contention-panel">
        <div class="contention-panel-header">
          <h3>Contended Documents (0)</h3>
        </div>
        <div class="contention-empty-panel">
          ${scope === "active"
            ? html`No active lock contention in <span class="mono">${activeRoot}</span>.`
            : "No active lock contention across any database."}
        </div>
      </section>
      <section class="contention-panel">
        <div class="contention-panel-header">
          <h3>Recent Events (0)</h3>
        </div>
        <div class="contention-empty-panel">
          No contention events recorded.
        </div>
      </section>
    </div>
  `;
}

/**
 * @param {DatabaseContentionSnapshot} db
 * @param {boolean} showDatabaseHeader
 * @param {number} elapsedSinceFetch
 * @param {(docName: string) => unknown} onOpenDocument
 * @returns {TemplateResult}
 */
function renderDatabaseSection(
  db,
  showDatabaseHeader,
  elapsedSinceFetch,
  onOpenDocument
) {
  const hasDocTruncation =
    db.truncated || db.totalContendedDocuments > db.contendedDocuments.length;
  const hasParticipantTruncation = db.contendedDocuments.some(
    (doc) =>
      doc.readers.length < doc.readerCount ||
      doc.waiters.length < doc.waiterCount
  );
  /** @type {string[]} */
  const truncationParts = [];
  if (hasDocTruncation) {
    truncationParts.push(
      `showing ${db.contendedDocuments.length} of ${db.totalContendedDocuments} contended documents`
    );
  }
  if (hasParticipantTruncation) {
    truncationParts.push(
      "participant lists capped at 64 readers / waiters per document"
    );
  }

  return html`
    <section class="contention-db-section">
      ${showDatabaseHeader
        ? html`
            <div class="contention-db-header">
              <h2 class="mono">${db.database}</h2>
              <div class="contention-db-mini-stats mono">
                <span>wounds: ${db.summary.woundsTotal}</span>
                <span>timeouts: ${db.summary.timeoutsTotal}</span>
                <span>upgrades: ${db.summary.upgradeAbortsTotal}</span>
                <span>retries: ${db.summary.writeBatchRetriesTotal}</span>
              </div>
            </div>
          `
        : nothing}

      ${truncationParts.length > 0
        ? html`
            <div class="conflict-banner contention-truncated-banner" role="status">
              <span>Snapshot truncated: ${truncationParts.join("; ")}.</span>
            </div>
          `
        : nothing}

      <div class="contention-panels">
        <section class="contention-panel">
          <div class="contention-panel-header">
            <h3>Contended Documents (${db.totalContendedDocuments})</h3>
          </div>
          ${db.contendedDocuments.length === 0
            ? html`<div class="contention-empty-panel">
                No active lock contention.
              </div>`
            : html`
                <div class="contention-doc-list">
                  ${db.contendedDocuments.map((doc) =>
                    renderContendedDocCard(
                      doc,
                      elapsedSinceFetch,
                      onOpenDocument
                    )
                  )}
                </div>
              `}
        </section>

        <section class="contention-panel">
          <div class="contention-panel-header">
            <h3>Recent Events (${db.recentEvents.length})</h3>
          </div>
          ${db.recentEvents.length === 0
            ? html`<div class="contention-empty-panel">
                No contention events recorded.
              </div>`
            : html`
                <ul class="contention-event-list">
                  ${db.recentEvents.map((ev) =>
                    renderEventRow(ev, onOpenDocument)
                  )}
                </ul>
              `}
        </section>
      </div>
    </section>
  `;
}

/**
 * @param {ContendedDocumentSnapshot} doc
 * @param {number} elapsedSinceFetch
 * @param {(docName: string) => unknown} onOpenDocument
 * @returns {TemplateResult}
 */
function renderContendedDocCard(doc, elapsedSinceFetch, onOpenDocument) {
  const docName = doc.document;
  return html`
    <article class="contention-doc-card">
      <div class="contention-doc-header">
        <button
          type="button"
          class="contention-doc-link mono"
          title=${`Open ${docName} in Explorer`}
          aria-label=${`Open document ${docName} in Explorer`}
          @click=${() => onOpenDocument(docName)}
        >
          ${docName}
        </button>
        <div class="contention-doc-counts mono">
          <span>readers: ${doc.readerCount}</span>
          <span>waiters: ${doc.waiterCount}</span>
        </div>
      </div>

      <div class="contention-lock-group">
        <div class="contention-group-title">Writer</div>
        ${doc.writer ? renderWriterRow(doc.writer) : html`<div class="contention-none">None</div>`}
      </div>

      <div class="contention-lock-group">
        <div class="contention-group-title">
          Readers (${doc.readerCount})${doc.readers.length < doc.readerCount
            ? ` — showing ${doc.readers.length}`
            : ""}
        </div>
        ${doc.readers.length === 0
          ? html`<div class="contention-none">None</div>`
          : html`
              <ul class="contention-participant-list">
                ${doc.readers.map((r) => renderReaderRow(r))}
              </ul>
            `}
      </div>

      <div class="contention-lock-group">
        <div class="contention-group-title">
          Waiters (${doc.waiterCount})${doc.waiters.length < doc.waiterCount
            ? ` — showing ${doc.waiters.length}`
            : ""}
        </div>
        ${doc.waiters.length === 0
          ? html`<div class="contention-none">None</div>`
          : html`
              <ol class="contention-participant-list">
                ${doc.waiters.map((w, idx) =>
                  renderWaiterRow(w, idx, elapsedSinceFetch)
                )}
              </ol>
            `}
      </div>
    </article>
  `;
}

/**
 * @param {WriterSnapshot} w
 * @returns {TemplateResult}
 */
function renderWriterRow(w) {
  return html`
    <div class="contention-participant mono">
      <span class="contention-pill mode-write">WRITE</span>
      <span>#${w.sessionId}</span>
      <span class="contention-kind-tag">${w.kind}</span>
      <span class="contention-state-tag">${w.state}</span>
      ${w.seq !== null ? html`<span>seq ${w.seq}</span>` : nothing}
      ${w.txnId !== null ? html`<span>txn ${w.txnId}</span>` : nothing}
      <span class="contention-flag">
        ${w.woundable ? "woundable" : "unwoundable"}
      </span>
    </div>
  `;
}

/**
 * @param {ReaderSnapshot} r
 * @returns {TemplateResult}
 */
function renderReaderRow(r) {
  return html`
    <li class="contention-participant mono">
      <span class="contention-pill mode-read">READ</span>
      <span>#${r.sessionId}</span>
      <span class="contention-kind-tag">${r.kind}</span>
      <span class="contention-state-tag">${r.state}</span>
      ${r.seq !== null ? html`<span>seq ${r.seq}</span>` : nothing}
      ${r.txnId !== null ? html`<span>txn ${r.txnId}</span>` : nothing}
      <span class="contention-flag">
        ${r.woundable ? "woundable" : "unwoundable"}
      </span>
      <span class="contention-flag">
        ${r.retained ? "retained" : "leased"}
      </span>
      <span>${r.activeLeases} ${r.activeLeases === 1 ? "lease" : "leases"}</span>
    </li>
  `;
}

/**
 * @param {WaiterSnapshot} w
 * @param {number} idx
 * @param {number} elapsedSinceFetch
 * @returns {TemplateResult}
 */
function renderWaiterRow(w, idx, elapsedSinceFetch) {
  const displayWaitMs = w.waitedMs + elapsedSinceFetch;
  const label = formatWaitDuration(displayWaitMs);
  return html`
    <li class="contention-participant mono">
      <span class="contention-queue-pos">#${idx + 1}</span>
      <span class="contention-pill ${w.mode === "WRITE" ? "mode-write" : "mode-read"}">
        ${w.mode}
      </span>
      <span>session #${w.sessionId}</span>
      <span class="contention-kind-tag">${w.kind}</span>
      <span>seq ${w.seq}</span>
      ${w.txnId !== null ? html`<span>txn ${w.txnId}</span>` : nothing}
      <span
        class="wait-elapsed"
        data-wait-base-ms=${String(w.waitedMs)}
        data-wait-ms=${String(displayWaitMs)}
        data-wait-label=${label}
      ></span>
    </li>
  `;
}

/**
 * @param {ContentionEvent} ev
 * @param {(docName: string) => unknown} onOpenDocument
 * @returns {TemplateResult}
 */
function renderEventRow(ev, onOpenDocument) {
  const docName = ev.docName;
  const hasWounder =
    ev.wounderSessionId !== undefined ||
    ev.wounderTxnId !== undefined ||
    ev.wounderKind !== undefined ||
    ev.wounderSeq !== undefined;

  return html`
    <li class="contention-event-item">
      <div class="contention-event-top">
        <span class="contention-event-badge kind-${ev.kind.toLowerCase()}">
          ${ev.kind}
        </span>
        <span class="contention-event-time mono">${ev.timestamp}</span>
      </div>

      <div class="contention-event-details mono">
        <span>session #${ev.sessionId}</span>
        <span class="contention-kind-tag">${ev.sessionKind}</span>
        ${ev.seq !== null ? html`<span>seq ${ev.seq}</span>` : nothing}
        ${ev.txnId !== null ? html`<span>txn ${ev.txnId}</span>` : nothing}
        ${ev.waitMs !== undefined
          ? html`<span>waited ${formatWaitDuration(ev.waitMs)}</span>`
          : nothing}
        ${ev.attempt !== undefined
          ? html`<span>attempt ${ev.attempt}</span>`
          : nothing}
      </div>

      ${hasWounder
        ? html`
            <div class="contention-wounder-row mono">
              <span class="contention-wounder-label">wounded by</span>
              ${ev.wounderSessionId !== undefined
                ? html`<span>session #${ev.wounderSessionId}</span>`
                : nothing}
              ${ev.wounderKind !== undefined
                ? html`<span class="contention-kind-tag">${ev.wounderKind}</span>`
                : nothing}
              ${ev.wounderSeq !== undefined
                ? html`<span>seq ${ev.wounderSeq}</span>`
                : nothing}
              ${ev.wounderTxnId !== undefined
                ? html`<span>txn ${ev.wounderTxnId}</span>`
                : nothing}
            </div>
          `
        : nothing}

      ${docName !== null
        ? html`
            <div class="contention-event-doc">
              <button
                type="button"
                class="contention-doc-link mono"
                title=${`Open ${docName} in Explorer`}
                aria-label=${`Open document ${docName} in Explorer`}
                @click=${() => onOpenDocument(docName)}
              >
                ${docName}
              </button>
            </div>
          `
        : nothing}
    </li>
  `;
}
