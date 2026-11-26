// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  clearDatabase,
  createDocument,
  deleteCollection,
  deleteDocument,
  exportSnapshot,
  importSnapshot,
  listCollectionIds,
  patchDatabase,
} from "../api.js";
import { generateAutoId } from "../values.js";
import { extractSessionFields, renderHybridEditor } from "./editor.js";
import {
  getActiveRootPath,
  isValidPathSegment,
  relativeSegments,
  startsWithSegments,
  syncBrowserUrl,
} from "../router.js";
import {
  bumpDbEpoch,
  bumpNavEpoch,
  clearActiveDatabaseState,
  deleteDocumentLevelCache,
  getDbEpoch,
  pruneCachesWithPrefix,
} from "../state.js";

/** @typedef {import('../api.js').ImportSummary} ImportSummary */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */
/** @typedef {import('../state.js').ActiveModal} ActiveModal */
/** @typedef {import('../state.js').AppState} AppState */

/**
 * @typedef {{
 *   onRerender: () => void,
 *   onSelectDatabase: (projectId: string, databaseId: string, initialSegments?: string[], replaceHistory?: boolean) => Promise<void>,
 *   onHydrateVisiblePath: (expectedDbEpoch?: number, expectedNavEpoch?: number, segmentsSnapshot?: string[]) => Promise<unknown>,
 *   onRefreshDatabaseList: () => Promise<boolean>,
 *   onToast: (text: string, tone?: 'info' | 'success' | 'error') => void,
 *   onImportSuccess: (summary: ImportSummary) => Promise<void>,
 * }} ModalCallbacks
 */

const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  'input:not([disabled]):not([type="hidden"])',
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(", ");

const EXPORT_REVOKE_DELAY_MS = 60_000;

/** @type {EventTarget | null} */
let backdropMouseDownTarget = null;
/** @type {HTMLInputElement | null} */
let importFileInputEl = null;

/**
 * Records whether a pointer press originated directly on the modal backdrop.
 * @param {MouseEvent} e
 */
function onBackdropMouseDown(e) {
  backdropMouseDownTarget =
    e.target === e.currentTarget ? e.currentTarget : null;
}

/**
 * Dismisses the modal only when both `mousedown` and `click` originated directly on `.modal-backdrop`.
 * @param {MouseEvent} e
 * @param {() => void} close
 */
function onBackdropClick(e, close) {
  const startedOnBackdrop =
    backdropMouseDownTarget !== null &&
    backdropMouseDownTarget === e.currentTarget;
  backdropMouseDownTarget = null;
  if (startedOnBackdrop && e.target === e.currentTarget) {
    close();
  }
}

/**
 * Returns focusable elements within a dialog container in DOM order.
 * @param {Element} dialogEl
 * @returns {HTMLElement[]}
 */
export function getModalFocusableElements(dialogEl) {
  if (typeof dialogEl.querySelectorAll !== "function") return [];
  return Array.from(dialogEl.querySelectorAll(FOCUSABLE_SELECTOR)).filter(
    (el) => {
      const candidate = /** @type {Partial<HTMLElement>} */ (el);
      if (typeof candidate.focus !== "function") return false;
      if (
        typeof el.hasAttribute === "function" &&
        el.hasAttribute("hidden")
      ) {
        return false;
      }
      return true;
    }
  ).map((el) => /** @type {HTMLElement} */ (el));
}

/**
 * Handles Escape dismissal (respecting `!m.busy`) and Tab focus wrap within an open dialog.
 * @param {KeyboardEvent} e
 * @param {AppState} state
 * @param {Element | null} containerEl
 * @param {() => void} onClose
 */
export function handleModalKeydown(e, state, containerEl, onClose) {
  const m = state.modal;
  if (!m || e.isComposing) return;
  if (e.key === "Escape") {
    if (!m.busy) {
      e.preventDefault?.();
      onClose();
    }
    return;
  }
  if (e.key !== "Tab" || e.ctrlKey || e.metaKey || e.altKey) return;

  const dialogEl =
    typeof containerEl?.querySelector === "function"
      ? containerEl.querySelector('[role="dialog"]')
      : null;
  if (!dialogEl) return;

  const focusables = getModalFocusableElements(dialogEl);
  if (focusables.length === 0) {
    e.preventDefault?.();
    const dlg = /** @type {Partial<HTMLElement>} */ (dialogEl);
    if (typeof dlg.focus === "function") dlg.focus();
    return;
  }

  const active =
    typeof document !== "undefined" && document
      ? (document.activeElement ?? null)
      : null;
  const first = focusables[0];
  const last = focusables[focusables.length - 1];
  const inside =
    active !== null &&
    (active === dialogEl ||
      (typeof dialogEl.contains === "function" && dialogEl.contains(active)));

  if (e.shiftKey) {
    if (!inside || active === first || active === dialogEl) {
      e.preventDefault?.();
      last.focus();
    }
  } else if (!inside || active === last) {
    e.preventDefault?.();
    first.focus();
  }
}

/**
 * Shared accessible modal shell with ARIA dialog attributes and safe backdrop dismissal.
 * @param {{
 *   cardClass?: string,
 *   titleId: string,
 *   title: string,
 *   busy: boolean,
 *   error: string | null,
 *   close: () => void,
 *   body: TemplateResult,
 *   footer: TemplateResult,
 * }} p
 * @returns {TemplateResult}
 */
function renderDialogShell(p) {
  return html`
    <div
      class="modal-backdrop"
      @mousedown=${onBackdropMouseDown}
      @click=${(/** @type {MouseEvent} */ e) => onBackdropClick(e, p.close)}
    >
      <div
        class=${p.cardClass ?? "modal-card modal-sm"}
        role="dialog"
        aria-modal="true"
        aria-labelledby=${p.titleId}
        tabindex="-1"
      >
        <div class="modal-header">
          <h2 id=${p.titleId} class="modal-title">${p.title}</h2>
          <button
            type="button"
            class="btn btn-sm btn-ghost"
            aria-label="Close dialog"
            ?disabled=${p.busy}
            @click=${p.close}
          >
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${p.error
            ? html`<div class="error-banner modal-banner" role="alert">${p.error}</div>`
            : nothing}
          ${p.body}
        </div>
        <div class="modal-footer">${p.footer}</div>
      </div>
    </div>
  `;
}

/**
 * Renders the active modal dialog, or `nothing` when no modal is open.
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @returns {TemplateResult | symbol}
 */
export function renderModal(state, callbacks) {
  const m = state.modal;
  if (!m) {
    backdropMouseDownTarget = null;
    importFileInputEl = null;
    return nothing;
  }

  const close = () => {
    if (m.busy) return;
    backdropMouseDownTarget = null;
    importFileInputEl = null;
    state.modal = null;
    callbacks.onRerender();
  };

  if (m.kind === "open_db") {
    return renderOpenDbModal(m, state, callbacks, close);
  }
  if (m.kind === "create_doc") {
    return renderCreateDocModal(m, state, callbacks, close);
  }
  if (m.kind === "delete_doc") {
    return renderDeleteDocModal(m, state, callbacks, close);
  }
  if (m.kind === "delete_col") {
    return renderDeleteColModal(m, state, callbacks, close);
  }
  if (m.kind === "export_snapshot") {
    return renderExportSnapshotModal(m, state, callbacks, close);
  }
  if (m.kind === "import_snapshot") {
    return renderImportSnapshotModal(m, state, callbacks, close);
  }
  return renderClearDbModal(m, state, callbacks, close);
}

/**
 * @param {Extract<ActiveModal, { kind: 'open_db' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderOpenDbModal(m, state, callbacks, close) {
  const submit = async () => {
    const p = m.projectId.trim();
    const d = m.databaseId.trim();
    if (!isValidPathSegment(p) || !isValidPathSegment(d)) {
      m.error =
        "Enter valid non-empty Project ID and Database ID (no '/', '.', or '..')";
      callbacks.onRerender();
      return;
    }
    m.busy = true;
    m.error = null;
    callbacks.onRerender();
    try {
      await patchDatabase(p, d, { databaseEdition: m.edition });
      state.modal = null;
      await callbacks.onSelectDatabase(p, d, []);
      callbacks.onToast(`Opened database ${p}/${d}`, "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
      callbacks.onRerender();
    }
  };

  return renderDialogShell({
    titleId: "modal-title-open-db",
    title: "Open or Create Database",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <div class="form-field">
        <label for="modal-open-db-project">Project ID</label>
        <input
          id="modal-open-db-project"
          type="text"
          class="input mono"
          .value=${m.projectId}
          @input=${(/** @type {Event} */ e) => {
            m.projectId = /** @type {HTMLInputElement} */ (e.target).value;
          }}
        />
      </div>
      <div class="form-field">
        <label for="modal-open-db-database">Database ID</label>
        <input
          id="modal-open-db-database"
          type="text"
          class="input mono"
          .value=${m.databaseId}
          @input=${(/** @type {Event} */ e) => {
            m.databaseId = /** @type {HTMLInputElement} */ (e.target).value;
          }}
        />
      </div>
      <div class="form-field">
        <label for="modal-open-db-edition">Edition</label>
        <select
          id="modal-open-db-edition"
          class="select"
          .value=${m.edition}
          @change=${(/** @type {Event} */ e) => {
            m.edition = /** @type {'STANDARD' | 'ENTERPRISE'} */ (
              /** @type {HTMLSelectElement} */ (e.target).value
            );
          }}
        >
          <option value="STANDARD">STANDARD (Pessimistic Concurrency)</option>
          <option value="ENTERPRISE">ENTERPRISE (Optimistic Concurrency)</option>
        </select>
      </div>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-primary"
        ?disabled=${m.busy}
        @click=${submit}
      >
        ${m.busy ? "Opening..." : "Open Database"}
      </button>
    `,
  });
}

/**
 * @param {Extract<ActiveModal, { kind: 'create_doc' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderCreateDocModal(m, state, callbacks, close) {
  const submit = async () => {
    const colId = m.collectionId.trim();
    const docId = m.docId.trim() || generateAutoId();
    if (!isValidPathSegment(colId)) {
      m.error =
        "Collection ID must be a valid non-empty segment (no '/', '.', or '..')";
      callbacks.onRerender();
      return;
    }
    if (!isValidPathSegment(docId)) {
      m.error = "Document ID cannot contain '/' or be '.' / '..'";
      callbacks.onRerender();
      return;
    }
    const extracted = extractSessionFields(m.session);
    if (!extracted.valid || !extracted.fields) {
      m.error = extracted.error ?? "Fix field validation errors";
      callbacks.onRerender();
      return;
    }

    m.busy = true;
    m.error = null;
    callbacks.onRerender();

    const myDbEpoch = getDbEpoch();
    try {
      const created = await createDocument(
        m.parentPath,
        colId,
        docId,
        extracted.fields
      );
      if (myDbEpoch !== getDbEpoch()) {
        state.modal = null;
        callbacks.onRerender();
        return;
      }
      state.modal = null;
      const fullName = created.name ?? "";
      const match =
        /^projects\/[^/]+\/databases\/[^/]+\/documents\/(.+)$/.exec(fullName);
      const rootPath = getActiveRootPath(
        state.activeProjectId,
        state.activeDatabaseId
      );
      if (match && rootPath) {
        const cols = await listCollectionIds(rootPath, {
          shouldAbort: () => myDbEpoch !== getDbEpoch(),
        });
        if (myDbEpoch !== getDbEpoch()) return;
        state.rootCollections = cols;
        state.segments = match[1].split("/");
        const myNavEpoch = bumpNavEpoch();
        syncBrowserUrl(
          state.activeProjectId,
          state.activeDatabaseId,
          state.segments
        );
        await callbacks.onHydrateVisiblePath(myDbEpoch, myNavEpoch);
      }
      callbacks.onToast(`Created ${colId}/${docId}`, "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return renderDialogShell({
    cardClass: "modal-card",
    titleId: "modal-title-create-doc",
    title: m.lockCollection
      ? `Add Document to "${m.collectionId}"`
      : "Start Collection & Create First Document",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      ${!m.lockCollection
        ? html`
            <div class="form-field">
              <label for="modal-create-doc-collection">Collection ID</label>
              <input
                id="modal-create-doc-collection"
                type="text"
                class="input mono"
                placeholder="e.g. users"
                .value=${m.collectionId}
                @input=${(/** @type {Event} */ e) => {
                  m.collectionId = /** @type {HTMLInputElement} */ (
                    e.target
                  ).value;
                }}
              />
            </div>
          `
        : nothing}

      <div class="form-row">
        <div class="form-field" style="flex: 1 1 auto;">
          <label for="modal-create-doc-id">Document ID</label>
          <input
            id="modal-create-doc-id"
            type="text"
            class="input mono"
            ?disabled=${m.lockDocId}
            placeholder="20-char auto-ID or custom ID"
            .value=${m.docId}
            @input=${(/** @type {Event} */ e) => {
              m.docId = /** @type {HTMLInputElement} */ (e.target).value;
            }}
          />
        </div>
        ${!m.lockDocId
          ? html`
              <button
                type="button"
                class="btn"
                @click=${() => {
                  m.docId = generateAutoId();
                  callbacks.onRerender();
                }}
              >
                Auto-ID
              </button>
            `
          : nothing}
      </div>

      <div style="border: 1px solid var(--border); border-radius: 8px; overflow: hidden;">
        ${renderHybridEditor({
          session: m.session,
          onChange: callbacks.onRerender,
          embeddedInModal: true,
        })}
      </div>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-primary"
        ?disabled=${m.busy}
        @click=${submit}
      >
        ${m.busy ? "Creating..." : "Save Document"}
      </button>
    `,
  });
}

/**
 * Refreshes explorer collection listings and active segments after deleting a document or collection.
 * @param {AppState} state
 * @param {string} targetPath
 * @param {boolean} shouldTruncateSegments
 * @param {number} myDbEpoch
 * @param {ModalCallbacks} callbacks
 * @returns {Promise<boolean>} False if the active database changed while refreshing; otherwise true.
 */
async function refreshExplorerAfterDelete(
  state,
  targetPath,
  shouldTruncateSegments,
  myDbEpoch,
  callbacks
) {
  const rootPath = getActiveRootPath(
    state.activeProjectId,
    state.activeDatabaseId
  );
  if (!rootPath) return true;
  const targetSegs = relativeSegments(rootPath, targetPath);
  if (
    shouldTruncateSegments &&
    targetSegs &&
    startsWithSegments(state.segments, targetSegs)
  ) {
    state.segments = state.segments.slice(0, targetSegs.length - 1);
  }
  const myNavEpoch = bumpNavEpoch();
  const cols = await listCollectionIds(rootPath, {
    shouldAbort: () => myDbEpoch !== getDbEpoch(),
  });
  if (myDbEpoch !== getDbEpoch()) return false;
  state.rootCollections = cols;
  syncBrowserUrl(state.activeProjectId, state.activeDatabaseId, state.segments);
  await callbacks.onHydrateVisiblePath(myDbEpoch, myNavEpoch);
  return true;
}

/**
 * @param {Extract<ActiveModal, { kind: 'delete_doc' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderDeleteDocModal(m, state, callbacks, close) {
  const confirmDelete = async () => {
    m.busy = true;
    m.error = null;
    callbacks.onRerender();
    const myDbEpoch = getDbEpoch();
    try {
      await deleteDocument(m.docPath, m.recursive);
      if (myDbEpoch !== getDbEpoch()) {
        state.modal = null;
        callbacks.onRerender();
        return;
      }
      deleteDocumentLevelCache(m.docPath, state);
      if (m.recursive) {
        pruneCachesWithPrefix(m.docPath, state);
      }
      state.modal = null;
      if (
        !(await refreshExplorerAfterDelete(
          state,
          m.docPath,
          m.recursive || !m.hasSubcollections,
          myDbEpoch,
          callbacks
        ))
      ) {
        return;
      }
      callbacks.onToast("Document deleted", "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return renderDialogShell({
    titleId: "modal-title-delete-doc",
    title: "Delete Document",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <p style="margin: 0;">
        Delete document <strong class="mono">${m.docPath}</strong>?
      </p>
      <label
        for="modal-delete-doc-recursive"
        style="display: flex; align-items: center; gap: 8px; cursor: pointer;"
      >
        <input
          id="modal-delete-doc-recursive"
          type="checkbox"
          .checked=${m.recursive}
          @change=${(/** @type {Event} */ e) => {
            m.recursive = /** @type {HTMLInputElement} */ (e.target).checked;
            callbacks.onRerender();
          }}
        />
        <span>Also delete all nested subcollections recursively</span>
      </label>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-danger"
        ?disabled=${m.busy}
        @click=${confirmDelete}
      >
        ${m.busy ? "Deleting..." : "Delete"}
      </button>
    `,
  });
}

/**
 * @param {Extract<ActiveModal, { kind: 'delete_col' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderDeleteColModal(m, state, callbacks, close) {
  const confirmDelete = async () => {
    m.busy = true;
    m.error = null;
    callbacks.onRerender();
    const myDbEpoch = getDbEpoch();
    try {
      await deleteCollection(m.colPath);
      if (myDbEpoch !== getDbEpoch()) {
        state.modal = null;
        callbacks.onRerender();
        return;
      }
      pruneCachesWithPrefix(m.colPath, state);
      state.modal = null;
      if (
        !(await refreshExplorerAfterDelete(
          state,
          m.colPath,
          true,
          myDbEpoch,
          callbacks
        ))
      ) {
        return;
      }
      callbacks.onToast(`Deleted collection ${m.colId}`, "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return renderDialogShell({
    titleId: "modal-title-delete-col",
    title: "Delete Collection",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <p style="margin: 0;">
        Recursively delete all documents and subcollections inside
        <strong class="mono">${m.colId}</strong>?
      </p>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-danger"
        ?disabled=${m.busy}
        @click=${confirmDelete}
      >
        ${m.busy ? "Deleting..." : "Delete Collection"}
      </button>
    `,
  });
}

/**
 * @param {Extract<ActiveModal, { kind: 'clear_db' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderClearDbModal(m, state, callbacks, close) {
  const confirmClearDb = async () => {
    m.busy = true;
    m.error = null;
    callbacks.onRerender();
    try {
      await clearDatabase(m.projectId, m.databaseId);
      const myDbEpoch = bumpDbEpoch();
      bumpNavEpoch();
      const clearedRoot = `projects/${m.projectId}/databases/${m.databaseId}/documents`;
      state.databases = state.databases.filter(
        (db) => db.rootPath !== clearedRoot
      );
      state.modal = null;
      clearActiveDatabaseState(state);
      const applied = await callbacks.onRefreshDatabaseList();
      if (myDbEpoch !== getDbEpoch()) return;
      if (applied && !state.activeProjectId && state.databases.length > 0) {
        const next = state.databases[0];
        await callbacks.onSelectDatabase(
          next.projectId,
          next.databaseId,
          [],
          true
        );
      } else if (!state.activeProjectId) {
        syncBrowserUrl(
          state.activeProjectId,
          state.activeDatabaseId,
          state.segments,
          true
        );
      }
      callbacks.onToast(
        `Cleared database ${m.projectId}/${m.databaseId}`,
        "success"
      );
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return renderDialogShell({
    titleId: "modal-title-clear-db",
    title: "Clear Entire Database",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <p style="margin: 0;">
        Wipe all collections and documents in
        <strong class="mono">${m.projectId}/${m.databaseId}</strong>? This
        cannot be undone.
      </p>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-danger"
        ?disabled=${m.busy}
        @click=${confirmClearDb}
      >
        ${m.busy ? "Clearing..." : "Clear Database"}
      </button>
    `,
  });
}

/**
 * @param {Extract<ActiveModal, { kind: 'export_snapshot' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderExportSnapshotModal(m, state, callbacks, close) {
  const activeRoot = getActiveRootPath(
    state.activeProjectId,
    state.activeDatabaseId
  );
  const activeLabel =
    state.activeProjectId && state.activeDatabaseId
      ? `${state.activeProjectId}/${state.activeDatabaseId}`
      : null;

  const triggerDownload = async (/** @type {'current' | 'all'} */ scope) => {
    if (m.busy) return;
    if (scope === "current" && !activeRoot) return;
    m.busy = true;
    m.busyScope = scope;
    m.error = null;
    callbacks.onRerender();
    try {
      const targets = scope === "current" && activeRoot ? [activeRoot] : [];
      const { blob, filename } = await exportSnapshot(targets);
      const objectUrl = URL.createObjectURL(blob);
      /** @type {HTMLAnchorElement | null} */
      let a = null;
      try {
        a = document.createElement("a");
        a.href = objectUrl;
        a.download = filename;
        if (a.style) {
          a.style.display = "none";
        }
        document.body?.appendChild(a);
        a.click();
      } finally {
        a?.remove?.();
        setTimeout(() => {
          URL.revokeObjectURL(objectUrl);
        }, EXPORT_REVOKE_DELAY_MS);
      }
      state.modal = null;
      callbacks.onToast(`Exported snapshot ${filename}`, "info");
    } catch (err) {
      m.busy = false;
      m.busyScope = null;
      m.error = err instanceof Error ? err.message : String(err);
      callbacks.onRerender();
    }
  };

  return renderDialogShell({
    titleId: "modal-title-export-snapshot",
    title: "Export Snapshot",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <p style="margin: 0;">
        Download a binary <span class="mono">.fsemu</span> snapshot preserving
        documents, subcollections, empty databases, and database settings.
      </p>
      ${activeLabel
        ? html`
            <p style="margin: 0; font-size: 12px; color: var(--text-muted);">
              Active database: <strong class="mono">${activeLabel}</strong>
            </p>
          `
        : nothing}
      <div class="snapshot-action-row">
        <button
          type="button"
          class="btn btn-primary"
          ?data-autofocus=${Boolean(activeRoot)}
          ?disabled=${m.busy || !activeRoot}
          @click=${() => triggerDownload("current")}
        >
          ${m.busy && m.busyScope === "current"
            ? "Exporting..."
            : "Current Database"}
        </button>
        <button
          type="button"
          class="btn"
          ?data-autofocus=${!activeRoot}
          ?disabled=${m.busy}
          @click=${() => triggerDownload("all")}
        >
          ${m.busy && m.busyScope === "all"
            ? "Exporting..."
            : "All Databases"}
        </button>
      </div>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
    `,
  });
}

/**
 * @param {Extract<ActiveModal, { kind: 'import_snapshot' }>} m
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @param {() => void} close
 * @returns {TemplateResult}
 */
function renderImportSnapshotModal(m, state, callbacks, close) {
  const hasDirtyEditors = Array.from(state.documentLevels.values()).some(
    (level) => Boolean(level.editorSession?.isDirty)
  );
  const describedByIds = hasDirtyEditors
    ? "import-snapshot-overwrite-warning import-snapshot-dirty-warning"
    : "import-snapshot-overwrite-warning";

  const submitImport = async () => {
    if (m.busy) return;
    if (!m.file) {
      m.error = "Select a .fsemu snapshot file to import";
      callbacks.onRerender();
      return;
    }
    const file = m.file;
    const fileInputEl = importFileInputEl;
    m.busy = true;
    m.error = null;
    callbacks.onRerender();
    try {
      const summary = await importSnapshot(file);
      state.modal = null;
      importFileInputEl = null;
      const dbWord = summary.totalDatabases === 1 ? "database" : "databases";
      const docWord = summary.totalDocuments === 1 ? "document" : "documents";
      callbacks.onToast(
        `Imported ${summary.totalDatabases} ${dbWord}, ${summary.totalDocuments} ${docWord}`,
        "success"
      );
      await callbacks.onImportSuccess(summary);
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    } finally {
      m.file = null;
      if (fileInputEl) {
        fileInputEl.value = "";
      }
      callbacks.onRerender();
    }
  };

  return renderDialogShell({
    titleId: "modal-title-import-snapshot",
    title: "Import Snapshot",
    busy: m.busy,
    error: m.error,
    close,
    body: html`
      <div
        class="conflict-banner modal-banner"
        id="import-snapshot-overwrite-warning"
      >
        <span>Importing completely replaces (never merges) each database in the snapshot, deleting extra documents and overwriting settings. Databases not in the snapshot remain untouched.</span>
      </div>
      ${hasDirtyEditors
        ? html`
            <div
              class="conflict-banner modal-banner"
              id="import-snapshot-dirty-warning"
            >
              <span>You have unsaved edits in an open document editor. If this import updates or deletes that document, your local draft is kept with an option to keep, reload, or discard it.</span>
            </div>
          `
        : nothing}
      <div class="form-field">
        <label for="modal-import-snapshot-file">Snapshot File (.fsemu)</label>
        <input
          id="modal-import-snapshot-file"
          type="file"
          class="input"
          accept=".fsemu,application/octet-stream"
          aria-describedby=${describedByIds}
          ?disabled=${m.busy}
          @change=${(/** @type {Event} */ e) => {
            const input = /** @type {HTMLInputElement} */ (e.target);
            importFileInputEl = input;
            m.file = input.files?.[0] ?? null;
            m.error = null;
            callbacks.onRerender();
          }}
        />
      </div>
    `,
    footer: html`
      <button type="button" class="btn" ?disabled=${m.busy} @click=${close}>
        Cancel
      </button>
      <button
        type="button"
        class="btn btn-primary"
        ?disabled=${m.busy || !m.file}
        @click=${submitImport}
      >
        ${m.busy ? "Importing..." : "Import Snapshot"}
      </button>
    `,
  });
}
