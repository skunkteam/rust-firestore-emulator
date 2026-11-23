// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  clearDatabase,
  createDocument,
  deleteCollection,
  deleteDocument,
  listCollectionIds,
  patchDatabase,
} from "../api.js";
import { generateAutoId } from "../values.js";
import { extractSessionFields, renderHybridEditor } from "./editor.js";
import {
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

/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */
/** @typedef {import('../state.js').ActiveModal} ActiveModal */
/** @typedef {import('../state.js').AppState} AppState */

/**
 * @typedef {{
 *   onRerender: () => void,
 *   onSelectDatabase: (projectId: string, databaseId: string, initialSegments?: string[], replaceHistory?: boolean) => Promise<void>,
 *   onHydrateVisiblePath: (expectedDbEpoch?: number, expectedNavEpoch?: number, segmentsSnapshot?: string[]) => Promise<void>,
 *   onRefreshDatabaseList: () => Promise<boolean>,
 *   onToast: (text: string, tone?: 'info' | 'success' | 'error') => void,
 * }} ModalCallbacks
 */

/**
 * Renders the active modal dialog, or `nothing` when no modal is open.
 * @param {AppState} state
 * @param {ModalCallbacks} callbacks
 * @returns {TemplateResult | symbol}
 */
export function renderModal(state, callbacks) {
  const m = state.modal;
  if (!m) return nothing;

  const close = () => {
    if (m.busy) return;
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

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card modal-sm"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>Open or Create Database</span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          <div class="form-field">
            <label>Project ID</label>
            <input
              type="text"
              class="input mono"
              .value=${m.projectId}
              @input=${(/** @type {Event} */ e) => {
                m.projectId = /** @type {HTMLInputElement} */ (e.target).value;
              }}
            />
          </div>
          <div class="form-field">
            <label>Database ID</label>
            <input
              type="text"
              class="input mono"
              .value=${m.databaseId}
              @input=${(/** @type {Event} */ e) => {
                m.databaseId = /** @type {HTMLInputElement} */ (e.target).value;
              }}
            />
          </div>
          <div class="form-field">
            <label>Edition</label>
            <select
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
        </div>
        <div class="modal-footer">
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
        </div>
      </div>
    </div>
  `;
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
      if (match && state.activeProjectId && state.activeDatabaseId) {
        const cols = await listCollectionIds(
          `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`
        );
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

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>
            ${m.lockCollection
              ? `Add Document to "${m.collectionId}"`
              : "Start Collection & Create First Document"}
          </span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          ${!m.lockCollection
            ? html`
                <div class="form-field">
                  <label>Collection ID</label>
                  <input
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
              <label>Document ID</label>
              <input
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
        </div>
        <div class="modal-footer">
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
        </div>
      </div>
    </div>
  `;
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
      deleteDocumentLevelCache(m.docPath);
      if (m.recursive) {
        pruneCachesWithPrefix(m.docPath);
      }
      state.modal = null;
      if (state.activeProjectId && state.activeDatabaseId) {
        const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
        const docSegs = relativeSegments(rootPath, m.docPath);
        if (
          (m.recursive || !m.hasSubcollections) &&
          docSegs &&
          startsWithSegments(state.segments, docSegs)
        ) {
          state.segments = state.segments.slice(0, docSegs.length - 1);
        }
        const myNavEpoch = bumpNavEpoch();
        const cols = await listCollectionIds(rootPath);
        if (myDbEpoch !== getDbEpoch()) return;
        state.rootCollections = cols;
        syncBrowserUrl(
          state.activeProjectId,
          state.activeDatabaseId,
          state.segments
        );
        await callbacks.onHydrateVisiblePath(myDbEpoch, myNavEpoch);
      }
      callbacks.onToast("Document deleted", "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card modal-sm"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>Delete Document</span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          <p style="margin: 0;">
            Delete document <strong class="mono">${m.docPath}</strong>?
          </p>
          <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
            <input
              type="checkbox"
              .checked=${m.recursive}
              @change=${(/** @type {Event} */ e) => {
                m.recursive = /** @type {HTMLInputElement} */ (
                  e.target
                ).checked;
                callbacks.onRerender();
              }}
            />
            <span>Also delete all nested subcollections recursively</span>
          </label>
        </div>
        <div class="modal-footer">
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
        </div>
      </div>
    </div>
  `;
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
      pruneCachesWithPrefix(m.colPath);
      state.modal = null;
      if (state.activeProjectId && state.activeDatabaseId) {
        const rootPath = `projects/${state.activeProjectId}/databases/${state.activeDatabaseId}/documents`;
        const colSegs = relativeSegments(rootPath, m.colPath);
        if (colSegs && startsWithSegments(state.segments, colSegs)) {
          state.segments = state.segments.slice(0, colSegs.length - 1);
        }
        const myNavEpoch = bumpNavEpoch();
        const cols = await listCollectionIds(rootPath);
        if (myDbEpoch !== getDbEpoch()) return;
        state.rootCollections = cols;
        syncBrowserUrl(
          state.activeProjectId,
          state.activeDatabaseId,
          state.segments
        );
        await callbacks.onHydrateVisiblePath(myDbEpoch, myNavEpoch);
      }
      callbacks.onToast(`Deleted collection ${m.colId}`, "success");
    } catch (err) {
      m.busy = false;
      m.error = err instanceof Error ? err.message : String(err);
    }
    callbacks.onRerender();
  };

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card modal-sm"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>Delete Collection</span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          <p style="margin: 0;">
            Recursively delete all documents and subcollections inside
            <strong class="mono">${m.colId}</strong>?
          </p>
        </div>
        <div class="modal-footer">
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
        </div>
      </div>
    </div>
  `;
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
      clearActiveDatabaseState();
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

  return html`
    <div class="modal-backdrop" @click=${close}>
      <div
        class="modal-card modal-sm"
        @click=${(/** @type {Event} */ e) => e.stopPropagation()}
      >
        <div class="modal-header">
          <span>Clear Entire Database</span>
          <button type="button" class="btn btn-sm btn-ghost" @click=${close}>
            ✕
          </button>
        </div>
        <div class="modal-body">
          ${m.error ? html`<div class="error-banner">${m.error}</div>` : nothing}
          <p style="margin: 0;">
            Wipe all collections and documents in
            <strong class="mono">${m.projectId}/${m.databaseId}</strong>? This
            cannot be undone.
          </p>
        </div>
        <div class="modal-footer">
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
        </div>
      </div>
    </div>
  `;
}
