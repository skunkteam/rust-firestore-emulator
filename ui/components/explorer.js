// @ts-check

import { html, nothing } from "../lit-html.js";
import { renderHybridEditor } from "./editor.js";

/** @typedef {import('../values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('./editor.js').EditorSession} EditorSession */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */

/**
 * @typedef {{
 *   collectionPath: string,
 *   collectionId: string,
 *   parentPath: string,
 *   documents: Array<{ id: string, fullName: string, phantom: boolean }>,
 *   nextPageToken: string,
 *   filterText: string,
 *   loading: boolean,
 * }} CollectionColumnState
 */

/**
 * @typedef {{
 *   docPath: string,
 *   docId: string,
 *   exists: boolean,
 *   loading: boolean,
 *   subcollections: string[],
 *   editorSession: EditorSession | null,
 * }} DocumentLevelState
 */

/**
 * Renders the Cloud-Console-style horizontal Miller-column explorer.
 *
 * @param {{
 *   projectId: string,
 *   databaseId: string,
 *   rootCollections: string[],
 *   rootLoading: boolean,
 *   segments: string[],
 *   collectionColumns: Map<string, CollectionColumnState>,
 *   documentLevels: Map<string, DocumentLevelState>,
 *   onSelectSegments: (segments: string[]) => void,
 *   onOpenStartCollection: (parentPath: string) => void,
 *   onOpenAddDocument: (parentPath: string, collectionId: string, fixedDocId?: string) => void,
 *   onOpenDeleteCollection: (colPath: string, colId: string) => void,
 *   onOpenDeleteDocument: (docPath: string, hasSubcollections: boolean) => void,
 *   onOpenQueryCollection: (parentPath: string, collectionId: string) => void,
 *   onLoadMoreDocuments: (colPath: string) => void,
 *   onSaveDocument: (docLevel: DocumentLevelState) => void,
 *   onFilterChange: (colPath: string, filterText: string) => void,
 *   onRerender: () => void,
 * }} props
 * @returns {TemplateResult}
 */
export function renderExplorer(props) {
  const {
    projectId,
    databaseId,
    rootCollections,
    rootLoading,
    segments,
    collectionColumns,
    documentLevels,
    onSelectSegments,
    onOpenStartCollection,
    onOpenAddDocument,
    onOpenDeleteCollection,
    onOpenDeleteDocument,
    onOpenQueryCollection,
    onLoadMoreDocuments,
    onSaveDocument,
    onFilterChange,
    onRerender,
  } = props;

  const rootPath = `projects/${projectId}/databases/${databaseId}/documents`;
  const selectedRootCol = segments.length >= 1 ? segments[0] : null;

  /** @type {TemplateResult[]} */
  const dynamicPanes = [];

  for (let i = 0; i < segments.length; i += 2) {
    const colId = segments[i];
    const colSegments = segments.slice(0, i + 1);
    const parentSegments = segments.slice(0, i);
    const parentPath =
      parentSegments.length === 0
        ? rootPath
        : `${rootPath}/${parentSegments.join("/")}`;
    const colPath = `${rootPath}/${colSegments.join("/")}`;
    const selectedDocId = segments.length > i + 1 ? segments[i + 1] : null;
    const colState = collectionColumns.get(colPath);

    dynamicPanes.push(
      renderDocumentsColumn({
        colId,
        colPath,
        parentPath,
        colSegments,
        selectedDocId,
        colState,
        onSelectDoc: (docId) => onSelectSegments([...colSegments, docId]),
        onAddDoc: () => onOpenAddDocument(parentPath, colId),
        onQueryCol: () => onOpenQueryCollection(parentPath, colId),
        onDeleteCol: () => onOpenDeleteCollection(colPath, colId),
        onLoadMore: () => onLoadMoreDocuments(colPath),
        onFilterChange: (val) => onFilterChange(colPath, val),
      })
    );

    if (selectedDocId !== null) {
      const docSegments = segments.slice(0, i + 2);
      const docPath = `${rootPath}/${docSegments.join("/")}`;
      const selectedSubcol = segments.length > i + 2 ? segments[i + 2] : null;
      const docState = documentLevels.get(docPath);

      // Only render the full document editor on the deepest selected document level;
      // intermediate ancestor documents show a compact subcollection navigation column
      // unless they are the leaf selected item!
      const isLeafDoc = segments.length <= i + 3;

      if (isLeafDoc) {
        dynamicPanes.push(
          renderDocumentInspector({
            colId,
            docId: selectedDocId,
            docPath,
            parentPath,
            docSegments,
            selectedSubcol,
            docState,
            onSelectSubcol: (subId) => onSelectSegments([...docSegments, subId]),
            onStartSubcol: () => onOpenStartCollection(docPath),
            onCreatePhantomDoc: () =>
              onOpenAddDocument(parentPath, colId, selectedDocId),
            onSaveDoc: () => docState && onSaveDocument(docState),
            onDeleteDoc: () =>
              onOpenDeleteDocument(
                docPath,
                (docState?.subcollections.length ?? 0) > 0
              ),
            onRerender,
          })
        );
      } else {
        dynamicPanes.push(
          renderCompactAncestorSubcollections({
            docId: selectedDocId,
            docPath,
            docSegments,
            selectedSubcol,
            docState,
            onSelectDocItself: () => onSelectSegments(docSegments),
            onSelectSubcol: (subId) => onSelectSegments([...docSegments, subId]),
            onStartSubcol: () => onOpenStartCollection(docPath),
          })
        );
      }
    }
  }

  return html`
    <div class="breadcrumb-bar">
      <div class="breadcrumbs">
        <button
          type="button"
          class="crumb ${segments.length === 0 ? "active" : ""}"
          @click=${() => onSelectSegments([])}
        >
          ${projectId}/${databaseId}
        </button>
        ${segments.map(
          (seg, idx) => html`
            <span class="crumb-sep">/</span>
            <button
              type="button"
              class="crumb ${idx === segments.length - 1 ? "active" : ""}"
              @click=${() => onSelectSegments(segments.slice(0, idx + 1))}
            >
              ${seg}
            </button>
          `
        )}
      </div>
    </div>

    <div class="miller-scroller">
      <!-- Column 0: Root Collections -->
      <section class="miller-col">
        <div class="col-header">
          <span>Collections</span>
          <div class="col-header-actions">
            <button
              type="button"
              class="btn btn-sm btn-primary"
              @click=${() => onOpenStartCollection(rootPath)}
            >
              + Start Collection
            </button>
          </div>
        </div>
        ${rootLoading && rootCollections.length === 0
          ? html`<div class="col-empty">Loading collections...</div>`
          : rootCollections.length === 0
            ? html`
                <div class="col-empty">
                  No collections yet.<br />Click
                  <strong>+ Start Collection</strong> to create your first
                  document.
                </div>
              `
            : html`
                <ul class="col-list">
                  ${rootCollections.map(
                    (colId) => html`
                      <li
                        class="col-item ${selectedRootCol === colId
                          ? "selected"
                          : ""}"
                        @click=${() => onSelectSegments([colId])}
                      >
                        <span class="item-label">${colId}</span>
                        <span>›</span>
                      </li>
                    `
                  )}
                </ul>
              `}
      </section>

      ${dynamicPanes}
    </div>
  `;
}

/**
 * @param {{
 *   colId: string,
 *   colPath: string,
 *   parentPath: string,
 *   colSegments: string[],
 *   selectedDocId: string | null,
 *   colState: CollectionColumnState | undefined,
 *   onSelectDoc: (docId: string) => void,
 *   onAddDoc: () => void,
 *   onQueryCol: () => void,
 *   onDeleteCol: () => void,
 *   onLoadMore: () => void,
 *   onFilterChange: (val: string) => void,
 * }} p
 * @returns {TemplateResult}
 */
function renderDocumentsColumn(p) {
  const docs = p.colState?.documents ?? [];
  const filter = (p.colState?.filterText ?? "").toLowerCase();
  const filtered = filter
    ? docs.filter((d) => d.id.toLowerCase().includes(filter))
    : docs;

  return html`
    <section class="miller-col">
      <div class="col-header">
        <span class="mono" title=${p.colPath}>${p.colId}</span>
        <div class="col-header-actions">
          <button
            type="button"
            class="btn btn-sm"
            title="Query this collection"
            @click=${p.onQueryCol}
          >
            Query
          </button>
          <button
            type="button"
            class="btn btn-sm btn-primary"
            title="Add document"
            @click=${p.onAddDoc}
          >
            + Add Doc
          </button>
          <button
            type="button"
            class="btn btn-sm btn-ghost"
            title="Delete collection"
            @click=${p.onDeleteCol}
          >
            🗑
          </button>
        </div>
      </div>

      ${docs.length > 5 || p.colState?.filterText
        ? html`
            <div class="col-filter">
              <input
                type="text"
                class="input mono"
                placeholder="Filter document IDs..."
                .value=${p.colState?.filterText ?? ""}
                @input=${(/** @type {Event} */ e) =>
                  p.onFilterChange(
                    /** @type {HTMLInputElement} */ (e.target).value
                  )}
              />
            </div>
          `
        : nothing}

      ${p.colState?.loading && docs.length === 0
        ? html`<div class="col-empty">Loading documents...</div>`
        : filtered.length === 0
          ? html`<div class="col-empty">No documents found.</div>`
          : html`
              <ul class="col-list">
                ${filtered.map(
                  (d) => html`
                    <li
                      class="col-item ${p.selectedDocId === d.id
                        ? "selected"
                        : ""} ${d.phantom ? "phantom" : ""}"
                      title=${d.phantom
                        ? "Non-existent ancestor document (has subcollections)"
                        : d.fullName}
                      @click=${() => p.onSelectDoc(d.id)}
                    >
                      <span class="item-label">${d.id}</span>
                      ${d.phantom
                        ? html`<span class="phantom-pill">Non-existent ancestor</span>`
                        : html`<span>›</span>`}
                    </li>
                  `
                )}
              </ul>
            `}

      ${p.colState?.nextPageToken
        ? html`
            <div style="padding: 6px; border-top: 1px solid var(--border);">
              <button
                type="button"
                class="btn btn-sm"
                style="width: 100%;"
                ?disabled=${p.colState.loading}
                @click=${p.onLoadMore}
              >
                Load more...
              </button>
            </div>
          `
        : nothing}
    </section>
  `;
}

/**
 * @param {{
 *   docId: string,
 *   docPath: string,
 *   docSegments: string[],
 *   selectedSubcol: string | null,
 *   docState: DocumentLevelState | undefined,
 *   onSelectDocItself: () => void,
 *   onSelectSubcol: (subId: string) => void,
 *   onStartSubcol: () => void,
 * }} p
 * @returns {TemplateResult}
 */
function renderCompactAncestorSubcollections(p) {
  const subs = p.docState?.subcollections ?? [];
  return html`
    <section class="miller-col">
      <div class="col-header">
        <span class="mono" title=${p.docPath}>${p.docId}</span>
        <div class="col-header-actions">
          <button
            type="button"
            class="btn btn-sm"
            title="Inspect parent document fields"
            @click=${p.onSelectDocItself}
          >
            Inspect
          </button>
          <button type="button" class="btn btn-sm" @click=${p.onStartSubcol}>
            + Sub
          </button>
        </div>
      </div>
      <ul class="col-list">
        ${subs.map(
          (sub) => html`
            <li
              class="col-item ${p.selectedSubcol === sub ? "selected" : ""}"
              @click=${() => p.onSelectSubcol(sub)}
            >
              <span class="item-label">${sub}</span>
              <span>›</span>
            </li>
          `
        )}
      </ul>
    </section>
  `;
}

/**
 * @param {{
 *   colId: string,
 *   docId: string,
 *   docPath: string,
 *   parentPath: string,
 *   docSegments: string[],
 *   selectedSubcol: string | null,
 *   docState: DocumentLevelState | undefined,
 *   onSelectSubcol: (subId: string) => void,
 *   onStartSubcol: () => void,
 *   onCreatePhantomDoc: () => void,
 *   onSaveDoc: () => void,
 *   onDeleteDoc: () => void,
 *   onRerender: () => void,
 * }} p
 * @returns {TemplateResult}
 */
function renderDocumentInspector(p) {
  if (!p.docState || (p.docState.loading && !p.docState.editorSession && !p.docState.subcollections.length)) {
    return html`
      <section class="miller-inspector">
        <div class="col-header">
          <span class="mono">${p.docId}</span>
        </div>
        <div class="col-empty">Loading document...</div>
      </section>
    `;
  }

  const subs = p.docState.subcollections;

  return html`
    <section class="miller-inspector">
      <div class="subcollections-section">
        <div class="subcollections-header">
          <span>Subcollections of <span class="mono">${p.docId}</span></span>
          <button type="button" class="btn btn-sm" @click=${p.onStartSubcol}>
            + Start Subcollection
          </button>
        </div>
        ${subs.length === 0
          ? html`<div style="font-size: 12px; color: var(--text-faint);">
              No subcollections under this document.
            </div>`
          : html`
              <div class="subcollection-chips">
                ${subs.map(
                  (sub) => html`
                    <button
                      type="button"
                      class="subcol-chip ${p.selectedSubcol === sub
                        ? "active"
                        : ""}"
                      @click=${() => p.onSelectSubcol(sub)}
                    >
                      <span>${sub}</span>
                      <span>›</span>
                    </button>
                  `
                )}
              </div>
            `}
      </div>

      ${!p.docState.exists || !p.docState.editorSession
        ? html`
            <div class="phantom-card">
              <h3>Non-existent ancestor document (<i>${p.docId}</i>)</h3>
              <p>
                This document does not exist in Firestore; it appears in the
                hierarchy because one or more subcollections exist underneath
                <span class="mono">${p.docSegments.join("/")}</span>. Queries on
                collection <span class="mono">${p.colId}</span> will not match
                this document unless you create it.
              </p>
              <div style="display: flex; gap: 8px; flex-wrap: wrap;">
                <button
                  type="button"
                  class="btn btn-primary"
                  @click=${p.onCreatePhantomDoc}
                >
                  + Create Document at ${p.colId}/${p.docId}
                </button>
                ${subs.length > 0
                  ? html`
                      <button
                        type="button"
                        class="btn btn-danger"
                        @click=${p.onDeleteDoc}
                      >
                        Delete Subcollections Recursively
                      </button>
                    `
                  : nothing}
              </div>
            </div>
          `
        : renderHybridEditor({
            session: p.docState.editorSession,
            onChange: p.onRerender,
            onSave: p.onSaveDoc,
            onDeleteDoc: p.onDeleteDoc,
          })}
    </section>
  `;
}
