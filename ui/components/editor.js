// @ts-check

import { html, nothing } from "../lit-html.js";
import {
  VALUE_TYPES,
  createDefaultValue,
  decodeBytes,
  encodeBytes,
  escapeFieldPathSegment,
  formatDoubleInput,
  getValueType,
  normalizeReference,
  validateDouble,
  validateGeoPoint,
  validateInteger64,
  validateRawProtoJsonFields,
  validateTimestamp,
} from "../values.js";

/** @typedef {import('../values.js').ValueTypeKey} ValueTypeKey */
/** @typedef {import('../values.js').FirestoreValue} FirestoreValue */
/** @typedef {import('../values.js').FirestoreDocument} FirestoreDocument */
/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */

/**
 * @typedef {{
 *   id: number,
 *   key: string,
 *   type: ValueTypeKey,
 *   text: string,
 *   boolVal: boolean,
 *   latText: string,
 *   lngText: string,
 *   bytesMode: 'base64' | 'utf8' | 'hex',
 *   children: DraftNode[],
 * }} DraftNode
 */

/**
 * @typedef {{
 *   docPath: string,
 *   projectId: string,
 *   databaseId: string,
 *   mode: 'structured' | 'raw',
 *   cleanDoc: FirestoreDocument | null,
 *   cleanJson: string,
 *   nodes: DraftNode[],
 *   rawText: string,
 *   rawError: string | null,
 *   isDirty: boolean,
 *   saving: boolean,
 *   saveError: string | null,
 *   externalDoc: FirestoreDocument | null,
 * }} EditorSession
 */

let nextNodeId = 1;

/**
 * Converts a ProtoJSON fields map into an editable DraftNode list.
 * @param {Record<string, FirestoreValue> | undefined} fields
 * @returns {DraftNode[]}
 */
export function fieldsToDraftNodes(fields) {
  if (!fields) return [];
  return Object.entries(fields)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, val]) => valueToDraftNode(key, val));
}

/**
 * @param {string} key
 * @param {FirestoreValue} val
 * @returns {DraftNode}
 */
function valueToDraftNode(key, val) {
  const type = getValueType(val);
  /** @type {DraftNode} */
  const node = {
    id: nextNodeId++,
    key,
    type,
    text: "",
    boolVal: false,
    latText: "0",
    lngText: "0",
    bytesMode: "base64",
    children: [],
  };

  switch (type) {
    case "stringValue":
      node.text = val.stringValue ?? "";
      break;
    case "integerValue":
      node.text = String(val.integerValue ?? "0");
      break;
    case "doubleValue":
      node.text = formatDoubleInput(val.doubleValue);
      break;
    case "booleanValue":
      node.boolVal = Boolean(val.booleanValue);
      break;
    case "nullValue":
      break;
    case "timestampValue":
      node.text = val.timestampValue ?? new Date().toISOString();
      break;
    case "referenceValue":
      node.text = val.referenceValue ?? "";
      break;
    case "bytesValue":
      node.text = val.bytesValue ?? "";
      break;
    case "geoPointValue":
      node.latText = String(val.geoPointValue?.latitude ?? 0);
      node.lngText = String(val.geoPointValue?.longitude ?? 0);
      break;
    case "arrayValue":
      node.children = (val.arrayValue?.values ?? []).map((item, idx) =>
        valueToDraftNode(String(idx), item)
      );
      break;
    case "mapValue":
      node.children = fieldsToDraftNodes(val.mapValue?.fields);
      break;
  }
  return node;
}

/**
 * Validates a DraftNode list as a map and converts it to a ProtoJSON fields object.
 * @param {DraftNode[]} nodes
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {{ valid: boolean, fields?: Record<string, FirestoreValue>, errors: Map<number, string> }}
 */
export function draftNodesToFields(nodes, projectId, databaseId) {
  /** @type {Map<number, string>} */
  const errors = new Map();
  const fields = collectMapNodes(nodes, projectId, databaseId, errors);
  return {
    valid: errors.size === 0,
    fields: errors.size === 0 ? fields : undefined,
    errors,
  };
}

/**
 * @param {DraftNode[]} nodes
 * @param {string} projectId
 * @param {string} databaseId
 * @param {Map<number, string>} errors
 * @returns {Record<string, FirestoreValue>}
 */
function collectMapNodes(nodes, projectId, databaseId, errors) {
  /** @type {Record<string, FirestoreValue>} */
  const out = {};
  /** @type {Set<string>} */
  const seenKeys = new Set();

  for (const node of nodes) {
    const trimmedKey = node.key.trim();
    if (!trimmedKey) {
      errors.set(node.id, "Field name is required");
    } else if (seenKeys.has(trimmedKey)) {
      errors.set(node.id, `Duplicate field name "${trimmedKey}"`);
    } else {
      seenKeys.add(trimmedKey);
    }
    const val = convertSingleNode(node, projectId, databaseId, false, errors);
    if (trimmedKey && val) {
      out[trimmedKey] = val;
    }
  }
  return out;
}

/**
 * @param {DraftNode} node
 * @param {string} projectId
 * @param {string} databaseId
 * @param {boolean} inArray
 * @param {Map<number, string>} errors
 * @returns {FirestoreValue | null}
 */
function convertSingleNode(node, projectId, databaseId, inArray, errors) {
  switch (node.type) {
    case "nullValue":
      return { nullValue: null };

    case "booleanValue":
      return { booleanValue: node.boolVal };

    case "stringValue":
      return { stringValue: node.text };

    case "integerValue": {
      const check = validateInteger64(node.text);
      if (!check.valid || !check.canonical) {
        errors.set(node.id, check.error ?? "Invalid int64");
        return null;
      }
      return { integerValue: check.canonical };
    }

    case "doubleValue": {
      const check = validateDouble(node.text);
      if (!check.valid || check.value === undefined) {
        errors.set(node.id, check.error ?? "Invalid double");
        return null;
      }
      return { doubleValue: check.value };
    }

    case "timestampValue": {
      const check = validateTimestamp(node.text);
      if (!check.valid) {
        errors.set(node.id, check.error ?? "Invalid timestamp");
        return null;
      }
      return { timestampValue: node.text.trim() };
    }

    case "referenceValue": {
      const check = normalizeReference(node.text, projectId, databaseId);
      if (!check.valid || !check.fullPath) {
        errors.set(node.id, check.error ?? "Invalid reference");
        return null;
      }
      return { referenceValue: check.fullPath };
    }

    case "bytesValue": {
      const check = encodeBytes(node.text, node.bytesMode);
      if (!check.valid || check.base64 === undefined) {
        errors.set(node.id, check.error ?? "Invalid bytes");
        return null;
      }
      return { bytesValue: check.base64 };
    }

    case "geoPointValue": {
      const check = validateGeoPoint(node.latText, node.lngText);
      if (!check.valid) {
        errors.set(node.id, check.error ?? "Invalid geopoint");
        return null;
      }
      return {
        geoPointValue: {
          latitude: check.latitude,
          longitude: check.longitude,
        },
      };
    }

    case "arrayValue": {
      if (inArray) {
        errors.set(node.id, "Nested arrays inside arrays are not allowed");
        return null;
      }
      /** @type {FirestoreValue[]} */
      const values = [];
      for (const child of node.children) {
        const v = convertSingleNode(child, projectId, databaseId, true, errors);
        if (v) values.push(v);
      }
      return { arrayValue: { values } };
    }

    case "mapValue": {
      const fields = collectMapNodes(node.children, projectId, databaseId, errors);
      return { mapValue: { fields } };
    }
  }
}

/**
 * Creates a fresh EditorSession for an existing document or a blank draft.
 * @param {string} docPath
 * @param {string} projectId
 * @param {string} databaseId
 * @param {FirestoreDocument | null} doc
 * @returns {EditorSession}
 */
export function createEditorSession(docPath, projectId, databaseId, doc) {
  const fields = doc?.fields ?? {};
  const cleanJson = JSON.stringify({ fields }, null, 2);
  return {
    docPath,
    projectId,
    databaseId,
    mode: "structured",
    cleanDoc: doc ? structuredClone(doc) : null,
    cleanJson,
    nodes: fieldsToDraftNodes(fields),
    rawText: cleanJson,
    rawError: null,
    isDirty: false,
    saving: false,
    saveError: null,
    externalDoc: null,
  };
}

/**
 * Replaces an EditorSession's clean state with a newly saved or reloaded document.
 * @param {EditorSession} session
 * @param {FirestoreDocument} doc
 */
export function syncSessionWithDocument(session, doc) {
  const fields = doc.fields ?? {};
  const cleanJson = JSON.stringify({ fields }, null, 2);
  session.cleanDoc = structuredClone(doc);
  session.cleanJson = cleanJson;
  session.nodes = fieldsToDraftNodes(fields);
  session.rawText = cleanJson;
  session.rawError = null;
  session.isDirty = false;
  session.saving = false;
  session.saveError = null;
  session.externalDoc = null;
}

/**
 * Extracts validated ProtoJSON fields from an EditorSession (either mode).
 * @param {EditorSession} session
 * @returns {{ valid: boolean, fields?: Record<string, FirestoreValue>, error?: string }}
 */
export function extractSessionFields(session) {
  if (session.mode === "raw") {
    const res = validateRawProtoJsonFields(session.rawText);
    session.rawError = res.valid ? null : (res.error ?? "Invalid ProtoJSON");
    return res;
  }
  const treeRes = draftNodesToFields(
    session.nodes,
    session.projectId,
    session.databaseId
  );
  if (!treeRes.valid || !treeRes.fields) {
    const firstErr = treeRes.errors.values().next().value;
    return {
      valid: false,
      error: firstErr ?? "Fix validation errors highlighted in red",
    };
  }
  return { valid: true, fields: treeRes.fields };
}

/**
 * Computes changed top-level field mask paths between cleanFields and nextFields.
 * @param {Record<string, FirestoreValue>} cleanFields
 * @param {Record<string, FirestoreValue>} nextFields
 * @returns {string[]}
 */
export function computeChangedFieldMask(cleanFields, nextFields) {
  const allKeys = new Set([
    ...Object.keys(cleanFields),
    ...Object.keys(nextFields),
  ]);
  /** @type {string[]} */
  const mask = [];
  for (const key of allKeys) {
    const a = JSON.stringify(cleanFields[key]);
    const b = JSON.stringify(nextFields[key]);
    if (a !== b) {
      mask.push(escapeFieldPathSegment(key));
    }
  }
  return mask;
}

/**
 * @param {EditorSession} session
 */
function recomputeDirty(session) {
  if (session.mode === "raw") {
    session.isDirty = session.rawText.trim() !== session.cleanJson.trim();
    return;
  }
  const res = draftNodesToFields(
    session.nodes,
    session.projectId,
    session.databaseId
  );
  if (!res.valid || !res.fields) {
    session.isDirty = true;
    return;
  }
  const currentJson = JSON.stringify({ fields: res.fields }, null, 2);
  session.isDirty = currentJson !== session.cleanJson;
}

/**
 * Renders the reusable Hybrid Editor (Structured Tree + Raw ProtoJSON) for either
 * the main document inspector or a Create Document modal.
 *
 * @param {{
 *   session: EditorSession,
 *   onChange: () => void,
 *   onSave?: () => void,
 *   onDeleteDoc?: () => void,
 *   embeddedInModal?: boolean,
 * }} props
 * @returns {TemplateResult}
 */
export function renderHybridEditor(props) {
  const { session, onChange, onSave, onDeleteDoc, embeddedInModal = false } = props;
  const validation =
    session.mode === "structured"
      ? draftNodesToFields(session.nodes, session.projectId, session.databaseId)
      : null;

  const switchMode = (/** @type {'structured' | 'raw'} */ targetMode) => {
    if (session.mode === targetMode) return;
    session.saveError = null;
    if (targetMode === "raw") {
      const check = draftNodesToFields(
        session.nodes,
        session.projectId,
        session.databaseId
      );
      if (!check.valid || !check.fields) {
        const firstErr = check.errors.values().next().value;
        session.saveError = `Cannot switch to Raw ProtoJSON until field errors are fixed: ${firstErr}`;
        onChange();
        return;
      }
      session.rawText = JSON.stringify({ fields: check.fields }, null, 2);
      session.rawError = null;
      session.mode = "raw";
    } else {
      const rawCheck = validateRawProtoJsonFields(session.rawText);
      if (!rawCheck.valid || !rawCheck.fields) {
        session.rawError = rawCheck.error ?? "Invalid ProtoJSON";
        onChange();
        return;
      }
      session.nodes = fieldsToDraftNodes(rawCheck.fields);
      session.rawError = null;
      session.mode = "structured";
    }
    recomputeDirty(session);
    onChange();
  };

  const addRootField = () => {
    const existing = new Set(session.nodes.map((n) => n.key));
    let idx = session.nodes.length + 1;
    let name = `field_${idx}`;
    while (existing.has(name)) {
      idx++;
      name = `field_${idx}`;
    }
    session.nodes.push(
      valueToDraftNode(
        name,
        createDefaultValue("stringValue", session.projectId, session.databaseId)
      )
    );
    recomputeDirty(session);
    onChange();
  };

  const revertEdits = () => {
    if (session.cleanDoc) {
      syncSessionWithDocument(session, session.cleanDoc);
    } else {
      session.nodes = [];
      session.rawText = JSON.stringify({ fields: {} }, null, 2);
      session.rawError = null;
      session.saveError = null;
      session.isDirty = false;
    }
    onChange();
  };

  return html`
    <div class="editor-container">
      <div class="editor-toolbar">
        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
          <div class="segmented">
            <button
              type="button"
              class=${session.mode === "structured" ? "active" : ""}
              @click=${() => switchMode("structured")}
            >
              Structured Fields
            </button>
            <button
              type="button"
              class=${session.mode === "raw" ? "active" : ""}
              @click=${() => switchMode("raw")}
            >
              Raw ProtoJSON
            </button>
          </div>
          ${session.mode === "structured"
            ? html`
                <button type="button" class="btn btn-sm" @click=${addRootField}>
                  + Add Field
                </button>
              `
            : nothing}
        </div>

        ${!embeddedInModal
          ? html`
              <div style="display: flex; align-items: center; gap: 6px;">
                ${session.isDirty
                  ? html`
                      <button
                        type="button"
                        class="btn btn-sm"
                        ?disabled=${session.saving}
                        @click=${revertEdits}
                      >
                        Revert
                      </button>
                      <button
                        type="button"
                        class="btn btn-sm btn-primary"
                        ?disabled=${session.saving}
                        @click=${() => onSave && onSave()}
                      >
                        ${session.saving ? "Saving..." : "Save Changes"}
                      </button>
                    `
                  : nothing}
                ${onDeleteDoc
                  ? html`
                      <button
                        type="button"
                        class="btn btn-sm btn-danger"
                        @click=${onDeleteDoc}
                      >
                        Delete Document
                      </button>
                    `
                  : nothing}
              </div>
            `
          : nothing}
      </div>

      ${!embeddedInModal && session.cleanDoc
        ? html`
            <div class="doc-meta-bar">
              <span class="mono" title=${session.docPath}>${session.docPath}</span>
              <span>
                Created:
                <strong class="mono">${session.cleanDoc.createTime ?? "—"}</strong>
                &nbsp;|&nbsp; Updated:
                <strong class="mono">${session.cleanDoc.updateTime ?? "—"}</strong>
              </span>
            </div>
          `
        : nothing}

      ${session.externalDoc
        ? html`
            <div class="conflict-banner">
              <span>
                This document was updated externally while you have unsaved edits.
              </span>
              <div style="display: flex; gap: 6px;">
                <button
                  type="button"
                  class="btn btn-sm"
                  @click=${() => {
                    if (session.externalDoc) {
                      syncSessionWithDocument(session, session.externalDoc);
                      onChange();
                    }
                  }}
                >
                  Reload external version
                </button>
                <button
                  type="button"
                  class="btn btn-sm"
                  @click=${() => {
                    if (session.externalDoc && session.cleanDoc) {
                      session.cleanDoc.updateTime = session.externalDoc.updateTime;
                    }
                    session.externalDoc = null;
                    onChange();
                  }}
                >
                  Keep my edits
                </button>
              </div>
            </div>
          `
        : nothing}

      ${session.saveError
        ? html`<div class="error-banner">${session.saveError}</div>`
        : nothing}

      <div class="editor-body">
        ${session.mode === "structured"
          ? renderStructuredTree(
              session,
              session.nodes,
              false,
              validation?.errors ?? new Map(),
              onChange
            )
          : renderRawProtoJson(session, onChange)}
      </div>
    </div>
  `;
}

/**
 * @param {EditorSession} session
 * @param {DraftNode[]} nodes
 * @param {boolean} parentIsArray
 * @param {Map<number, string>} errors
 * @param {() => void} onChange
 * @returns {TemplateResult}
 */
function renderStructuredTree(session, nodes, parentIsArray, errors, onChange) {
  if (nodes.length === 0) {
    return html`
      <div class="col-empty">
        ${parentIsArray
          ? "Empty array — click + Item to add an element."
          : "No fields in this map — click + Add Field to add one."}
      </div>
    `;
  }

  return html`
    <div class="field-tree">
      ${nodes.map((node, index) =>
        renderDraftNode(session, nodes, node, index, parentIsArray, errors, onChange)
      )}
    </div>
  `;
}

/**
 * @param {EditorSession} session
 * @param {DraftNode[]} siblings
 * @param {DraftNode} node
 * @param {number} index
 * @param {boolean} parentIsArray
 * @param {Map<number, string>} errors
 * @param {() => void} onChange
 * @returns {TemplateResult}
 */
function renderDraftNode(
  session,
  siblings,
  node,
  index,
  parentIsArray,
  errors,
  onChange
) {
  const errText = errors.get(node.id);
  const allowedTypes = parentIsArray
    ? VALUE_TYPES.filter((t) => t.key !== "arrayValue")
    : VALUE_TYPES;

  const onTypeChange = (/** @type {Event} */ ev) => {
    const newType = /** @type {ValueTypeKey} */ (
      /** @type {HTMLSelectElement} */ (ev.target).value
    );
    const fresh = valueToDraftNode(
      node.key,
      createDefaultValue(newType, session.projectId, session.databaseId)
    );
    node.type = fresh.type;
    node.text = fresh.text;
    node.boolVal = fresh.boolVal;
    node.latText = fresh.latText;
    node.lngText = fresh.lngText;
    node.bytesMode = "base64";
    node.children = fresh.children;
    session.saveError = null;
    recomputeDirty(session);
    onChange();
  };

  const removeNode = () => {
    siblings.splice(index, 1);
    session.saveError = null;
    recomputeDirty(session);
    onChange();
  };

  const addChild = () => {
    if (node.type === "arrayValue") {
      node.children.push(
        valueToDraftNode(
          String(node.children.length),
          createDefaultValue("stringValue", session.projectId, session.databaseId)
        )
      );
    } else if (node.type === "mapValue") {
      const existing = new Set(node.children.map((c) => c.key));
      let n = node.children.length + 1;
      let k = `key_${n}`;
      while (existing.has(k)) {
        n++;
        k = `key_${n}`;
      }
      node.children.push(
        valueToDraftNode(
          k,
          createDefaultValue("stringValue", session.projectId, session.databaseId)
        )
      );
    }
    session.saveError = null;
    recomputeDirty(session);
    onChange();
  };

  return html`
    <div class="field-node">
      <div class="field-row">
        ${parentIsArray
          ? html`<span class="field-index-label">[${index}]</span>`
          : html`
              <input
                type="text"
                class="input field-key-input ${!node.key.trim() ? "input-error" : ""}"
                placeholder="field name"
                .value=${node.key}
                @input=${(/** @type {Event} */ e) => {
                  node.key = /** @type {HTMLInputElement} */ (e.target).value;
                  session.saveError = null;
                  recomputeDirty(session);
                  onChange();
                }}
              />
            `}

        <select
          class="select field-type-select"
          .value=${node.type}
          @change=${onTypeChange}
        >
          ${allowedTypes.map(
            (t) =>
              html`<option value=${t.key} ?selected=${node.type === t.key}>
                ${t.label}
              </option>`
          )}
        </select>

        <span class="type-badge ${node.type}">
          ${VALUE_TYPES.find((t) => t.key === node.type)?.shortLabel ?? node.type}
        </span>

        <div class="field-value-wrap">
          ${renderValueEditorControl(session, node, Boolean(errText), onChange)}
        </div>

        ${node.type === "mapValue" || node.type === "arrayValue"
          ? html`
              <button type="button" class="btn btn-sm" @click=${addChild}>
                ${node.type === "arrayValue" ? "+ Item" : "+ Field"}
              </button>
            `
          : nothing}

        <button
          type="button"
          class="btn btn-sm btn-ghost"
          title="Remove field"
          @click=${removeNode}
        >
          ✕
        </button>

        ${errText ? html`<div class="field-error-text">${errText}</div>` : nothing}
      </div>

      ${node.type === "mapValue" || node.type === "arrayValue"
        ? html`
            <div class="field-node nested">
              ${renderStructuredTree(
                session,
                node.children,
                node.type === "arrayValue",
                errors,
                onChange
              )}
            </div>
          `
        : nothing}
    </div>
  `;
}

/**
 * @param {EditorSession} session
 * @param {DraftNode} node
 * @param {boolean} hasError
 * @param {() => void} onChange
 * @returns {TemplateResult}
 */
function renderValueEditorControl(session, node, hasError, onChange) {
  const errClass = hasError ? "input-error" : "";

  const updateText = (/** @type {Event} */ e) => {
    node.text = /** @type {HTMLInputElement} */ (e.target).value;
    session.saveError = null;
    recomputeDirty(session);
    onChange();
  };

  switch (node.type) {
    case "nullValue":
      return html`<span class="mono" style="color: var(--text-faint);">null</span>`;

    case "booleanValue":
      return html`
        <select
          class="select"
          .value=${String(node.boolVal)}
          @change=${(/** @type {Event} */ e) => {
            node.boolVal =
              /** @type {HTMLSelectElement} */ (e.target).value === "true";
            session.saveError = null;
            recomputeDirty(session);
            onChange();
          }}
        >
          <option value="true" ?selected=${node.boolVal}>true</option>
          <option value="false" ?selected=${!node.boolVal}>false</option>
        </select>
      `;

    case "integerValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          placeholder="64-bit integer (e.g. 9223372036854775807)"
          .value=${node.text}
          @input=${updateText}
        />
      `;

    case "doubleValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          placeholder="64-bit float (e.g. 1.5, -0.0, NaN, Infinity)"
          .value=${node.text}
          @input=${updateText}
        />
      `;

    case "stringValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          placeholder="string value"
          .value=${node.text}
          @input=${updateText}
        />
      `;

    case "timestampValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          placeholder="YYYY-MM-DDTHH:MM:SS.fffffffffZ"
          .value=${node.text}
          @input=${updateText}
        />
        <button
          type="button"
          class="btn btn-sm"
          title="Set to current UTC timestamp"
          @click=${() => {
            node.text = new Date().toISOString();
            session.saveError = null;
            recomputeDirty(session);
            onChange();
          }}
        >
          Now
        </button>
      `;

    case "referenceValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          placeholder="users/alice or projects/..."
          .value=${node.text}
          @input=${updateText}
        />
      `;

    case "bytesValue": {
      const switchBytesMode = (/** @type {'base64' | 'utf8' | 'hex'} */ nextMode) => {
        if (node.bytesMode === nextMode) return;
        const encoded = encodeBytes(node.text, node.bytesMode);
        if (encoded.valid && encoded.base64 !== undefined) {
          const decoded = decodeBytes(encoded.base64, nextMode);
          if (decoded.ok) {
            node.text = decoded.text;
            node.bytesMode = nextMode;
          }
        } else {
          node.bytesMode = nextMode;
        }
        onChange();
      };

      return html`
        <select
          class="select"
          style="flex: 0 0 auto; font-size: 11.5px;"
          .value=${node.bytesMode}
          @change=${(/** @type {Event} */ e) =>
            switchBytesMode(
              /** @type {'base64' | 'utf8' | 'hex'} */ (
                /** @type {HTMLSelectElement} */ (e.target).value
              )
            )}
        >
          <option value="base64" ?selected=${node.bytesMode === "base64"}>
            Base64
          </option>
          <option value="utf8" ?selected=${node.bytesMode === "utf8"}>UTF-8</option>
          <option value="hex" ?selected=${node.bytesMode === "hex"}>Hex</option>
        </select>
        <input
          type="text"
          class="input ${errClass}"
          placeholder=${node.bytesMode === "hex"
            ? "hex bytes (e.g. 0001ff)"
            : node.bytesMode === "utf8"
              ? "UTF-8 text"
              : "padded Base64"}
          .value=${node.text}
          @input=${updateText}
        />
      `;
    }

    case "geoPointValue":
      return html`
        <input
          type="text"
          class="input ${errClass}"
          style="max-width: 120px;"
          placeholder="latitude"
          .value=${node.latText}
          @input=${(/** @type {Event} */ e) => {
            node.latText = /** @type {HTMLInputElement} */ (e.target).value;
            session.saveError = null;
            recomputeDirty(session);
            onChange();
          }}
        />
        <input
          type="text"
          class="input ${errClass}"
          style="max-width: 120px;"
          placeholder="longitude"
          .value=${node.lngText}
          @input=${(/** @type {Event} */ e) => {
            node.lngText = /** @type {HTMLInputElement} */ (e.target).value;
            session.saveError = null;
            recomputeDirty(session);
            onChange();
          }}
        />
      `;

    case "arrayValue":
      return html`
        <span style="color: var(--text-faint); font-size: 12px;">
          ${node.children.length}
          ${node.children.length === 1 ? "element" : "elements"}
        </span>
      `;

    case "mapValue":
      return html`
        <span style="color: var(--text-faint); font-size: 12px;">
          ${node.children.length}
          ${node.children.length === 1 ? "field" : "fields"}
        </span>
      `;
  }
}

/**
 * @param {EditorSession} session
 * @param {() => void} onChange
 * @returns {TemplateResult}
 */
function renderRawProtoJson(session, onChange) {
  return html`
    <div class="raw-json-wrap">
      <div style="font-size: 12px; color: var(--text-muted);">
        Edit Firestore v1 ProtoJSON directly (<code>integerValue</code> as decimal
        string, <code>doubleValue</code> as number or
        <code>"NaN"</code>/<code>"Infinity"</code>, <code>timestampValue</code> as
        RFC 3339).
      </div>
      ${session.rawError
        ? html`<div class="error-banner">${session.rawError}</div>`
        : nothing}
      <textarea
        class="textarea raw-json-textarea ${session.rawError ? "input-error" : ""}"
        spellcheck="false"
        .value=${session.rawText}
        @input=${(/** @type {Event} */ e) => {
          session.rawText = /** @type {HTMLTextAreaElement} */ (e.target).value;
          const res = validateRawProtoJsonFields(session.rawText);
          session.rawError = res.valid ? null : (res.error ?? "Invalid ProtoJSON");
          session.saveError = null;
          recomputeDirty(session);
          onChange();
        }}
      ></textarea>
    </div>
  `;
}
