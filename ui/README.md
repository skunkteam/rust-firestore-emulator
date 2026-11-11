# UI (`ui`)

## Overview
Zero-build, multi-file ES-module web application for inspecting, creating, editing, deleting, and querying Cloud Firestore Emulator state. Served by `crates/emulator-http` under the default `ui` feature (live from disk in debug builds and embedded into the binary via `crates/emulator-http/build.rs` in release builds), it communicates with the core Firestore v1 ProtoJSON REST API (`/v1/projects/{project_id}/databases/{database_id}/...`) and the emulator control plane (`/emulator/v1/...`, including SSE live invalidations on `/emulator/v1/events`) and renders declaratively using vendored `lit-html@3.1.2`.

## Subdirectories
- `components`: [./components/README.md](./components/README.md) - Declarative UI view components for Miller-column collection/document navigation (`explorer.js`), the hybrid structured field-tree and Raw ProtoJSON document editor (`editor.js`), and the structured query runner (`query.js`).

## Interface
- `[index.html]`: Minimal HTML shell mounting `<header id="topbar">`, `<main id="workspace">`, `<div id="modal-root">`, `<div id="toast-root">`, `/ui/styles.css`, and `<script type="module" src="/ui/app.js">`.
- `[styles.css]`: CSS custom properties (automatic light/dark theme tokens), responsive topbar, horizontal Miller-column layout, type badges, hybrid field-tree / Raw ProtoJSON editor styles, query builder layout, modal dialogs, and toast notifications.
- `[app.js]`: Top-level SPA controller managing URL `pushState`/`popstate` routing (`/projects/{project}/databases/{database}/documents/...`), database discovery and bootstrap (`+ Open / Create Database`), inline `STANDARD`/`ENTERPRISE` and `PESSIMISTIC`/`OPTIMISTIC` toggles, debounced SSE live invalidation (`Pause Live Updates` + dirty-editor protection), modals, and `lit-html` root rendering.
- `[api.js:ApiError]`: Structured HTTP client error capturing Google Cloud `google.rpc.Status` fields (`httpStatus`, `rpcStatus`, `message`).
- `[api.js:listDatabases]` / `[api.js:getDatabase]` / `[api.js:patchDatabase]` / `[api.js:clearDatabase]`: Emulator database discovery, metadata inspection, edition/concurrency patching, and database clearing helpers.
- `[api.js:listCollectionIds]` / `[api.js:listDocuments]` / `[api.js:getDocument]` / `[api.js:createDocument]` / `[api.js:updateDocument]` / `[api.js:deleteDocument]` / `[api.js:deleteCollection]` / `[api.js:runQuery]`: Firestore v1 ProtoJSON REST CRUD, recursive prefix deletion, and structured query helpers.
- `[api.js:connectEventStream]`: Connects an `EventSource` to `/emulator/v1/events`, coalescing `change` and `resync` events over a `150ms` trailing debounce window.
- `[values.js:VALUE_TYPES]` / `[values.js:getValueType]` / `[values.js:createDefaultValue]` / `[values.js:formatValuePreview]`: Type descriptors, discriminators, default constructors, and compact preview formatters for all 11 storable Firestore ProtoJSON `Value` types.
- `[values.js:validateInteger64]` / `[values.js:validateDouble]` / `[values.js:validateTimestamp]` / `[values.js:validateGeoPoint]` / `[values.js:encodeBytes]` / `[values.js:decodeBytes]` / `[values.js:normalizeReference]` / `[values.js:escapeFieldPathSegment]` / `[values.js:validateRawProtoJsonFields]`: Lossless client-side validators and encoders (`BigInt`-checked signed 64-bit integers, IEEE-754 doubles including `NaN`/`Infinity`/`-Infinity`/`-0.0`, RFC 3339 timestamps, Base64/UTF-8/hex bytes, resource references, field-mask escaping, and recursive raw ProtoJSON validation).
- `[values.js:generateAutoId]`: Generates a uniform, bias-free 20-character alphanumeric Firestore document ID via `crypto.getRandomValues` rejection sampling.
- `[lit-html.js]` / `[lit-html.d.ts]`: Vendored `lit-html@3.1.2` ES module (`html`, `svg`, `render`, `noChange`, `nothing`) and local ambient TypeScript declarations for `tsc --checkJs --strict`.

## Invariants
- **Zero-Build ES Modules**: Every `.js` module uses browser-native relative ES module imports (`./...` or `../...`) with `// @ts-check` JSDoc annotations and requires zero bundling or transpilation steps.
- **Lossless `i64` vs. `f64` Fidelity (`[values.js:validateInteger64]`, `[values.js:validateDouble]`)**: `integerValue` strings are validated strictly via `BigInt` against `[-9223372036854775808n, 9223372036854775807n]` without `Number` coercion, while `doubleValue` preserves `NaN`, `Infinity`, `-Infinity`, and `-0.0`.
- **Client-Supplied Document IDs (`[app.js:submitModal]`, `[values.js:generateAutoId]`)**: Creating a document always supplies a non-empty `documentId` query parameter (auto-filling with `generateAutoId()` if left blank in the modal).
- **Dirty-Editor SSE Protection (`[app.js:handleSseBatch]`, `[components/editor.js:syncSessionWithDocument]`)**: Incoming SSE updates never overwrite an `EditorSession` with `isDirty === true`; instead `externalDoc` is recorded and surfaced as a non-destructive reload/keep banner.

## Side Effects
- `[app.js]`: Mutates DOM subtrees in `#topbar`, `#workspace`, `#modal-root`, and `#toast-root`, updates browser history via `history.pushState` / `history.replaceState`, and maintains a long-lived `EventSource` connection to `/emulator/v1/events`.
- `[api.js]`: Issues `fetch` HTTP `GET`, `POST`, `PATCH`, and `DELETE` requests to `/v1/projects/...` and `/emulator/v1/...`.
- `[lit-html.js]`: Registers a `"lit-html"` `trustedTypes` policy when supported by the browser and appends `"3.1.2"` to `globalThis.litHtmlVersions`.

## Verification
- Run: `just ui-check`
- Run: `cargo nextest run -p emulator-http --all-features`
