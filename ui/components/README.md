# UI Components (`ui/components`)

## Overview
Stateless `lit-html` view templates and session state helpers powering the three primary workspaces of the Firestore Emulator Web UI: horizontal Miller-column collection/document exploration (`explorer.js`), hybrid structured field-tree and Raw ProtoJSON document editing (`editor.js`), and interactive `StructuredQuery` building and execution (`query.js`).

## Interface
- `[explorer.js:renderExplorer]`: Renders arbitrary-depth horizontal Miller columns (`Root Collections` -> `Documents` -> `Document Inspector & Subcollections` -> nested levels), including phantom ancestor document indicators (`showMissing=true`), collection filters, pagination (`Load more`), and collection/document action triggers.
- `[editor.js:createEditorSession]` / `[editor.js:syncSessionWithDocument]` / `[editor.js:extractSessionFields]` / `[editor.js:computeChangedFieldMask]` / `[editor.js:renderHybridEditor]`: Manages bidirectional conversion and validation between recursive `DraftNode` trees and Raw ProtoJSON `{"fields": ...}` text, computes `updateMask.fieldPaths` diffs (including deleted fields), and renders both the Structured Tree View and Raw ProtoJSON editor tabs.
- `[query.js:createQueryRunnerState]` / `[query.js:buildStructuredQueryFromVisual]` / `[query.js:renderQueryRunner]`: Manages visual filter rows (`EQUAL`, `NOT_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `ARRAY_CONTAINS`, `IN`, `IS_NULL`, `IS_NOT_NULL`, `IS_NAN`, `IS_NOT_NAN`), `orderBy` clauses, `allDescendants` collection-group scope, `limit`, synced Raw `StructuredQuery` JSON, and clickable query result tables.

## Invariants
- `[editor.js:setSessionMode]`: Switching from Raw ProtoJSON mode back to Structured Tree mode is blocked while `rawError !== null` so invalid JSON text cannot corrupt the structured `DraftNode` tree.
- `[editor.js:extractNodeMap]`: Sibling field keys inside any `Document.fields` or `mapValue.fields` object must be non-empty and unique before saving or syncing to ProtoJSON.
- `[query.js:FILTER_OPERATORS]`: Only operators supported by `crates/emulator-database/src/database/query/filter.rs` are exposed in the visual query builder (`ARRAY_CONTAINS_ANY` and `NOT_IN` are intentionally omitted).

## Side Effects
- Pure DOM template construction and in-memory `EditorSession` / `QueryRunnerState` mutation; network I/O is delegated upward to callbacks in `[../app.js]`.

## Verification
- Run: `just ui-check`
