# UI (`ui`)

## Overview
Browser-based web interface for inspecting and managing Firestore Emulator state. It consists of a single-page HTML application (`index.html`) that routes based on the browser URL path to browse emulated databases, collections, and documents via the `/emulator/v1` HTTP REST API, paired with a bundled copy of `lit-html@3.1.2` (`lit-html.js`) for declarative DOM rendering.

## Interface
- `[index.html:page]`: IIFE route resolver that matches `location.pathname` against `/` (`dashboard`) or `/projects/<project>/databases/<database>/documents(/<ref>)?` (`database_root` or `ref_root`).
- `[index.html:refresh]`: Executes the active route handler (`page()`) and renders the returned `lit-html` template into `#canvas` (or delegates errors to `showError`).
- `[index.html:showError]`: Renders error messages and stack traces into `#canvas`.
- `[index.html:dashboard]`: Fetches `GET /emulator/v1` and renders the list of active emulator databases.
- `[index.html:database_root]`: Fetches `GET /emulator/v1/projects/<project>/databases/<database>/documents` to list root collections and binds the `clear` handler (`[index.html:clear]`) to delete database contents via `DELETE /emulator/v1/projects/<project>/databases/<database>/documents`.
- `[index.html:ref_root]`: Fetches `GET /emulator/v1/projects/<project>/databases/<database>/documents/<ref>` and renders either a collection's document list (`type === "collection"`) or a document's JSON payload and subcollections (`type === "document"`).
- `[index.html:api]`: Asynchronous `fetch` helper that issues HTTP requests, throws response text on non-2xx statuses, and parses JSON or text responses.
- `[lit-html.js:html]`: Tagged template literal producer for HTML templates (`lit-html@3.1.2`).
- `[lit-html.js:svg]`: Tagged template literal producer for SVG templates (`lit-html@3.1.2`).
- `[lit-html.js:render]`: Renders a `lit-html` `TemplateResult` into a DOM container (`lit-html@3.1.2`).
- `[lit-html.js:noChange]`: Sentinel symbol (`Symbol.for("lit-noChange")`) indicating no DOM update is needed.
- `[lit-html.js:nothing]`: Sentinel symbol (`Symbol.for("lit-nothing")`) that clears a rendered part.
- `[lit-html.js:_$LH]`: Internal `lit-html` primitives export object.

## Invariants
- `[index.html:page]`: Non-root URL paths must match `/^\/projects\/(?<project>[^/]+)\/databases\/(?<database>[^/]+)\/documents(\/(?<ref>.*))?$/`; any non-matching path resets `location.pathname = ""` to redirect to the root dashboard.
- `[index.html:ref_root]`: Expects the `/emulator/v1` document/collection endpoint payload to provide a `type` discriminator of either `"collection"` (with `documents: string[]`) or `"document"` (with `document: unknown` and `collections: string[]`).
- `[lit-html.js:C]`: Template literal calls must pass a valid template strings array possessing a `raw` property, or an `"invalid template strings array"` `Error` is thrown.

## Side Effects
- `[index.html:page]`: Mutates `window.location.pathname` to `""` when the current URL path does not match a valid database or document route.
- `[index.html:refresh]` / `[index.html:showError]`: Mutates the DOM inside `<div id="canvas">` via `render()`.
- `[index.html:api]`: Performs network HTTP `GET` and `DELETE` requests against `/emulator/v1` endpoints via `fetch()`.
- `[index.html:clear]`: Issues an HTTP `DELETE` request to `/emulator/v1/projects/<project>/databases/<database>/documents` to wipe the selected database and refreshes the UI.
- `[lit-html.js:s]`: Registers a browser `trustedTypes` policy named `"lit-html"` when `globalThis.trustedTypes` is available, and appends `"3.1.2"` to `globalThis.litHtmlVersions`.

## Verification
- Run: `cargo test`
