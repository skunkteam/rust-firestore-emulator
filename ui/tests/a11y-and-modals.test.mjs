// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  findClickHandler,
  findElementByClass,
  harness,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as app from "../app.js";
import {
  createEditorSession,
  renderHybridEditor,
} from "../components/editor.js";
import { renderExplorer } from "../components/explorer.js";
import {
  getModalFocusableElements,
  handleModalKeydown,
} from "../components/modals.js";
import {
  createQueryRunnerState,
  renderQueryRunner,
} from "../components/query.js";
import * as stateMod from "../state.js";

describe("Accessibility and modal lifecycle regressions", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("regression_a11y_miller_columns_forms_and_icon_labels", () => {
    const rootPath = "projects/p/databases/(default)/documents";
    const usersColPath = `${rootPath}/users`;
    const aliceDocPath = `${usersColPath}/alice`;
    const postsColPath = `${aliceDocPath}/posts`;
    const post1DocPath = `${postsColPath}/p1`;

    const collectionColumns = new Map([
      [
        usersColPath,
        {
          collectionPath: usersColPath,
          collectionId: "users",
          parentPath: rootPath,
          documents: [
            { id: "alice", fullName: aliceDocPath, phantom: false },
            { id: "ghost", fullName: `${usersColPath}/ghost`, phantom: true },
          ],
          nextPageToken: "",
          filterText: "",
          loading: false,
        },
      ],
      [
        postsColPath,
        {
          collectionPath: postsColPath,
          collectionId: "posts",
          parentPath: aliceDocPath,
          documents: [{ id: "p1", fullName: post1DocPath, phantom: false }],
          nextPageToken: "",
          filterText: "",
          loading: false,
        },
      ],
    ]);

    const session = createEditorSession(post1DocPath, "p", "(default)", {
      name: post1DocPath,
      fields: { title: { stringValue: "Hello" } },
    });

    const documentLevels = new Map([
      [
        aliceDocPath,
        {
          docPath: aliceDocPath,
          docId: "alice",
          exists: true,
          loading: false,
          subcollections: ["posts", "tags"],
          editorSession: null,
        },
      ],
      [
        post1DocPath,
        {
          docPath: post1DocPath,
          docId: "p1",
          exists: true,
          loading: false,
          subcollections: [],
          editorSession: session,
        },
      ],
    ]);

    /** @type {string[] | null} */
    let selectedPath = null;
    const explorerTpl = renderExplorer({
      projectId: "p",
      databaseId: "(default)",
      rootCollections: ["users", "orders"],
      rootLoading: false,
      segments: ["users", "alice", "posts", "p1"],
      collectionColumns,
      documentLevels,
      onSelectSegments: (s) => {
        selectedPath = s;
      },
      onOpenStartCollection: () => {},
      onOpenAddDocument: () => {},
      onOpenDeleteCollection: () => {},
      onOpenDeleteDocument: () => {},
      onOpenQueryCollection: () => {},
      onLoadMoreDocuments: () => {},
      onSaveDocument: () => {},
      onFilterChange: () => {},
      onRerender: () => {},
    });
    const explorerMarkup = renderTemplateMarkup(explorerTpl);

    assert.equal(
      explorerMarkup.includes('<li class="col-item'),
      false,
      "<li> must wrap <button class=col-item>"
    );
    assert.ok(
      explorerMarkup.includes('<nav aria-label="Breadcrumb" class="breadcrumbs">')
    );
    assert.ok(explorerMarkup.includes('aria-current="page"'));
    assert.ok(explorerMarkup.includes('aria-label="Delete collection users"'));
    assert.ok(explorerMarkup.includes('aria-label="Delete collection posts"'));

    const currentTrueMatches = explorerMarkup.match(/aria-current="true"/g) ?? [];
    assert.equal(
      currentTrueMatches.length,
      4,
      "expected aria-current=true on all 4 selected Miller items"
    );

    const clickOrders = findClickHandler(explorerTpl, "orders");
    assert.ok(
      clickOrders,
      "findClickHandler must still match <button class=col-item>"
    );
    clickOrders();
    assert.deepEqual(selectedPath, ["orders"]);

    const editorMarkup = renderTemplateMarkup(
      renderHybridEditor({
        session,
        onChange: () => {},
      })
    );
    assert.ok(editorMarkup.includes('aria-label="Remove field title"'));

    const qState = createQueryRunnerState("users");
    qState.filters.push({
      id: 1,
      fieldPath: "age",
      op: "EQUAL",
      valueType: "integerValue",
      valueText: "21",
      latText: "0",
      lngText: "0",
      valueTypeManuallySet: false,
      valueUserEdited: false,
    });
    qState.orders.push({ id: 2, fieldPath: "age", direction: "ASCENDING" });
    const queryMarkup = renderTemplateMarkup(
      renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["users"],
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      })
    );
    assert.ok(
      queryMarkup.includes('for="query-parent-subpath"') &&
        queryMarkup.includes('id="query-parent-subpath"')
    );
    assert.ok(
      queryMarkup.includes('for="query-collection-id"') &&
        queryMarkup.includes('id="query-collection-id"')
    );
    assert.ok(
      queryMarkup.includes('for="query-all-descendants"') &&
        queryMarkup.includes('id="query-all-descendants"')
    );
    assert.ok(
      queryMarkup.includes('for="query-limit"') &&
        queryMarkup.includes('id="query-limit"')
    );
    assert.ok(queryMarkup.includes('aria-label="Remove filter 1"'));
    assert.ok(queryMarkup.includes('aria-label="Remove sort order 1"'));

    app.state.databases = [{ projectId: "p", databaseId: "(default)", rootPath }];
    app.state.toasts = [{ id: 1, text: "Saved", tone: "success" }];
    const topbarMarkup = renderTemplateMarkup(app.renderTopbar());
    const toastsMarkup = renderTemplateMarkup(app.renderToasts());
    assert.ok(topbarMarkup.includes('aria-label="Select database"'));
    assert.equal(
      topbarMarkup.includes("aria-pressed"),
      false,
      ".sse-badge dynamic label must not pair aria-pressed"
    );
    assert.ok(toastsMarkup.includes('aria-label="Dismiss notification"'));
  });

  test("regression_a11y_modals_aria_labels_escape_focus_and_backdrop", () => {
    /** @type {Array<{ kind: string, modal: import('../state.js').ActiveModal, titleId: string, expectedForIds: string[] }>} */
    const modalFixtures = [
      {
        kind: "open_db",
        modal: {
          kind: "open_db",
          projectId: "p",
          databaseId: "(default)",
          edition: "STANDARD",
          busy: false,
          error: null,
        },
        titleId: "modal-title-open-db",
        expectedForIds: [
          "modal-open-db-project",
          "modal-open-db-database",
          "modal-open-db-edition",
        ],
      },
      {
        kind: "create_doc",
        modal: {
          kind: "create_doc",
          parentPath: "projects/p/databases/(default)/documents",
          collectionId: "",
          lockCollection: false,
          docId: "d1",
          lockDocId: false,
          session: createEditorSession("", "p", "(default)", null),
          busy: false,
          error: null,
        },
        titleId: "modal-title-create-doc",
        expectedForIds: ["modal-create-doc-collection", "modal-create-doc-id"],
      },
      {
        kind: "delete_doc",
        modal: {
          kind: "delete_doc",
          docPath: "projects/p/databases/(default)/documents/users/alice",
          recursive: false,
          hasSubcollections: true,
          busy: false,
          error: null,
        },
        titleId: "modal-title-delete-doc",
        expectedForIds: ["modal-delete-doc-recursive"],
      },
      {
        kind: "delete_col",
        modal: {
          kind: "delete_col",
          colPath: "projects/p/databases/(default)/documents/users",
          colId: "users",
          busy: false,
          error: null,
        },
        titleId: "modal-title-delete-col",
        expectedForIds: [],
      },
      {
        kind: "clear_db",
        modal: {
          kind: "clear_db",
          projectId: "p",
          databaseId: "(default)",
          busy: false,
          error: null,
        },
        titleId: "modal-title-clear-db",
        expectedForIds: [],
      },
    ];

    for (const fx of modalFixtures) {
      app.state.modal = fx.modal;
      const markup = renderTemplateMarkup(app.renderModal());
      assert.ok(markup.includes('role="dialog"'), `${fx.kind} missing role=dialog`);
      assert.ok(
        markup.includes('aria-modal="true"'),
        `${fx.kind} missing aria-modal=true`
      );
      assert.ok(
        markup.includes(`aria-labelledby="${fx.titleId}"`),
        `${fx.kind} missing aria-labelledby`
      );
      assert.ok(
        markup.includes(`<h2 id="${fx.titleId}"`),
        `${fx.kind} missing matching heading id`
      );
      assert.ok(
        markup.includes('aria-label="Close dialog"'),
        `${fx.kind} missing Close dialog aria-label`
      );
      assert.equal(
        markup.includes('aria-label="Close dialog" disabled'),
        false,
        `${fx.kind} idle Close dialog must not be disabled`
      );
      assert.equal(
        /<label(?![^>]*\bfor=)/.test(markup),
        false,
        `${fx.kind} has unassociated <label>`
      );
      for (const id of fx.expectedForIds) {
        assert.ok(markup.includes(`for="${id}"`), `${fx.kind} missing for=${id}`);
        assert.ok(markup.includes(`id="${id}"`), `${fx.kind} missing id=${id}`);
      }
      fx.modal.busy = true;
      const busyMarkup = renderTemplateMarkup(app.renderModal());
      assert.ok(
        busyMarkup.includes('aria-label="Close dialog" disabled'),
        `${fx.kind} busy Close dialog must be disabled`
      );
      fx.modal.busy = false;
    }

    // 2. Safe backdrop dismissal.
    {
      app.state.modal = {
        kind: "clear_db",
        projectId: "p",
        databaseId: "(default)",
        busy: false,
        error: null,
      };
      const backdropElInfo = findElementByClass(
        app.renderModal(),
        "modal-backdrop"
      );
      assert.ok(
        backdropElInfo?.handlers.mousedown && backdropElInfo.handlers.click,
        "expected mousedown & click on .modal-backdrop"
      );
      const handlers = backdropElInfo.handlers;

      const backdropEl = { kind: "backdrop" };
      const innerCardEl = { kind: "card" };

      handlers.mousedown({ target: innerCardEl, currentTarget: backdropEl });
      handlers.click({ target: backdropEl, currentTarget: backdropEl });
      assert.ok(
        app.state.modal !== null,
        "drag-release onto backdrop must not dismiss modal"
      );

      handlers.mousedown({ target: backdropEl, currentTarget: backdropEl });
      handlers.click({ target: innerCardEl, currentTarget: backdropEl });
      assert.ok(
        app.state.modal !== null,
        "press on backdrop released inside card must not dismiss modal"
      );

      app.state.modal.busy = true;
      handlers.mousedown({ target: backdropEl, currentTarget: backdropEl });
      handlers.click({ target: backdropEl, currentTarget: backdropEl });
      assert.ok(
        app.state.modal !== null,
        "busy modal must not dismiss on backdrop click"
      );

      app.state.modal.busy = false;
      handlers.mousedown({ target: backdropEl, currentTarget: backdropEl });
      handlers.click({ target: backdropEl, currentTarget: backdropEl });
      assert.equal(app.state.modal, null, "direct backdrop click must close modal");
    }

    // 3. Guarded Escape close, keydown listener cleanup, and previousFocus restoration.
    {
      app.state.modal = null;
      const triggerBtn = {
        isConnected: true,
        focusCalls: 0,
        focus() {
          this.focusCalls++;
          harness.activeElement = this;
        },
      };
      harness.activeElement = triggerBtn;

      const openModalBtn = findClickHandler(
        app.renderTopbar(),
        "+ Open / Create Database"
      );
      assert.ok(openModalBtn);
      openModalBtn();
      assert.equal(app.state.modal?.kind, "open_db");
      assert.equal(
        harness.docKeydownListeners.length,
        1,
        "opening modal registers document keydown listener"
      );

      app.state.modal.busy = true;
      let preventedBusy = false;
      harness.docKeydownListeners[0]({
        key: "Escape",
        preventDefault() {
          preventedBusy = true;
        },
      });
      assert.ok(app.state.modal !== null, "Escape must not close busy modal");
      assert.equal(preventedBusy, false);

      app.state.modal.busy = false;
      let preventedComposing = false;
      harness.docKeydownListeners[0]({
        key: "Escape",
        isComposing: true,
        preventDefault() {
          preventedComposing = true;
        },
      });
      assert.ok(
        app.state.modal !== null,
        "isComposing Escape must not close modal"
      );
      assert.equal(preventedComposing, false);

      app.state.modal.busy = false;
      let preventedIdle = false;
      harness.docKeydownListeners[0]({
        key: "Escape",
        preventDefault() {
          preventedIdle = true;
        },
      });
      assert.equal(preventedIdle, true);
      assert.equal(app.state.modal, null);
      assert.equal(
        harness.docKeydownListeners.length,
        0,
        "closing modal removes document keydown listener"
      );
      assert.equal(
        triggerBtn.focusCalls,
        1,
        "closing modal restores focus to trigger button"
      );
    }

    // 4. Tab and Shift+Tab focus wrap within the dialog container.
    {
      const el1 = {
        focusCount: 0,
        hasAttribute: () => false,
        focus() {
          this.focusCount++;
          harness.activeElement = this;
        },
      };
      const el2 = {
        focusCount: 0,
        hasAttribute: () => false,
        focus() {
          this.focusCount++;
          harness.activeElement = this;
        },
      };
      const el3 = {
        focusCount: 0,
        hasAttribute: () => false,
        focus() {
          this.focusCount++;
          harness.activeElement = this;
        },
      };
      const outsideEl = {
        focus() {
          harness.activeElement = this;
        },
      };
      const focusables = [el1, el2, el3];
      const dialogEl = {
        contains: (/** @type {unknown} */ target) =>
          focusables.includes(/** @type {typeof el1} */ (target)),
        querySelectorAll: () => focusables,
        focus() {
          harness.activeElement = this;
        },
      };
      const containerEl = /** @type {HTMLElement} */ (
        /** @type {unknown} */ ({
          querySelector: (/** @type {string} */ sel) =>
            sel === '[role="dialog"]' ? dialogEl : null,
        })
      );

      app.state.modal = {
        kind: "delete_col",
        colPath: "p/d/c",
        colId: "c",
        busy: false,
        error: null,
      };

      const makeKeyEvent = (/** @type {Record<string, unknown>} */ props) =>
        /** @type {KeyboardEvent} */ (/** @type {unknown} */ (props));

      harness.activeElement = el3;
      let tabLastPrevented = false;
      handleModalKeydown(
        makeKeyEvent({
          key: "Tab",
          shiftKey: false,
          preventDefault() {
            tabLastPrevented = true;
          },
        }),
        app.state,
        containerEl,
        () => {}
      );
      assert.equal(tabLastPrevented, true);
      assert.equal(harness.activeElement, el1);

      let shiftTabFirstPrevented = false;
      handleModalKeydown(
        makeKeyEvent({
          key: "Tab",
          shiftKey: true,
          preventDefault() {
            shiftTabFirstPrevented = true;
          },
        }),
        app.state,
        containerEl,
        () => {}
      );
      assert.equal(shiftTabFirstPrevented, true);
      assert.equal(harness.activeElement, el3);

      harness.activeElement = el2;
      let tabMiddlePrevented = false;
      handleModalKeydown(
        makeKeyEvent({
          key: "Tab",
          shiftKey: false,
          preventDefault() {
            tabMiddlePrevented = true;
          },
        }),
        app.state,
        containerEl,
        () => {}
      );
      assert.equal(tabMiddlePrevented, false);

      harness.activeElement = outsideEl;
      let tabOutsidePrevented = false;
      handleModalKeydown(
        makeKeyEvent({
          key: "Tab",
          shiftKey: false,
          preventDefault() {
            tabOutsidePrevented = true;
          },
        }),
        app.state,
        containerEl,
        () => {}
      );
      assert.equal(tabOutsidePrevented, true);
      assert.equal(harness.activeElement, el1);

      harness.activeElement = el3;
      for (const mod of ["ctrlKey", "metaKey", "altKey"]) {
        let modPrevented = false;
        handleModalKeydown(
          makeKeyEvent({
            key: "Tab",
            [mod]: true,
            preventDefault() {
              modPrevented = true;
            },
          }),
          app.state,
          containerEl,
          () => {}
        );
        assert.equal(modPrevented, false, mod + "+Tab must not be intercepted");
      }
      assert.equal(harness.activeElement, el3);

      const hiddenEl = {
        hasAttribute: (/** @type {string} */ attr) => attr === "hidden",
        focus() {},
      };
      const noFocusEl = { hasAttribute: () => false };
      const hiddenCheckDialog = /** @type {Element} */ (
        /** @type {unknown} */ ({
          querySelectorAll: () => [hiddenEl, el1, noFocusEl, el2],
        })
      );
      assert.deepEqual(
        getModalFocusableElements(hiddenCheckDialog),
        /** @type {unknown} */ ([el1, el2]),
        "getModalFocusableElements must filter out [hidden] and non-focusable elements"
      );
      assert.deepEqual(
        getModalFocusableElements(
          /** @type {Element} */ (/** @type {unknown} */ ({}))
        ),
        []
      );

      let emptyDialogFocusCalls = 0;
      const emptyDialogEl = {
        querySelectorAll: () => [hiddenEl],
        focus() {
          emptyDialogFocusCalls++;
          harness.activeElement = this;
        },
      };
      const emptyContainerEl = /** @type {HTMLElement} */ (
        /** @type {unknown} */ ({
          querySelector: (/** @type {string} */ sel) =>
            sel === '[role="dialog"]' ? emptyDialogEl : null,
        })
      );
      harness.activeElement = outsideEl;
      let emptyTabPrevented = false;
      handleModalKeydown(
        makeKeyEvent({
          key: "Tab",
          shiftKey: false,
          preventDefault() {
            emptyTabPrevented = true;
          },
        }),
        app.state,
        emptyContainerEl,
        () => {}
      );
      assert.equal(
        emptyTabPrevented,
        true,
        "Tab with zero focusables must preventDefault"
      );
      assert.equal(
        emptyDialogFocusCalls,
        1,
        "Tab with zero focusables must focus dialog container"
      );
      assert.equal(harness.activeElement, emptyDialogEl);

      app.state.modal = null;
      const detachedTrigger = {
        isConnected: false,
        focusCalls: 0,
        focus() {
          this.focusCalls++;
        },
      };
      harness.activeElement = detachedTrigger;
      const openBtn2 = findClickHandler(
        app.renderTopbar(),
        "+ Open / Create Database"
      );
      assert.ok(openBtn2);
      openBtn2();
      harness.docKeydownListeners[0]({ key: "Escape", preventDefault() {} });
      assert.equal(app.state.modal, null);
      assert.equal(
        detachedTrigger.focusCalls,
        0,
        "disconnected trigger must not be refocused"
      );
    }
  });
});
