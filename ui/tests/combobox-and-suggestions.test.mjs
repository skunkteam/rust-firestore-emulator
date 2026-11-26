// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  deferred,
  findAllElements,
  findClickHandler,
  findElementByClass,
  findElementById,
  findInputByAriaLabel,
  findSelectByAriaLabel,
  harness,
  jsonResponse,
  nextTick,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as app from "../app.js";
import * as combobox from "../components/combobox.js";
import * as query from "../components/query.js";
import * as qsugg from "../components/query-suggestions.js";
import * as stateMod from "../state.js";

describe("Combobox ARIA/geometry/focus and suggestion caching", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("slice2_discovery_suggestions", async () => {
    const rootPath = "projects/p/databases/(default)/documents";

    // 1. Key-only collection-group :runQuery (limit 500), phantom ancestors, uniform direct child seeding,
    //    Partial footer, empty [{readTime}] doc count 0, and odd-segment/invalid parentSubpath skip.
    {
      const store = qsugg.createQuerySuggestionStore();
      /** @type {Array<{ url: string, method: string, body: unknown, hasSignal: boolean }>} */
      const requests = [];
      let rerenders = 0;
      const ctx = {
        rootPath,
        rootCollections: ["empty_root", "orgs"],
        onRerender: () => {
          rerenders++;
        },
      };

      let mode = "root_500";
      harness.fetchImpl = async (url, init) => {
        requests.push({
          url,
          method: String(init.method ?? "GET"),
          body: init.body ? JSON.parse(String(init.body)) : null,
          hasSignal: Boolean(init.signal),
        });
        if (url === `/v1/${rootPath}:runQuery` && mode === "root_500") {
          const rows = Array.from({ length: 499 }, (_, i) => ({
            document: {
              name: `${rootPath}/orgs/org-${i}/projects/p-${i}/tasks/t-${i}`,
            },
            readTime: "2026-01-01T00:00:00Z",
          }));
          rows.push({
            document: { name: `${rootPath}/orgs/acme/audit/a1` },
            readTime: "2026-01-01T00:00:00Z",
          });
          return jsonResponse(rows);
        }
        if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
          return jsonResponse({ collectionIds: ["empty_subcol", "projects"] });
        }
        if (url === `/v1/${rootPath}/orgs/acme:runQuery`) {
          return jsonResponse([{ readTime: "2026-01-01T00:00:00Z" }]);
        }
        throw new Error("unexpected fetch in 1: " + url);
      };

      for (const badParent of [
        "orgs",
        "orgs/acme/projects",
        "__bad__/doc",
        "./a",
        "../a",
        "a..b",
      ]) {
        const skipped = await qsugg.ensureCollectionGroups(store, ctx, badParent);
        assert.deepEqual(skipped.items, []);
      }
      assert.equal(requests.length, 0);

      const rootEntry = await qsugg.ensureCollectionGroups(store, ctx, "");
      assert.equal(requests.length, 1);
      assert.equal(requests[0].hasSignal, true);
      assert.deepEqual(requests[0].body, {
        structuredQuery: {
          from: [{ collectionId: "", allDescendants: true }],
          select: { fields: [{ fieldPath: "__name__" }] },
          limit: 500,
        },
      });
      assert.deepEqual(
        rootEntry.items.map((i) => i.value),
        ["audit", "empty_root", "orgs", "projects", "tasks"]
      );
      assert.equal(rootEntry.partial, true);
      assert.equal(qsugg.formatSuggestionFooter(rootEntry), "Partial");
      assert.ok(store.collectionGroupsByParent.has(""));
      assert.ok(rerenders >= 2);

      mode = "sub_empty";
      const subEntry = await qsugg.ensureCollectionGroups(
        store,
        ctx,
        "/orgs/acme/"
      );
      assert.equal(subEntry.partial, false);
      assert.equal(qsugg.formatSuggestionFooter(subEntry), "");
      assert.deepEqual(
        subEntry.items.map((i) => i.value),
        ["empty_subcol", "projects"]
      );
      assert.ok(store.collectionGroupsByParent.has("orgs/acme"));
      assert.ok(store.subcollectionsByParent.has("orgs/acme"));
    }

    // 2. Slash-triggered parentSubpath alternating listDocuments(showMissing=true) and :listCollectionIds,
    //    dynamic keepOpenOnSelect, and zero Explorer cache dependency.
    {
      const store = qsugg.createQuerySuggestionStore();
      const qState = query.createQueryRunnerState();
      /** @type {Array<{ url: string, method: string }>} */
      const requests = [];

      harness.fetchImpl = async (url, init) => {
        requests.push({ url, method: String(init.method ?? "GET") });
        if (url.startsWith(`/v1/${rootPath}/orgs?`)) {
          assert.ok(url.includes("showMissing=true"));
          assert.ok(url.includes("mask.fieldPaths=__name__"));
          assert.ok(url.includes("pageSize=100"));
          return jsonResponse({
            documents: [
              { name: `${rootPath}/orgs/acme` },
              { name: `${rootPath}/orgs/beta` },
            ],
            nextPageToken: "tok-2",
          });
        }
        if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
          return jsonResponse({ collectionIds: ["projects"] });
        }
        if (url.startsWith(`/v1/${rootPath}/orgs/acme/projects?`)) {
          return jsonResponse({
            documents: [{ name: `${rootPath}/orgs/acme/projects/p1` }],
          });
        }
        throw new Error("unexpected fetch in 2: " + url);
      };

      const renderFn = () =>
        query.renderQueryRunner({
          projectId: "p",
          databaseId: "(default)",
          knownCollections: ["orgs", "users"],
          suggestions: store,
          state: qState,
          onRerender: () => {},
          onExecuteQuery: () => {},
          onOpenResultDocument: () => {},
        });

      for (const invalidPrefix of [
        "/orgs/",
        "orgs//acme/",
        "./",
        "../",
        "__res__/",
      ]) {
        const res = await qsugg.ensureParentSubpathSuggestions(
          store,
          { rootPath, rootCollections: ["orgs"], onRerender: () => {} },
          invalidPrefix
        );
        assert.deepEqual(res.items, []);
      }
      assert.equal(requests.length, 0);

      const colInput = findElementById(renderFn(), "query-collection-id");
      assert.ok(colInput?.handlers.focus);
      colInput.handlers.focus({
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 100,
            bottom: 130,
            left: 10,
            width: 200,
          }),
        },
      });
      assert.equal(store.comboboxUi.openId, "query-collection-id");
      assert.equal(
        requests.length,
        0,
        "root Collection ID must use knownCollections with 0 HTTP calls"
      );
      const colMarkup = renderTemplateMarkup(renderFn());
      assert.ok(colMarkup.includes(">orgs<") || colMarkup.includes("orgs"));

      const parentInput = findElementById(renderFn(), "query-parent-subpath");
      assert.ok(parentInput?.handlers.focus);
      parentInput.handlers.focus({
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 50,
            bottom: 80,
            left: 10,
            width: 200,
          }),
        },
      });
      assert.equal(store.comboboxUi.openId, "query-parent-subpath");
      assert.equal(requests.length, 0);

      findElementById(renderFn(), "query-parent-subpath")?.handlers.keydown({
        key: "ArrowDown",
        preventDefault() {},
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 50,
            bottom: 80,
            left: 10,
            width: 200,
          }),
        },
      });
      assert.equal(store.comboboxUi.activeIndex, 0);
      findElementById(renderFn(), "query-parent-subpath")?.handlers.keydown({
        key: "Enter",
        preventDefault() {},
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 50,
            bottom: 80,
            left: 10,
            width: 200,
          }),
        },
      });
      assert.equal(qState.parentSubpath, "orgs/");
      assert.equal(
        store.comboboxUi.openId,
        "query-parent-subpath",
        "col/ selection must keep popup open"
      );
      assert.equal(
        store.comboboxUi.activeIndex,
        -1,
        "keepOpenOnSelect must reset activeIndex to -1"
      );

      await nextTick();
      assert.equal(requests.length, 1);
      const docEntry = store.docIdsByCollection.get("orgs");
      assert.equal(
        docEntry?.partial,
        true,
        "nextPageToken must mark docIdsByCollection entry partial"
      );
      assert.deepEqual(
        docEntry?.items.map((i) => i.value),
        ["orgs/acme", "orgs/beta"]
      );

      const orgsTpl = renderFn();
      const orgsMarkup = renderTemplateMarkup(orgsTpl);
      assert.ok(orgsMarkup.includes("Partial"));
      assert.ok(orgsMarkup.includes("acme"));

      const acmeClick = findClickHandler(orgsTpl, "acme");
      assert.ok(acmeClick);
      acmeClick();
      assert.equal(qState.parentSubpath, "orgs/acme");
      assert.equal(
        store.comboboxUi.openId,
        null,
        "col/doc selection must close popup"
      );

      findElementById(renderFn(), "query-parent-subpath")?.handlers.input({
        target: { value: "orgs/acme/" },
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 50,
            bottom: 80,
            left: 10,
            width: 200,
          }),
        },
      });
      await nextTick();
      assert.equal(requests.length, 2);
      assert.deepEqual(
        store.subcollectionsByParent.get("orgs/acme")?.items.map((i) => i.value),
        ["projects"]
      );

      qState.parentSubpath = "orgs/acme";
      findElementById(renderFn(), "query-collection-id")?.handlers.focus({
        currentTarget: {
          getBoundingClientRect: () => ({
            top: 100,
            bottom: 130,
            left: 10,
            width: 200,
          }),
        },
      });
      await nextTick();
      assert.equal(
        requests.length,
        2,
        "direct subcollection Collection ID shares subcollectionsByParent cache"
      );
      assert.ok(renderTemplateMarkup(renderFn()).includes("projects"));
    }
  });

  test("slice2_field_sampling_and_auto_type", async () => {
    const rootPath = "projects/p/databases/(default)/documents";
    const store = qsugg.createQuerySuggestionStore();
    /** @type {Array<{ url: string, body: unknown }>} */
    const requests = [];
    const ctx = {
      rootPath,
      rootCollections: ["products"],
      onRerender: () => {},
    };

    const cappedArray = [
      ...Array.from({ length: 32 }, () => ({ integerValue: "1" })),
      ...Array.from({ length: 40 }, () => ({ stringValue: "later" })),
    ];
    const tiedArray = [{ integerValue: "5" }, { stringValue: "a" }];

    /** @type {Record<string, import('../values.js').FirestoreValue>} */
    const protoDocFields = JSON.parse("{}");
    protoDocFields["__proto__"] = { stringValue: "ignored-reserved" };
    protoDocFields["__a\nb__"] = { stringValue: "ignored-newline-reserved" };
    protoDocFields["____"] = { stringValue: "allowed-4-underscores" };
    protoDocFields["constructor"] = { stringValue: "safe-key" };
    protoDocFields["price"] = { doubleValue: 19.99 };
    protoDocFields["nullableInt"] = { nullValue: null };
    protoDocFields["allNull"] = { nullValue: null };
    protoDocFields["mixedVal"] = { integerValue: "1" };
    protoDocFields["active"] = { booleanValue: true };
    protoDocFields["tiedScalar"] = { integerValue: "1" };
    protoDocFields["cappedArr"] = { arrayValue: { values: cappedArray } };
    protoDocFields["tiedArr"] = { arrayValue: { values: tiedArray } };
    protoDocFields["embedding"] = {
      mapValue: {
        fields: {
          __type__: { stringValue: "__vector__" },
          value: { arrayValue: { values: [{ doubleValue: 0.5 }] } },
        },
      },
    };
    protoDocFields["meta"] = {
      mapValue: {
        fields: {
          "display.name": { stringValue: "Widget" },
          d2: {
            mapValue: {
              fields: {
                d3: {
                  mapValue: {
                    fields: {
                      d4: {
                        mapValue: {
                          fields: {
                            d5: {
                              mapValue: {
                                fields: {
                                  d6: { stringValue: "too-deep" },
                                },
                              },
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    };

    const doc2Fields = {
      nullableInt: { integerValue: "42" },
      allNull: { nullValue: null },
      mixedVal: { stringValue: "two" },
      tiedScalar: { stringValue: "wins-tie" },
      mixedScalarAndArr: { booleanValue: true },
    };

    const doc3Fields = {
      mixedScalarAndArr: {
        arrayValue: { values: [{ geoPointValue: { latitude: 1, longitude: 2 } }] },
      },
    };

    let nextMockMode = "three";
    harness.fetchImpl = async (url, init) => {
      requests.push({ url, body: JSON.parse(String(init.body ?? "{}")) });
      if (nextMockMode === "empty") {
        return jsonResponse([{ readTime: "2026-01-01T00:00:00Z" }]);
      }
      if (nextMockMode === "one") {
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/one/d1`,
              fields: { a: { integerValue: "1" } },
            },
          },
        ]);
      }
      if (nextMockMode === "twenty_five") {
        return jsonResponse(
          Array.from({ length: 25 }, (_, i) => ({
            document: {
              name: `${rootPath}/full/d${i}`,
              fields: { k: { integerValue: String(i) } },
            },
          }))
        );
      }
      return jsonResponse([
        { document: { name: `${rootPath}/products/p1`, fields: protoDocFields } },
        { document: { name: `${rootPath}/products/p2`, fields: doc2Fields } },
        { document: { name: `${rootPath}/products/p3`, fields: doc3Fields } },
      ]);
    };

    await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "",
      collectionId: "  ",
      allDescendants: false,
    });
    await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "orgs",
      collectionId: "products",
      allDescendants: false,
    });
    assert.equal(requests.length, 0);

    const entry = await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "",
      collectionId: "products",
      allDescendants: false,
    });
    assert.equal(requests.length, 1);
    assert.deepEqual(requests[0].body, {
      structuredQuery: {
        from: [{ collectionId: "products", allDescendants: false }],
        limit: 25,
      },
    });
    assert.equal(entry.sampledDocs, 3);
    assert.equal(entry.partial, false);
    assert.equal(qsugg.formatSuggestionFooter(entry), "Sampled 3 docs");

    const itemValues = entry.items.map((i) => i.value);
    assert.equal(itemValues.includes("__proto__"), false);
    assert.equal(itemValues.includes("__a\nb__"), false);
    assert.equal(itemValues.includes("____"), true);
    assert.equal(itemValues.includes("constructor"), true);
    assert.equal(itemValues.includes("embedding"), true);
    assert.equal(
      itemValues.includes("embedding.__type__"),
      false,
      "vector map must not expand"
    );
    assert.equal(
      itemValues.includes("embedding.value"),
      false,
      "vector map must not expand"
    );
    assert.equal(itemValues.includes("meta"), true);
    assert.equal(itemValues.includes("meta.`display.name`"), true);
    assert.equal(itemValues.includes("meta.d2.d3.d4.d5"), true, "depth 5 included");
    assert.equal(
      itemValues.includes("meta.d2.d3.d4.d5.d6"),
      false,
      "depth 6 excluded"
    );
    assert.equal(
      itemValues[itemValues.length - 1],
      "__name__",
      "__name__ pinned last in items"
    );
    assert.equal(
      entry.aggItems.some((i) => i.value === "__name__"),
      false,
      "__name__ excluded from aggItems"
    );
    assert.equal(entry.aggItems[0].value, "mixedVal");
    assert.equal(entry.aggItems[1].value, "nullableInt");
    assert.equal(entry.aggItems[2].value, "price");
    assert.equal(
      entry.fieldHints.get("__proto__"),
      undefined,
      "Map prototype safety"
    );

    const byVal = new Map(entry.items.map((i) => [i.value, i]));
    assert.equal(byVal.get("nullableInt")?.badge, "int64");
    assert.equal(byVal.get("mixedVal")?.badge, "mixed");
    assert.equal(byVal.get("allNull")?.badge, "null");
    assert.equal(byVal.get("embedding")?.badge, "vector");
    assert.equal(byVal.get("meta.`display.name`")?.matchText, "meta.display.name");

    assert.equal(entry.fieldHints.get("cappedArr")?.arrayElementType, "integerValue");
    assert.equal(entry.fieldHints.get("tiedArr")?.arrayElementType, "stringValue");
    assert.equal(entry.fieldHints.get("tiedScalar")?.scalarType, "stringValue");

    nextMockMode = "empty";
    const emptyEntry = await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "",
      collectionId: "empty",
      allDescendants: false,
    });
    assert.equal(emptyEntry.sampledDocs, 0);
    assert.equal(qsugg.formatSuggestionFooter(emptyEntry), "Sampled 0 docs");

    nextMockMode = "one";
    const oneEntry = await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "",
      collectionId: "one",
      allDescendants: false,
    });
    assert.equal(qsugg.formatSuggestionFooter(oneEntry), "Sampled 1 doc");

    nextMockMode = "twenty_five";
    const fullEntry = await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "",
      collectionId: "full",
      allDescendants: false,
    });
    assert.equal(fullEntry.partial, true);
    assert.equal(
      qsugg.formatSuggestionFooter(fullEntry),
      "Sampled 25 docs · Partial"
    );
    fullEntry.stale = true;
    assert.equal(
      qsugg.formatSuggestionFooter(fullEntry),
      "Sampled 25 docs · Partial · Stale"
    );

    // 2. Non-clobbering Where filter auto-type detection & booleanValue normalization in renderQueryRunner.
    const qState = query.createQueryRunnerState("products");
    const f = query.createFilterRow();
    assert.equal(f.valueTypeManuallySet, false);
    assert.equal(f.valueUserEdited, false);
    qState.filters.push(f);

    const renderFn = () =>
      query.renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["products"],
        suggestions: store,
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      });

    const filterId = `query-filter-field-${f.id}`;
    const focusAnchor = {
      currentTarget: {
        getBoundingClientRect: () => ({
          top: 100,
          bottom: 130,
          left: 10,
          width: 200,
        }),
      },
    };
    findElementById(renderFn(), filterId)?.handlers.focus(focusAnchor);
    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "display.name" },
    });
    assert.ok(renderTemplateMarkup(renderFn()).includes("meta.`display.name`"));
    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "`display.name`" },
    });
    assert.ok(renderTemplateMarkup(renderFn()).includes("meta.`display.name`"));

    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "active" },
    });
    findClickHandler(renderFn(), "active")?.();
    assert.equal(f.fieldPath, "active");
    assert.equal(f.valueType, "booleanValue");
    assert.equal(f.valueText, "true");
    assert.equal(f.valueUserEdited, false);

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "IN" },
    });
    assert.equal(f.valueText, "[]");
    assert.equal(f.valueUserEdited, false);
    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "EQUAL" },
    });
    assert.equal(f.valueType, "booleanValue");
    assert.equal(f.valueText, "true");
    assert.equal(f.valueUserEdited, false);

    f.valueTypeManuallySet = true;
    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "ARRAY_CONTAINS_ANY" },
    });
    assert.equal(f.valueText, "[]");
    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "EQUAL" },
    });
    assert.equal(f.valueText, "true");
    f.valueTypeManuallySet = false;

    findElementById(renderFn(), filterId)?.handlers.focus(focusAnchor);
    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "price" },
    });
    findClickHandler(renderFn(), "price")?.();
    assert.equal(f.valueType, "doubleValue");
    assert.equal(f.valueText, "");

    findElementById(renderFn(), filterId)?.handlers.focus(focusAnchor);
    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "mixedScalarAndArr" },
    });
    findClickHandler(renderFn(), "mixedScalarAndArr")?.();
    assert.equal(f.valueType, "booleanValue");
    assert.equal(f.valueText, "true");

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "ARRAY_CONTAINS" },
    });
    assert.equal(f.valueType, "geoPointValue");
    assert.equal(f.valueText, "");

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "ARRAY_CONTAINS_ANY" },
    });
    assert.equal(f.valueType, "geoPointValue");
    assert.equal(f.valueText, "[]");

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "IS_NULL" },
    });
    assert.equal(f.valueType, "geoPointValue", "unary IS_NULL preserves valueType");
    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "IS_NAN" },
    });
    assert.equal(f.valueType, "geoPointValue", "unary IS_NAN preserves valueType");

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "NOT_IN" },
    });
    assert.equal(f.valueType, "booleanValue");
    assert.equal(f.valueText, "[]");

    findSelectByAriaLabel(renderFn(), "Filter operator")?.handlers.change({
      target: { value: "EQUAL" },
    });
    assert.equal(f.valueText, "true");
    findSelectByAriaLabel(renderFn(), "Filter boolean value")?.handlers.change({
      target: { value: "false" },
    });
    assert.equal(f.valueUserEdited, true);
    assert.equal(f.valueText, "false");

    findElementById(renderFn(), filterId)?.handlers.focus(focusAnchor);
    findElementById(renderFn(), filterId)?.handlers.input({
      target: { value: "price" },
    });
    findClickHandler(renderFn(), "price")?.();
    assert.equal(
      f.valueType,
      "booleanValue",
      "valueUserEdited must prevent auto-type clobbering"
    );
    assert.equal(f.valueText, "false");

    const f2 = query.createFilterRow();
    qState.filters.push(f2);
    const f2Id = `query-filter-field-${f2.id}`;
    const typeSelects = findAllElements(
      renderFn(),
      (el) =>
        el.tagName === "select" && el.attrs["aria-label"] === "Filter value type"
    );
    assert.equal(typeSelects.length, 2);
    typeSelects[1].handlers.change({ target: { value: "geoPointValue" } });
    assert.equal(f2.valueTypeManuallySet, true);
    findElementById(renderFn(), f2Id)?.handlers.focus(focusAnchor);
    findElementById(renderFn(), f2Id)?.handlers.input({
      target: { value: "price" },
    });
    findClickHandler(renderFn(), "price")?.();
    assert.equal(
      f2.valueType,
      "geoPointValue",
      "valueTypeManuallySet must prevent auto-type clobbering"
    );
    const latInput = findInputByAriaLabel(renderFn(), "Filter latitude");
    assert.ok(latInput?.handlers.input);
    latInput.handlers.input({ target: { value: "52.3" } });
    assert.equal(f2.valueUserEdited, true);
  });

  test("slice2_combobox_aria_geometry_and_reconciliation", async () => {
    let mockRect = { top: 100, bottom: 130, left: 24, width: 300, right: 324 };
    const mockTopbarRect = { top: 0, bottom: 52, left: 0, width: 1200 };
    const mockPaneRect = { top: 52, bottom: 780, left: 0, width: 450 };
    let anchorConnected = true;
    let scrollIntoViewCalls = 0;

    window.innerHeight = 800;
    window.innerWidth = 1200;
    harness.getElementById = (id) => {
      if (id === "cb-test") {
        return {
          isConnected: anchorConnected,
          getBoundingClientRect: () => mockRect,
        };
      }
      if (id === "topbar") {
        return { getBoundingClientRect: () => mockTopbarRect };
      }
      if (id === "cb-test-listbox-opt-0" || id === "cb-test-listbox-opt-1") {
        return {
          scrollIntoView: () => {
            scrollIntoViewCalls++;
          },
        };
      }
      return null;
    };
    harness.querySelector = (sel) => {
      if (sel === ".query-builder-pane") {
        return { getBoundingClientRect: () => mockPaneRect };
      }
      return null;
    };

    const uiState = combobox.createComboboxUiState();
    let val = "";
    let loading = true;
    /** @type {import('../components/combobox.js').ComboboxItem[]} */
    let items = [];
    let opened = 0;
    let refreshed = 0;
    /** @type {string[]} */
    const selected = [];

    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-test",
        value: val,
        placeholder: "Pick field",
        ariaLabel: "Field path",
        items,
        uiState,
        footerText: "Sampled 2 docs",
        loading,
        topObstructionSelector: "#topbar",
        clipContainerSelector: ".query-builder-pane",
        onInput: (next) => {
          val = next;
        },
        onSelect: (item) => {
          val = item.value;
          selected.push(item.value);
        },
        onOpen: () => {
          opened++;
        },
        onRefresh: () => {
          refreshed++;
        },
        onRerender: () => {},
      });

    // Closed state.
    const closedMarkup = renderTemplateMarkup(renderBox());
    assert.ok(closedMarkup.includes('role="combobox"'));
    assert.ok(closedMarkup.includes('aria-autocomplete="list"'));
    assert.ok(closedMarkup.includes('aria-expanded="false"'));
    assert.equal(closedMarkup.includes("combobox-popup"), false);
    assert.equal(closedMarkup.includes("aria-controls"), false);
    assert.equal(closedMarkup.includes("aria-activedescendant"), false);

    // Open while loading && items.length === 0.
    findElementById(renderBox(), "cb-test")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(opened, 1);
    assert.equal(uiState.openId, "cb-test");
    assert.equal(uiState.activeIndex, -1);
    assert.equal((harness.winListeners.get("scroll") ?? []).length, 1);
    assert.equal(
      /** @type {{ capture?: boolean }} */ (
        harness.winListeners.get("scroll")?.[0].opts
      )?.capture,
      true
    );
    assert.equal((harness.winListeners.get("resize") ?? []).length, 1);

    const loadingMarkup = renderTemplateMarkup(renderBox()).replace(/\s+/g, " ");
    assert.ok(loadingMarkup.includes('aria-expanded="true"'));
    assert.ok(loadingMarkup.includes('aria-controls="cb-test-listbox"'));
    assert.ok(
      loadingMarkup.includes(
        '<ul id="cb-test-listbox" class="combobox-listbox" role="listbox" aria-label="Field path"'
      )
    );
    assert.ok(
      loadingMarkup.includes(
        '<div class="combobox-empty" role="status">Loading...</div>'
      )
    );
    assert.ok(
      loadingMarkup.includes('class="combobox-refresh-btn" tabindex="-1"')
    );

    // Filtered-empty state when items.length > 0 and loading === true.
    items = [
      { value: "alpha", label: "alpha", badge: "string", badgeClass: "stringValue" },
      { value: "beta", label: "beta", badge: "int64", badgeClass: "integerValue" },
    ];
    val = "nomatch";
    const filteredEmptyMarkup = renderTemplateMarkup(renderBox());
    assert.ok(filteredEmptyMarkup.includes("No matching suggestions"));
    assert.equal(filteredEmptyMarkup.includes("Loading..."), false);

    // Valid <ul role="listbox"> containing strictly <li role="option"> children only.
    loading = false;
    val = "";
    const openMarkup = renderTemplateMarkup(renderBox()).replace(/\s+/g, " ");
    const ulMatch = /<ul [^>]*role="listbox"[^>]*>(.*?)<\/ul>/.exec(openMarkup);
    assert.ok(ulMatch, "expected <ul role=listbox>");
    assert.equal(
      ulMatch[1].includes("<div"),
      false,
      "ul must contain only <li role=option> children"
    );
    assert.ok(ulMatch[1].includes('role="option"'));

    // Keyboard navigation: ArrowDown, ArrowUp, Enter free-form, ArrowUp when closed, Tab, Escape.
    let prevented = 0;
    let stopped = 0;
    const key = (/** @type {string} */ k) =>
      findElementById(renderBox(), "cb-test")?.handlers.keydown({
        key: k,
        preventDefault: () => {
          prevented++;
        },
        stopPropagation: () => {
          stopped++;
        },
        currentTarget: { getBoundingClientRect: () => mockRect },
      });
    key("ArrowDown");
    assert.equal(uiState.activeIndex, 0);
    assert.ok(scrollIntoViewCalls >= 1);
    assert.ok(
      renderTemplateMarkup(renderBox()).includes(
        'aria-activedescendant="cb-test-listbox-opt-0"'
      )
    );
    key("ArrowDown");
    assert.equal(uiState.activeIndex, 1);
    key("ArrowUp");
    assert.equal(uiState.activeIndex, 0);
    key("ArrowUp");
    assert.equal(uiState.activeIndex, 1);

    // Enter when activeIndex === -1 closes popup without calling onSelect.
    val = "custom-typed";
    uiState.activeIndex = -1;
    key("Enter");
    assert.equal(uiState.openId, null);
    assert.equal(val, "custom-typed");
    assert.equal(selected.length, 0);

    // ArrowUp when closed opens and sets activeIndex = items.length - 1; Tab closes without preventDefault.
    val = "";
    key("ArrowUp");
    assert.equal(uiState.openId, "cb-test");
    assert.equal(uiState.activeIndex, 1);
    const prevBeforeTab = prevented;
    key("Tab");
    assert.equal(uiState.openId, null);
    assert.equal(prevented, prevBeforeTab, "Tab must not call preventDefault");

    // Escape calls preventDefault & stopPropagation.
    key("ArrowDown");
    assert.equal(uiState.openId, "cb-test");
    const prevBeforeEsc = prevented;
    key("Escape");
    assert.equal(uiState.openId, null);
    assert.equal(prevented, prevBeforeEsc + 1);
    assert.equal(stopped, 1);

    // mousedown preventDefault on option and Refresh button.
    key("ArrowDown");
    const optEl = findElementByClass(renderBox(), "combobox-option");
    const refreshEl = findElementByClass(renderBox(), "combobox-refresh-btn");
    assert.ok(optEl?.handlers.mousedown && refreshEl?.handlers.mousedown);
    let mdPrevented = 0;
    optEl.handlers.mousedown({
      preventDefault: () => {
        mdPrevented++;
      },
    });
    refreshEl.handlers.mousedown({
      preventDefault: () => {
        mdPrevented++;
      },
    });
    assert.equal(mdPrevented, 2);
    refreshEl.handlers.click();
    assert.equal(refreshed, 1);
    assert.equal(uiState.openId, "cb-test");

    // @blur active-ID guard.
    uiState.openId = "other-id";
    findElementById(renderBox(), "cb-test")?.handlers.blur({});
    assert.equal(uiState.openId, "other-id");
    uiState.openId = "cb-test";
    findElementById(renderBox(), "cb-test")?.handlers.blur({});
    assert.equal(uiState.openId, null);
    assert.equal(
      (harness.winListeners.get("scroll") ?? []).length,
      0,
      "capture scroll listener must be removed"
    );
    assert.equal((harness.winListeners.get("resize") ?? []).length, 0);

    // flipUp threshold/positioning.
    mockRect = { top: 600, bottom: 630, left: 24, width: 150, right: 174 };
    findElementById(renderBox(), "cb-test")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.flipUp, true);
    const flippedMarkup = renderTemplateMarkup(renderBox());
    assert.ok(
      flippedMarkup.includes(
        "left: 24px; width: 220px; bottom: 202px; top: auto;"
      )
    );

    // Scroll inside .combobox-listbox ignored; external scroll repositions; clipped dismisses.
    const scrollFn = harness.winListeners.get("scroll")?.[0].fn;
    const resizeFn = harness.winListeners.get("resize")?.[0].fn;
    assert.ok(scrollFn && resizeFn);
    scrollFn({
      target: { classList: { contains: (/** @type {string} */ c) => c === "combobox-listbox" } },
    });
    scrollFn({
      target: {
        closest: (/** @type {string} */ s) =>
          s === ".combobox-listbox" ? {} : null,
      },
    });
    assert.equal(uiState.openId, "cb-test");

    mockRect = { top: 150, bottom: 180, left: 30, width: 260, right: 290 };
    scrollFn({ target: {} });
    assert.equal(uiState.rect?.flipUp, false);
    assert.equal(uiState.rect?.top, 150);

    mockRect = { top: 40, bottom: 70, left: 30, width: 260, right: 290 };
    scrollFn({ target: {} });
    assert.equal(
      uiState.openId,
      null,
      "clipped behind #topbar must dismiss popup"
    );

    mockRect = { top: 150, bottom: 180, left: 30, width: 260, right: 290 };
    findElementById(renderBox(), "cb-test")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    mockRect = { top: 785, bottom: 815, left: 30, width: 260, right: 290 };
    resizeFn();
    assert.equal(uiState.openId, null);

    mockRect = { top: 150, bottom: 180, left: 30, width: 260, right: 290 };
    findElementById(renderBox(), "cb-test")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    anchorConnected = false;
    harness.winListeners.get("scroll")?.[0].fn({ target: {} });
    assert.equal(uiState.openId, null);

    // Per-member Node guards when window/document members are stripped.
    /** @type {Record<string, unknown>} */
    const winObj = /** @type {Record<string, unknown>} */ (
      /** @type {unknown} */ (window)
    );
    const docObj = /** @type {Record<string, unknown>} */ (
      /** @type {unknown} */ (document)
    );
    const gObj = /** @type {Record<string, unknown>} */ (globalThis);
    delete winObj.innerHeight;
    delete docObj.getElementById;
    delete docObj.querySelector;
    delete gObj.requestAnimationFrame;
    findElementById(renderBox(), "cb-test")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    key("ArrowDown");
    assert.equal(uiState.openId, "cb-test");
    combobox.closeActiveCombobox(uiState);

    // 2. Stable row.id keying & reconcileComboboxUi dismissal across row removal, COUNT op, raw mode, view switch/modal.
    const store = qsugg.createQuerySuggestionStore();
    const qState = query.createQueryRunnerState("orders");
    const r1 = query.createFilterRow();
    const r2 = query.createFilterRow();
    qState.filters.push(r1, r2);

    const renderQ = () =>
      query.renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["orders"],
        suggestions: store,
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      });

    let cleaned = 0;
    store.comboboxUi.openId = `query-filter-field-${r2.id}`;
    store.comboboxUi.cleanup = () => {
      cleaned++;
    };

    qState.filters.splice(0, 1);
    renderQ();
    assert.equal(store.comboboxUi.openId, `query-filter-field-${r2.id}`);
    assert.equal(cleaned, 0);

    qState.filters.splice(0, 1);
    renderQ();
    assert.equal(store.comboboxUi.openId, null);
    assert.equal(cleaned, 1);

    qState.queryKind = "aggregations";
    qState.aggregations[0].op = "SUM";
    store.comboboxUi.openId = `query-agg-field-${qState.aggregations[0].id}`;
    renderQ();
    assert.equal(
      store.comboboxUi.openId,
      `query-agg-field-${qState.aggregations[0].id}`
    );
    qState.aggregations[0].op = "COUNT";
    renderQ();
    assert.equal(store.comboboxUi.openId, null);

    qState.mode = "visual";
    store.comboboxUi.openId = "query-parent-subpath";
    renderQ();
    assert.equal(store.comboboxUi.openId, "query-parent-subpath");
    qState.mode = "raw";
    renderQ();
    assert.equal(store.comboboxUi.openId, null);

    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.view = "query";
    app.state.queryState = query.createQueryRunnerState("orders");
    app.state.querySuggestions.comboboxUi.openId = "query-collection-id";
    await app.switchView("explorer");
    assert.equal(app.state.querySuggestions.comboboxUi.openId, null);

    app.state.view = "query";
    app.state.querySuggestions.comboboxUi.openId = "query-collection-id";
    app.openCreateDocumentModal(
      "projects/p/databases/(default)/documents",
      "orders"
    );
    assert.equal(app.state.querySuggestions.comboboxUi.openId, null);
    assert.equal(harness.docKeydownListeners.length, 1);
    harness.docKeydownListeners[0]({ key: "Escape", preventDefault() {} });
    assert.equal(app.state.modal, null);
    assert.equal(harness.docKeydownListeners.length, 0);
  });

  test("slice2_cache_reset_and_mark_suggestions_stale_boundaries", async () => {
    const rootPath = "projects/p/databases/db1/documents";
    const store = qsugg.createQuerySuggestionStore();
    let cleaned = 0;
    store.comboboxUi.openId = "query-collection-id";
    store.comboboxUi.cleanup = () => {
      cleaned++;
    };

    const slowRunQuery = deferred();
    const sugCapture = { signal: /** @type {AbortSignal | null} */ (null) };
    harness.fetchImpl = async (_url, init) => {
      sugCapture.signal = /** @type {AbortSignal} */ (init.signal);
      return slowRunQuery.promise;
    };

    const pField = qsugg.ensureFieldSuggestions(
      store,
      { rootPath, rootCollections: [], onRerender: () => {} },
      { parentSubpath: "", collectionId: "orders", allDescendants: false }
    );
    assert.ok(sugCapture.signal);
    assert.equal(sugCapture.signal.aborted, false);
    const epochBefore = store.epoch;

    qsugg.resetQuerySuggestionStore(store);
    assert.equal(sugCapture.signal.aborted, true);
    assert.equal(store.epoch, epochBefore + 1);
    assert.equal(store.fieldsByScope.size, 0);
    assert.equal(store.comboboxUi.openId, null);
    assert.equal(cleaned, 1);

    slowRunQuery.resolve(
      jsonResponse([
        {
          document: {
            name: `${rootPath}/orders/o1`,
            fields: { total: { integerValue: "1" } },
          },
        },
      ])
    );
    await pField;
    assert.equal(store.fieldsByScope.size, 0);

    // Exact segment-boundary markSuggestionsStale across all 4 caches (JSON 3-tuple keys on fieldsByScope).
    /**
     * @param {{ parentSubpath: string, collectionId: string, allDescendants: boolean }} scope
     * @returns {import('../components/query-suggestions.js').FieldScopeEntry}
     */
    const mkEntry = (scope) => ({
      items: [],
      aggItems: [],
      fieldHints: new Map(),
      scope,
      loading: false,
      stale: false,
      partial: false,
      sampledDocs: 0,
    });
    store.subcollectionsByParent.set("orgs/acme", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.subcollectionsByParent.set("orgs/acme/projects/p1", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.subcollectionsByParent.set("orgs/acme2", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });

    store.docIdsByCollection.set("orgs/acme/projects/p1/tasks", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.docIdsByCollection.set("orgs/acme/projects", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.docIdsByCollection.set("orgs", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.docIdsByCollection.set("orgs/acme/projects2", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });

    store.collectionGroupsByParent.set("", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.collectionGroupsByParent.set("orgs/acme", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });
    store.collectionGroupsByParent.set("orgs/other", {
      items: [],
      loading: false,
      stale: false,
      partial: false,
    });

    const keyDirect = JSON.stringify(["orgs/acme/projects/p1", "tasks", false]);
    const keyAncCg = JSON.stringify(["orgs/acme", "tasks", true]);
    const keyRootCg = JSON.stringify(["", "tasks", true]);
    const keyOtherCg = JSON.stringify(["orgs/other", "tasks", true]);
    const keyTasksV2 = JSON.stringify(["orgs/acme/projects/p1", "tasks_v2", false]);

    store.fieldsByScope.set(
      keyDirect,
      mkEntry({
        parentSubpath: "orgs/acme/projects/p1",
        collectionId: "tasks",
        allDescendants: false,
      })
    );
    store.fieldsByScope.set(
      keyAncCg,
      mkEntry({
        parentSubpath: "orgs/acme",
        collectionId: "tasks",
        allDescendants: true,
      })
    );
    store.fieldsByScope.set(
      keyRootCg,
      mkEntry({ parentSubpath: "", collectionId: "tasks", allDescendants: true })
    );
    store.fieldsByScope.set(
      keyOtherCg,
      mkEntry({
        parentSubpath: "orgs/other",
        collectionId: "tasks",
        allDescendants: true,
      })
    );
    store.fieldsByScope.set(
      keyTasksV2,
      mkEntry({
        parentSubpath: "orgs/acme/projects/p1",
        collectionId: "tasks_v2",
        allDescendants: false,
      })
    );

    qsugg.markSuggestionsStale(
      store,
      new Set([`${rootPath}/orgs/acme/projects/p1/tasks/t1`])
    );

    assert.equal(store.subcollectionsByParent.get("orgs/acme")?.stale, true);
    assert.equal(
      store.subcollectionsByParent.get("orgs/acme/projects/p1")?.stale,
      true
    );
    assert.equal(store.subcollectionsByParent.get("orgs/acme2")?.stale, false);

    assert.equal(
      store.docIdsByCollection.get("orgs/acme/projects/p1/tasks")?.stale,
      true
    );
    assert.equal(
      store.docIdsByCollection.get("orgs/acme/projects")?.stale,
      true
    );
    assert.equal(store.docIdsByCollection.get("orgs")?.stale, true);
    assert.equal(
      store.docIdsByCollection.get("orgs/acme/projects2")?.stale,
      false
    );

    assert.equal(store.collectionGroupsByParent.get("")?.stale, true);
    assert.equal(store.collectionGroupsByParent.get("orgs/acme")?.stale, true);
    assert.equal(store.collectionGroupsByParent.get("orgs/other")?.stale, false);

    assert.equal(store.fieldsByScope.get(keyDirect)?.stale, true);
    assert.equal(store.fieldsByScope.get(keyAncCg)?.stale, true);
    assert.equal(store.fieldsByScope.get(keyRootCg)?.stale, true);
    assert.equal(store.fieldsByScope.get(keyOtherCg)?.stale, false);
    assert.equal(store.fieldsByScope.get(keyTasksV2)?.stale, false);
  });

  test("thread_12: popup tabindex, listbox aria-label, focusout/blur containment, and Escape refocus", () => {
    const mockRect = { top: 100, bottom: 130, left: 20, width: 240 };
    const uiState = combobox.createComboboxUiState();
    let focusCalls = 0;
    let refreshed = 0;
    let rerenders = 0;

    const popupChild = { kind: "scrollbar-or-footer" };
    const outsideEl = { kind: "outside-button" };
    const inputEl = {
      isConnected: true,
      getBoundingClientRect: () => mockRect,
      focus() {
        focusCalls++;
        inputEl.onFocusTriggered?.();
      },
      /** @type {(() => void) | null} */
      onFocusTriggered: null,
    };
    const popupEl = {
      id: "cb-t12-popup",
      contains: (/** @type {unknown} */ target) => target === popupChild,
    };

    harness.getElementById = (id) => {
      if (id === "cb-t12") return inputEl;
      if (id === "cb-t12-popup") return popupEl;
      return null;
    };

    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-t12",
        value: "",
        placeholder: "Field",
        ariaLabel: "Filter 1 field path",
        items: [{ value: "age", label: "age" }],
        uiState,
        footerText: "",
        loading: false,
        onInput: () => {},
        onSelect: () => {},
        onOpen: () => {},
        onRefresh: () => {
          refreshed++;
        },
        onRerender: () => {
          rerenders++;
        },
      });

    // Open combobox.
    const inputInfo = findElementById(renderBox(), "cb-t12");
    assert.ok(inputInfo?.handlers.focus);
    inputEl.onFocusTriggered = () => {
      findElementById(renderBox(), "cb-t12")?.handlers.focus({
        currentTarget: inputEl,
      });
    };
    inputInfo.handlers.focus({ currentTarget: inputEl });
    assert.equal(uiState.openId, "cb-t12");

    // Verify -popup tabindex="-1" and listbox aria-label.
    const openTpl = renderBox();
    const popupInfo = findElementById(openTpl, "cb-t12-popup");
    assert.ok(popupInfo);
    assert.equal(popupInfo.attrs.tabindex, "-1");
    const listboxInfo = findElementById(openTpl, "cb-t12-listbox");
    assert.ok(listboxInfo);
    assert.equal(listboxInfo.attrs.role, "listbox");
    assert.equal(listboxInfo.attrs["aria-label"], "Filter 1 field path");

    // Input @blur with relatedTarget inside popup (popupEl itself or descendant) stays open!
    rerenders = 0;
    findElementById(openTpl, "cb-t12")?.handlers.blur({
      relatedTarget: popupEl,
    });
    assert.equal(uiState.openId, "cb-t12");
    assert.equal(rerenders, 0);

    findElementById(openTpl, "cb-t12")?.handlers.blur({
      relatedTarget: popupChild,
    });
    assert.equal(uiState.openId, "cb-t12");
    assert.equal(rerenders, 0);

    // Popup @focusout with relatedTarget inside popup or equal to input stays open.
    popupInfo.handlers.focusout({
      currentTarget: popupEl,
      relatedTarget: popupChild,
    });
    assert.equal(uiState.openId, "cb-t12");

    popupInfo.handlers.focusout({
      currentTarget: popupEl,
      relatedTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-t12");

    // Refresh button click works while open.
    const refreshBtn = findElementByClass(openTpl, "combobox-refresh-btn");
    assert.ok(refreshBtn?.handlers.click);
    refreshBtn.handlers.click();
    assert.equal(refreshed, 1);

    // Popup @focusout with outside element closes popup and rerenders.
    rerenders = 0;
    popupInfo.handlers.focusout({
      currentTarget: popupEl,
      relatedTarget: outsideEl,
    });
    assert.equal(uiState.openId, null);
    assert.equal(rerenders, 1);

    // Reopen and press Escape on popup -> closes, refocuses input, and does NOT reopen via synchronous @focus!
    inputInfo.handlers.focus({ currentTarget: inputEl });
    assert.equal(uiState.openId, "cb-t12");
    const popupOpen2 = findElementById(renderBox(), "cb-t12-popup");
    assert.ok(popupOpen2?.handlers.keydown);
    let prevented = false;
    let stopped = false;
    rerenders = 0;
    popupOpen2.handlers.keydown({
      key: "Escape",
      preventDefault() {
        prevented = true;
      },
      stopPropagation() {
        stopped = true;
      },
    });
    assert.equal(prevented, true);
    assert.equal(stopped, true);
    assert.equal(focusCalls, 1);
    assert.equal(
      uiState.openId,
      null,
      "popup Escape must refocus input without synchronously reopening popup"
    );
    assert.equal(rerenders, 1);
  });

  test("thread_13: Home and End keys are NOT prevented on combobox input", () => {
    const mockRect = { top: 100, bottom: 130, left: 20, width: 240 };
    const uiState = combobox.createComboboxUiState();
    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-t13",
        value: "alpha",
        placeholder: "Field",
        ariaLabel: "Field",
        items: [
          { value: "alpha", label: "alpha" },
          { value: "alpha2", label: "alpha2" },
        ],
        uiState,
        footerText: "",
        loading: false,
        onInput: () => {},
        onSelect: () => {},
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {},
      });

    findElementById(renderBox(), "cb-t13")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    findElementById(renderBox(), "cb-t13")?.handlers.keydown({
      key: "ArrowDown",
      preventDefault() {},
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.activeIndex, 0);

    for (const [keyName, shiftKey] of /** @type {const} */ ([
      ["Home", false],
      ["End", false],
      ["Home", true],
      ["End", true],
    ])) {
      let prevented = false;
      findElementById(renderBox(), "cb-t13")?.handlers.keydown({
        key: keyName,
        shiftKey,
        preventDefault() {
          prevented = true;
        },
        currentTarget: { getBoundingClientRect: () => mockRect },
      });
      assert.equal(
        prevented,
        false,
        `${shiftKey ? "Shift+" : ""}${keyName} must not be prevented on <input>`
      );
      assert.equal(
        uiState.activeIndex,
        0,
        `${shiftKey ? "Shift+" : ""}${keyName} must not alter activeIndex`
      );
      assert.equal(uiState.openId, "cb-t13");
    }
  });

  test("thread_14: skip unchanged scroll rerender, clamp horizontal/vertical geometry, and honor custom clip selectors", () => {
    let mockRect = { top: 100, bottom: 130, left: 20, width: 200, right: 220 };
    let topElRect = { bottom: 50 };
    let paneRect = { top: 50, bottom: 750 };
    let customTopRect = { bottom: 120 };
    let customPaneRect = { top: 120, bottom: 300 };

    harness.getElementById = (id) => {
      if (id === "cb-t14") {
        return { isConnected: true, getBoundingClientRect: () => mockRect };
      }
      if (id === "topbar") return { getBoundingClientRect: () => topElRect };
      if (id === "custom-top") {
        return { getBoundingClientRect: () => customTopRect };
      }
      return null;
    };
    harness.querySelector = (sel) => {
      if (sel === ".query-builder-pane") {
        return { getBoundingClientRect: () => paneRect };
      }
      if (sel === ".custom-pane") {
        return { getBoundingClientRect: () => customPaneRect };
      }
      return null;
    };

    const uiState = combobox.createComboboxUiState();
    let rerenders = 0;

    /**
     * @param {{ topObstructionSelector?: string, clipContainerSelector?: string }} [clipOpts]
     */
    const renderBox = (clipOpts = {}) =>
      combobox.renderCombobox({
        id: "cb-t14",
        value: "",
        placeholder: "Field",
        ariaLabel: "Field",
        items: [{ value: "a", label: "a" }],
        uiState,
        footerText: "",
        loading: false,
        ...clipOpts,
        onInput: () => {},
        onSelect: () => {},
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {
          rerenders++;
        },
      });

    // 1. Scroll with identical rect skips onRerender().
    findElementById(renderBox(), "cb-t14")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    rerenders = 0;
    const scrollFn = harness.winListeners.get("scroll")?.[0].fn;
    assert.ok(scrollFn);
    scrollFn({ target: {} });
    assert.equal(
      rerenders,
      0,
      "unrelated scroll with unchanged anchor rect must not call onRerender"
    );

    mockRect = { top: 110, bottom: 140, left: 20, width: 200, right: 220 };
    scrollFn({ target: {} });
    assert.equal(rerenders, 1, "changed rect must call onRerender once");
    assert.equal(uiState.rect?.top, 110);
    combobox.closeActiveCombobox(uiState);

    // 2. Horizontal clamping in narrow viewport and edge positions.
    window.innerWidth = 300;
    window.innerHeight = 800;
    mockRect = { top: 100, bottom: 130, left: 220, width: 100, right: 320 };
    findElementById(renderBox(), "cb-t14")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.popupWidth, 220);
    assert.equal(uiState.rect?.popupLeft, 72); // 300 - 220 - 8

    mockRect = { top: 100, bottom: 130, left: 2, width: 100, right: 102 };
    findElementById(renderBox(), "cb-t14")?.handlers.input({
      target: { value: "", getBoundingClientRect: () => mockRect },
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.popupLeft, 8);

    // Very narrow viewport < POPUP_MIN_WIDTH_PX (220).
    window.innerWidth = 160;
    findElementById(renderBox(), "cb-t14")?.handlers.input({
      target: { value: "", getBoundingClientRect: () => mockRect },
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.popupWidth, 144); // 160 - 16
    assert.equal(uiState.rect?.popupLeft, 8);
    combobox.closeActiveCombobox(uiState);

    // 3. Vertical maxListboxHeight clamping: max(80, min(260, floor(availableSpace - 44))).
    window.innerWidth = 1200;
    window.innerHeight = 400;
    mockRect = { top: 200, bottom: 250, left: 20, width: 240, right: 260 }; // below=150, above=200 -> flipUp=true, space=200
    findElementById(renderBox(), "cb-t14")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.flipUp, true);
    assert.equal(uiState.rect?.maxListboxHeight, 156); // 200 - 44
    const listEl = findElementById(renderBox(), "cb-t14-listbox");
    assert.ok(String(listEl?.attrs.style).includes("max-height: 156px;"));

    window.innerHeight = 150;
    mockRect = { top: 40, bottom: 80, left: 20, width: 240, right: 260 }; // below=70, above=40 -> flipUp=false, space=70
    findElementById(renderBox(), "cb-t14")?.handlers.input({
      target: { value: "", getBoundingClientRect: () => mockRect },
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.maxListboxHeight, 80); // floor(70-44)=26 clamped to 80
    combobox.closeActiveCombobox(uiState);

    // 4. Custom clip boundaries vs omitted defaults (#topbar / .query-builder-pane ignored unless configured).
    window.innerHeight = 800;
    topElRect = { bottom: 200 };
    paneRect = { top: 200, bottom: 250 };
    mockRect = { top: 100, bottom: 130, left: 20, width: 240, right: 260 };

    // Without clip opts, #topbar / .query-builder-pane do NOT dismiss on scroll.
    findElementById(renderBox(), "cb-t14")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    mockRect = { top: 105, bottom: 135, left: 20, width: 240, right: 260 };
    harness.winListeners.get("scroll")?.[0].fn({ target: {} });
    assert.equal(
      uiState.openId,
      "cb-t14",
      "without clip options, #topbar/.query-builder-pane must not dismiss"
    );
    combobox.closeActiveCombobox(uiState);

    // With custom selectors, clipping past #custom-top or .custom-pane dismisses.
    findElementById(
      renderBox({
        topObstructionSelector: "#custom-top",
        clipContainerSelector: ".custom-pane",
      }),
      "cb-t14"
    )?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    customTopRect = { bottom: 120 };
    mockRect = { top: 110, bottom: 140, left: 20, width: 240, right: 260 };
    harness.winListeners.get("scroll")?.[0].fn({ target: {} });
    assert.equal(uiState.openId, null, "clipped past custom #custom-top must dismiss");

    customTopRect = { bottom: 50 };
    customPaneRect = { top: 50, bottom: 250 };
    mockRect = { top: 150, bottom: 180, left: 20, width: 240, right: 260 };
    findElementById(
      renderBox({
        topObstructionSelector: "#custom-top",
        clipContainerSelector: ".custom-pane",
      }),
      "cb-t14"
    )?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    mockRect = { top: 260, bottom: 290, left: 20, width: 240, right: 260 };
    harness.winListeners.get("scroll")?.[0].fn({ target: {} });
    assert.equal(uiState.openId, null, "clipped past custom .custom-pane must dismiss");
  });

  test("thread_15: runCachedFetch awaits in-flight promise and mid-flight markSuggestionsStale forces stale=true", async () => {
    const rootPath = "projects/p/databases/(default)/documents";
    const store = qsugg.createQuerySuggestionStore();
    const ctx = {
      rootPath,
      rootCollections: ["orders"],
      onRerender: () => {},
    };

    const queryDef = deferred();
    let fetchCount = 0;
    harness.fetchImpl = async () => {
      fetchCount++;
      return queryDef.promise;
    };

    const scope = {
      parentSubpath: "",
      collectionId: "orders",
      allDescendants: false,
    };

    // Concurrent calls share the exact in-flight promise rather than resolving early with loading:true.
    let p2Resolved = false;
    const p1 = qsugg.ensureFieldSuggestions(store, ctx, scope);
    const p2 = qsugg.ensureFieldSuggestions(store, ctx, scope).then((entry) => {
      p2Resolved = true;
      return entry;
    });
    await nextTick();
    assert.equal(fetchCount, 1);
    assert.equal(
      p2Resolved,
      false,
      "second concurrent caller must await in-flight promise instead of resolving early with empty loading entry"
    );

    // Mid-flight invalidation increments invalidationSeq and marks resolved entry stale = true!
    qsugg.markSuggestionsStale(store, [`${rootPath}/orders/o1`]);
    assert.equal(store.fieldsByScope.get(JSON.stringify(["", "orders", false]))?.stale, true);

    queryDef.resolve(
      jsonResponse([
        {
          document: {
            name: `${rootPath}/orders/o1`,
            fields: { total: { integerValue: "10" } },
          },
        },
      ])
    );

    const [e1, e2] = await Promise.all([p1, p2]);
    assert.equal(p2Resolved, true);
    assert.equal(e1, e2);
    assert.equal(e1.items[0].value, "total");
    assert.equal(
      e1.stale,
      true,
      "mid-flight markSuggestionsStale must force resolved entry stale=true"
    );
    assert.equal(store.inflight.size, 0);
    assert.equal(qsugg.formatSuggestionFooter(e1), "Sampled 1 doc · Stale");
  });

  test("thread_16: pipe | in doc/col IDs does not collide and exported selectors work directly", async () => {
    const rootPath = "projects/p/databases/(default)/documents";
    const store = qsugg.createQuerySuggestionStore();
    const ctx = {
      rootPath,
      rootCollections: ["a", "a|b"],
      onRerender: () => {},
    };

    harness.fetchImpl = async (_url, init) => {
      const body = /** @type {{ structuredQuery: { from: Array<{ collectionId: string }> } }} */ (
        JSON.parse(String(init.body ?? "{}"))
      );
      const colId = body.structuredQuery.from[0].collectionId;
      if (colId === "c") {
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/a|b/d1/c/doc1`,
              fields: { fromAB_C: { stringValue: "1" } },
            },
          },
        ]);
      }
      if (colId === "b|c") {
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/a/d1/b|c/doc2`,
              fields: { fromA_BC: { integerValue: "2" } },
            },
          },
        ]);
      }
      throw new Error("unexpected colId: " + colId);
    };

    await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "a|b/d1",
      collectionId: "c",
      allDescendants: false,
    });
    await qsugg.ensureFieldSuggestions(store, ctx, {
      parentSubpath: "a/d1",
      collectionId: "b|c",
      allDescendants: false,
    });

    const entry1 = qsugg.getFieldScopeEntry(store, "a|b/d1", "c", false);
    const entry2 = qsugg.getFieldScopeEntry(store, "/a/d1/", " b|c ", false);
    assert.ok(entry1 && entry2);
    assert.notEqual(entry1, entry2);
    assert.equal(entry1.items[0].value, "fromAB_C");
    assert.equal(entry2.items[0].value, "fromA_BC");
    assert.equal(qsugg.getFieldScopeEntry(null, "a/d1", "b|c", false), null);
    assert.equal(qsugg.getFieldScopeEntry(store, "odd", "b|c", false), null);
    assert.equal(qsugg.getFieldScopeEntry(store, "a/d1", "__bad__", false), null);

    // Invalidating only a/d1/b|c/doc2 must mark entry2 stale without affecting entry1!
    qsugg.markSuggestionsStale(store, [`${rootPath}/a/d1/b|c/doc2`]);
    assert.equal(entry1.stale, false);
    assert.equal(entry2.stale, true);

    // Exported selectors getParentSubpathSuggestions and getCollectionIdSuggestions:
    store.docIdsByCollection.set("orgs", {
      items: [{ value: "orgs/acme", label: "acme" }],
      loading: false,
      stale: false,
      partial: true,
    });
    store.subcollectionsByParent.set("orgs/acme", {
      items: [{ value: "projects", label: "projects" }],
      loading: false,
      stale: true,
      partial: false,
    });
    store.collectionGroupsByParent.set("", {
      items: [{ value: "tasks", label: "tasks" }],
      loading: true,
      stale: false,
      partial: false,
    });

    assert.deepEqual(
      qsugg.getParentSubpathSuggestions(store, ["orgs", "users"], rootPath, "or")
        .items,
      [
        { value: "orgs/", label: "orgs/" },
        { value: "users/", label: "users/" },
      ]
    );
    const oddParent = qsugg.getParentSubpathSuggestions(
      store,
      ["orgs"],
      rootPath,
      "orgs/ac"
    );
    assert.deepEqual(oddParent.items, [{ value: "orgs/acme", label: "acme" }]);
    assert.equal(oddParent.footerText, "Partial");

    const evenParent = qsugg.getParentSubpathSuggestions(
      store,
      ["orgs"],
      rootPath,
      "orgs/acme/pr"
    );
    assert.deepEqual(evenParent.items, [
      { value: "orgs/acme/projects/", label: "projects/" },
    ]);
    assert.equal(evenParent.footerText, "Stale");

    assert.deepEqual(
      qsugg.getParentSubpathSuggestions(store, ["orgs"], "", "orgs/").items,
      []
    );
    assert.deepEqual(
      qsugg.getParentSubpathSuggestions(store, ["orgs"], rootPath, "../x").items,
      []
    );

    assert.deepEqual(
      qsugg.getCollectionIdSuggestions(store, ["orgs"], "", false).items,
      [{ value: "orgs", label: "orgs" }]
    );
    assert.deepEqual(
      qsugg.getCollectionIdSuggestions(store, ["orgs"], "/orgs/acme/", false)
        .items,
      [{ value: "projects", label: "projects" }]
    );
    const cgSel = qsugg.getCollectionIdSuggestions(store, ["orgs"], "", true);
    assert.deepEqual(cgSel.items, [{ value: "tasks", label: "tasks" }]);
    assert.equal(cgSel.loading, true);
    assert.deepEqual(
      qsugg.getCollectionIdSuggestions(store, ["orgs"], "bad_odd", false).items,
      []
    );
  });

  test("f3_scrollbar_pointer_focus_recovery_selectItem_refocus_and_listener_cleanup", () => {
    const mockRect = { top: 100, bottom: 130, left: 20, width: 240 };
    const uiState = combobox.createComboboxUiState();
    let focusCalls = 0;
    let rerenders = 0;
    let val = "";
    /** @type {string[]} */
    const selected = [];
    const popupChild = { kind: "scrollbar-thumb" };
    const outsideEl = { kind: "outside-control" };
    const inputEl = {
      isConnected: true,
      getBoundingClientRect: () => mockRect,
      focus() {
        focusCalls++;
        harness.activeElement = inputEl;
        inputEl.onFocusTriggered?.();
      },
      /** @type {(() => void) | null} */
      onFocusTriggered: null,
    };
    const popupEl = {
      id: "cb-f3-popup",
      contains: (/** @type {unknown} */ target) => target === popupChild,
    };

    harness.getElementById = (id) => {
      if (id === "cb-f3") return inputEl;
      if (id === "cb-f3-popup") return popupEl;
      return null;
    };

    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-f3",
        value: val,
        placeholder: "Path",
        ariaLabel: "Parent Document Path",
        items: [
          { value: "orgs/", label: "orgs/" },
          { value: "orgs/acme", label: "acme" },
        ],
        uiState,
        footerText: "Sampled 2 docs",
        loading: false,
        keepOpenOnSelect: (item) => item.value.endsWith("/"),
        onInput: (next) => {
          val = next;
        },
        onSelect: (item) => {
          val = item.value;
          selected.push(item.value);
        },
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {
          rerenders++;
        },
      });

    const pointerListenerCount = () =>
      (harness.winListeners.get("mouseup") ?? []).length +
      (harness.winListeners.get("pointerup") ?? []).length +
      (harness.winListeners.get("mousemove") ?? []).length +
      (harness.winListeners.get("blur") ?? []).length +
      (harness.winListeners.get("mousedown") ?? []).length;

    inputEl.onFocusTriggered = () => {
      findElementById(renderBox(), "cb-f3")?.handlers.focus({
        currentTarget: inputEl,
      });
    };

    // Open combobox.
    harness.activeElement = inputEl;
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-f3");
    assert.equal(pointerListenerCount(), 0);

    // 1. Scrollbar/popup mousedown (no preventDefault) + Chrome blur (relatedTarget=popupEl)
    //    AND Safari blur (relatedTarget=null) both keep popup open; mouseup restores focus to <input>.
    const popupInfo = findElementById(renderBox(), "cb-f3-popup");
    assert.ok(popupInfo?.handlers.mousedown);
    let mdPrevented = false;
    popupInfo.handlers.mousedown({
      preventDefault() {
        mdPrevented = true;
      },
    });
    assert.equal(
      mdPrevented,
      false,
      "popup container mousedown must not preventDefault so native scrollbar dragging works"
    );
    assert.equal((harness.winListeners.get("mouseup") ?? []).length, 1);

    rerenders = 0;
    harness.activeElement = popupEl;
    findElementById(renderBox(), "cb-f3")?.handlers.blur({
      relatedTarget: popupEl,
    });
    assert.equal(uiState.openId, "cb-f3");
    assert.equal(rerenders, 0);

    // Safari/WebKit native scrollbar blur/focusout with relatedTarget === null while pointer is down.
    harness.activeElement = null;
    findElementById(renderBox(), "cb-f3")?.handlers.blur({
      relatedTarget: null,
    });
    findElementById(renderBox(), "cb-f3-popup")?.handlers.focusout({
      currentTarget: popupEl,
      relatedTarget: null,
    });
    assert.equal(
      uiState.openId,
      "cb-f3",
      "Safari scrollbar blur/focusout with relatedTarget=null must not close popup while pointer is down"
    );
    assert.equal(rerenders, 0);

    // Mouseup releases pointer state, cleans up all temporary listeners, and refocuses <input>.
    focusCalls = 0;
    harness.winListeners.get("mouseup")?.[0].fn();
    assert.equal(uiState.openId, "cb-f3");
    assert.equal(pointerListenerCount(), 0, "mouseup must clean up pointer listeners");
    assert.equal(focusCalls, 1, "mouseup must restore focus to <input>");
    assert.equal(harness.activeElement, inputEl);

    // Keyboard ArrowDown works immediately on <input> after scrollbar release.
    findElementById(renderBox(), "cb-f3")?.handlers.keydown({
      key: "ArrowDown",
      preventDefault() {},
      currentTarget: inputEl,
    });
    assert.equal(uiState.activeIndex, 0);

    // 2. Swallowed mouseup fallback: mousemove with buttons === 0 recovers focus & cleans listeners.
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    harness.activeElement = popupEl;
    focusCalls = 0;
    harness.winListeners.get("mousemove")?.[0].fn({ buttons: 1 });
    assert.equal(focusCalls, 0, "dragging with buttons=1 must not release early");
    harness.winListeners.get("mousemove")?.[0].fn({ buttons: 0 });
    assert.equal(focusCalls, 1, "mousemove with buttons=0 must restore focus");
    assert.equal(pointerListenerCount(), 0);

    // 2b. Window blur closes popup & cleans listeners without stealing focus (no Safari body orphan).
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    harness.activeElement = popupEl;
    focusCalls = 0;
    rerenders = 0;
    harness.winListeners.get("blur")?.[0].fn();
    assert.equal(uiState.openId, null, "window blur must close popup instead of leaving it stranded");
    assert.equal(rerenders, 1);
    assert.equal(focusCalls, 0, "window blur must not steal focus");
    assert.equal(pointerListenerCount(), 0);

    // Reopen for 2c.
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-f3");

    // 2c. Outside mousedown after swallowed mouseup closes popup & cleans listeners.
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    rerenders = 0;
    harness.winListeners.get("mousedown")?.[0].fn({ target: outsideEl });
    assert.equal(uiState.openId, null);
    assert.equal(rerenders, 1);
    assert.equal(pointerListenerCount(), 0);

    // Reopen for remaining checks.
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-f3");

    // 3. Mouseup when activeElement deliberately moved outside popup & input closes without stealing focus.
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    harness.activeElement = outsideEl;
    focusCalls = 0;
    rerenders = 0;
    harness.winListeners.get("mouseup")?.[0].fn();
    assert.equal(uiState.openId, null, "mouseup with outside focus must close popup");
    assert.equal(rerenders, 1);
    assert.equal(focusCalls, 0, "mouseup must not steal focus from outside control");
    assert.equal(pointerListenerCount(), 0);

    // Reopen for selection checks.
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-f3");

    // 4. selectItem refocuses <input> on keepOpenOnSelect=true ("orgs/") while staying open.
    harness.activeElement = popupEl;
    focusCalls = 0;
    const orgsClick = findClickHandler(renderBox(), "orgs/");
    assert.ok(orgsClick);
    orgsClick();
    assert.equal(val, "orgs/");
    assert.equal(uiState.openId, "cb-f3");
    assert.equal(uiState.activeIndex, -1);
    assert.equal(focusCalls, 1, "keepOpenOnSelect must restore focus to <input>");
    assert.equal(harness.activeElement, inputEl);

    // 5. selectItem refocuses <input> on keepOpenOnSelect=false ("orgs/acme") WITHOUT reopening.
    harness.activeElement = popupEl;
    focusCalls = 0;
    const acmeClick = findClickHandler(renderBox(), "acme");
    assert.ok(acmeClick);
    acmeClick();
    assert.equal(val, "orgs/acme");
    assert.deepEqual(selected, ["orgs/", "orgs/acme"]);
    assert.equal(
      uiState.openId,
      null,
      "closing selectItem must not reopen popup via synchronous @focus"
    );
    assert.equal(focusCalls, 1, "closing selectItem must restore focus to <input>");
    assert.equal(harness.activeElement, inputEl);

    // 6. Shift+Tab on popup closes popup and refocuses <input> without reopening.
    val = "";
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    assert.equal(uiState.openId, "cb-f3");
    harness.activeElement = popupEl;
    focusCalls = 0;
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "Tab",
      shiftKey: true,
      preventDefault() {},
    });
    assert.equal(uiState.openId, null);
    assert.equal(focusCalls, 1);
    assert.equal(harness.activeElement, inputEl);

    // 7. Closing via closeActiveCombobox / Escape while pointer is down cleans up all listeners.
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    assert.ok(pointerListenerCount() > 0);
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "Escape",
      preventDefault() {},
      stopPropagation() {},
    });
    assert.equal(uiState.openId, null);
    assert.equal(pointerListenerCount(), 0);
    assert.equal((harness.winListeners.get("scroll") ?? []).length, 0);
    assert.equal((harness.winListeners.get("resize") ?? []).length, 0);

    // 8. ArrowDown on popup recovers focus to <input>, and Enter on button is not intercepted.
    findElementById(renderBox(), "cb-f3")?.handlers.focus({
      currentTarget: inputEl,
    });
    harness.activeElement = popupEl;
    focusCalls = 0;
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "ArrowDown",
      preventDefault() {},
    });
    assert.equal(uiState.activeIndex, 0);
    assert.equal(focusCalls, 1);
    let btnEnterPrevented = false;
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "Enter",
      target: { tagName: "BUTTON" },
      preventDefault() {
        btnEnterPrevented = true;
      },
    });
    assert.equal(btnEnterPrevented, false);
    assert.equal(uiState.openId, "cb-f3");

    // 9. Swallowed scrollbar mouseup + immediate printable char / Backspace on popup forwards to <input>.
    val = "or";
    findElementById(renderBox(), "cb-f3-popup")?.handlers.mousedown({});
    assert.ok(pointerListenerCount() > 0);
    harness.activeElement = popupEl;
    uiState.activeIndex = 0;
    focusCalls = 0;
    rerenders = 0;
    let charPrevented = false;
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "g",
      ctrlKey: false,
      metaKey: false,
      altKey: false,
      preventDefault() {
        charPrevented = true;
      },
    });
    assert.equal(charPrevented, true);
    assert.equal(val, "org");
    assert.equal(uiState.activeIndex, -1);
    assert.equal(pointerListenerCount(), 0);
    assert.equal(focusCalls, 1);
    assert.equal(harness.activeElement, inputEl);
    assert.equal(rerenders, 1);

    harness.activeElement = popupEl;
    focusCalls = 0;
    rerenders = 0;
    let bsPrevented = false;
    findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
      key: "Backspace",
      ctrlKey: false,
      metaKey: false,
      altKey: false,
      preventDefault() {
        bsPrevented = true;
      },
    });
    assert.equal(bsPrevented, true);
    assert.equal(val, "or");
    assert.equal(focusCalls, 1);
    assert.equal(harness.activeElement, inputEl);
    assert.equal(rerenders, 1);

    // Modifier combos and Space on a <button> inside popup are NOT intercepted.
    for (const ev of [
      { key: "a", ctrlKey: true, metaKey: false, altKey: false },
      { key: "c", ctrlKey: false, metaKey: true, altKey: false },
      { key: "x", ctrlKey: false, metaKey: false, altKey: true },
      { key: " ", ctrlKey: false, metaKey: false, altKey: false, target: { tagName: "BUTTON" } },
    ]) {
      let prevented = false;
      findElementById(renderBox(), "cb-f3-popup")?.handlers.keydown({
        ...ev,
        preventDefault() {
          prevented = true;
        },
      });
      assert.equal(prevented, false);
    }
    assert.equal(val, "or");
    combobox.closeActiveCombobox(uiState);
  });

  test("f7_vertical_resize_while_flipUp_updates_popupBottom_and_rerenders", () => {
    const mockRect = { top: 600, bottom: 630, left: 24, width: 220, right: 244 };
    window.innerWidth = 1200;
    window.innerHeight = 750;
    harness.getElementById = (id) =>
      id === "cb-f7"
        ? { isConnected: true, getBoundingClientRect: () => mockRect }
        : null;

    const uiState = combobox.createComboboxUiState();
    let rerenders = 0;
    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-f7",
        value: "",
        placeholder: "Field",
        ariaLabel: "Field",
        items: [{ value: "age", label: "age" }],
        uiState,
        footerText: "",
        loading: false,
        onInput: () => {},
        onSelect: () => {},
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {
          rerenders++;
        },
      });

    findElementById(renderBox(), "cb-f7")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.rect?.flipUp, true);
    assert.equal(uiState.rect?.maxListboxHeight, 260);
    assert.equal(uiState.rect?.popupBottom, 152); // 750 - 600 + 2
    assert.ok(
      String(findElementById(renderBox(), "cb-f7-popup")?.attrs.style).includes(
        "bottom: 152px; top: auto;"
      )
    );

    // Resize vertically while flipUp remains true and maxListboxHeight stays capped at 260.
    rerenders = 0;
    window.innerHeight = 880;
    const resizeFn = harness.winListeners.get("resize")?.[0].fn;
    assert.ok(resizeFn);
    resizeFn();
    assert.equal(
      rerenders,
      1,
      "vertical resize changing popupBottom while flipUp=true must trigger onRerender"
    );
    assert.equal(uiState.rect?.flipUp, true);
    assert.equal(uiState.rect?.maxListboxHeight, 260);
    assert.equal(uiState.rect?.popupBottom, 282); // 880 - 600 + 2
    assert.ok(
      String(findElementById(renderBox(), "cb-f7-popup")?.attrs.style).includes(
        "bottom: 282px; top: auto;"
      )
    );

    // Repeat resize with identical viewport height skips rerender.
    rerenders = 0;
    resizeFn();
    assert.equal(rerenders, 0);
    combobox.closeActiveCombobox(uiState);
  });

  test("f9_resetHarness_syncs_modal_listener_state_after_raw_modal_null_in_explorer", async () => {
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.view = "explorer";
    app.openCreateDocumentModal(
      "projects/p/databases/(default)/documents",
      "users"
    );
    assert.equal(harness.docKeydownListeners.length, 1);

    // Simulate a test that clears app.state.modal directly without renderApp() while in explorer view.
    app.state.modal = null;
    await resetHarness(app, stateMod);
    assert.equal(harness.docKeydownListeners.length, 0);

    // Subsequent test opening a modal must cleanly register its document keydown listener again.
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.view = "explorer";
    app.openCreateDocumentModal(
      "projects/p/databases/(default)/documents",
      "users"
    );
    assert.equal(
      harness.docKeydownListeners.length,
      1,
      "resetHarness must resync app.js wasModalOpen/modalKeydownListener after raw modal=null"
    );
    harness.docKeydownListeners[0]({ key: "Escape", preventDefault() {} });
    assert.equal(app.state.modal, null);
    assert.equal(harness.docKeydownListeners.length, 0);
  });

  test("f10_empty_filtered_listbox_is_hidden_and_shown_when_options_present", () => {
    const mockRect = { top: 100, bottom: 130, left: 20, width: 240 };
    const uiState = combobox.createComboboxUiState();
    let val = "";
    /** @type {import('../components/combobox.js').ComboboxItem[]} */
    let items = [];
    let loading = true;

    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-f10",
        value: val,
        placeholder: "Field",
        ariaLabel: "Field",
        items,
        uiState,
        footerText: "",
        loading,
        onInput: (next) => {
          val = next;
        },
        onSelect: () => {},
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {},
      });

    findElementById(renderBox(), "cb-f10")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });

    // Empty while loading -> <ul> is explicitly hidden.
    assert.equal(
      findElementById(renderBox(), "cb-f10-listbox")?.attrs.hidden,
      true
    );

    // Non-matching filter query -> <ul> is explicitly hidden.
    loading = false;
    items = [{ value: "price", label: "price" }];
    val = "zzz-nomatch";
    assert.equal(
      findElementById(renderBox(), "cb-f10-listbox")?.attrs.hidden,
      true
    );

    // Matching items -> <ul> is visible (hidden attribute absent).
    val = "pri";
    assert.equal(
      findElementById(renderBox(), "cb-f10-listbox")?.attrs.hidden,
      undefined
    );
    combobox.closeActiveCombobox(uiState);
  });

  test("f13a_combobox_keydown_ignores_ime_isComposing_events", () => {
    const mockRect = { top: 100, bottom: 130, left: 20, width: 240 };
    const uiState = combobox.createComboboxUiState();
    /** @type {string[]} */
    const selected = [];
    const renderBox = () =>
      combobox.renderCombobox({
        id: "cb-f13a",
        value: "",
        placeholder: "Field",
        ariaLabel: "Field",
        items: [{ value: "name", label: "name" }],
        uiState,
        footerText: "",
        loading: false,
        onInput: () => {},
        onSelect: (item) => {
          selected.push(item.value);
        },
        onOpen: () => {},
        onRefresh: () => {},
        onRerender: () => {},
      });

    findElementById(renderBox(), "cb-f13a")?.handlers.focus({
      currentTarget: { getBoundingClientRect: () => mockRect },
    });
    assert.equal(uiState.openId, "cb-f13a");

    // Input @keydown ignores ArrowDown, Enter, Escape when isComposing === true.
    for (const keyName of ["ArrowDown", "ArrowUp", "Enter", "Escape"]) {
      let prevented = false;
      let stopped = false;
      findElementById(renderBox(), "cb-f13a")?.handlers.keydown({
        key: keyName,
        isComposing: true,
        preventDefault() {
          prevented = true;
        },
        stopPropagation() {
          stopped = true;
        },
        currentTarget: { getBoundingClientRect: () => mockRect },
      });
      assert.equal(prevented, false, `composing ${keyName} must not preventDefault`);
      assert.equal(stopped, false, `composing ${keyName} must not stopPropagation`);
      assert.equal(uiState.openId, "cb-f13a");
      assert.equal(uiState.activeIndex, -1);
      assert.equal(selected.length, 0);
    }

    // Popup @keydown also ignores Escape/Tab/ArrowDown/Enter when isComposing === true.
    uiState.activeIndex = 0;
    for (const keyName of ["Escape", "Tab", "ArrowDown", "Enter"]) {
      let prevented = false;
      findElementById(renderBox(), "cb-f13a-popup")?.handlers.keydown({
        key: keyName,
        isComposing: true,
        preventDefault() {
          prevented = true;
        },
        stopPropagation() {},
      });
      assert.equal(prevented, false);
      assert.equal(uiState.openId, "cb-f13a");
      assert.equal(uiState.activeIndex, 0);
      assert.equal(selected.length, 0);
    }
    combobox.closeActiveCombobox(uiState);
  });

  test("f13b_query_limit_warning_exposes_img_role_with_aria_label", () => {
    const qState = query.createQueryRunnerState("orders");
    qState.queryKind = "aggregations";
    qState.limitText = "10";
    const tpl = query.renderQueryRunner({
      projectId: "p",
      databaseId: "(default)",
      knownCollections: ["orders"],
      state: qState,
      onRerender: () => {},
      onExecuteQuery: () => {},
      onOpenResultDocument: () => {},
    });
    const warningEl = findElementByClass(tpl, "query-limit-warning");
    assert.ok(warningEl, "expected .query-limit-warning in aggregation mode with limit");
    assert.equal(warningEl.attrs.role, "img");
    assert.equal(
      warningEl.attrs["aria-label"],
      "Aggregation runs over the limited query window"
    );
  });
});
