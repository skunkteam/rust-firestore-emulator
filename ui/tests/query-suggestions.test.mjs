// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  deferred,
  harness,
  jsonResponse,
  nextTick,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as app from "../app.js";
import * as qsugg from "../components/query-suggestions.js";
import * as stateMod from "../state.js";

describe("Query suggestions transient failures and force-refresh supersession", () => {
  const rootPath = "projects/p/databases/(default)/documents";

  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("f4_transient_initial_failure_evicts_placeholder_and_retries_on_next_call", async () => {
    const store = qsugg.createQuerySuggestionStore();
    let rerenders = 0;
    const ctx = {
      rootPath,
      rootCollections: ["orders"],
      onRerender: () => {
        rerenders++;
      },
    };
    const scope = {
      parentSubpath: "",
      collectionId: "orders",
      allDescendants: false,
    };

    let fieldCalls = 0;
    let subCalls = 0;
    let docCalls = 0;
    let failInitial = true;

    harness.fetchImpl = async (url) => {
      if (url === `/v1/${rootPath}:runQuery`) {
        fieldCalls++;
        if (failInitial) {
          return jsonResponse(
            {
              error: {
                code: 503,
                status: "UNAVAILABLE",
                message: "transient backend error",
              },
            },
            503
          );
        }
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/orders/o1`,
              fields: { total: { integerValue: "42" } },
            },
          },
        ]);
      }
      if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
        subCalls++;
        if (failInitial) {
          throw new Error("network reset");
        }
        return jsonResponse({ collectionIds: ["projects"] });
      }
      if (url.startsWith(`/v1/${rootPath}/orgs?`)) {
        docCalls++;
        if (failInitial) {
          return jsonResponse(
            { error: { code: 503, status: "UNAVAILABLE", message: "down" } },
            503
          );
        }
        return jsonResponse({
          documents: [{ name: `${rootPath}/orgs/acme` }],
        });
      }
      throw new Error("unexpected url: " + url);
    };

    // 1. Field suggestions initial failure evicts placeholder, returns stale: true, and does NOT cache "Sampled 0 docs".
    const failedFields = await qsugg.ensureFieldSuggestions(store, ctx, scope);
    assert.equal(fieldCalls, 1);
    assert.equal(failedFields.loading, false);
    assert.equal(failedFields.stale, true);
    assert.deepEqual(failedFields.items, []);
    assert.equal(
      qsugg.getFieldScopeEntry(store, "", "orders", false),
      null,
      "failed initial fetch must evict placeholder from fieldsByScope"
    );
    assert.equal(
      qsugg.formatSuggestionFooter(
        qsugg.getFieldScopeEntry(store, "", "orders", false)
      ),
      "",
      "evicted placeholder must not display 'Sampled 0 docs'"
    );
    assert.equal(store.controllers.size, 0);
    assert.equal(store.inflight.size, 0);
    assert.ok(rerenders >= 2);

    // 2. Subcollections and parentSubpath doc IDs also evict placeholders on initial failure.
    const failedSub = await qsugg.ensureSubcollections(store, ctx, "orgs/acme");
    assert.equal(subCalls, 1);
    assert.equal(failedSub.loading, false);
    assert.equal(failedSub.stale, true);
    assert.equal(store.subcollectionsByParent.has("orgs/acme"), false);

    const failedDocs = await qsugg.ensureParentSubpathSuggestions(
      store,
      ctx,
      "orgs/"
    );
    assert.equal(docCalls, 1);
    assert.equal(failedDocs.loading, false);
    assert.equal(failedDocs.stale, true);
    assert.equal(store.docIdsByCollection.has("orgs"), false);

    // 3. Subsequent non-forced calls retry and populate fresh entries cleanly.
    failInitial = false;
    const recoveredFields = await qsugg.ensureFieldSuggestions(
      store,
      ctx,
      scope,
      false
    );
    assert.equal(fieldCalls, 2, "next non-forced call must retry after eviction");
    assert.equal(recoveredFields.loading, false);
    assert.equal(recoveredFields.stale, false);
    assert.equal(recoveredFields.sampledDocs, 1);
    assert.equal(recoveredFields.items[0].value, "total");
    assert.equal(qsugg.formatSuggestionFooter(recoveredFields), "Sampled 1 doc");
    assert.equal(
      qsugg.getFieldScopeEntry(store, "", "orders", false),
      recoveredFields
    );

    const recoveredSub = await qsugg.ensureSubcollections(
      store,
      ctx,
      "orgs/acme",
      false
    );
    assert.equal(subCalls, 2);
    assert.equal(recoveredSub.stale, false);
    assert.deepEqual(
      recoveredSub.items.map((i) => i.value),
      ["projects"]
    );

    const recoveredDocs = await qsugg.ensureParentSubpathSuggestions(
      store,
      ctx,
      "orgs/",
      false
    );
    assert.equal(docCalls, 2);
    assert.equal(recoveredDocs.stale, false);
    assert.deepEqual(
      recoveredDocs.items.map((i) => i.value),
      ["orgs/acme"]
    );

    // 4. Initial fetch superseded by force: true that also fails transiently still evicts unpopulated placeholder.
    const scope2 = {
      parentSubpath: "",
      collectionId: "invoices",
      allDescendants: false,
    };
    const firstDef = deferred();
    const secondDef = deferred();
    let invCalls = 0;
    harness.fetchImpl = async (_url, init) => {
      invCalls++;
      const signal = /** @type {AbortSignal | undefined} */ (init.signal);
      if (invCalls === 1) {
        signal?.addEventListener("abort", () => {
          const err = new Error("Aborted");
          err.name = "AbortError";
          firstDef.reject(err);
        });
        return firstDef.promise;
      }
      return secondDef.promise;
    };

    const pInv1 = qsugg.ensureFieldSuggestions(store, ctx, scope2, false);
    const pInv2 = qsugg.ensureFieldSuggestions(store, ctx, scope2, true);
    secondDef.reject(new Error("still unavailable"));
    const [resInv1, resInv2] = await Promise.all([pInv1, pInv2]);
    assert.equal(invCalls, 2);
    assert.equal(resInv2.loading, false);
    assert.equal(resInv2.stale, true);
    assert.equal(resInv1.stale, true);
    assert.equal(
      qsugg.getFieldScopeEntry(store, "", "invoices", false),
      null,
      "force-superseded unpopulated placeholder must still be evicted on transient error"
    );
  });

  test("f4_transient_forced_refresh_failure_retains_cached_items_and_marks_stale", async () => {
    const store = qsugg.createQuerySuggestionStore();
    const ctx = {
      rootPath,
      rootCollections: ["products"],
      onRerender: () => {},
    };
    const scope = {
      parentSubpath: "",
      collectionId: "products",
      allDescendants: false,
    };

    let fieldCall = 0;
    harness.fetchImpl = async () => {
      fieldCall++;
      if (fieldCall === 1) {
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/products/p1`,
              fields: { price: { doubleValue: 9.99 } },
            },
          },
          {
            document: {
              name: `${rootPath}/products/p2`,
              fields: { title: { stringValue: "Widget" } },
            },
          },
        ]);
      }
      if (fieldCall === 2) {
        return jsonResponse(
          { error: { code: 503, status: "UNAVAILABLE", message: "retry later" } },
          503
        );
      }
      return jsonResponse([
        {
          document: {
            name: `${rootPath}/products/p1`,
            fields: {
              price: { doubleValue: 9.99 },
              sku: { stringValue: "W-1" },
            },
          },
        },
      ]);
    };

    // 1. Initial fetch populates cache.
    const initial = await qsugg.ensureFieldSuggestions(store, ctx, scope, false);
    assert.equal(fieldCall, 1);
    assert.equal(initial.stale, false);
    assert.equal(initial.sampledDocs, 2);
    assert.deepEqual(
      initial.items.map((i) => i.value),
      ["price", "title", "__name__"]
    );

    // 2. Forced refresh fails transiently -> retains last-known items and marks stale = true.
    const failedRefresh = await qsugg.ensureFieldSuggestions(
      store,
      ctx,
      scope,
      true
    );
    assert.equal(fieldCall, 2);
    assert.equal(failedRefresh.loading, false);
    assert.equal(failedRefresh.stale, true);
    assert.equal(failedRefresh.sampledDocs, 2);
    assert.deepEqual(
      failedRefresh.items.map((i) => i.value),
      ["price", "title", "__name__"]
    );
    const cachedAfterFail = qsugg.getFieldScopeEntry(
      store,
      "",
      "products",
      false
    );
    assert.equal(cachedAfterFail, failedRefresh);
    assert.equal(
      qsugg.formatSuggestionFooter(cachedAfterFail),
      "Sampled 2 docs · Stale"
    );

    // 3. Next non-forced focus sees stale === true and retries automatically.
    const retried = await qsugg.ensureFieldSuggestions(store, ctx, scope, false);
    assert.equal(fieldCall, 3);
    assert.equal(retried.loading, false);
    assert.equal(retried.stale, false);
    assert.equal(retried.sampledDocs, 1);
    assert.deepEqual(
      retried.items.map((i) => i.value),
      ["price", "sku", "__name__"]
    );
    assert.equal(qsugg.formatSuggestionFooter(retried), "Sampled 1 doc");
  });

  test("f5_composed_ensureCollectionGroups_dedups_inflight_ensureSubcollections", async () => {
    const store = qsugg.createQuerySuggestionStore();
    const ctx = {
      rootPath,
      rootCollections: ["orgs"],
      onRerender: () => {},
    };

    const subDef = deferred();
    let listColCalls = 0;
    let runQueryCalls = 0;

    harness.fetchImpl = async (url) => {
      if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
        listColCalls++;
        return subDef.promise;
      }
      if (url === `/v1/${rootPath}/orgs/acme:runQuery`) {
        runQueryCalls++;
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/orgs/acme/projects/p1/tasks/t1`,
            },
          },
        ]);
      }
      throw new Error("unexpected url: " + url);
    };

    // Start ensureSubcollections first so it is in flight.
    const pSub = qsugg.ensureSubcollections(store, ctx, "orgs/acme");
    let cgResolved = false;
    const pCg = qsugg
      .ensureCollectionGroups(store, ctx, "orgs/acme")
      .then((entry) => {
        cgResolved = true;
        return entry;
      });

    await nextTick();
    await nextTick();
    assert.equal(listColCalls, 1, "ensureCollectionGroups must dedup onto in-flight ensureSubcollections");
    assert.equal(runQueryCalls, 1);
    assert.equal(
      cgResolved,
      false,
      "ensureCollectionGroups must wait for in-flight subcollections instead of resolving early"
    );

    subDef.resolve(jsonResponse({ collectionIds: ["empty_subcol", "projects"] }));
    const [subEntry, cgEntry] = await Promise.all([pSub, pCg]);
    assert.equal(cgResolved, true);
    assert.equal(subEntry.stale, false);
    assert.equal(cgEntry.loading, false);
    assert.equal(cgEntry.stale, false);
    assert.deepEqual(
      cgEntry.items.map((i) => i.value),
      ["empty_subcol", "projects", "tasks"]
    );
    assert.equal(store.inflight.size, 0);
  });

  test("f4_composed_ensureCollectionGroups_propagates_stale_when_subcollections_fail", async () => {
    const store = qsugg.createQuerySuggestionStore();
    const ctx = {
      rootPath,
      rootCollections: ["orgs"],
      onRerender: () => {},
    };

    let failSub = true;
    let listColCalls = 0;
    let runQueryCalls = 0;

    harness.fetchImpl = async (url) => {
      if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
        listColCalls++;
        if (failSub) {
          return jsonResponse(
            { error: { code: 503, status: "UNAVAILABLE", message: "subcol fail" } },
            503
          );
        }
        return jsonResponse({ collectionIds: ["empty_subcol", "projects"] });
      }
      if (url === `/v1/${rootPath}/orgs/acme:runQuery`) {
        runQueryCalls++;
        return jsonResponse([
          {
            document: {
              name: `${rootPath}/orgs/acme/projects/p1/tasks/t1`,
            },
          },
        ]);
      }
      throw new Error("unexpected url: " + url);
    };

    // 1. Subcollections fetch fails transiently while runQuery succeeds -> CG entry must be stale: true.
    const firstCg = await qsugg.ensureCollectionGroups(store, ctx, "orgs/acme");
    assert.equal(listColCalls, 1);
    assert.equal(runQueryCalls, 1);
    assert.equal(firstCg.loading, false);
    assert.equal(
      firstCg.stale,
      true,
      "ensureCollectionGroups must propagate stale: true when ensureSubcollections fails"
    );
    assert.deepEqual(
      firstCg.items.map((i) => i.value),
      ["projects", "tasks"]
    );
    assert.equal(qsugg.formatSuggestionFooter(firstCg), "Stale");
    assert.equal(
      store.subcollectionsByParent.has("orgs/acme"),
      false,
      "failed initial subcollections placeholder must be evicted"
    );

    // 2. Next non-forced call retries both subcollections and collection groups and recovers cleanly.
    failSub = false;
    const secondCg = await qsugg.ensureCollectionGroups(
      store,
      ctx,
      "orgs/acme",
      false
    );
    assert.equal(listColCalls, 2);
    assert.equal(runQueryCalls, 2);
    assert.equal(secondCg.loading, false);
    assert.equal(secondCg.stale, false);
    assert.deepEqual(
      secondCg.items.map((i) => i.value),
      ["empty_subcol", "projects", "tasks"]
    );
    assert.equal(qsugg.formatSuggestionFooter(secondCg), "");
  });

  test("f5_force_refresh_superseding_inflight_subcollections_chains_composed_caller", async () => {
    // Part A: Aborted sub1 takes the catch branch (rejects with AbortError) while sub2 is in flight.
    {
      const store = qsugg.createQuerySuggestionStore();
      const ctx = {
        rootPath,
        rootCollections: ["orgs"],
        onRerender: () => {},
      };

      const sub1Def = deferred();
      const sub2Def = deferred();
      let subCalls = 0;

      harness.fetchImpl = async (url, init) => {
        const signal = /** @type {AbortSignal | undefined} */ (init.signal);
        if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
          subCalls++;
          if (subCalls === 1) {
            signal?.addEventListener("abort", () => {
              const err = new Error("Aborted");
              err.name = "AbortError";
              sub1Def.reject(err);
            });
            return sub1Def.promise;
          }
          return sub2Def.promise;
        }
        if (url === `/v1/${rootPath}/orgs/acme:runQuery`) {
          return jsonResponse([
            {
              document: {
                name: `${rootPath}/orgs/acme/projects/p1/tasks/t1`,
              },
            },
          ]);
        }
        throw new Error("unexpected url in Part A: " + url);
      };

      let cgResolved = false;
      const pCg = qsugg
        .ensureCollectionGroups(store, ctx, "orgs/acme", false)
        .then((entry) => {
          cgResolved = true;
          return entry;
        });

      await nextTick();
      await nextTick();
      assert.equal(subCalls, 1);
      assert.equal(cgResolved, false);

      // Force-refresh subcollections while ensureCollectionGroups is awaiting sub1!
      const pSub2 = qsugg.ensureSubcollections(store, ctx, "orgs/acme", true);
      await nextTick();
      await nextTick();
      assert.equal(subCalls, 2);
      assert.equal(
        cgResolved,
        false,
        "ensureCollectionGroups must wait for replacement sub2 rather than returning empty placeholder on sub1 abort"
      );

      sub2Def.resolve(jsonResponse({ collectionIds: ["empty_subcol", "projects"] }));
      const [sub2Entry, cgEntry] = await Promise.all([pSub2, pCg]);
      assert.equal(cgResolved, true);
      assert.deepEqual(
        sub2Entry.items.map((i) => i.value),
        ["empty_subcol", "projects"]
      );
      assert.equal(cgEntry.loading, false);
      assert.equal(cgEntry.stale, false);
      assert.deepEqual(
        cgEntry.items.map((i) => i.value),
        ["empty_subcol", "projects", "tasks"]
      );
      assert.equal(store.inflight.size, 0);
    }

    // Part B: Aborted sub1 takes the try branch (listCollectionIds returns early without throwing
    // after fetch resolves on an aborted signal) while sub2 is still in flight.
    {
      const store = qsugg.createQuerySuggestionStore();
      const ctx = {
        rootPath,
        rootCollections: ["orgs"],
        onRerender: () => {},
      };

      const sub1Def = deferred();
      const sub2Def = deferred();
      let subCalls = 0;

      harness.fetchImpl = async (url) => {
        if (url === `/v1/${rootPath}/orgs/acme:listCollectionIds`) {
          subCalls++;
          return subCalls === 1 ? sub1Def.promise : sub2Def.promise;
        }
        if (url === `/v1/${rootPath}/orgs/acme:runQuery`) {
          return jsonResponse([
            {
              document: {
                name: `${rootPath}/orgs/acme/projects/p1/tasks/t1`,
              },
            },
          ]);
        }
        throw new Error("unexpected url in Part B: " + url);
      };

      let cgResolved = false;
      const pCg = qsugg
        .ensureCollectionGroups(store, ctx, "orgs/acme", false)
        .then((entry) => {
          cgResolved = true;
          return entry;
        });

      await nextTick();
      assert.equal(subCalls, 1);

      const pSub2 = qsugg.ensureSubcollections(store, ctx, "orgs/acme", true);
      assert.equal(subCalls, 2);

      // Resolve sub1 normally (non-throwing abort path inside listCollectionIds / try block) while sub2 is pending.
      sub1Def.resolve(jsonResponse({ collectionIds: [] }));
      await nextTick();
      await nextTick();
      assert.equal(
        cgResolved,
        false,
        "try-branch abort must also chain to replacement sub2 instead of resolving early"
      );

      sub2Def.resolve(jsonResponse({ collectionIds: ["audit", "projects"] }));
      const [sub2Entry, cgEntry] = await Promise.all([pSub2, pCg]);
      assert.equal(cgResolved, true);
      assert.deepEqual(
        sub2Entry.items.map((i) => i.value),
        ["audit", "projects"]
      );
      assert.equal(cgEntry.stale, false);
      assert.deepEqual(
        cgEntry.items.map((i) => i.value),
        ["audit", "projects", "tasks"]
      );
      assert.equal(store.inflight.size, 0);
    }

    // Part C: Cross-epoch resetQuerySuggestionStore does NOT chain old aborted fetch onto a new epoch's fetch.
    {
      const store = qsugg.createQuerySuggestionStore();
      const ctx = {
        rootPath,
        rootCollections: ["orgs"],
        onRerender: () => {},
      };

      const oldDef = deferred();
      const newDef = deferred();
      let calls = 0;
      harness.fetchImpl = async () => {
        calls++;
        return calls === 1 ? oldDef.promise : newDef.promise;
      };

      const pOld = qsugg.ensureSubcollections(store, ctx, "orgs/acme");
      qsugg.resetQuerySuggestionStore(store);

      let newResolved = false;
      const pNew = qsugg
        .ensureSubcollections(store, ctx, "orgs/acme")
        .then((e) => {
          newResolved = true;
          return e;
        });

      const abortErr = new Error("Aborted");
      abortErr.name = "AbortError";
      oldDef.reject(abortErr);
      const oldResult = await pOld;
      assert.equal(
        newResolved,
        false,
        "old-epoch aborted promise must resolve immediately without chaining onto new epoch's fetch"
      );
      assert.deepEqual(oldResult.items, []);

      newDef.resolve(jsonResponse({ collectionIds: ["fresh"] }));
      const newResult = await pNew;
      assert.deepEqual(
        newResult.items.map((i) => i.value),
        ["fresh"]
      );
      assert.equal(store.inflight.size, 0);
    }
  });
});
