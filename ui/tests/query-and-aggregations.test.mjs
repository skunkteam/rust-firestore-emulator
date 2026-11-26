// @ts-check
import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  deferred,
  findAllElements,
  findClickHandler,
  findInputByPlaceholder,
  findSelectByAriaLabel,
  harness,
  jsonResponse,
  renderTemplateMarkup,
  resetHarness,
} from "./helpers/dom-harness.mjs";
import * as app from "../app.js";
import * as query from "../components/query.js";
import * as queryModel from "../components/query-model.js";
import * as stateMod from "../state.js";
import * as values from "../values.js";

describe("Query runner, aggregations, and Raw JSON validation", () => {
  beforeEach(async () => {
    await resetHarness(app, stateMod);
  });

  afterEach(async () => {
    await resetHarness(app, stateMod);
  });

  test("regression_raw_protojson_overflow_float_is_rejected", () => {
    const res = values.validateRawProtoJsonFields(
      '{"fields":{"d":{"doubleValue":1e500}}}'
    );
    assert.equal(
      res.valid,
      false,
      'JSON numeric literal 1e500 overflows IEEE-754 double to Infinity (which stringifyProtoJson turns into null) and must be rejected unless quoted as "Infinity"'
    );
  });

  test("query_builder_array_operators_validation_and_error_banners", async () => {
    /**
     * @param {string} op
     * @param {string} valueText
     */
    const makeRow = (op, valueText) => ({
      id: 1,
      fieldPath: "tags",
      op,
      valueType: /** @type {import('../values.js').ValueTypeKey} */ (
        "stringValue"
      ),
      valueText,
      latText: "0",
      lngText: "0",
      valueTypeManuallySet: false,
      valueUserEdited: false,
    });

    // 1. IN, ARRAY_CONTAINS_ANY, and NOT_IN emit selected operator and encode values losslessly.
    for (const op of ["IN", "ARRAY_CONTAINS_ANY", "NOT_IN"]) {
      const qState = query.createQueryRunnerState("items");
      const baseArr =
        '[42, -0, 1.5, 1e25, {"doubleValue": "Infinity"}, {"doubleValue": "-Infinity"}, null, true, "ok", {"doubleValue": 9007199254740992}';
      qState.filters.push(
        makeRow(
          op,
          op === "ARRAY_CONTAINS_ANY"
            ? `${baseArr}]`
            : `${baseArr}, {"arrayValue": {"values": [{"integerValue": "7"}]}}]`
        )
      );
      const built = query.buildStructuredQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(
        built.valid,
        true,
        "expected valid build for " + op + ": " + built.error
      );
      assert.ok(built.query);
      const whereObj = /** @type {{ fieldFilter: { op: string, value: { arrayValue: { values: unknown[] } } } }} */ (
        built.query.where
      );
      const ff = whereObj.fieldFilter;
      assert.equal(ff.op, op);
      const items = ff.value.arrayValue.values;
      assert.deepEqual(items[0], { integerValue: "42" });
      assert.equal(
        Object.is(
          /** @type {{ doubleValue: number }} */ (items[1]).doubleValue,
          -0
        ),
        true
      );
      assert.deepEqual(items[2], { doubleValue: 1.5 });
      assert.deepEqual(items[3], { doubleValue: 1e25 });
      assert.deepEqual(items[4], { doubleValue: "Infinity" });
      assert.deepEqual(items[5], { doubleValue: "-Infinity" });
      assert.deepEqual(items[6], { nullValue: null });
      assert.deepEqual(items[7], { booleanValue: true });
      assert.deepEqual(items[8], { stringValue: "ok" });
      assert.deepEqual(items[9], { doubleValue: 9007199254740992 });
      if (op !== "ARRAY_CONTAINS_ANY") {
        assert.deepEqual(items[10], {
          arrayValue: { values: [{ integerValue: "7" }] },
        });
      }

      const serialized = values.stringifyProtoJson(built.query);
      assert.ok(
        serialized.includes('"doubleValue":-0.0'),
        "expected -0.0 in " + serialized
      );
      assert.ok(
        serialized.includes('"doubleValue":"Infinity"'),
        "expected Infinity string in " + serialized
      );
      assert.ok(
        serialized.includes('"doubleValue":"-Infinity"'),
        "expected -Infinity string in " + serialized
      );
    }

    // 2. Rejects unquoted |n| >= 2^53 integers, overflowing literals, nested arrays, and bad objects.
    assert.equal(
      values.validateRawProtoJsonFields(
        '{"fields":{"d":{"doubleValue":9007199254740992}}}'
      ).valid,
      true
    );
    assert.equal(
      values.validateRawProtoJsonFields(
        '{"fields":{"i":{"integerValue":9007199254740993}}}'
      ).valid,
      false
    );
    for (const [op, rawText, expectedSubstr] of [
      ["IN", "[9007199254740992]", "Unquoted 64-bit integer 9007199254740992 in IN"],
      ["ARRAY_CONTAINS_ANY", "[9007199254740993]", "ARRAY_CONTAINS_ANY"],
      ["NOT_IN", "[-9007199254740993]", "NOT_IN"],
      [
        "IN",
        '[{"integerValue": 9007199254740993}]',
        "Unquoted 64-bit integer 9007199254740993 in IN",
      ],
      ["IN", "[1e309]", "overflows IEEE-754 double"],
      ["NOT_IN", "[-1e309]", "overflows IEEE-754 double"],
      ["IN", '[{"doubleValue": 1e309}]', "overflows IEEE-754 double"],
      [
        "IN",
        "[[1]]",
        'must use explicit ProtoJSON {"arrayValue": {"values": [...]}}',
      ],
      [
        "NOT_IN",
        "[[1]]",
        'must use explicit ProtoJSON {"arrayValue": {"values": [...]}}',
      ],
      [
        "ARRAY_CONTAINS_ANY",
        "[[1]]",
        "Nested arrays are not supported in ARRAY_CONTAINS_ANY",
      ],
      [
        "ARRAY_CONTAINS_ANY",
        '[{"arrayValue":{"values":[{"integerValue":"1"}]}}]',
        "nested arrays",
      ],
      [
        "IN",
        '[{"arrayValue":{"values":[{"arrayValue":{"values":[{"integerValue":"1"}]}}]}}]',
        "nested arrays",
      ],
      ["ARRAY_CONTAINS_ANY", '[{"foo": 1}]', "unsupported Value key"],
      ["NOT_IN", "not json", 'Filter "tags NOT_IN"'],
      [
        "ARRAY_CONTAINS_ANY",
        "{}",
        'Filter "tags ARRAY_CONTAINS_ANY" requires a JSON array',
      ],
    ]) {
      const qState = query.createQueryRunnerState("items");
      qState.filters.push(makeRow(op, rawText));
      const res = query.buildStructuredQueryFromVisual(qState, "p", "(default)");
      assert.equal(
        res.valid,
        false,
        "expected invalid result for " + op + " " + rawText
      );
      assert.ok(
        res.error?.includes(expectedSubstr),
        `expected error containing ${JSON.stringify(expectedSubstr)}, got ${res.error}`
      );
    }

    // 3. renderFilterRow initializes "[]" when switching into an array operator and renders JSON input.
    {
      const qState = query.createQueryRunnerState("items");
      qState.filters.push(makeRow("EQUAL", "alice"));
      const tpl1 = query.renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["items"],
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      });
      const opSelect = findSelectByAriaLabel(tpl1, "Filter operator");
      assert.ok(
        opSelect?.handlers.change,
        "expected operator <select> @change handler"
      );
      opSelect.handlers.change({ target: { value: "NOT_IN" } });
      assert.equal(qState.filters[0].op, "NOT_IN");
      assert.equal(qState.filters[0].valueText, "[]");
      const tpl2 = query.renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["items"],
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      });
      assert.ok(
        findInputByPlaceholder(tpl2, "JSON array"),
        "expected JSON array input after switch"
      );
    }

    // 4. executeCurrentQuery formats ApiError banners with "[code status] message".
    {
      app.state.activeProjectId = "p";
      app.state.activeDatabaseId = "(default)";
      app.state.queryState = query.createQueryRunnerState("items");
      harness.fetchImpl = async (url) => {
        assert.ok(url.endsWith(":runQuery"));
        return jsonResponse(
          {
            error: {
              code: 400,
              status: "INVALID_ARGUMENT",
              message: "bad filter",
            },
          },
          400
        );
      };
      await app.executeCurrentQuery();
      assert.equal(
        app.state.queryState.error,
        "[400 INVALID_ARGUMENT] bad filter"
      );
    }
  });

  test("slice1_values_and_aggregation_builder_validation", () => {
    // 1. findNonFiniteNumber and isReservedName exports.
    assert.equal(
      values.findNonFiniteNumber({
        a: 1,
        b: [-0, "Infinity", "NaN", { c: 42 }],
      }),
      null
    );
    assert.equal(values.findNonFiniteNumber(Infinity), "$");
    assert.equal(
      values.findNonFiniteNumber({ a: { b: [1, -Infinity] } }),
      "$.a.b[1]"
    );
    assert.equal(values.findNonFiniteNumber([1, NaN], "tags"), "tags[1]");
    assert.equal(
      values.findNonFiniteNumber({ "foo bar": Infinity }),
      '$["foo bar"]'
    );

    for (const reserved of ["__a__", "__name__", "__custom__", "__proto__"]) {
      assert.equal(
        values.isReservedName(reserved),
        true,
        "expected reserved: " + reserved
      );
    }
    for (const allowed of ["____", "___", "_a_", "__a", "a__", "", "normal"]) {
      assert.equal(
        values.isReservedName(allowed),
        false,
        "expected non-reserved: " + allowed
      );
    }

    // 2. buildAggregationQueryFromVisual: default COUNT, SUM/AVG, empty field, 1..=5 cap.
    {
      const qState = query.createQueryRunnerState("orders");
      const built = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(built.valid, true, built.error ?? "");
      assert.deepEqual(built.query, {
        structuredQuery: {
          from: [{ collectionId: "orders" }],
          limit: 50,
        },
        aggregations: [{ count: {} }],
      });

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "total" },
        { id: 2, op: "SUM", fieldPath: "amount", alias: "" },
        { id: 3, op: "AVG", fieldPath: "score", alias: "" },
      ];
      const mixed = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(mixed.valid, true, mixed.error ?? "");
      assert.deepEqual(mixed.query?.aggregations, [
        { count: {}, alias: "total" },
        { sum: { field: { fieldPath: "amount" } } },
        { avg: { field: { fieldPath: "score" } } },
      ]);

      qState.aggregations = [{ id: 1, op: "SUM", fieldPath: "  ", alias: "" }];
      const emptySum = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(emptySum.valid, false);
      assert.ok(emptySum.error?.includes("SUM"));

      qState.aggregations = [{ id: 1, op: "AVG", fieldPath: "", alias: "" }];
      const emptyAvg = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(emptyAvg.valid, false);
      assert.ok(emptyAvg.error?.includes("AVG"));

      qState.aggregations = [];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        false
      );
      qState.aggregations = Array.from({ length: 5 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: "",
      }));
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        true
      );
      qState.aggregations.push({ id: 6, op: "COUNT", fieldPath: "", alias: "" });
      const overCap = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(overCap.valid, false);
      assert.ok(overCap.error?.includes("5"));
    }

    // 3. Alias collision, reserved, >1500 UTF-8 bytes, prototype keys, Enterprise cap.
    {
      const qState = query.createQueryRunnerState("orders");
      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "field_1" },
        { id: 2, op: "COUNT", fieldPath: "", alias: "" },
      ];
      const collision = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(collision.valid, false);
      assert.ok(collision.error?.includes("field_1"));

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "__custom__" },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        false
      );

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "____" },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        true
      );

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "a".repeat(1500) },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        true
      );

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "€".repeat(501) },
      ];
      const tooLong = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(tooLong.valid, false);
      assert.ok(tooLong.error?.includes("1500"));

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "constructor" },
        { id: 2, op: "COUNT", fieldPath: "", alias: "toString" },
        { id: 3, op: "COUNT", fieldPath: "", alias: "hasOwnProperty" },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        true
      );
      qState.aggregations.push({
        id: 4,
        op: "COUNT",
        fieldPath: "",
        alias: "constructor",
      });
      const protoDup = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(protoDup.valid, false);
      assert.ok(protoDup.error?.includes("constructor"));

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "__name__" },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        false
      );
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: false,
        }).valid,
        false
      );
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: true,
        }).valid,
        true
      );

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "__proto__" },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        false
      );
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: true,
        }).valid,
        true
      );
      qState.aggregations.push({
        id: 2,
        op: "COUNT",
        fieldPath: "",
        alias: "__proto__",
      });
      const entProtoDup = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)",
        { enterprise: true }
      );
      assert.equal(entProtoDup.valid, false);
      assert.ok(entProtoDup.error?.includes("__proto__"));

      qState.aggregations = Array.from({ length: 6 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: `agg_${idx}`,
      }));
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)").valid,
        false
      );
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: true,
        }).valid,
        true
      );

      qState.aggregations = Array.from({ length: 64 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: "",
      }));
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: true,
        }).valid,
        true
      );
      qState.aggregations.push({
        id: 65,
        op: "COUNT",
        fieldPath: "",
        alias: "",
      });
      const entOverCap = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)",
        { enterprise: true }
      );
      assert.equal(entOverCap.valid, false);
      assert.ok(entOverCap.error?.includes("64"));

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "€".repeat(501) },
      ];
      assert.equal(
        query.buildAggregationQueryFromVisual(qState, "p", "(default)", {
          enterprise: true,
        }).valid,
        false
      );
    }

    // 4. UI row controls: sequential placeholders, COUNT clears/disables fieldPath, cap/delete disabled state.
    {
      const qState = query.createQueryRunnerState("orders");
      qState.queryKind = "aggregations";
      qState.aggregations = [
        { id: 1, op: "SUM", fieldPath: "amount", alias: "total" },
        { id: 2, op: "AVG", fieldPath: "score", alias: "" },
        { id: 3, op: "COUNT", fieldPath: "", alias: "" },
      ];
      /** @type {Parameters<typeof query.renderQueryRunner>[0]} */
      const props = {
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["orders"],
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      };
      const markup = renderTemplateMarkup(query.renderQueryRunner(props));
      assert.ok(markup.includes('placeholder="field_1"'));
      assert.ok(markup.includes('placeholder="field_2"'));
      assert.equal(markup.includes('placeholder="field_3"'), false);

      const tpl = query.renderQueryRunner(props);
      const aggOpSelect = findSelectByAriaLabel(
        tpl,
        "Aggregation 1 operator"
      );
      assert.ok(
        aggOpSelect?.handlers.change,
        "expected Aggregation 1 operator select handler"
      );
      aggOpSelect.handlers.change({ target: { value: "COUNT" } });
      assert.equal(qState.aggregations[0].op, "COUNT");
      assert.equal(qState.aggregations[0].fieldPath, "");
      const afterCountMarkup = renderTemplateMarkup(
        query.renderQueryRunner(props)
      ).replace(/\s+/g, " ");
      assert.ok(
        afterCountMarkup.includes(
          'aria-label="Aggregation 1 field path" placeholder="—" disabled'
        )
      );

      qState.aggregations = [
        { id: 1, op: "COUNT", fieldPath: "", alias: "" },
      ];
      const oneRowMarkup = renderTemplateMarkup(
        query.renderQueryRunner(props)
      ).replace(/\s+/g, " ");
      assert.ok(
        oneRowMarkup.includes(
          'disabled title="Remove aggregation" aria-label="Remove aggregation 1"'
        )
      );
      qState.aggregations = Array.from({ length: 5 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: "",
      }));
      const fiveRowMarkup = renderTemplateMarkup(
        query.renderQueryRunner(props)
      ).replace(/\s+/g, " ");
      assert.ok(
        fiveRowMarkup.includes('disabled title="Maximum 5 aggregations per query"')
      );
      const entFiveRowMarkup = renderTemplateMarkup(
        query.renderQueryRunner({ ...props, enterprise: true })
      ).replace(/\s+/g, " ");
      assert.equal(
        entFiveRowMarkup.includes("Maximum 5 aggregations per query"),
        false
      );
      assert.ok(entFiveRowMarkup.includes('title="Add aggregation"'));
      qState.aggregations = Array.from({ length: 64 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: "",
      }));
      const entSixtyFourMarkup = renderTemplateMarkup(
        query.renderQueryRunner({ ...props, enterprise: true })
      ).replace(/\s+/g, " ");
      assert.ok(
        entSixtyFourMarkup.includes(
          'disabled title="Maximum 64 aggregations per query"'
        )
      );
    }
  });

  test("slice1_modes_limit_raw_validation_and_execution_isolation", async () => {
    /**
     * @param {import('../components/query-model.js').QueryRunnerState} qState
     * @returns {Parameters<typeof query.renderQueryRunner>[0]}
     */
    const makeProps = (qState) => ({
      projectId: "p",
      databaseId: "(default)",
      knownCollections: ["orders"],
      state: qState,
      onRerender: () => {},
      onExecuteQuery: () => {},
      onOpenResultDocument: () => {},
    });

    // 1. limitUserModified toggling ("50" <-> "" when untouched; preserved once modified) & inline "!" warning.
    {
      const qState = query.createQueryRunnerState("orders");
      assert.equal(qState.limitText, "50");
      assert.equal(qState.limitUserModified, false);
      const docMarkup = renderTemplateMarkup(
        query.renderQueryRunner(makeProps(qState))
      );
      assert.equal(docMarkup.includes("query-limit-warning"), false);
      assert.ok(docMarkup.includes('min="0"'));

      findClickHandler(
        query.renderQueryRunner(makeProps(qState)),
        "Aggregations"
      )?.();
      assert.equal(qState.queryKind, "aggregations");
      assert.equal(qState.limitText, "");
      assert.equal(
        renderTemplateMarkup(query.renderQueryRunner(makeProps(qState))).includes(
          "query-limit-warning"
        ),
        false
      );

      findClickHandler(
        query.renderQueryRunner(makeProps(qState)),
        "Documents"
      )?.();
      assert.equal(qState.queryKind, "documents");
      assert.equal(qState.limitText, "50");

      const limitInput = findInputByPlaceholder(
        query.renderQueryRunner(makeProps(qState)),
        "No limit"
      );
      assert.ok(limitInput?.handlers.input);
      limitInput.handlers.input({ target: { value: "0" } });
      assert.equal(qState.limitUserModified, true);
      assert.equal(qState.limitText, "0");

      findClickHandler(
        query.renderQueryRunner(makeProps(qState)),
        "Aggregations"
      )?.();
      assert.equal(qState.limitText, "0");
      const aggLimitMarkup = renderTemplateMarkup(
        query.renderQueryRunner(makeProps(qState))
      );
      assert.ok(aggLimitMarkup.includes('class="query-limit-warning"'));
      assert.ok(
        aggLimitMarkup.includes(
          'title="Aggregation runs over the limited query window"'
        )
      );
      assert.ok(
        aggLimitMarkup.includes(
          'aria-label="Aggregation runs over the limited query window"'
        )
      );

      const builtLimitZero = query.buildAggregationQueryFromVisual(
        qState,
        "p",
        "(default)"
      );
      assert.equal(builtLimitZero.valid, true);
      assert.equal(
        /** @type {{ limit?: number } | undefined} */ (
          builtLimitZero.query?.structuredQuery
        )?.limit,
        0
      );
    }

    // 2. Visual -> Raw transitions: pristine pass-through, valid compile, invalid blocking, & buffer swap.
    {
      const pristine = query.createQueryRunnerState();
      pristine.rawQueryText = '{"from":[{"collectionId":"custom"}]}';
      pristine.rawAggregationText =
        '{"structuredQuery":{"from":[{"collectionId":"custom"}]},"aggregations":[{"count":{}}]}';

      findClickHandler(
        query.renderQueryRunner(makeProps(pristine)),
        "Raw JSON"
      )?.();
      assert.equal(pristine.mode, "raw");
      assert.equal(pristine.error, null);
      assert.equal(
        pristine.rawQueryText,
        '{"from":[{"collectionId":"custom"}]}'
      );

      const rawAggBtn = findClickHandler(
        query.renderQueryRunner(makeProps(pristine)),
        "Aggregations"
      );
      assert.ok(rawAggBtn);
      rawAggBtn();
      assert.equal(pristine.mode, "raw");
      assert.equal(pristine.queryKind, "aggregations");
      assert.equal(
        pristine.rawQueryText,
        '{"from":[{"collectionId":"custom"}]}'
      );
      assert.equal(
        pristine.rawAggregationText,
        '{"structuredQuery":{"from":[{"collectionId":"custom"}]},"aggregations":[{"count":{}}]}'
      );
      assert.ok(
        renderTemplateMarkup(
          query.renderQueryRunner(makeProps(pristine))
        ).includes("▶ Run Aggregation")
      );

      findClickHandler(
        query.renderQueryRunner(makeProps(pristine)),
        "Visual"
      )?.();
      pristine.rawAggregationText =
        '{"structuredQuery":{"from":[{"collectionId":"kept"}]},"aggregations":[{"count":{"upTo":"5"}}]}';
      findClickHandler(
        query.renderQueryRunner(makeProps(pristine)),
        "Raw JSON"
      )?.();
      assert.equal(pristine.mode, "raw");
      assert.equal(pristine.error, null);
      assert.ok(pristine.rawAggregationText.includes('"upTo":"5"'));

      const validNonPristine = query.createQueryRunnerState("orders");
      findClickHandler(
        query.renderQueryRunner(makeProps(validNonPristine)),
        "Raw JSON"
      )?.();
      assert.equal(validNonPristine.mode, "raw");
      assert.ok(
        validNonPristine.rawQueryText.includes('"collectionId": "orders"')
      );

      const invalidLimit = query.createQueryRunnerState("orders");
      invalidLimit.limitText = "-1";
      invalidLimit.limitUserModified = true;
      findClickHandler(
        query.renderQueryRunner(makeProps(invalidLimit)),
        "Raw JSON"
      )?.();
      assert.equal(invalidLimit.mode, "visual");
      assert.ok(invalidLimit.error?.includes("non-negative integer"));

      const invalidSum = query.createQueryRunnerState("orders");
      invalidSum.queryKind = "aggregations";
      invalidSum.aggregations = [
        { id: 1, op: "SUM", fieldPath: "", alias: "" },
      ];
      findClickHandler(
        query.renderQueryRunner(makeProps(invalidSum)),
        "Raw JSON"
      )?.();
      assert.equal(invalidSum.mode, "visual");
      assert.ok(invalidSum.error?.includes("SUM aggregation"));

      const invalidSubpathOnly = query.createQueryRunnerState("", "rooms/r1");
      findClickHandler(
        query.renderQueryRunner(makeProps(invalidSubpathOnly)),
        "Raw JSON"
      )?.();
      assert.equal(invalidSubpathOnly.mode, "visual");
      assert.ok(invalidSubpathOnly.error?.includes("Collection ID is required"));
    }

    // 3. Raw JSON client-side validation & bare vs optional-wrapper execution.
    {
      app.state.activeProjectId = "p";
      app.state.activeDatabaseId = "(default)";

      /**
       * @param {'documents' | 'aggregations'} queryKind
       * @param {string} rawText
       * @param {string} expectedSubstr
       */
      const assertRawError = async (queryKind, rawText, expectedSubstr) => {
        const qState = query.createQueryRunnerState("orders");
        qState.queryKind = queryKind;
        qState.mode = "raw";
        if (queryKind === "aggregations") qState.rawAggregationText = rawText;
        else qState.rawQueryText = rawText;
        app.state.queryState = qState;
        let called = false;
        harness.fetchImpl = async () => {
          called = true;
          throw new Error("fetch should not be called");
        };
        await app.executeCurrentQuery();
        assert.equal(
          called,
          false,
          "fetch must not run for invalid raw payload: " + rawText
        );
        assert.ok(
          qState.error && qState.error.includes(expectedSubstr),
          `expected error containing ${JSON.stringify(expectedSubstr)}, got ${qState.error}`
        );
      };

      for (const nonObj of ["[]", "123", '"abc"', "null", "true"]) {
        await assertRawError(
          "documents",
          nonObj,
          "Raw JSON must be a JSON object."
        );
        await assertRawError(
          "aggregations",
          nonObj,
          "Raw JSON must be a JSON object."
        );
      }
      await assertRawError(
        "documents",
        '{"from":[{"collectionId":"orders"}],"limit":9007199254740993}',
        "9007199254740993"
      );
      await assertRawError(
        "aggregations",
        '{"structuredQuery":{"from":[{"collectionId":"orders"}]},"aggregations":[{"count":{"upTo":9007199254740993}}]}',
        "9007199254740993"
      );
      await assertRawError(
        "documents",
        '{"from":[{"collectionId":"orders"}],"limit":1e309}',
        "Raw JSON contains a non-finite number at $.limit (number overflow)."
      );
      await assertRawError(
        "aggregations",
        '{"structuredAggregationQuery":{"structuredQuery":{"from":[{"collectionId":"orders"}],"limit":-1e309},"aggregations":[{"count":{}}]}}',
        "Raw JSON contains a non-finite number at $.structuredAggregationQuery.structuredQuery.limit (number overflow)."
      );

      // Non-null unsupported envelope keys rejected in both bare and wrapped payloads.
      for (const badKey of [
        "readTime",
        "transaction",
        "newTransaction",
        "explainOptions",
        "requestOptions",
      ]) {
        await assertRawError(
          "documents",
          `{"from":[{"collectionId":"orders"}], "${badKey}": {}}`,
          "Unsupported top-level field(s) in Raw JSON"
        );
        await assertRawError(
          "documents",
          `{"structuredQuery":{"from":[{"collectionId":"orders"}]}, "${badKey}": {}}`,
          "Unsupported top-level field(s) in Raw JSON alongside structuredQuery"
        );
        await assertRawError(
          "aggregations",
          `{"structuredQuery":{"from":[{"collectionId":"orders"}]}, "aggregations":[{"count":{}}], "${badKey}": {}}`,
          "Unsupported top-level field(s) in Raw JSON"
        );
        await assertRawError(
          "aggregations",
          `{"structuredAggregationQuery":{"structuredQuery":{"from":[{"collectionId":"orders"}]}, "aggregations":[{"count":{}}]}, "${badKey}": {}}`,
          "Unsupported top-level field(s) in Raw JSON alongside structuredAggregationQuery"
        );
      }
      await assertRawError(
        "aggregations",
        '{"structuredAggregationQuery":{"structuredQuery":{"from":[{"collectionId":"orders"}]},"aggregations":[{"count":{}}]},"structuredQuery":{}}',
        "alongside structuredAggregationQuery"
      );
      await assertRawError(
        "documents",
        '{"structuredQuery": null}',
        '"structuredQuery" in Raw JSON must be a JSON object.'
      );

      /** @type {Array<{ url: string, body: unknown, hasSignal: boolean }>} */
      const requests = [];
      harness.fetchImpl = async (url, init) => {
        requests.push({
          url,
          body: JSON.parse(String(init.body ?? "{}")),
          hasSignal: Boolean(init.signal),
        });
        if (url.endsWith(":runQuery")) {
          return jsonResponse([
            {
              document: {
                name: "projects/p/databases/(default)/documents/orders/o1",
                fields: {},
              },
              readTime: "2026-01-01T00:00:01Z",
            },
          ]);
        }
        if (url.endsWith(":runAggregationQuery")) {
          return jsonResponse(
            '[{"result":{"aggregateFields":{"big_count":{"integerValue":"9007199254740993"},"neg_zero":{"doubleValue":-0.0},"empty_avg":{"nullValue":null}}},"readTime":"2026-01-01T00:00:02Z"}]'
          );
        }
        throw new Error("unexpected url: " + url);
      };

      const shared = query.createQueryRunnerState("orders");
      shared.mode = "raw";
      shared.rawQueryText =
        '{"structuredQuery":{"from":[{"collectionId":"orders"}],"limit":5}}';
      shared.rawAggregationText =
        '{"structuredAggregationQuery":{"structuredQuery":{"from":[{"collectionId":"orders"}],"orderBy":[{"field":{"fieldPath":"age"},"direction":"ASCENDING"}],"limit":0},"aggregations":[{"count":{},"alias":"big_count"}]}}';
      app.state.queryState = shared;
      await app.executeCurrentQuery();
      assert.equal(shared.results?.length, 1);
      assert.equal(shared.readTime, "2026-01-01T00:00:01Z");
      assert.equal(typeof shared.elapsedMs, "number");
      assert.equal(shared.aggResults, null);

      shared.queryKind = "aggregations";
      await app.executeCurrentQuery();
      assert.equal(
        shared.results?.length,
        1,
        "Documents results must not be clobbered"
      );
      assert.equal(shared.readTime, "2026-01-01T00:00:01Z");
      assert.equal(shared.aggReadTime, "2026-01-01T00:00:02Z");
      assert.equal(typeof shared.aggElapsedMs, "number");
      assert.deepEqual(shared.aggProvenance, { limit: 0, hasOrderBy: true });
      assert.equal(requests[0].hasSignal, true);
      assert.equal(requests[1].hasSignal, true);
      assert.deepEqual(requests[1].body, {
        structuredAggregationQuery: {
          structuredQuery: {
            from: [{ collectionId: "orders" }],
            orderBy: [{ field: { fieldPath: "age" }, direction: "ASCENDING" }],
            limit: 0,
          },
          aggregations: [{ count: {}, alias: "big_count" }],
        },
      });

      const aggMarkup = renderTemplateMarkup(
        query.renderQueryRunner(makeProps(shared))
      ).replace(/\s+/g, " ");
      assert.ok(aggMarkup.includes("Constrained by orderBy + limit: 0"));
      assert.ok(aggMarkup.includes("9007199254740993"));
      assert.ok(
        aggMarkup.includes('<span class="type-badge integerValue"> int64 </span>')
      );
      assert.ok(aggMarkup.includes("-0.0"));
      assert.ok(
        aggMarkup.includes('<span class="type-badge doubleValue"> double </span>')
      );
      assert.ok(
        aggMarkup.includes('<span class="type-badge nullValue"> null </span>')
      );

      shared.aggProvenance = { limit: 50, hasOrderBy: false };
      assert.ok(
        renderTemplateMarkup(
          query.renderQueryRunner(makeProps(shared))
        ).includes("Constrained by limit: 50")
      );
      shared.aggProvenance = { limit: null, hasOrderBy: true };
      assert.ok(
        renderTemplateMarkup(
          query.renderQueryRunner(makeProps(shared))
        ).includes("Constrained by orderBy")
      );
      shared.aggProvenance = null;
      assert.equal(
        renderTemplateMarkup(
          query.renderQueryRunner(makeProps(shared))
        ).includes("agg-provenance-note"),
        false
      );

      shared.aggResults = JSON.parse(
        '{"__proto__":{"integerValue":"7"},"constructor":{"doubleValue":1.5}}'
      );
      const protoMarkup = renderTemplateMarkup(
        query.renderQueryRunner(makeProps(shared))
      );
      assert.ok(protoMarkup.includes("__proto__"));
      assert.ok(protoMarkup.includes("constructor"));

      shared.queryKind = "documents";
      shared.rawQueryText = '{"from":[{"collectionId":"orders"}],"limit":3}';
      await app.executeCurrentQuery();
      assert.deepEqual(requests[2].body, {
        structuredQuery: { from: [{ collectionId: "orders" }], limit: 3 },
      });
      shared.queryKind = "aggregations";
      shared.rawAggregationText =
        '{"structuredQuery":{"from":[{"collectionId":"orders"}]},"aggregations":[{"count":{}}]}';
      await app.executeCurrentQuery();
      assert.deepEqual(requests[3].body, {
        structuredAggregationQuery: {
          structuredQuery: { from: [{ collectionId: "orders" }] },
          aggregations: [{ count: {} }],
        },
      });
      assert.equal(shared.aggProvenance, null);
    }

    // 4. Execution cancellation, stale-response dropping, ApiError formatting, and reset/onOpenQueryCollection aborts.
    {
      app.state.activeProjectId = "p";
      app.state.activeDatabaseId = "(default)";

      // 4A. Superseded query.
      const run1Def = deferred();
      const run2Def = deferred();
      /** @type {AbortSignal[]} */
      const signals = [];
      let callIdx = 0;
      harness.fetchImpl = async (_url, init) => {
        signals.push(/** @type {AbortSignal} */ (init.signal));
        const idx = callIdx++;
        if (idx === 0) return run1Def.promise;
        return run2Def.promise;
      };

      const qState = query.createQueryRunnerState("orders");
      qState.queryKind = "aggregations";
      app.state.queryState = qState;
      const pRun1 = app.executeCurrentQuery();
      assert.equal(qState.running, true);
      const pRun2 = app.executeCurrentQuery();
      assert.equal(
        signals[0].aborted,
        true,
        "prior query signal must abort when superseded"
      );
      assert.equal(signals[1].aborted, false);

      const abortErr = new Error("Aborted");
      abortErr.name = "AbortError";
      run1Def.reject(abortErr);
      await pRun1;
      assert.equal(
        qState.running,
        true,
        "stale aborted run1 must not reset run2 running flag"
      );
      assert.equal(qState.error, null);

      run2Def.resolve(
        jsonResponse([
          {
            result: { aggregateFields: { field_1: { integerValue: "12" } } },
            readTime: "2026-01-01T00:00:03Z",
          },
        ])
      );
      await pRun2;
      assert.equal(qState.running, false);
      assert.equal(qState.aggResults?.field_1.integerValue, "12");

      // 4B. resetDatabaseCaches() aborts active query and prevents late write.
      const staleDef = deferred();
      const staleCapture = { signal: /** @type {AbortSignal | null} */ (null) };
      harness.fetchImpl = async (_url, init) => {
        staleCapture.signal = /** @type {AbortSignal} */ (init.signal);
        return staleDef.promise;
      };
      const pStale = app.executeCurrentQuery();
      assert.ok(staleCapture.signal);
      assert.equal(staleCapture.signal.aborted, false);
      stateMod.bumpDbEpoch();
      stateMod.resetDatabaseCaches();
      assert.equal(
        staleCapture.signal.aborted,
        true,
        "resetDatabaseCaches must abort active query run"
      );
      staleDef.resolve(
        jsonResponse([
          { result: { aggregateFields: { field_1: { integerValue: "99" } } } },
        ])
      );
      await pStale;
      assert.equal(app.state.queryState.aggResults, null);

      // 4C. onOpenQueryCollection() calls abortActiveQueryRun() before replacing state.queryState.
      const openDef = deferred();
      const openCapture = { signal: /** @type {AbortSignal | null} */ (null) };
      harness.fetchImpl = async (_url, init) => {
        openCapture.signal = /** @type {AbortSignal} */ (init.signal);
        return openDef.promise;
      };
      app.state.queryState = query.createQueryRunnerState("orders");
      const pOpen = app.executeCurrentQuery();
      assert.ok(openCapture.signal);
      assert.equal(openCapture.signal.aborted, false);
      app.state.view = "explorer";
      app.state.rootCollections = ["orders"];
      app.state.segments = ["orders"];
      app.state.collectionColumns = new Map([
        [
          "projects/p/databases/(default)/documents/orders",
          {
            collectionPath: "projects/p/databases/(default)/documents/orders",
            collectionId: "orders",
            parentPath: "projects/p/databases/(default)/documents",
            documents: [],
            nextPageToken: "",
            filterText: "",
            loading: false,
          },
        ],
      ]);
      const explorerQueryBtn = findClickHandler(app.renderWorkspace(), "Query");
      assert.ok(explorerQueryBtn, "expected Explorer Query button");
      explorerQueryBtn();
      assert.equal(
        openCapture.signal.aborted,
        true,
        "onOpenQueryCollection must abort active query run"
      );
      openDef.resolve(
        jsonResponse([
          {
            document: { name: "projects/p/databases/(default)/documents/orders/o1" },
            readTime: "t",
          },
        ])
      );
      await pOpen;
      assert.equal(app.state.queryState.results, null);

      // 4D. ApiError banner formatting on :runAggregationQuery + Enterprise visual execution.
      app.state.activeDbMeta = {
        name: "projects/p/databases/(default)",
        uid: "u1",
        type: "FIRESTORE_NATIVE",
        databaseEdition: "ENTERPRISE",
        concurrencyMode: "PESSIMISTIC",
      };
      app.state.view = "query";
      app.state.queryState = query.createQueryRunnerState("orders");
      app.state.queryState.queryKind = "aggregations";
      app.state.queryState.aggregations = Array.from({ length: 6 }, (_, idx) => ({
        id: idx + 1,
        op: "COUNT",
        fieldPath: "",
        alias: idx === 0 ? "__name__" : `c_${idx}`,
      }));
      const wsEntMarkup = renderTemplateMarkup(app.renderWorkspace()).replace(
        /\s+/g,
        " "
      );
      assert.ok(wsEntMarkup.includes('title="Add aggregation"'));
      const entCapture = {
        body: /** @type {{ structuredAggregationQuery: { aggregations: Array<{ alias?: string }> } } | null} */ (
          null
        ),
      };
      harness.fetchImpl = async (url, init) => {
        assert.ok(url.endsWith(":runAggregationQuery"));
        entCapture.body = JSON.parse(String(init.body ?? "{}"));
        return jsonResponse([
          {
            result: { aggregateFields: { __name__: { integerValue: "3" } } },
            readTime: "t",
          },
        ]);
      };
      await app.executeCurrentQuery();
      assert.equal(app.state.queryState.error, null);
      assert.equal(entCapture.body?.structuredAggregationQuery.aggregations.length, 6);
      assert.equal(
        entCapture.body?.structuredAggregationQuery.aggregations[0].alias,
        "__name__"
      );

      app.state.activeDbMeta = null;
      app.state.queryState = query.createQueryRunnerState("orders");
      app.state.queryState.queryKind = "aggregations";
      harness.fetchImpl = async (url) => {
        assert.ok(url.endsWith(":runAggregationQuery"));
        return jsonResponse(
          { error: { code: 400, status: "INVALID_ARGUMENT", message: "bad agg" } },
          400
        );
      };
      await app.executeCurrentQuery();
      assert.equal(app.state.queryState.error, "[400 INVALID_ARGUMENT] bad agg");
    }
  });

  test("thread_11: canonical null envelope keys and matching/mismatched parent in Raw JSON", async () => {
    const parentPath = "projects/p/databases/(default)/documents/rooms/r1";
    const sqInner = { from: [{ collectionId: "messages" }], limit: 5 };
    const aggInner = {
      structuredQuery: sqInner,
      aggregations: [{ count: {} }],
    };

    // 1. Canonical null on explainOptions, newTransaction, requestOptions, readTime, transaction accepted & stripped.
    for (const key of [
      "explainOptions",
      "newTransaction",
      "requestOptions",
      "readTime",
      "transaction",
    ]) {
      const bareDoc = queryModel.parseRawQueryPayload(
        JSON.stringify({ ...sqInner, [key]: null }),
        "documents",
        parentPath
      );
      assert.equal(bareDoc.valid, true, `bare documents null ${key}: ${bareDoc.error}`);
      assert.equal(Object.hasOwn(bareDoc.payload ?? {}, key), false);

      const wrappedDoc = queryModel.parseRawQueryPayload(
        JSON.stringify({ structuredQuery: sqInner, [key]: null }),
        "documents",
        parentPath
      );
      assert.equal(
        wrappedDoc.valid,
        true,
        `wrapped documents null ${key}: ${wrappedDoc.error}`
      );
      assert.deepEqual(wrappedDoc.payload, sqInner);

      const bareAgg = queryModel.parseRawQueryPayload(
        JSON.stringify({ ...aggInner, [key]: null }),
        "aggregations",
        parentPath
      );
      assert.equal(bareAgg.valid, true, `bare agg null ${key}: ${bareAgg.error}`);
      assert.equal(Object.hasOwn(bareAgg.payload ?? {}, key), false);

      const wrappedAgg = queryModel.parseRawQueryPayload(
        JSON.stringify({ structuredAggregationQuery: aggInner, [key]: null }),
        "aggregations",
        parentPath
      );
      assert.equal(
        wrappedAgg.valid,
        true,
        `wrapped agg null ${key}: ${wrappedAgg.error}`
      );
      assert.deepEqual(wrappedAgg.payload, aggInner);

      // Non-null values remain rejected.
      assert.equal(
        queryModel.parseRawQueryPayload(
          JSON.stringify({ structuredQuery: sqInner, [key]: {} }),
          "documents",
          parentPath
        ).valid,
        false
      );
      assert.equal(
        queryModel.parseRawQueryPayload(
          JSON.stringify({ structuredAggregationQuery: aggInner, [key]: {} }),
          "aggregations",
          parentPath
        ).valid,
        false
      );
    }

    // 2. Wrapped parent validation: matching, empty "", and null allowed; mismatched or non-string rejected.
    for (const okParent of [parentPath, "", null]) {
      const docOk = queryModel.parseRawQueryPayload(
        JSON.stringify({ structuredQuery: sqInner, parent: okParent }),
        "documents",
        parentPath
      );
      assert.equal(docOk.valid, true, docOk.error ?? "");
      const aggOk = queryModel.parseRawQueryPayload(
        JSON.stringify({ structuredAggregationQuery: aggInner, parent: okParent }),
        "aggregations",
        parentPath
      );
      assert.equal(aggOk.valid, true, aggOk.error ?? "");
    }

    const docMismatch = queryModel.parseRawQueryPayload(
      JSON.stringify({
        structuredQuery: sqInner,
        parent: "projects/p/databases/(default)/documents/rooms/r2",
      }),
      "documents",
      parentPath
    );
    assert.equal(docMismatch.valid, false);
    assert.ok(docMismatch.error?.includes("does not match"));

    const aggMismatch = queryModel.parseRawQueryPayload(
      JSON.stringify({
        structuredAggregationQuery: aggInner,
        parent: "projects/other/databases/(default)/documents",
      }),
      "aggregations",
      parentPath
    );
    assert.equal(aggMismatch.valid, false);
    assert.ok(aggMismatch.error?.includes("does not match"));

    for (const badType of [123, {}, []]) {
      const res = queryModel.parseRawQueryPayload(
        JSON.stringify({ structuredQuery: sqInner, parent: badType }),
        "documents",
        parentPath
      );
      assert.equal(res.valid, false);
      assert.ok(res.error?.includes("must be a string or null"));
    }

    // 3. End-to-end executeCurrentQuery forwards subpath parentPath to parseRawQueryPayload.
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.queryState = query.createQueryRunnerState("messages", "rooms/r1");
    app.state.queryState.mode = "raw";
    app.state.queryState.rawQueryText = JSON.stringify({
      parent: parentPath,
      explainOptions: null,
      newTransaction: null,
      requestOptions: null,
      structuredQuery: sqInner,
    });
    /** @type {Array<{ url: string, body: unknown }>} */
    const sent = [];
    harness.fetchImpl = async (url, init) => {
      sent.push({ url, body: JSON.parse(String(init.body ?? "{}")) });
      return jsonResponse([]);
    };
    await app.executeCurrentQuery();
    assert.equal(app.state.queryState.error, null);
    assert.equal(sent.length, 1);
    assert.equal(sent[0].url, `/v1/${parentPath}:runQuery`);
    assert.deepEqual(sent[0].body, { structuredQuery: sqInner });

    app.state.queryState.rawQueryText = JSON.stringify({
      parent: "projects/p/databases/(default)/documents/rooms/wrong",
      structuredQuery: sqInner,
    });
    await app.executeCurrentQuery();
    assert.ok(
      /** @type {string | null} */ (app.state.queryState.error)?.includes(
        "does not match"
      )
    );
    assert.equal(sent.length, 1, "mismatched parent must not send fetch");
  });

  test("thread_11: cross-tab query error bleed prevented when switching between Documents and Aggregations", async () => {
    app.state.activeProjectId = "p";
    app.state.activeDatabaseId = "(default)";
    app.state.view = "query";
    app.state.queryState = query.createQueryRunnerState("orders");

    const docRunDef = deferred();
    const docCapture = { signal: /** @type {AbortSignal | null} */ (null) };
    harness.fetchImpl = async (_url, init) => {
      docCapture.signal = /** @type {AbortSignal} */ (init.signal);
      return docRunDef.promise;
    };

    const pDoc = app.executeCurrentQuery();
    assert.equal(app.state.queryState.running, true);
    assert.ok(docCapture.signal);
    assert.equal(docCapture.signal.aborted, false);

    // Clicking "Aggregations" tab triggers onAbortQueryRun, resets running, switches queryKind, clears error.
    const aggTabBtn = findClickHandler(app.renderWorkspace(), "Aggregations");
    assert.ok(aggTabBtn);
    aggTabBtn();
    assert.equal(
      docCapture.signal.aborted,
      true,
      "switching queryKind must abort active run"
    );
    assert.equal(app.state.queryState.queryKind, "aggregations");
    assert.equal(app.state.queryState.running, false);
    assert.equal(app.state.queryState.error, null);

    // Even if the stale Documents promise rejects with a non-Abort error, isCurrentRun() blocks cross-tab error bleed.
    docRunDef.reject(new Error("late network failure from documents tab"));
    await pDoc;
    assert.equal(
      app.state.queryState.error,
      null,
      "stale documents error must not bleed onto aggregations tab"
    );
    assert.equal(app.state.queryState.running, false);
  });

  test("thread_17: IN -> IS_NULL -> EQUAL boolean normalization, EQUAL -> IN -> GREATER_THAN reset, and boolean parity", () => {
    /** @type {import('../components/query-suggestions.js').FieldScopeEntry} */
    const fieldEntry = {
      items: [],
      aggItems: [],
      fieldHints: new Map([
        [
          "active",
          {
            scalarType: "booleanValue",
            arrayElementType: null,
            isNumeric: false,
          },
        ],
        [
          "title",
          {
            scalarType: "stringValue",
            arrayElementType: null,
            isNumeric: false,
          },
        ],
      ]),
      scope: { parentSubpath: "", collectionId: "users", allDescendants: false },
      loading: false,
      stale: false,
      partial: false,
      sampledDocs: 1,
    };

    // 1. IN -> IS_NULL -> EQUAL boolean transition (not gated on prevMeta.kind === "array").
    const boolRow = queryModel.createFilterRow();
    boolRow.fieldPath = "active";
    boolRow.op = "IN";
    boolRow.valueType = "booleanValue";
    boolRow.valueText = "[]";

    queryModel.transitionFilterOperator(boolRow, "IS_NULL", fieldEntry);
    assert.equal(boolRow.op, "IS_NULL");
    assert.equal(boolRow.valueType, "booleanValue");

    queryModel.transitionFilterOperator(boolRow, "EQUAL", fieldEntry);
    assert.equal(boolRow.op, "EQUAL");
    assert.equal(boolRow.valueType, "booleanValue");
    assert.equal(
      boolRow.valueText,
      "true",
      "IS_NULL -> EQUAL must normalize boolean valueText from [] to true"
    );

    // Also when fieldEntry is null (manually chosen booleanValue with empty/array text).
    const manualBoolRow = queryModel.createFilterRow();
    manualBoolRow.op = "IS_NULL";
    manualBoolRow.valueType = "booleanValue";
    manualBoolRow.valueText = "";
    manualBoolRow.valueTypeManuallySet = true;
    queryModel.transitionFilterOperator(manualBoolRow, "EQUAL", null);
    assert.equal(manualBoolRow.valueText, "true");

    // 2. EQUAL -> IN -> GREATER_THAN resets untouched "[]" back to "".
    const strRow = queryModel.createFilterRow();
    strRow.fieldPath = "title";
    strRow.op = "EQUAL";
    strRow.valueType = "stringValue";
    strRow.valueText = "";
    strRow.valueUserEdited = false;

    queryModel.transitionFilterOperator(strRow, "IN", fieldEntry);
    assert.equal(strRow.op, "IN");
    assert.equal(strRow.valueText, "[]");
    assert.equal(strRow.valueUserEdited, false);

    queryModel.transitionFilterOperator(strRow, "GREATER_THAN", fieldEntry);
    assert.equal(strRow.op, "GREATER_THAN");
    assert.equal(
      strRow.valueText,
      "",
      "untouched [] must reset to '' when leaving array operator to field operator"
    );

    // If user manually edited "[]", preserve it on IN -> GREATER_THAN.
    strRow.op = "IN";
    strRow.valueText = "[]";
    strRow.valueUserEdited = true;
    queryModel.transitionFilterOperator(strRow, "GREATER_THAN", fieldEntry);
    assert.equal(strRow.valueText, "[]");

    // 3. buildScalarFilterValue boolean parity with <select> (.value === "false" ? "false" : "true").
    for (const [rawText, expectedBool] of /** @type {const} */ ([
      ["false", false],
      [" false ", false],
      ["true", true],
      ["", true],
      ["other", true],
    ])) {
      const f = queryModel.createFilterRow();
      f.valueType = "booleanValue";
      f.valueText = rawText;
      const res = queryModel.buildScalarFilterValue(f, "p", "(default)");
      assert.equal(res.valid, true);
      assert.deepEqual(res.value, { booleanValue: expectedBool });
    }

    // Also confirm query.js filter row UI uses transitionFilterOperator on @change.
    const qState = query.createQueryRunnerState("users");
    const uiRow = query.createFilterRow();
    uiRow.fieldPath = "active";
    uiRow.op = "IN";
    uiRow.valueType = "booleanValue";
    uiRow.valueText = "[]";
    qState.filters.push(uiRow);
    const tpl = query.renderQueryRunner({
      projectId: "p",
      databaseId: "(default)",
      knownCollections: ["users"],
      state: qState,
      onRerender: () => {},
      onExecuteQuery: () => {},
      onOpenResultDocument: () => {},
    });
    const opSel = findSelectByAriaLabel(tpl, "Filter operator");
    assert.ok(opSel?.handlers.change);
    opSel.handlers.change({ target: { value: "IS_NULL" } });
    assert.equal(uiRow.op, "IS_NULL");
    const opSel2 = findSelectByAriaLabel(
      query.renderQueryRunner({
        projectId: "p",
        databaseId: "(default)",
        knownCollections: ["users"],
        state: qState,
        onRerender: () => {},
        onExecuteQuery: () => {},
        onOpenResultDocument: () => {},
      }),
      "Filter operator"
    );
    assert.ok(opSel2?.handlers.change);
    opSel2.handlers.change({ target: { value: "EQUAL" } });
    assert.equal(uiRow.op, "EQUAL");
    assert.equal(uiRow.valueText, "true");
    assert.equal(findAllElements(tpl, (e) => e.tagName === "select").length >= 1, true);
  });

  test("f6_untouched_array_placeholder_resets_across_unary_hops_and_preserves_user_edits", () => {
    /** @type {import('../components/query-suggestions.js').FieldScopeEntry} */
    const fieldEntry = {
      items: [],
      aggItems: [],
      fieldHints: new Map([
        [
          "title",
          {
            scalarType: "stringValue",
            arrayElementType: null,
            isNumeric: false,
          },
        ],
        [
          "age",
          {
            scalarType: "integerValue",
            arrayElementType: null,
            isNumeric: true,
          },
        ],
        [
          "score",
          {
            scalarType: "doubleValue",
            arrayElementType: null,
            isNumeric: true,
          },
        ],
      ]),
      scope: { parentSubpath: "", collectionId: "users", allDescendants: false },
      loading: false,
      stale: false,
      partial: false,
      sampledDocs: 1,
    };

    // 1. EQUAL -> IN -> IS_NULL -> GREATER_THAN resets untouched "[]" for string, integer, and double.
    for (const [fieldPath, valueType, targetFieldOp] of /** @type {const} */ ([
      ["title", "stringValue", "GREATER_THAN"],
      ["age", "integerValue", "GREATER_THAN"],
      ["score", "doubleValue", "EQUAL"],
    ])) {
      for (const entry of [fieldEntry, null]) {
        const row = queryModel.createFilterRow();
        row.fieldPath = fieldPath;
        row.op = "EQUAL";
        row.valueType = valueType;
        row.valueText = "";
        row.valueUserEdited = false;

        queryModel.transitionFilterOperator(row, "IN", entry);
        assert.equal(row.op, "IN");
        assert.equal(row.valueText, "[]");
        assert.equal(row.valueUserEdited, false);

        queryModel.transitionFilterOperator(row, "IS_NULL", entry);
        assert.equal(row.op, "IS_NULL");
        assert.equal(row.valueText, "[]");

        queryModel.transitionFilterOperator(row, targetFieldOp, entry);
        assert.equal(row.op, targetFieldOp);
        assert.equal(row.valueType, valueType);
        assert.equal(
          row.valueText,
          "",
          `untouched [] must reset across EQUAL -> IN -> IS_NULL -> ${targetFieldOp} for ${valueType}`
        );
      }
    }

    // 2. Preserves user-edited "[]" across unary hops when valueUserEdited === true.
    for (const valueType of /** @type {const} */ ([
      "stringValue",
      "integerValue",
      "doubleValue",
    ])) {
      const editedRow = queryModel.createFilterRow();
      editedRow.fieldPath = "title";
      editedRow.op = "EQUAL";
      editedRow.valueType = valueType;
      editedRow.valueTypeManuallySet = true;
      editedRow.valueText = "";
      editedRow.valueUserEdited = false;

      queryModel.transitionFilterOperator(editedRow, "IN", fieldEntry);
      assert.equal(editedRow.valueText, "[]");
      editedRow.valueUserEdited = true;

      queryModel.transitionFilterOperator(editedRow, "IS_NULL", fieldEntry);
      assert.equal(editedRow.op, "IS_NULL");
      assert.equal(editedRow.valueText, "[]");

      queryModel.transitionFilterOperator(editedRow, "GREATER_THAN", fieldEntry);
      assert.equal(editedRow.op, "GREATER_THAN");
      assert.equal(
        editedRow.valueText,
        "[]",
        `user-edited [] must be preserved across unary hops for ${valueType}`
      );
    }
  });
});
