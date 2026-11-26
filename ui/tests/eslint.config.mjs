// @ts-check
import js from "../../test-suite/node_modules/@eslint/js/src/index.js";

/** @type {import("../../test-suite/node_modules/eslint/lib/types/index.js").Linter.Config[]} */
export default [
  {
    files: ["**/*.mjs"],
    ...js.configs.recommended,
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        AbortController: "readonly",
        AbortSignal: "readonly",
        Buffer: "readonly",
        clearInterval: "readonly",
        clearTimeout: "readonly",
        console: "readonly",
        document: "writable",
        EventSource: "writable",
        fetch: "writable",
        globalThis: "readonly",
        performance: "readonly",
        process: "readonly",
        requestAnimationFrame: "writable",
        setInterval: "readonly",
        setTimeout: "readonly",
        TextEncoder: "readonly",
        URL: "readonly",
        window: "writable",
      },
    },
    rules: {
      ...js.configs.recommended.rules,
      eqeqeq: ["error", "always", { null: "ignore" }],
      "no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
      "no-var": "error",
      "prefer-const": "error",
    },
  },
];
