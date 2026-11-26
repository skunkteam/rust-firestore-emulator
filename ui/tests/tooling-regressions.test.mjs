// @ts-check
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import Module from "node:module";
import path from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../..");

describe("Test-suite tooling regressions", () => {
  test("test:rust-emulator exits non-zero on SIGTERM instead of swallowing TERM", async () => {
    const pkgText = fs.readFileSync(
      path.join(repoRoot, "test-suite/package.json"),
      "utf8"
    );
    const pkgJson = /** @type {{ scripts?: { 'test:rust-emulator'?: string } }} */ (
      JSON.parse(pkgText)
    );
    const rawScript = pkgJson.scripts?.["test:rust-emulator"];
    assert.ok(rawScript, "missing scripts.test:rust-emulator");

    const nodePollStart = rawScript.indexOf("node -e ");
    assert.ok(nodePollStart >= 0, "expected readiness poll in test:rust-emulator");
    const returnSuffix = "|| return 1;";
    const returnRel = rawScript.indexOf(returnSuffix, nodePollStart);
    assert.ok(
      returnRel >= 0,
      "expected `|| return 1;` at end of readiness poll"
    );
    const nodePollEnd = returnRel + returnSuffix.length;

    const stubbedScript = (
      rawScript.slice(0, nodePollStart) +
      'echo READY; sleep 2 & WAIT_PID=$!; wait "$WAIT_PID"; kill "$WAIT_PID" 2>/dev/null || true;' +
      rawScript.slice(nodePollEnd)
    )
      .replace("../target/debug/firestore-emulator", "sleep 30")
      .replace(
        "RUST_EMULATOR=true NODE_OPTIONS='--experimental-vm-modules' jest \"$@\"",
        "true"
      );

    const child = spawn("/bin/bash", ["-c", stubbedScript], {
      stdio: ["ignore", "pipe", "pipe"],
    });

    /** @type {{ code: number | null, signal: NodeJS.Signals | null }} */
    const exitInfo = await new Promise((resolve, reject) => {
      let stdoutBuf = "";
      let termSent = false;
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        stdoutBuf += chunk;
        if (!termSent && stdoutBuf.includes("\n")) {
          const firstLine = stdoutBuf.slice(0, stdoutBuf.indexOf("\n")).trim();
          assert.equal(
            firstLine,
            "READY",
            `unexpected startup output: ${JSON.stringify(firstLine)}`
          );
          termSent = true;
          const killed = child.kill("SIGTERM");
          assert.equal(killed, true, "failed to send SIGTERM to bash child");
        }
      });
      child.on("error", reject);
      child.on("close", (code, signal) => {
        resolve({ code, signal });
      });
    });

    assert.ok(
      exitInfo.signal !== null || (exitInfo.code !== null && exitInfo.code !== 0),
      `test:rust-emulator swallowed SIGTERM and exited cleanly: ${JSON.stringify(exitInfo)}`
    );
  });

  test("test-suite/eslint.config.cjs preserves core @eslint/js recommended rules", () => {
    const cjsModule = /** @type {{ _load?: (req: string, parent: unknown, isMain: boolean) => unknown }} */ (
      /** @type {unknown} */ (Module)
    );
    const origLoad = cjsModule._load;
    assert.ok(typeof origLoad === "function", "Module._load must be callable");

    cjsModule._load = function (request, parent, isMain) {
      if (request === "@typescript-eslint/eslint-plugin") {
        return {
          configs: {
            "flat/recommended-type-checked": [
              { name: "typescript-eslint/base" },
              {
                name: "typescript-eslint/eslint-recommended",
                rules: {
                  "no-var": "error",
                  "prefer-const": "error",
                },
              },
              {
                name: "typescript-eslint/recommended-type-checked",
                rules: {
                  "@typescript-eslint/no-explicit-any": "error",
                },
              },
            ],
          },
        };
      }
      if (request === "eslint-config-prettier/flat") {
        return { name: "eslint-config-prettier", rules: {} };
      }
      if (request === "@eslint/js") {
        return {
          configs: {
            recommended: {
              rules: {
                "no-debugger": "error",
                "no-unreachable": "error",
                "no-constant-condition": "error",
              },
            },
          },
        };
      }
      return origLoad.call(this, request, parent, isMain);
    };

    try {
      const eslintConfigPath = path.join(
        repoRoot,
        "test-suite/eslint.config.cjs"
      );
      const requireCjs = Module.createRequire(eslintConfigPath);
      const cached = requireCjs.cache[eslintConfigPath];
      if (cached) {
        delete requireCjs.cache[eslintConfigPath];
      }
      const flatConfig = /** @type {Array<{ rules?: Record<string, unknown> }>} */ (
        requireCjs("./eslint.config.cjs")
      );
      assert.ok(
        Array.isArray(flatConfig),
        "eslint.config.cjs must export a flat-config array"
      );

      /** @type {Record<string, unknown>} */
      const mergedRules = Object.assign(
        {},
        ...flatConfig.filter((entry) => entry && entry.rules).map((entry) => entry.rules)
      );
      for (const coreRule of [
        "no-debugger",
        "no-unreachable",
        "no-constant-condition",
      ]) {
        assert.equal(
          mergedRules[coreRule],
          "error",
          `eslint.config.cjs dropped eslint:recommended rule "${coreRule}" during flat-config migration`
        );
      }
    } finally {
      cjsModule._load = origLoad;
    }
  });
});
