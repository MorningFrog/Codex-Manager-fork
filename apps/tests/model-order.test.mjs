import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import ts from "../node_modules/typescript/lib/typescript.js";

const sourcePath = path.resolve(import.meta.dirname, "../src/lib/model-order.ts");
const compiled = ts.transpileModule(await fs.readFile(sourcePath, "utf8"), {
  compilerOptions: {
    module: ts.ModuleKind.ES2022,
    target: ts.ScriptTarget.ES2022,
  },
  fileName: sourcePath,
});
const { moveVisibleItemOrder, moveRouteOrder } = await import(
  `data:text/javascript;base64,${Buffer.from(compiled.outputText).toString("base64")}`
);

test("visible model moves preserve the slots of hidden and filtered models", () => {
  const all = Object.freeze(["a", "hidden", "b", "unmatched", "c"]);
  const visible = Object.freeze(["a", "b", "c"]);
  assert.deepEqual(moveVisibleItemOrder(all, visible, "c", "a"), [
    "c", "hidden", "a", "unmatched", "b",
  ]);
  assert.deepEqual(moveVisibleItemOrder(all, visible, "a", "c"), [
    "b", "hidden", "c", "unmatched", "a",
  ]);
  assert.deepEqual(all, ["a", "hidden", "b", "unmatched", "c"]);
  assert.deepEqual(visible, ["a", "b", "c"]);
});

test("a visible page subset cannot remove or move models on other pages", () => {
  const all = ["page-one-a", "page-one-b", "page-two-a", "page-two-b", "page-three"];
  assert.deepEqual(
    moveVisibleItemOrder(all, ["page-two-a", "page-two-b"], "page-two-b", "page-two-a"),
    ["page-one-a", "page-one-b", "page-two-b", "page-two-a", "page-three"],
  );
});

test("invalid, stale, duplicate, and self drops keep the full original order", () => {
  const all = ["a", "hidden", "b"];
  for (const [visible, source, target] of [
    [["a", "b"], "a", "a"],
    [["a", "b"], "missing", "b"],
    [["a", "b"], "a", "missing"],
    [["a", "b"], "hidden", "a"],
    [["a", "b", "stale"], "b", "a"],
    [["a", "a", "b"], "b", "a"],
    [[], "a", "b"],
  ]) {
    assert.deepEqual(moveVisibleItemOrder(all, visible, source, target), all);
  }
  assert.deepEqual(moveVisibleItemOrder(["a", "a", "b"], ["a", "b"], "b", "a"), ["a", "a", "b"]);
  assert.deepEqual(moveVisibleItemOrder([], [], "a", "b"), []);
});

function routes() {
  return [
    { key: "first", priority: "30", id: "r1", sourceId: "default", upstreamModel: "one", weight: 2, enabled: true },
    { key: "second", priority: "20", id: "r2", sourceId: "agg-1", upstreamModel: "two", weight: 7, enabled: false },
    { key: "third", priority: "20", id: "r3", sourceId: "agg-2", upstreamModel: "three", weight: 5, enabled: true },
  ].map(Object.freeze);
}

test("explicit route moves establish descending runtime priority and preserve route data", () => {
  const original = Object.freeze(routes());
  assert.deepEqual(moveRouteOrder(original, "third", "first"), [
    { ...original[2], priority: "30" },
    { ...original[0], priority: "20" },
    { ...original[1], priority: "10" },
  ]);
  assert.deepEqual(original.map((route) => route.priority), ["30", "20", "20"]);
});

test("route no-ops and invalid drops preserve same-priority weighted pools", () => {
  const original = routes();
  for (const [source, target] of [["second", "second"], ["missing", "first"], ["first", "missing"]]) {
    assert.deepEqual(moveRouteOrder(original, source, target), original);
  }
  const duplicateKeys = [{ key: "a", priority: "20" }, { key: "a", priority: "20" }, { key: "b", priority: "10" }];
  assert.deepEqual(moveRouteOrder(duplicateKeys, "b", "a"), duplicateKeys);
});
