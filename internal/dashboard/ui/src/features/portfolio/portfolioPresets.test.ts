import test from "node:test";
import assert from "node:assert/strict";
import { normalizePortfolioSubview, normalizePortfolioViews } from "./usePortfolioPresets.ts";

test("normalizePortfolioSubview preserves all supported portfolio workbench panels", () => {
  assert.equal(normalizePortfolioSubview("overview"), "overview");
  assert.equal(normalizePortfolioSubview("triage"), "triage");
  assert.equal(normalizePortfolioSubview("holding"), "holding");
  assert.equal(normalizePortfolioSubview("pipeline"), "pipeline");
});

test("normalizePortfolioSubview falls back to overview for unknown views", () => {
  assert.equal(normalizePortfolioSubview("weird"), "overview");
  assert.equal(normalizePortfolioSubview(""), "overview");
});

test("normalizePortfolioViews keeps saved overview and triage slots intact", () => {
  const result = normalizePortfolioViews([
    { label: "Overview Slice", subview: "overview", focusKey: "alpha" },
    { label: "Triage Slice", subview: "triage", focusKey: "beta" },
  ]);

  assert.equal(result[0].subview, "overview");
  assert.equal(result[1].subview, "triage");
  assert.equal(result[0].focusKey, "alpha");
  assert.equal(result[1].focusKey, "beta");
});
