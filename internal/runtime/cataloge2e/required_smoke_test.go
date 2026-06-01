package cataloge2e

import (
	"path/filepath"
	"testing"
)

func TestCatalogRequiredSmoke(t *testing.T) {
	t.Run("startup_policies", TestCatalogFixtureStartupPolicies_AreExplicit)
	t.Run("tier8_warning_truth", TestTier8RuntimeBootMatchesAuthoritativeStartupTruthForWarningFixtures)
	t.Run("assertions_causal_entity_ids", TestCatalogCausalEntityIDs_FollowsSourceEventIDChain)
	t.Run("assertions_handler_outcome_names", func(t *testing.T) {
		if !catalogAssertsAuthoritativeHandlerOutcome("success") {
			t.Fatal("success handler outcome was not authoritative")
		}
		if catalogAssertsAuthoritativeHandlerOutcome("reject") {
			t.Fatal("reject handler outcome was treated as authoritative success")
		}
		if !catalogRecognizesHandlerOutcome("terminal_reject") {
			t.Fatal("terminal_reject handler outcome was not recognized")
		}
		if catalogRecognizesHandlerOutcome("succes") {
			t.Fatal("misspelled handler outcome was recognized")
		}
	})
	t.Run("assertions_ignore_top_level_non_success_preview", TestAssertCatalogRuntimeOutcome_IgnoresTopLevelNonSuccessPreviewProof)
	t.Run("assertions_cross_flow_emitted_events", TestAssertEmittedEvents_AcceptsCrossFlowInheritDispatcherEmission)
	t.Run("tier1_emits_single_runtime", func(t *testing.T) {
		runCatalogRequiredSmokeFixture(t, filepath.Join(repoRootFromCatalogE2E(t), "tests", "tier1-primitives", "test-emits-single"), false)
	})
}

func runCatalogRequiredSmokeFixture(t *testing.T, fixtureRoot string, startRuntime bool) {
	t.Helper()

	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

	h := newRuntimeHarness(t, fixtureRoot, startRuntime)
	h.seedEntityFields(expected)
	for _, step := range expected.triggerSequence() {
		h.publishAndWait(step, catalogRuntimePublishTimeout)
	}
	h.waitForExpectedEmittedEvents(expected, catalogRuntimePublishTimeout)
	assertCatalogRuntimeOutcome(t, h, expected)
}
