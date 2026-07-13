package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRunAcceptsGeneratedActivityResultsWithoutAuthoredSchemas(t *testing.T) {
	report := runGeneratedActivityFixture(t, false)
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "transition_reference_validation"} {
		if reportContains(report.Findings, checkID, "send") {
			t.Fatalf("unexpected %s generated-result finding: %#v", checkID, report.Findings)
		}
	}
}

func TestRunAcceptsSubscriptionsToGeneratedActivityResultSchemas(t *testing.T) {
	report := runGeneratedActivityFixture(t, true)
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "event_producer_exists", "transition_reference_validation", "condition_payload_alignment"} {
		if reportContains(report.Findings, checkID, "send") {
			t.Fatalf("unexpected %s generated-result finding: %#v", checkID, report.Findings)
		}
	}
}

func TestRunAcceptsNestedFlowGeneratedActivityResultOwnership(t *testing.T) {
	source := loadNestedGeneratedActivityFixture(t)
	report := Run(context.Background(), source, Options{})
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "event_producer_exists", "transition_reference_validation", "condition_payload_alignment"} {
		if reportContains(report.Findings, checkID, "child.send") {
			t.Fatalf("unexpected %s nested generated-result finding: %#v", checkID, report.Findings)
		}
	}

	generated := generatedActivityResultEventNamesLocal(source)
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, eventType := range []string{"child.send.succeeded", "child.send.failed"} {
		if _, ok := generated[eventType]; !ok {
			t.Fatalf("nested generated identities = %#v, missing %s", generated, eventType)
		}
		proof := semanticview.ResolveFlowEventProof(source, "child", eventType)
		if !proof.HasSchema {
			t.Fatalf("nested generated event %s has no engine-owned payload schema: %#v", eventType, proof)
		}
		routed := false
		for _, endpoint := range census.MatchingConsumers("child", proof.EventKey()) {
			if endpoint.Kind == semanticview.EventEndpointNodeHandler && endpoint.NodeID == "observer-node" && endpoint.HandlerEvent == eventType {
				routed = true
				break
			}
		}
		if !routed {
			t.Fatalf("nested generated event %s has no canonical observer route", eventType)
		}
	}
}

func TestGeneratedActivityResultNamesCoverHandlerAndRuleSites(t *testing.T) {
	handlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"request": {
			Activity: runtimecontracts.ActivitySpec{ID: "direct", Tool: "send"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:       "fallback",
				Activity: runtimecontracts.ActivitySpec{Tool: "send"},
			}},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes:     map[string]runtimecontracts.SystemNodeContract{"activity-node": {EventHandlers: handlers}},
		Semantics: runtimecontracts.WorkflowSemanticView{NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{"activity-node": handlers}},
	}
	names := generatedActivityResultEventNamesLocal(semanticview.Wrap(bundle))
	sites := runtimecontracts.ActivitySitesForNode("", "activity-node", handlers)
	if len(sites) != 2 {
		t.Fatalf("activity sites = %#v", sites)
	}
	for _, site := range sites {
		results := runtimecontracts.ActivityResultEventsForSite(site)
		for _, eventType := range []string{results.SuccessEvent, results.FailureEvent} {
			if _, ok := names[eventidentity.Normalize(eventType)]; !ok {
				t.Fatalf("generated names = %#v, missing %s", names, eventType)
			}
		}
	}
}

func runGeneratedActivityFixture(t *testing.T, subscribeResults bool) Report {
	t.Helper()
	root := canonicalrouting.CopyGeneratedActivity(t, false, subscribeResults)
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}

func loadNestedGeneratedActivityFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyGeneratedActivity(t, true, false)
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return semanticview.Wrap(bundle)
}
