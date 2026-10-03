package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestProviderSourceLivenessUsesLocalIdentityInExactDeclaringFlow(t *testing.T) {
	root := writeDeadEventSchemaFixture(t, deadEventSchemaFixtureOptions{
		name: "provider-local-liveness",
		flows: map[string]deadEventSchemaFlowFiles{
			"provider": {events: "ticket.ready:\n"},
			"importer": {events: "ticket.ready:\n"},
		},
	})
	repo := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	source := semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), map[string][]string{"provider": {"ticket.ready"}})
	report := Run(context.Background(), source, Options{})
	if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "provider/ticket.ready") {
		t.Fatal("qualified diagnostic identity hid the exact provider-local producer")
	}
	if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "importer/ticket.ready") {
		t.Fatal("provider authority leaked to the same-named unbound flow")
	}
}

func TestLoadRejectsRetiredRoleAnnotationsAcrossExecutableContexts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant canonicalrouting.EventMetadataAuthorityVariant
	}{
		{"node producer", canonicalrouting.EventMetadataAuthorityTaskProducerNode},
		{"node consumer", canonicalrouting.EventMetadataAuthorityTaskConsumerNode},
		{"source", canonicalrouting.EventMetadataAuthorityTaskSourceNode},
		{"agent producer", canonicalrouting.EventMetadataAuthorityTaskProducerAgent},
		{"agent consumer", canonicalrouting.EventMetadataAuthorityTaskConsumerAgent},
		{"timer producer", canonicalrouting.EventMetadataAuthorityTaskProducerTimer},
		{"external and platform labels", canonicalrouting.EventMetadataAuthorityExternalProof},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyEventMetadataAuthority(t, tc.variant)
			repo := repoRootForBootverifyTest(t)
			_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), "is reserved") || !strings.Contains(err.Error(), "swarm") {
				t.Fatalf("retired annotations must fail at source admission: %v", err)
			}
		})
	}
}

func TestExecutableEventRolesRemainVisibleWithoutMetadata(t *testing.T) {
	root := canonicalrouting.CopyEventMetadataAuthority(t, canonicalrouting.EventMetadataAuthorityDefault)
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	report := Run(context.Background(), source, Options{})
	if reportContains(report.Warnings(), "event_producer_exists", "task.done") ||
		reportContains(report.Warnings(), "event_consumer_exists", "task.done") ||
		reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "task.done") {
		t.Fatalf("real handler relations lost authority: %#v", report.Warnings())
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	producers := census.MatchingProducers(".", "task.done")
	consumers := census.MatchingConsumers(".", "task.done")
	if len(producers) != 1 || producers[0].NodeID != "worker" || producers[0].Kind != semanticview.EventEndpointNodeHandler {
		t.Fatalf("producer proof: %#v", producers)
	}
	if len(consumers) != 1 || consumers[0].NodeID != "observer" || consumers[0].Kind != semanticview.EventEndpointNodeHandler {
		t.Fatalf("consumer proof: %#v", consumers)
	}
}
