package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestEventMetadataAuthorityRejectsInternalSwarmRestatements(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant canonicalrouting.EventMetadataAuthorityVariant
		want    string
		wantMsg string
	}{
		{
			name:    "producer names emitting node",
			variant: canonicalrouting.EventMetadataAuthorityTaskProducerNode,
			want:    "swarm.producer",
			wantMsg: "system node worker handler emits",
		},
		{
			name:    "consumer names subscribing node",
			variant: canonicalrouting.EventMetadataAuthorityTaskConsumerNode,
			want:    "swarm.consumer",
			wantMsg: "system node observer handler subscribes",
		},
		{
			name:    "source names internal producer",
			variant: canonicalrouting.EventMetadataAuthorityTaskSourceNode,
			want:    "swarm.source",
			wantMsg: "derived internal producer system node worker handler emits",
		},
		{
			name:    "producer names agent emit_events role",
			variant: canonicalrouting.EventMetadataAuthorityTaskProducerAgent,
			want:    "swarm.producer",
			wantMsg: "agent role reviewer emit_events",
		},
		{
			name:    "consumer names agent subscription role",
			variant: canonicalrouting.EventMetadataAuthorityTaskConsumerAgent,
			want:    "swarm.consumer",
			wantMsg: "agent role reviewer subscriptions",
		},
		{
			name:    "producer names timer",
			variant: canonicalrouting.EventMetadataAuthorityTaskProducerTimer,
			want:    "swarm.producer",
			wantMsg: "timer reminder fires event",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadEventMetadataAuthorityFixture(t, tc.variant)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.want) ||
				!reportContains(report.HardInvalidities(), "event_metadata_authority", tc.wantMsg) {
				t.Fatalf("expected event_metadata_authority hard invalidity containing %q and %q, got %#v", tc.want, tc.wantMsg, report.HardInvalidities())
			}
		})
	}
}

func TestEventMetadataAuthorityRejectsFlowSurfaceRestatements(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidity canonicalrouting.ParentConnectEventMetadataInvalidity
		want       string
		wantMsg    string
	}{
		{
			name:       "producer names flow auto emit",
			invalidity: canonicalrouting.ParentConnectMetadataProducerFlowAutoEmit,
			want:       "swarm.producer",
			wantMsg:    "flow producer auto_emit_on_create producer",
		},
		{
			name:       "producer names flow output pin",
			invalidity: canonicalrouting.ParentConnectMetadataProducerFlowOutput,
			want:       "swarm.producer",
			wantMsg:    "flow producer output pin producer",
		},
		{
			name:       "consumer names flow input pin",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerFlowInput,
			want:       "swarm.consumer",
			wantMsg:    "flow consumer input pin consumer",
		},
		{
			name:       "producer names parent connect output",
			invalidity: canonicalrouting.ParentConnectMetadataProducerConnectOutput,
			want:       "swarm.producer",
			wantMsg:    "parent connect output producer",
		},
		{
			name:       "consumer names parent connect input",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerConnectInput,
			want:       "swarm.consumer",
			wantMsg:    "parent connect input consumer",
		},
		{
			name:       "producer rejects wrong-event flow output pin",
			invalidity: canonicalrouting.ParentConnectMetadataProducerWrongFlowEvent,
			want:       "swarm.producer",
			wantMsg:    "flow producer output pin producer",
		},
		{
			name:       "consumer rejects wrong-event flow input pin",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerWrongFlowEvent,
			want:       "swarm.consumer",
			wantMsg:    "flow consumer input pin consumer",
		},
		{
			name:       "producer rejects wrong-event parent connect output",
			invalidity: canonicalrouting.ParentConnectMetadataProducerWrongConnectEvent,
			want:       "swarm.producer",
			wantMsg:    "parent connect output producer",
		},
		{
			name:       "consumer rejects wrong-event parent connect input",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerWrongConnectEvent,
			want:       "swarm.consumer",
			wantMsg:    "parent connect input consumer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadEventMetadataFlowAuthorityFixture(t, tc.invalidity)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.want) ||
				!reportContains(report.HardInvalidities(), "event_metadata_authority", tc.wantMsg) {
				t.Fatalf("expected event_metadata_authority hard invalidity containing %q and %q, got %#v", tc.want, tc.wantMsg, report.HardInvalidities())
			}
		})
	}
}

func TestEventMetadataAuthorityAcceptsExternalProof(t *testing.T) {
	source := loadEventMetadataAuthorityFixture(t, canonicalrouting.EventMetadataAuthorityExternalProof)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "event_metadata_authority", "") {
		t.Fatalf("external/platform metadata proof should be accepted, got %#v", report.HardInvalidities())
	}
}

func TestEventMetadataAuthorityNarrowReadbackExplainsDerivedAndExternalProof(t *testing.T) {
	source := loadEventMetadataAuthorityFixture(t, canonicalrouting.EventMetadataAuthorityDefault)
	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "event_producer_exists", "task.done") ||
		reportContains(report.Warnings(), "event_consumer_exists", "task.done") {
		t.Fatalf("derived handler roles should satisfy producer/consumer checks, got warnings %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "task.done") {
		t.Fatalf("derived handler roles should keep event schema alive, got warnings %#v", report.Warnings())
	}

	entry, _, ok := source.ResolveFlowEventCatalogEntry("", "task.done")
	if !ok {
		t.Fatalf("task.done event entry missing")
	}
	producers, consumers := eventMetadataRoleNames(source, deadEventDeclaration{
		Canonical: "task.done",
		Entry:     entry,
	})
	if label, ok := producers.match("worker"); !ok || !strings.Contains(label, "handler emits") {
		t.Fatalf("producer role readback = %q/%v, want worker handler emit proof", label, ok)
	}
	if label, ok := consumers.match("observer"); !ok || !strings.Contains(label, "handler subscribes") {
		t.Fatalf("consumer role readback = %q/%v, want observer handler subscription proof", label, ok)
	}
}

func loadEventMetadataAuthorityFixture(t *testing.T, variant canonicalrouting.EventMetadataAuthorityVariant) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyEventMetadataAuthority(t, variant)
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec))
}

func loadEventMetadataFlowAuthorityFixture(t *testing.T, invalidity canonicalrouting.ParentConnectEventMetadataInvalidity) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyParentConnectEventMetadataInvalidity(t, invalidity)
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec))
}
