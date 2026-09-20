package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type publicationCensusSource struct {
	semanticview.Source
	reads atomic.Int64
}

func (s *publicationCensusSource) AuthoredEventEntries() map[string]contracts.EventCatalogEntry {
	s.reads.Add(1)
	return s.Source.AuthoredEventEntries()
}

// This probe observes real EventBus preparation, not physical claim persistence.
// Both-store publication/rollback proofs exercise the real group owner.
type outputPreparationGroup struct {
	publicationSettlementProbe
	prepared  int
	originals map[string]events.Event
}

func (g *outputPreparationGroup) ClaimBatch(_ context.Context, requests []pipelineobligation.PublicationClaimRequest) ([]pipelineobligation.Claim, error) {
	issuer := pipelineobligation.NewClaimIssuer()
	g.originals = make(map[string]events.Event, len(requests))
	claims := make([]pipelineobligation.Claim, len(requests))
	for i, request := range requests {
		claim, err := issuer.Issue(request.Event.ID(), pipelineobligation.PurposePublication)
		if err != nil {
			return nil, err
		}
		claims[i] = claim
		g.originals[claim.EventID()] = request.Event
	}
	return claims, nil
}

func (g *outputPreparationGroup) RecordPrepared(_ context.Context, claim pipelineobligation.Claim, preparation pipelineobligation.PublicationPreparation) error {
	g.prepared++
	return preparation.ValidatePreparedFanOutEvent(g.originals[claim.EventID()])
}

func TestFanOutPreparationSharesOutputCompilationNotSettlement(t *testing.T) {
	root := &contracts.FlowContractView{
		Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."},
		Schema: contracts.FlowSchemaDocument{Pins: contracts.FlowPins{Outputs: contracts.FlowOutputPins{EventPins: []contracts.FlowOutputEventPin{{Event: "root.ready"}, {Event: "root.unconsumed"}}}}},
		Events: map[string]contracts.EventCatalogEntry{
			"root.ready":      {Swarm: contracts.EventSwarmMetadata{Consumer: []string{"external"}}},
			"root.unconsumed": {},
		},
	}
	bundle := &contracts.WorkflowContractBundle{RootSchema: &root.Schema, Events: root.Events, FlowTree: contracts.FlowTree{Root: root, ByID: map[string]*contracts.FlowContractView{".": root}}}
	if err := contracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := &publicationCensusSource{Source: semanticview.Wrap(bundle)}
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	for operation := 0; operation < 2; operation++ {
		group := &outputPreparationGroup{}
		requests := make([]pipeline.FanOutPublicationRequest, 25)
		for i := range requests {
			name := events.EventType("root.ready")
			if i%2 == 1 {
				name = "root.unconsumed"
			}
			event := connectRoutePlanRootProducerEvent(uuid.NewString(), name, "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			requests[i] = pipeline.FanOutPublicationRequest{Ordinal: i, Intent: engine.EmitIntent{Event: event}}
		}
		before := source.reads.Load()
		results, err := eb.PrepareFanOutPublications(context.Background(), group, requests)
		if err != nil || len(results) != len(requests) || group.prepared != len(requests) {
			t.Fatalf("operation%d: results=%d recorded=%d err=%v", operation, len(results), group.prepared, err)
		}
		// IsAuthored still reads the catalog once per ordinal during admission;
		// the remaining single read is the output classifier's endpoint census.
		if reads := source.reads.Load() - before; reads != int64(len(requests)+1) {
			t.Fatalf("operation%d read catalog %d times, want25 admissions plus1 output census", operation, reads)
		}
		for i, result := range results {
			if result.Err != nil {
				t.Fatalf("ordinal%d preparation: %v", i, result.Err)
			}
			plan, ok := result.Publication.(EnginePublicationPlan)
			if !ok || plan.prepared.Event.ID() != requests[i].Intent.Event.ID() {
				t.Fatalf("ordinal%d substituted publication identity", i)
			}
			want := events.NoDeliveryNoSubscriberByDesign
			if i%2 == 1 {
				want = events.NoDeliveryDeclaredConsumerNoPlan
			}
			if plan.prepared.settlement.Reason() != want {
				t.Fatalf("ordinal%d reused another event's settlement: got=%v want=%v", i, plan.prepared.settlement.Reason(), want)
			}
		}
		if len(store.settlements) != 0 {
			t.Fatal("preparation persisted publication settlement")
		}
	}
}
