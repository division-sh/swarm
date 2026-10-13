package runforkexecution

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func selectedFanOutHistoryFixture(t *testing.T, compiled rc.FanOutCompiledPlan, ref rc.FanOutPlanRef, source fanoutobligation.SourceRef, semantics rc.FanOutPlanSemantics) runfork.RunForkFanOutObligation {
	t.Helper()
	run, event := uuid.NewString(), uuid.NewString()
	producer, err := events.NewRootRoutingSource(run)
	if err != nil {
		t.Fatal(err)
	}
	if source.Kind == fanoutobligation.SourceEventPayloadField {
		source.EventID = event
	} else {
		source.RunID, source.EntityID = run, run
	}
	now := time.Now().UTC()
	request := fanoutobligation.IntentRequest{
		Key:     fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: uuid.NewString(), ElementRef: ref.ElementRef},
		PlanRef: ref, Source: source, Cardinality: 1,
		Capsule: fanoutobligation.Capsule{SourcePlanRef: ref, SourceProjection: semantics, NodeKey: compiled.Site.Node.Key(),
			ExecutionFlowID: compiled.Site.Node.FlowPath(), EntityID: run, HandlerEventKey: compiled.Site.EventType,
			Route: flowidentity.StoredRoute(compiled.Site.Node.FlowPath(), run, run), ProducerSource: producer,
			Lineage: events.EventLineage{RunID: run, ParentEventID: event, ExecutionMode: executionmode.Live}},
	}
	intent := fanoutobligation.Intent{Request: request, Source: source, Status: fanoutobligation.StatusOpen,
		NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	if source.Kind == fanoutobligation.SourceEntityField {
		intent.Source.MutationID = uuid.NewString()
	}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
	return runfork.RunForkFanOutObligation{Intent: intent}
}

func TestSelectedContractSettledFanOutHistoryRetainsOriginalPlan(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, "internal/runtime/testdata/generic-swarm-bundle")
	source := semanticview.Wrap(bundle)
	compiled := source.FanOutPlans()[0]
	ref := compiled.Ref
	ref.BundleHash = "original-artifact"
	sourceRef := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, Field: strings.TrimPrefix(compiled.ItemsFrom, "payload.")}
	if strings.HasPrefix(compiled.ItemsFrom, "entity.") {
		sourceRef.Kind, sourceRef.Field = fanoutobligation.SourceEntityField, strings.TrimPrefix(compiled.ItemsFrom, "entity.")
	}
	obligation := selectedFanOutHistoryFixture(t, compiled, ref, sourceRef, compiled.SemanticEvidence())
	obligation.Intent.Status, obligation.Intent.Cursor = fanoutobligation.StatusClosed, 1
	obligation.Outcomes = []fanoutobligation.Outcome{{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted,
		SourceEventID: uuid.NewString(), InheritedDisposition: fanoutobligation.InheritedSucceeded, CreatedAt: obligation.Intent.CreatedAt}}
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{obligation}}
	for _, selected := range []semanticview.Source{source, selectedDeferredWorkTestSource(nil, nil)} {
		refs, err := admitSelectedContractFanOutPlans(plan, selected)
		if err != nil || len(refs) != 0 || !reflect.DeepEqual(plan.FanOutObligations[0], obligation) {
			t.Fatalf("settled history borrowed or changed selected semantics: %v %v", refs, err)
		}
	}
	plan.FanOutObligations[0].Intent.Request.Capsule.SourcePlanRef.SemanticDigest = "wrong"
	if _, err := admitSelectedContractFanOutPlans(plan, source); err == nil {
		t.Fatal("inert history bypassed original source validation")
	}
}
