package runforkadmission

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestStartFirstTurnUsesCanonicalRootInputAdmissionWithoutHistoricalDeliveries(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source, runID := semanticview.Wrap(bundle), uuid.NewString()
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "item.received", "operator", "", []byte(`{"item_id":"one"}`), 0,
		runID, "", events.EventEnvelope{}, time.Unix(100, 0).UTC())
	event, err = eventtest.AdmitPayload(event, ".", "item.received")
	if err != nil {
		t.Fatal(err)
	}
	input, err := runfork.InputPublicationFromEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	plan := (runfork.RunForkPlan{SourceRunID: runID,
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}, StartFirstTurn: &input}).WithHistoricalEvents(1, nil)
	admitted, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.FrontierEvents) != 1 || admitted.FrontierEvents[0].SourceEventID != event.ID() ||
		!reflect.DeepEqual(admitted.FrontierEvents[0].SourceClassifications, []string{"start_first_turn"}) ||
		len(admitted.FrontierEvents[0].DerivedRecipients) != 1 || len(admitted.FrontierEvents[0].HistoricalDeliveryRoutes) != 0 {
		t.Fatalf("first turn copied intake or bypassed root recipients: %+v", admitted)
	}
	if ids, known := plan.HistoricalEventIDs(1); !known || len(ids) != 0 || len(plan.PendingWork) != 0 {
		t.Fatal("non-mutating admission manufactured historical membership")
	}
	plan.StartFirstTurn = nil
	empty, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil || len(empty.FrontierEvents) != 0 {
		t.Fatalf("eventless start invented intake: %+v %v", empty, err)
	}
	plan.StartFirstTurn = &input
	plan.SourceRunID = uuid.NewString()
	if _, err := AdmitContractFrontier(ContractFrontierRequest{Plan: plan, Source: source}); err == nil {
		t.Fatal("foreign input minted selected root capability")
	}
}
