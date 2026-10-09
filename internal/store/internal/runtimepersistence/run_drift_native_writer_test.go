package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestVerifyRunNativeConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, record := constructWorkflowMutationFixture(t, backend, "drift-proof", time.Now().UTC())
			reader := fixture.store.(interface {
				InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
			})
			runID := correlation.RunIDFromContext(fixture.ctx)
			report, err := reader.InspectRunMutationDrift(fixture.ctx, runID)
			if err != nil || len(report.Rows) != 0 {
				t.Fatalf("native construction disagrees with physical history: run=%s report=%+v err=%v", runID, report, err)
			}
			node := mustPersistenceNode("drift-proof", "advance")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "drift-proof", FlowInstance: "drift-proof/receiver", EntityID: record.EntityID})}
			selected := fixture.store.(stateOnlyAcquisitionStore)
			for i, handled := range []bool{true, false, true} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "drift.advance", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticEventFixtureWithRoutes(fixture.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(fixture.ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
				} else {
					record.ExpectedState = "done"
					record.ExpectedRevision++
					record.UpdatedAt = record.UpdatedAt.Add(time.Second)
				}
				record.Fields = json.RawMessage(fmt.Sprintf(`{"receiver_key":"receiver","account_id":"preserved","handled":%t}`, handled))
				if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(fixture.ctx, pipeline.WorkflowEngineMutationCommand{State: record, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}}); err != nil {
					t.Fatalf("native state mutation %d: %v", i, err)
				}
				report, err = reader.InspectRunMutationDrift(fixture.ctx, runID)
				if err != nil || len(report.Rows) != 0 {
					t.Fatalf("native mutation %d disagrees with physical history: %+v %v", i, report, err)
				}
			}
		})
	}
}
