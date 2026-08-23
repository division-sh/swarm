package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestWorkflowEngineMutationSettlesExactNodeDeliveryAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			owner, ok := selected.(runtimepipeline.WorkflowEngineMutationOwner)
			if !ok {
				t.Fatalf("%s selected store does not expose the workflow mutation owner", backend)
			}

			for _, test := range []struct {
				name            string
				preSettle       bool
				wantCommitError bool
			}{
				{name: "success commits state and delivery"},
				{name: "stale claim rolls back state", preSettle: true, wantCommitError: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					flowID := "engine-delivery-" + uuid.NewString()
					instancePath := flowID + "/receiver"
					entityID := uuid.NewString()
					createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)

					node := mustPersistenceRootNode("engine-settlement")
					route := events.DeliveryRoute{
						Recipient: events.MustNodeDeliveryRecipient(node),
						Target: events.MustExistingEntityTarget(events.RouteIdentity{
							FlowID: flowID, FlowInstance: instancePath, EntityID: entityID,
						}),
					}
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), "engine.delivery.requested", "fixture", "", []byte(`{}`), 0, runID,
						events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath),
						createdAt,
					)
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatalf("commit workflow engine delivery fixture: %v", err)
					}
					claimed, err := claimDeliveryFixture(ctx, selected, event, route)
					if err != nil {
						t.Fatalf("claim workflow engine delivery fixture: %v", err)
					}
					if test.preSettle {
						if _, err := selected.SettleSuccess(ctx, claimed.Claim, []string{"fixture_pre_settled"}, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
							t.Fatalf("pre-settle workflow engine delivery fixture: %v", err)
						}
					}

					record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
					committed, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
						State: record,
						DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
							Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
							RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
						},
					})
					if test.wantCommitError {
						if err == nil {
							t.Fatal("stale delivery claim committed the workflow engine mutation")
						}
						assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "", "active", 1, 0)
						return
					}
					if err != nil {
						t.Fatalf("commit workflow engine mutation and delivery: %v", err)
					}
					if committed.DeliverySuccess == nil || !committed.DeliverySuccess.Same(claimed.Claim) {
						t.Fatal("workflow engine mutation did not return the exact committed delivery claim")
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 2, 1)
					snapshot, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
					if err != nil {
						t.Fatalf("read settled workflow engine delivery: %v", err)
					}
					if snapshot.Status != runtimedelivery.StatusDelivered {
						t.Fatalf("workflow engine delivery status = %q, want delivered", snapshot.Status)
					}
				})
			}
		})
	}
}
