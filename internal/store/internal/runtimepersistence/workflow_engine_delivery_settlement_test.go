package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
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
						if _, err := selected.SettleSuccess(
							ctx,
							claimed.Claim,
							[]string{"fixture_pre_settled"},
							time.Millisecond,
							runtimedelivery.NotApplicableHandlerRuleSelection(),
						); err != nil {
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

func TestWorkflowEngineMutationCommitsPayloadFanOutIntentAndDeliveryAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			owner, ok := selected.(runtimepipeline.WorkflowEngineMutationOwner)
			if !ok {
				t.Fatalf("%s selected store does not expose the workflow mutation owner", backend)
			}

			flowID := "engine-fan-out-" + uuid.NewString()
			instancePath := flowID + "/receiver"
			entityID := uuid.NewString()
			createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)

			node := mustPersistenceRootNode("engine-fan-out")
			targetRoute := events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: entityID}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustExistingEntityTarget(targetRoute),
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(), "engine.fan_out.requested", "fixture", "",
				[]byte(`{"candidate_ids":["one","two","three"]}`), 0, runID,
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath),
				eventtest.RootRoutingSource(uuid.NewString()), createdAt,
			)
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit workflow engine fan-out fixture: %v", err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim workflow engine fan-out fixture: %v", err)
			}

			element := runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: uuid.NewString()}
			intent := fanoutobligation.IntentRequest{
				Key: fanoutobligation.IntentKey{
					RunID: runID, TriggeringDeliveryID: claimed.Claim.DeliveryID(), ElementRef: element,
				},
				PlanRef: runtimecontracts.FanOutPlanRef{
					BundleHash: "bundle-v1:sha256:" + strings.Repeat("1", 64), ElementRef: element,
					SemanticDigest: "sha256:" + strings.Repeat("2", 64),
				},
				Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: event.ID(), Field: "candidate_ids"},
				Cardinality: 3,
				Capsule: fanoutobligation.Capsule{
					NodeKey: node.Key(), ExecutionFlowID: flowID,
					Route:    runtimeflowidentity.StoredRoute(flowID, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath),
					EntityID: entityID, HandlerEventKey: string(event.Type()), ProducerSource: event.RoutingSource(),
					DeliveryRoute: &route, Lineage: events.LineageFromEvent(event), CurrentState: "active",
				},
			}
			record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
			committed, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
				State: record, FanOutIntent: &intent,
				DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
					Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
					RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
				},
			})
			if err != nil {
				t.Fatalf("commit workflow engine fan-out intent and delivery: %v", err)
			}
			if committed.DeliverySuccess == nil || !committed.DeliverySuccess.Same(claimed.Claim) {
				t.Fatal("workflow engine fan-out mutation did not return the exact committed delivery claim")
			}
			assertFanOutIntentCount(t, ctx, db, backend, runID, 1)
		})
	}
}

func assertFanOutIntentCount(t *testing.T, ctx context.Context, db *sql.DB, backend, runID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM fan_out_intents WHERE run_id = ?`
	if backend == "postgres" {
		query = `SELECT COUNT(*) FROM fan_out_intents WHERE run_id = $1::uuid`
	}
	var got int
	if err := db.QueryRowContext(ctx, query, runID).Scan(&got); err != nil {
		t.Fatalf("count fan-out intents: %v", err)
	}
	if got != want {
		t.Fatalf("fan-out intent count = %d, want %d", got, want)
	}
}
