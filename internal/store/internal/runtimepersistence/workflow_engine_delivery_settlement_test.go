package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
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

			element := runtimecontracts.FanOutElementRef{
				FlowPath:     flowID,
				Family:       "fan_out",
				SemanticPath: `nodes["engine-fan-out"].handlers["engine.fan_out.requested"].fan_out`,
			}
			declaration, err := element.DeclarationIdentity()
			if err != nil {
				t.Fatal(err)
			}
			intent := fanoutobligation.IntentRequest{
				Key: fanoutobligation.IntentKey{
					RunID: runID, TriggeringDeliveryID: claimed.Claim.DeliveryID(), ElementRef: element,
				},
				PlanRef: runtimecontracts.FanOutPlanRef{
					BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), ElementRef: element,
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
			joinRef, err := timeridentity.NewFanOutDeliveryJoinRef(
				mustPersistenceNode(flowID, "engine-fan-out"), string(event.Type()), "fan-out-complete",
				declaration, intent.PlanRef.BundleHash, intent.PlanRef.SemanticDigest,
			)
			if err != nil {
				t.Fatal(err)
			}
			joinRef, err = joinRef.BindFanOutIntent(claimed.Claim.DeliveryID(), joinRef.Generation())
			if err != nil {
				t.Fatal(err)
			}
			handle, err := timeridentity.JoinCompleteHandle(joinRef)
			if err != nil {
				t.Fatal(err)
			}
			barrierSource, err := events.NewFlowOwnedControlRoutingSource(targetRoute)
			if err != nil {
				t.Fatal(err)
			}
			barrier := fanoutbarrier.Registration{
				IntentKey: intent.Key, PlanRef: intent.PlanRef, Handle: handle,
				Route:    runtimeflowidentity.StoredRoute(flowID, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath),
				EntityID: entityID, RoutingSource: barrierSource,
				ExecutionMode: event.ExecutionMode(), CreatedAt: createdAt,
			}
			if err := barrier.Validate(); err != nil {
				t.Fatalf("validate workflow engine fan-out barrier: %v", err)
			}
			record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
			hostile := barrier
			hostile.IntentKey.RunID = uuid.NewString()
			if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
				State: record, FanOutIntent: &intent, FanOutBarrier: &hostile,
				DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
					Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
					RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
				},
			}); err == nil {
				t.Fatal("mismatched fan-out barrier registration committed")
			}
			assertFanOutIntentCount(t, ctx, db, backend, runID, 0)
			assertFanOutBarrierCount(t, ctx, db, runID, 0)

			hostilePlanRef := intent.PlanRef
			hostilePlanRef.BundleHash = "bundle-v2:sha256:" + strings.Repeat("a", 64)
			hostilePlanRef.SemanticDigest = "sha256:" + strings.Repeat("b", 64)
			hostileJoinRef, err := timeridentity.NewFanOutDeliveryJoinRef(
				mustPersistenceNode(flowID, "engine-fan-out"), string(event.Type()), "fan-out-complete",
				declaration, hostilePlanRef.BundleHash, hostilePlanRef.SemanticDigest,
			)
			if err != nil {
				t.Fatal(err)
			}
			hostileJoinRef, err = hostileJoinRef.BindFanOutIntent(claimed.Claim.DeliveryID(), hostileJoinRef.Generation())
			if err != nil {
				t.Fatal(err)
			}
			hostileHandle, err := timeridentity.JoinCompleteHandle(hostileJoinRef)
			if err != nil {
				t.Fatal(err)
			}
			hostile = barrier
			hostile.PlanRef = hostilePlanRef
			hostile.Handle = hostileHandle
			if err := hostile.Validate(); err != nil {
				t.Fatalf("hostile alternate-plan barrier should be internally self-consistent: %v", err)
			}
			if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
				State: record, FanOutIntent: &intent, FanOutBarrier: &hostile,
				DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
					Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
					RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
				},
			}); err == nil || !strings.Contains(err.Error(), "disagrees with its exact intent") {
				t.Fatalf("alternate compiled plan barrier error = %v, want exact-plan rejection", err)
			}
			assertFanOutIntentCount(t, ctx, db, backend, runID, 0)
			assertFanOutBarrierCount(t, ctx, db, runID, 0)

			committed, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{
				State: record, FanOutIntent: &intent, FanOutBarrier: &barrier,
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
			assertFanOutBarrierCount(t, ctx, db, runID, 1)
			var status string
			var persistedHandle []byte
			var summary, schedule any
			if err := db.QueryRowContext(ctx, `
				SELECT status,timer_handle,summary,schedule_key
				FROM fan_out_obligation_barriers
				WHERE run_id=$1 AND triggering_delivery_id=$2
				  AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
			`, runID, claimed.Claim.DeliveryID(), element.FlowPath, element.Family, element.SemanticPath).Scan(&status, &persistedHandle, &summary, &schedule); err != nil {
				t.Fatalf("load committed fan-out barrier: %v", err)
			}
			var persisted timeridentity.TimerHandle
			if err := json.Unmarshal(persistedHandle, &persisted); err != nil {
				t.Fatalf("decode committed fan-out barrier handle: %v", err)
			}
			if status != "armed" || persisted.TaskID() != handle.TaskID() || summary != nil || schedule != nil {
				t.Fatalf("committed fan-out barrier = status:%s handle:%s summary:%v schedule:%v", status, persisted.TaskID(), summary, schedule)
			}
		})
	}
}

func assertFanOutBarrierCount(t *testing.T, ctx context.Context, db *sql.DB, runID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_obligation_barriers WHERE run_id=$1`, runID).Scan(&got); err != nil {
		t.Fatalf("count fan-out barriers: %v", err)
	}
	if got != want {
		t.Fatalf("fan-out barrier count = %d, want %d", got, want)
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
