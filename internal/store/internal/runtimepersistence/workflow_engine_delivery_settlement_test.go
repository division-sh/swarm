package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func constructWorkflowMutationFixture(t *testing.T, backend, flowID string, at time.Time) (receiverConfigActivationFixture, runtimepipeline.WorkflowEngineStateRecord) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":             "name: workflow-mutation\n",
		flowID + "/schema.yaml":   "name: " + flowID + "\ninstance: receiver_key\nstages:\n  active: {initial: true}\n  done: {}\npins:\n  inputs: [construct.requested]\n",
		flowID + "/entities.yaml": "review_item:\n  receiver_key: text\n  account_id: {type: text, initial: preserved}\n  handled: boolean?\n",
		flowID + "/events.yaml":   "construct.requested:\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	req := sqliteFlowActivationRequest(f.bundle, flowID, "receiver", "", flowID+"/receiver")
	req.OccurredAt = at
	req.ConstructorInput, req.ResolvedKey = "construct.requested", "receiver"
	req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "construct.requested", "constructor-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	activation, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, activation)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct workflow mutation source: %+v %v", committed, err)
	}
	persisted, err := activation.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	record := persisted.State
	record.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	record.ExpectedState, record.ExpectedRevision = "active", 1
	record.CurrentState = "done"
	record.Fields = json.RawMessage(`{"receiver_key":"receiver","account_id":"preserved","handled":true}`)
	record.EnteredStageAt, record.UpdatedAt = at.Add(time.Minute), at.Add(time.Minute)
	return f, record
}

func workflowMutationDeliveryEntry(t *testing.T, record runtimepipeline.WorkflowEngineStateRecord, node identity.ExecutableNode, event events.Event, claim runtimedelivery.Claim) runtimepipeline.WorkflowEngineStateRecord {
	t.Helper()
	// This writer fixture supplies a compiled transition and an actually claimed
	// occurrence; it does not stand in for an authored handler execution proof.
	graph := runtimecontracts.BuildWorkflowStageTopology(record.Identity.Route.ScopeKey, "active", []string{"active", "done"}, nil,
		[]runtimecontracts.HandlerTransitionSemantic{{Node: node, EventType: string(event.Type()), AdvancesTo: "done"}}, nil, nil)
	compiled, err := graph.AdmitTransition(runtimecontracts.WorkflowTransitionSite{Node: node, HandlerEvent: string(event.Type()), AdvanceCarrier: runtimecontracts.HandlerAdvanceCarrierHandler}, "active", "done")
	if err != nil {
		t.Fatal(err)
	}
	transition, err := workflowlifecycle.NewCompiledTransition(compiled, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewAcceptedEvent(record.Identity.Route, identity.NormalizeEntityID(record.EntityID), event.ID(), string(event.Type()), event.ExecutionMode(), record.UpdatedAt, &transition)
	if err != nil {
		t.Fatal(err)
	}
	effect, err = effect.WithExecutionOccurrence("delivery", claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := effect.StageEntry(record.Identity)
	if err != nil || !found {
		t.Fatalf("prepare exact mutation stage entry: found=%v err=%v", found, err)
	}
	var bookkeeping map[string]any
	if err := json.Unmarshal(record.Bookkeeping, &bookkeeping); err != nil {
		t.Fatal(err)
	}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	record.Bookkeeping, err = json.Marshal(bookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestWorkflowEngineMutationSettlesExactNodeDeliveryAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
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
					createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					fixture, record := constructWorkflowMutationFixture(t, backend, flowID, createdAt)
					selected, db, ctx, runID := fixture.store.(stateOnlyAcquisitionStore), fixture.db, fixture.ctx, correlation.RunIDFromContext(fixture.ctx)
					owner, entityID := fixture.store.(runtimepipeline.WorkflowEngineMutationOwner), record.EntityID

					node := mustPersistenceNode(flowID, "engine-settlement")
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
					record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
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

					before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
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
						assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "active", 1, 1)
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
							t.Fatal("stale delivery claim changed constructed state or history")
						}
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
			flowID := "engine-fan-out-" + uuid.NewString()
			instancePath := flowID + "/receiver"
			createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture, record := constructWorkflowMutationFixture(t, backend, flowID, createdAt)
			selected, db, ctx, runID := fixture.store.(stateOnlyAcquisitionStore), fixture.db, fixture.ctx, correlation.RunIDFromContext(fixture.ctx)
			owner, entityID := fixture.store.(runtimepipeline.WorkflowEngineMutationOwner), record.EntityID

			node := mustPersistenceNode(flowID, "engine-fan-out")
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
			record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)

			element := runtimecontracts.FanOutElementRef{
				FlowPath:     flowID,
				Family:       "fan_out",
				SemanticPath: `nodes["engine-fan-out"].handlers["engine.fan_out.requested"].fan_out`,
			}
			declaration, err := element.DeclarationIdentity()
			if err != nil {
				t.Fatal(err)
			}
			eagerScore, err := workflowexpr.EvalValueExpressionWithOptions(
				"25.0 * 3.0", workflowexpr.ValueContext{}, workflowexpr.ValueExpressionOptions{},
			)
			if err != nil || eagerScore != float64(75) {
				t.Fatalf("eager computed score = %#v err=%v, want native double 75", eagerScore, err)
			}
			// This reduced writer proof admits an exact text-list source witness;
			// it does not stand in for the authored compiler or public pump proof.
			collection := runtimecontracts.CatalogTypeReference{Type: "list<text>"}
			projection, err := runtimecontracts.AdmitCollectionProjection(collection)
			if err != nil {
				t.Fatal(err)
			}
			sourceEvidence := runtimecontracts.FanOutPlanSemantics{
				ElementRef: element, ItemsFrom: "payload.candidate_ids", CollectionType: collection,
				CollectionProjection: projection, ItemType: projection.ItemType(), ItemAlias: "candidate",
				Identity: "candidate", IdentityDerived: true, MaxItems: runtimecontracts.DefaultFanOutMaxItems,
				Emit: runtimecontracts.EmitSpec{Event: "engine.item.requested"},
			}
			sourceDigest, err := canonicaljson.Hash(sourceEvidence)
			if err != nil {
				t.Fatal(err)
			}
			intent := fanoutobligation.IntentRequest{
				Key: fanoutobligation.IntentKey{
					RunID: runID, TriggeringDeliveryID: claimed.Claim.DeliveryID(), ElementRef: element,
				},
				PlanRef: runtimecontracts.FanOutPlanRef{
					BundleHash: fixture.bundle.SourceArtifact.BundleHash(), ElementRef: element,
					SemanticDigest: sourceDigest,
				},
				Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: event.ID(), Field: "candidate_ids"},
				Cardinality: 3,
				Capsule: fanoutobligation.Capsule{
					SourceProjection: sourceEvidence,
					NodeKey:          node.Key(), ExecutionFlowID: flowID,
					Route:    runtimeflowidentity.StoredRoute(flowID, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath),
					EntityID: entityID, HandlerEventKey: string(event.Type()), ProducerSource: event.RoutingSource(),
					Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(event), CurrentState: "active",
					Computed: map[string]any{
						"integer": int64(75), "double": eagerScore,
						"nested": []any{json.Number("75.0"), json.Number("75e0")},
					},
				},
			}
			if err := intent.Validate(); err != nil {
				t.Fatalf("admit writer source witness before hostile cases: %v", err)
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
			var capsuleRaw []byte
			if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2`, runID, claimed.Claim.DeliveryID()).Scan(&capsuleRaw); err != nil {
				t.Fatalf("load committed fan-out capsule: %v", err)
			}
			var persistedCapsule fanoutobligation.Capsule
			if err := canonicaljson.DecodePreservingNumberLexemes(capsuleRaw, &persistedCapsule); err != nil {
				t.Fatalf("decode committed fan-out capsule: %v", err)
			}
			projected, err := workflowexpr.ProjectCELValue(persistedCapsule.Computed)
			if err != nil {
				t.Fatalf("project committed fan-out capsule: %v", err)
			}
			result, err := workflowexpr.EvalValueExpressionWithOptions(
				"computed.integer + 1 == 76 && computed.double + 1.0 == 76.0 && computed.nested[0] + 1.0 == 76.0 && computed.nested[1] + 1.0 == 76.0",
				workflowexpr.ValueContext{Computed: projected.(map[string]any)}, workflowexpr.ValueExpressionOptions{},
			)
			if matched, ok := result.(bool); err != nil || !ok || !matched {
				t.Fatalf("deferred capsule arithmetic = %#v err=%v capsule=%s", result, err, capsuleRaw)
			}
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
