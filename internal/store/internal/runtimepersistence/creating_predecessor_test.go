package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func TestCreatingPredecessorAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"pending", "claimed", "retry", "delivered", "dead_letter", "no_delivery", "different_sender", "missing_receipt"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
				f.constructKeylessRoot(t)
				ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
				defer cancel()
				req := f.request("ordered", "ti-ordered", "initial")
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan); err != nil {
					t.Fatal(err)
				}
				store := f.store.(deliverylifecycle.Store)
				creator := req.TriggerEvent
				if err := insertCanonicalEventRecordFixture(ctx, f.store, creator); err != nil {
					t.Fatal(err)
				}
				target := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "ordered-node")), Target: events.MustExistingEntityTarget(target)}
				var first deliverylifecycle.ClaimedObligation
				if scenario != "no_delivery" {
					if err := commitDeliveryObligationFixture(ctx, store, creator, route); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "claimed" || scenario == "retry" || scenario == "delivered" || scenario == "dead_letter" {
					first, err = claimDeliveryFixture(ctx, store, creator, route)
					if err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "delivered" {
					if _, err := store.SettleSuccess(ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "retry" || scenario == "dead_letter" {
					failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "creating_turn_test_failed", "predecessor_test", "settle", nil))
					if !ok {
						t.Fatal("missing typed failure")
					}
					disposition := deliverylifecycle.FailureDeadLetter
					if scenario == "retry" {
						disposition = deliverylifecycle.FailureRetry
					}
					if _, err := store.SettleFailure(ctx, first.Claim, deliverylifecycle.Settlement{Disposition: disposition, ReasonCode: failure.Detail.Code, Failure: &failure, RetryBase: time.Millisecond, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
						t.Fatal(err)
					}
				}
				sender := creator.SourceAgent()
				if scenario == "different_sender" {
					sender = "independent-sender"
				}
				follower := eventtest.ExistingRunRootIngress(uuid.NewString(), creator.Type(), sender, creator.TaskID(), creator.Payload(), creator.ChainDepth(), creator.RunID(), creator.Envelope(), creator.CreatedAt().Add(time.Microsecond))
				if err := insertCanonicalEventRecordFixture(ctx, f.store, follower); err != nil {
					t.Fatal(err)
				}
				if err := commitDeliveryObligationFixture(ctx, store, follower, route); err != nil {
					t.Fatal(err)
				}
				before := loadDeliverySnapshotFixture(t, ctx, store, follower.ID(), route)
				if scenario == "missing_receipt" {
					err := runDeliveryFixtureAttempt(ctx, store, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						adapter := sqliteDeliveryAdapter
						if backend == "postgres" {
							adapter = postgresDeliveryAdapter
						}
						if err := adapter.CommitPipelineHandoff(txctx, attempt, follower.ID()); err != nil {
							return err
						}
						return pipelinepersistence.RemoveReceiverConstructionReceiptForTest(txctx, attempt, flowidentity.RunScopedFlowInstance{RunID: creator.RunID(), Route: plan.Identity.Route()}, plan.Identity.EntityID)
					})
					if err != nil {
						t.Fatal(err)
					}
					before = loadDeliverySnapshotFixture(t, ctx, store, follower.ID(), route)
					batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
					if err != nil || len(batch) != 1 || batch[0].Disposition != deliverylifecycle.ClaimInvariantInvalid || batch[0].Invariant == nil {
						t.Fatalf("missing native receipt was admitted by batch: %+v err=%v", batch, err)
					}
					observation, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
					if err != nil || observation.Disposition != deliverylifecycle.ClaimInvariantInvalid || observation.Invariant == nil {
						t.Fatalf("missing native receipt was admitted by observation: %+v err=%v", observation, err)
					}
					claim, err := store.ClaimDelivery(ctx, before.Authority, follower, route)
					if err != nil || claim.Disposition != deliverylifecycle.ClaimInvariantInvalid || claim.Invariant == nil {
						t.Fatalf("missing native receipt was admitted by direct claim: %+v err=%v", claim, err)
					}
					page, err := store.ScanDeliveryContinuations(ctx, before.Authority, deliverylifecycle.ContinuationCursor{}, 64)
					if err != nil {
						t.Fatal(err)
					}
					var found bool
					for _, item := range page.Items {
						if item.DeliveryID == before.DeliveryID {
							found = item.Disposition == deliverylifecycle.ClaimInvariantInvalid && item.Invariant != nil
						}
					}
					after, err := store.Snapshot(ctx, before.DeliveryID)
					if !found || err != nil || !reflect.DeepEqual(before, after) || after.ClaimVersion != 0 {
						t.Fatalf("corrupt receipt scan admitted or mutated follower: page=%+v before=%+v after=%+v err=%v", page, before, after, err)
					}
					return
				}
				blocked := scenario == "pending" || scenario == "claimed" || scenario == "retry"
				want := deliverylifecycle.ClaimAcquired
				if blocked {
					want = deliverylifecycle.ClaimDeferred
				}
				batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
				if err != nil || len(batch) != 1 || batch[0].DeliveryID != before.DeliveryID || batch[0].Disposition != want || batch[0].Invariant != nil {
					t.Fatalf("durable %s batch observation: %+v err=%v", scenario, batch, err)
				}
				afterBatch, err := store.Snapshot(ctx, before.DeliveryID)
				if err != nil || !reflect.DeepEqual(before, afterBatch) || afterBatch.Status != deliverylifecycle.StatusPending || afterBatch.ClaimVersion != 0 {
					t.Fatalf("batch observation mutated the unclaimed follower: before=%+v after=%+v err=%v", before, afterBatch, err)
				}
				// A fresh native observation has no process-local predecessor cache.
				for range 2 {
					observation, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
					if err != nil || observation.Disposition != want {
						t.Fatalf("durable %s observation: %+v err=%v", scenario, observation, err)
					}
				}
				claim, err := store.ClaimDelivery(ctx, before.Authority, follower, route)
				if err != nil || claim.Disposition != want {
					t.Fatalf("direct %s admission: %+v err=%v", scenario, claim, err)
				}
				if blocked {
					after, err := store.Snapshot(ctx, before.DeliveryID)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("deferral mutated the follower: before=%+v after=%+v err=%v", before, after, err)
					}
					outcomes, err := store.Outcomes(ctx, before.DeliveryID)
					if err != nil || len(outcomes) != 0 {
						t.Fatalf("unclaimed follower gained outcome evidence: %+v err=%v", outcomes, err)
					}
					if scenario == "pending" {
						first, err = claimDeliveryFixture(ctx, store, creator, route)
						if err != nil {
							t.Fatal(err)
						}
					}
					if scenario == "retry" {
						if err := WaitManagerRetryDecisionForTest(ctx, f.store, creator, route); err != nil {
							t.Fatal(err)
						}
						original := first.Claim
						first, err = claimDeliveryFixture(ctx, store, creator, route)
						if err != nil || first.Claim.Version() != original.Version()+1 {
							t.Fatalf("retry did not mint the exact successor claim: %+v err=%v", first, err)
						}
						if _, err := store.SettleSuccess(ctx, original, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err == nil {
							t.Fatal("stale creating claim settled its successor")
						}
					}
					if _, err := store.SettleSuccess(ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
					batch, err = store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
					if err != nil || len(batch) != 1 || batch[0].DeliveryID != before.DeliveryID || batch[0].Disposition != deliverylifecycle.ClaimAcquired || batch[0].Invariant != nil {
						t.Fatalf("committed creator did not release batch follower: %+v err=%v", batch, err)
					}
					afterBatch, err = store.Snapshot(ctx, before.DeliveryID)
					if err != nil || !reflect.DeepEqual(before, afterBatch) || afterBatch.Status != deliverylifecycle.StatusPending || afterBatch.ClaimVersion != 0 {
						t.Fatalf("eligible batch observation claimed the follower: before=%+v after=%+v err=%v", before, afterBatch, err)
					}
					observation, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
					if err != nil || observation.Disposition != deliverylifecycle.ClaimAcquired {
						t.Fatalf("committed creator did not release follower: %+v err=%v", observation, err)
					}
					claim, err = store.ClaimDelivery(ctx, before.Authority, follower, route)
					if err != nil || claim.Disposition != deliverylifecycle.ClaimAcquired {
						t.Fatalf("released follower direct claim: %+v err=%v", claim, err)
					}
				}
				acquired, ok := claim.Acquired()
				if !ok || acquired.Claim.Version() != 1 {
					t.Fatalf("released follower did not get its own first claim: %+v", claim)
				}
				if _, err := store.SettleSuccess(ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestCreatingPredecessorCanceledTurnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixture(t, backend)
			f.constructKeylessRoot(t)
			ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
			defer cancel()
			req := f.request("canceled", "ti-canceled", "initial")
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
				t.Fatal(err)
			}
			agents, err := f.store.LoadAgents(ctx)
			if err != nil || len(agents) != 1 {
				t.Fatalf("exact constructed actor: count=%d err=%v", len(agents), err)
			}
			creator := req.TriggerEvent
			if err := insertCanonicalEventRecordFixture(ctx, f.store, creator); err != nil {
				t.Fatal(err)
			}
			target := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID})
			creatorRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agents[0].Config.Identity.AgentID()), AgentIdentity: agents[0].Config.Identity, Target: target}
			store := f.store.(deliverylifecycle.Store)
			if err := commitDeliveryObligationFixture(ctx, store, creator, creatorRoute); err != nil {
				t.Fatal(err)
			}
			follower := eventtest.ExistingRunRootIngress(uuid.NewString(), creator.Type(), creator.SourceAgent(), "", creator.Payload(), 0, creator.RunID(), creator.Envelope(), creator.CreatedAt().Add(time.Microsecond))
			if err := insertCanonicalEventRecordFixture(ctx, f.store, follower); err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "ordered-node")), Target: target}
			if err := commitDeliveryObligationFixture(ctx, store, follower, route); err != nil {
				t.Fatal(err)
			}
			before := loadDeliverySnapshotFixture(t, ctx, store, follower.ID(), route)
			claim, err := store.ClaimDelivery(ctx, before.Authority, follower, route)
			if err != nil || claim.Disposition != deliverylifecycle.ClaimDeferred {
				t.Fatalf("unsettled creating agent failed to hold follower: %+v err=%v", claim, err)
			}
			queued := loadDeliverySnapshotFixture(t, ctx, store, creator.ID(), creatorRoute)
			var canceled deliverylifecycle.Snapshot
			err = runDeliveryFixtureAttempt(ctx, store, func(txctx context.Context, mutation *mutationprotocol.Attempt) error {
				var err error
				switch selected := f.store.(type) {
				case *SQLiteRuntimeStore:
					canceled, err = selected.deliverySQLiteOwner.CancelQueuedAgentTx(txctx, mutation, queued)
				case *PostgresStore:
					canceled, err = selected.deliveryPostgresOwner.CancelQueuedAgentTx(txctx, mutation, queued)
				}
				return err
			})
			if err != nil || canceled.Status != deliverylifecycle.StatusCanceled || canceled.ReasonCode != string(deliverylifecycle.CancellationTerminate) {
				t.Fatalf("native exact queued cancellation: %+v err=%v", canceled, err)
			}
			observation, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
			if err != nil || observation.Disposition != deliverylifecycle.ClaimAcquired {
				t.Fatalf("canceled creator stranded follower: %+v err=%v", observation, err)
			}
			claim, err = store.ClaimDelivery(ctx, before.Authority, follower, route)
			acquired, ok := claim.Acquired()
			if err != nil || !ok || acquired.Claim.Version() != 1 {
				t.Fatalf("canceled creator did not release own follower claim: %+v err=%v", claim, err)
			}
			if _, err := store.SettleSuccess(ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreatingPredecessorNoInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"fieldless", "fielded"} {
			t.Run(backend+"/"+shape, func(t *testing.T) {
				documents := map[string]string{"schema.yaml": "name: creating-no-input\n", "service/schema.yaml": "name: service\n"}
				if shape == "fielded" {
					documents["service/entities.yaml"] = "receipt:\n  marker: {type: text, initial: unchanged}\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
				defer cancel()
				runID := correlation.RunIDFromContext(ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				var err error
				req.Instance, err = flowidentity.StandingForGeneration(req.ContractBundle, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				created, finish, err := f.manager.PrepareStandingFlowInstance(ctx, req)
				if err != nil || !created || finish == nil {
					t.Fatalf("canonical no-input construction: created=%t err=%v", created, err)
				}
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				instance, err := flowidentity.StandingForGeneration(req.ContractBundle, "service", runID)
				if err != nil {
					t.Fatal(err)
				}
				target := events.RouteIdentity{FlowID: instance.TemplateID, FlowInstance: instance.InstancePath}
				ownership := events.MustEntitylessReceiverTarget(target)
				if shape == "fielded" {
					target.EntityID = instance.EntityID
					ownership = events.MustExistingEntityTarget(target)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "service", "ordinary-node")), Target: ownership}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "ordinary.input", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := insertCanonicalEventRecordFixture(ctx, f.store, event); err != nil {
					t.Fatal(err)
				}
				store := f.store.(deliverylifecycle.Store)
				if err := commitDeliveryObligationFixture(ctx, store, event, route); err != nil {
					t.Fatal(err)
				}
				snapshot := loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
				observation, err := store.ObserveDeliveryContinuation(ctx, snapshot.Authority, snapshot.DeliveryID)
				if err != nil || observation.Disposition != deliverylifecycle.ClaimAcquired {
					t.Fatalf("no-input continuation invented a predecessor: %+v err=%v", observation, err)
				}
				claim, err := store.ClaimDelivery(ctx, snapshot.Authority, event, route)
				acquired, ok := claim.Acquired()
				if err != nil || !ok {
					t.Fatalf("no-input direct claim invented a predecessor: %+v err=%v", claim, err)
				}
				if _, err := store.SettleSuccess(ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
