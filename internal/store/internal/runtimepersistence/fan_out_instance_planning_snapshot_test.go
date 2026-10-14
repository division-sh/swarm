package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestFanOutInstancePlanningReobservesTerminationBeforeEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
			source := coordinator.SemanticSource()
			fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
			if !found {
				t.Fatal("missing admitted source")
			}
			producer := fanOutConstructedProducerForTest(t, f.ctx, f.raw.(pipeline.FlowInstanceIndexReader), source, f.seed.runID)
			scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, f.seed.runID, []string{"producer"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			planning, ok := f.group.(interface {
				WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
			})
			if !ok {
				t.Fatal("group has no canonical instance planning owner")
			}
			var retained pipeline.FlowInstanceIndexReader
			if err := planning.WithFlowInstancePlanningSnapshot(f.ctx, source, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				retained = reader
				for i := 0; i < 4; i++ {
					observations, err := reader.ListFlowInstances(ctx, scope)
					if err != nil {
						return err
					}
					if len(observations) != 1 || observations[0].Identity() != producer {
						t.Fatalf("capture lost exact producer: %+v", observations)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := retained.ListFlowInstances(f.ctx, scope); err == nil {
				t.Fatal("planning reader escaped its ended read lifetime")
			}
			f.prepare(t)
			f.seal(t)
			owner, err := flowidentity.NewRunScopedFlowInstance(f.seed.runID, producer.Route())
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(producer.EntityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			failure := failures.Normalize(err, "test", "commit")
			if err == nil || failure.Class != failures.ClassDependencyUnavailable || !failure.Retryable || failure.Detail.Code != "flow_instance_planning_stale" {
				t.Fatalf("changed valid receiver was not a clean retry: %+v err=%v", committed, err)
			}
			if len(committed.Publications) != 0 || !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("stale view leaked publication, constructor, receipt, outcome or cursor effects")
			}
		})
	}
}

func TestFanOutInstancePlanningCorruptionWinsOverEarlierStalenessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
			observations := captureFanOutPlanningScopes(t, f, coordinator.SemanticSource(), "producer", "one")
			producer, receiver := observations[0].Identity(), observations[1].Identity()
			f.prepare(t)
			f.seal(t)
			if err := coordinator.MarkTerminated(f.ctx, flowidentity.RunScopedFlowInstance{RunID: f.seed.runID, Route: producer.Route()}, identity.NormalizeEntityID(producer.EntityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.raw, flowidentity.RunScopedFlowInstance{RunID: f.seed.runID, Route: receiver.Route()}, "instance_key", "foreign")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restore(context.Background()); err != nil {
					t.Error(err)
				}
			})
			before := f.snapshot(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if !errors.As(err, &corruption) || corruption.FlowID != "one" || len(committed.Publications) != 0 {
				t.Fatalf("earlier stale producer hid corrupt receiver: committed=%+v err=%v", committed, err)
			}
			if !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("corruption refusal leaked constructor, publication, claim, outcome or cursor effects")
			}
		})
	}
}

func TestFanOutInstancePlanningReadinessGrantAndCancellationBeforeEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"readiness_retired", "grant_retired", "cancelled"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f, _, coordinator, agents := newMixedExecutionFixtureWithManager(t, backend, nil)
				observations := captureFanOutPlanningScopes(t, f, coordinator.SemanticSource(), "producer")
				producer := observations[0].Identity()
				f.prepare(t)
				f.seal(t)
				commitCtx := f.ctx
				switch change {
				case "readiness_retired":
					readiness, found := observations[0].Readiness()
					if !found || readiness.AttemptState != "accepted" {
						t.Fatalf("missing actual accepted attachment: %+v", readiness)
					}
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					attempt, err := pipeline.NewDynamicFlowRuntimeActivationAttempt(strconv.FormatUint(readiness.AttemptOrdinal, 10), f.seed.runID, producer.InstancePath, binding)
					if err != nil {
						t.Fatal(err)
					}
					if err := coordinator.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, attempt); err != nil {
						t.Fatal(err)
					}
				case "grant_retired":
					if err := agents.Shutdown(); err != nil {
						t.Fatal(err)
					}
					if err := f.grant.Retire(f.ctx); err != nil {
						t.Fatal(err)
					}
				case "cancelled":
					var cancel context.CancelFunc
					commitCtx, cancel = context.WithCancel(f.ctx)
					cancel()
				}
				before := f.snapshot(t)
				committed, err := f.owner.CommitFanOutChunk(commitCtx, f.command)
				if err == nil || len(committed.Publications) != 0 {
					t.Fatalf("changed dependency authorized effects: %+v err=%v", committed, err)
				}
				if change == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation became another disposition: %v", err)
				}
				if change == "readiness_retired" {
					failure := failures.Normalize(err, "test", "commit")
					if !failure.Retryable || failure.Detail.Code != "flow_instance_planning_stale" {
						t.Fatalf("valid changed attachment was not a clean retry: %+v err=%v", failure, err)
					}
				}
				if !reflect.DeepEqual(before, f.snapshot(t)) {
					t.Fatal("dependency refusal leaked constructor, publication, claim, outcome or cursor effects")
				}
			})
		}
	}
}

func captureFanOutPlanningScopes(t *testing.T, f *groupProofFixture, source semanticview.Source, flows ...string) []pipeline.FlowInstanceObservation {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
	if !found {
		t.Fatal("missing admitted source")
	}
	planning, ok := f.group.(interface {
		WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
	})
	if !ok {
		t.Fatal("group has no canonical instance planning owner")
	}
	var captured []pipeline.FlowInstanceObservation
	if err := planning.WithFlowInstancePlanningSnapshot(f.ctx, source, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
		for _, flow := range flows {
			scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, f.seed.runID, []string{flow}, nil)
			if err != nil {
				return err
			}
			observations, err := reader.ListFlowInstances(ctx, scope)
			if err != nil {
				return err
			}
			if len(observations) != 1 {
				t.Fatalf("capture requires exactly one admitted %s: %+v", flow, observations)
			}
			captured = append(captured, observations[0])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return captured
}

func TestFanOutInstancePlanningReobservesAbsentKeyAndNewMemberBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, createdKey := range []string{"requested", "other"} {
			t.Run(backend+"/"+createdKey, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
					"schema.yaml":          "name: planning-key-proof\n",
					"worker/schema.yaml":   "name: worker\ninstance: item_id\npins:\n  inputs: [item.created]\n",
					"worker/entities.yaml": "item:\n  item_id: text\n",
					"worker/events.yaml":   "item.created:\n  item_id: text\n",
				}, nil)
				seed := seedFanOutOwnerFixtureWithArtifact(t, f.ctx, f.db, f.store, backend == "postgres", 1, time.Now().UTC().Truncate(time.Microsecond), f.bundle.SourceArtifact)
				ctx := correlation.WithRunID(f.ctx, seed.runID)
				source := semanticview.Wrap(f.bundle)
				rootRequest := sqliteFlowActivationRequest(f.bundle, ".", seed.runID, "", seed.runID)
				rootRequest.Instance = flowidentity.Stored(source, ".", seed.runID, seed.runID, seed.runID, "")
				root, err := f.manager.PrepareFlowInstanceActivation(ctx, rootRequest)
				if err != nil {
					t.Fatal(err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if _, err := committer.CommitFlowInstanceActivation(ctx, root); err != nil {
					t.Fatal(err)
				}
				evidence, err := f.grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				process := worklifetime.NewProcess()
				occurrence, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: evidence.RuntimeInstanceID, BundleHash: evidence.BundleHash})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, err := occurrence.RetireAndWait(deadline); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(deadline); err != nil {
						t.Error(err)
					}
				})
				executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
				workers := 1
				registration, err := startupownership.StartFanOutServing(ctx, f.grant, occurrence, &workers, executor)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registration.Close)
				var owner pipeline.FanOutObligationOwner
				select {
				case turn := <-executor.turns:
					owner = turn.owner
					registration.Close()
					<-executor.done
				case err := <-executor.errors:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				key := fanoutobligation.IntentKey{RunID: seed.runID, TriggeringDeliveryID: seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: seed.flowPath, Family: "fan_out", SemanticPath: seed.semanticPath}}
				_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "planning-key-proof", BundleHash: seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
				if err != nil || !found {
					t.Fatalf("exact claim found=%v err=%v", found, err)
				}
				group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := group.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				fact, _ := correlation.SourceArtifactFactFromContext(ctx)
				material, err := pipeline.AdmitFlowInstanceKeyMaterial(source, "worker", "requested")
				if err != nil {
					t.Fatal(err)
				}
				request, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, seed.runID, "worker", root.Identity, material)
				if err != nil {
					t.Fatal(err)
				}
				planning := group.(interface {
					WithFlowInstancePlanningSnapshot(context.Context, semanticview.Source, func(context.Context, pipeline.FlowInstanceIndexReader) error) error
				})
				if err := planning.WithFlowInstancePlanningSnapshot(ctx, source, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
					observed, found, err := reader.LookupFlowInstance(ctx, request)
					if err != nil || found || observed.Valid() {
						t.Fatalf("captured absent key invented a receiver: %+v found=%v err=%v", observed, found, err)
					}
					scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, seed.runID, []string{"worker"}, nil)
					if err != nil {
						return err
					}
					members, err := reader.ListFlowInstances(ctx, scope)
					if err != nil || len(members) != 0 {
						t.Fatalf("captured empty declaration invented membership: %+v err=%v", members, err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := group.Seal(ctx, 1, nil); err != nil {
					t.Fatal(err)
				}
				childRequest := sqliteFlowActivationRequest(f.bundle, "worker", "captured-slot", "", "")
				childRequest.Instance, err = flowidentity.KeyedChild(source, root.Identity, "worker", "captured-slot")
				if err != nil {
					t.Fatal(err)
				}
				childRequest.Instance.EntityID = uuid.NewString()
				childRequest.ConstructorInput, childRequest.ResolvedKey = "item.created", createdKey
				payload, err := json.Marshal(map[string]any{"item_id": createdKey})
				if err != nil {
					t.Fatal(err)
				}
				childRequest.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", payload, 0, seed.runID, events.EventEnvelope{}, childRequest.OccurredAt)
				child, err := f.manager.PrepareFlowInstanceActivation(ctx, childRequest)
				if err != nil {
					t.Fatal(err)
				}
				constructed, err := committer.CommitFlowInstanceActivation(ctx, child)
				if err != nil || !constructed.Created || !constructed.Acknowledged {
					t.Fatalf("competing constructor did not commit: %+v err=%v", constructed, err)
				}
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC()))
				failure := failures.Normalize(err, "test", "commit")
				if err == nil || !failure.Retryable || failure.Detail.Code != "flow_instance_planning_stale" || len(committed.Publications) != 0 {
					t.Fatalf("new member was not a clean retry: %+v err=%v", committed, err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, f.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("freshness refusal changed durable state: %v", err)
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, f.db, seed, 0, 0)
			})
		}
	}
}
