package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
				f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
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
