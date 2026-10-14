package runtimepersistence

import (
	"context"
	"reflect"
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
