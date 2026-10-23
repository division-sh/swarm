package bus_test

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestEventBusRunContextUsesOriginalSelectedLifecycleOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, sourceKind := range []string{"default", "admitted"} {
			t.Run(backend+"/"+sourceKind, func(t *testing.T) {
				var selected interface {
					eventBusRunFixtureStore
					runtimebus.RunLifecycleReadPersistence
				}
				if backend == "sqlite" {
					selected = storetest.StartSQLiteRuntimeStore(t)
				} else {
					selected = storetest.StartPostgresRuntimeStore(t)
				}
				var source semanticview.Source
				if sourceKind == "admitted" {
					_, bundle := mixedNodeRouteWorkflowModule(t)
					source = semanticview.Wrap(bundle)
				}
				probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
				var ctx context.Context
				if sourceKind == "admitted" {
					ctx = eventBusTestRunContextForSource(t, selected, source)
				} else {
					ctx = eventBusTestRunContext(t, selected)
				}
				if counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {
					t.Fatalf("run construction escaped original writer or retained work: %+v", counts)
				}
				fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
				storedFact, err := selected.RequireActiveRunSource(ctx, eventBusTestRunID)
				if err != nil || !found || fact.BundleHash() != testSourceArtifactFact(source).BundleHash() || storedFact != fact {
					t.Fatalf("source/context convergence: fact=%+v stored=%+v found=%t err=%v", fact, storedFact, found, err)
				}
				if runtimecorrelation.RunIDFromContext(ctx) != eventBusTestRunID {
					t.Fatal("fixture context lost its exact run")
				}
				snapshot, err := selected.LoadRunLifecycleSnapshot(ctx, eventBusTestRunID)
				if err != nil || snapshot.RunID != eventBusTestRunID || snapshot.Status != "running" || snapshot.EndedAt != nil || !snapshot.StartedAt.Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) {
					t.Fatalf("constructed lifecycle: snapshot=%+v err=%v", snapshot, err)
				}
				if probe.Snapshot().Active != 0 {
					t.Fatal("run/context readback retained selected work")
				}
			})
		}
	}
}

func TestRunControlFixtureUsesOriginalSelectedLifecycleOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				storetest.RunFixtureStore
				runtimebus.RunLifecycleReadPersistence
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx, runID := testAuthorActivityContext(context.Background()), uuid.NewString()
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			seedRunControlTestRun(t, ctx, selected, runID)
			if counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {
				t.Fatalf("run-control setup escaped native writer or retained work: %+v", counts)
			}
			fact, err := selected.RequireActiveRunSource(ctx, runID)
			if err != nil || fact.BundleHash() != authorActivityTestBundleHash {
				t.Fatalf("run-control source changed: fact=%+v err=%v", fact, err)
			}
			snapshot, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil || snapshot.RunID != runID || snapshot.Status != "running" || snapshot.StartedAt.IsZero() || snapshot.EndedAt != nil {
				t.Fatalf("native run-control lifecycle: snapshot=%+v err=%v", snapshot, err)
			}
			if probe.Snapshot().Active != 0 {
				t.Fatal("run-control setup/readback retained selected work")
			}
		})
	}
}
