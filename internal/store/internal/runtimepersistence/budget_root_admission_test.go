package runtimepersistence

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type rootedBudgetStore interface {
	rootedRunStore
	ListBudgetProjectionTargets(context.Context) ([]budgetspend.ProjectionTarget, error)
	SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
}

func TestS1BudgetTargetsKeepEachRunsExactRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedBudgetStore)
			sourceA, pathA := admittedRootForStorage(t, "budget-a")
			sourceB, pathB := admittedRootForStorage(t, "budget-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			if sourceA.Artifact().BundleHash() != sourceB.Artifact().BundleHash() {
				t.Fatal("fixture must exercise equal bytes with different roots")
			}
			requests := make([]runlifecycle.CreateRequest, 0, 2)
			want := map[string]sourceartifact.RootAdmissionReference{}
			entities := map[string]string{}
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
				request := rootedRunCreationRequest(source)
				bound := runtimecorrelation.WithRootAdmission(ctx, source.Admission())
				entityID := uuid.NewString()
				if _, err := selected.SetupScenarioEntities(bound, pipeline.ScenarioSetupRequest{
					RunID: request.RunID, CreatedAt: request.StartedAt,
					Entities: []pipeline.ScenarioSetupEntityRequest{{
						Alias: "subject", EntityID: entityID, FlowInstance: "global", EntityType: "budget_entity", CurrentState: "Ready",
					}},
				}); err != nil {
					t.Fatal(err)
				}
				requests = append(requests, request)
				want[request.RunID] = source.Admission().Reference()
				entities[request.RunID] = entityID
			}
			for _, path := range []string{pathA, pathB} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			assertTargets := func() {
				t.Helper()
				targets, err := selected.ListBudgetProjectionTargets(ctx)
				if err != nil || len(targets) != len(want) {
					t.Fatalf("read exact budget targets: %+v, %v", targets, err)
				}
				seen := map[string]bool{}
				for _, target := range targets {
					expected, present := want[target.RunID]
					if !present || seen[target.RunID] || target.RootReference != expected || target.EntityID != entities[target.RunID] || target.Stage != "Ready" || target.FlowInstance != "global" {
						t.Fatalf("budget target lost its run-bound source/root or exact entity state: %+v, want %v", target, expected)
					}
					seen[target.RunID] = true
				}
			}
			assertTargets()
			request := requests[0]
			if _, err := selected.ReviseRunSource(ctx, runlifecycle.SourceRevisionRequest{
				RunID: request.RunID, Source: request.Source, RootAdmission: sourceB.Admission(),
			}); err != nil {
				t.Fatal(err)
			}
			want[request.RunID] = sourceB.Admission().Reference()
			assertTargets()
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if targets, err := selected.ListBudgetProjectionTargets(canceled); !errors.Is(err, context.Canceled) || len(targets) != 0 {
				t.Fatalf("canceled budget read disclosed partial targets: %+v, %v", targets, err)
			}
			if _, _, err := selected.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{
				RunID: request.RunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			delete(want, request.RunID)
			assertTargets()
		})
	}
}
