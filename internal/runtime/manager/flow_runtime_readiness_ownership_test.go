package manager

import (
	"context"
	"errors"
	"fmt"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"sync"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type readinessOwnershipProjectionStore struct {
	*flowActivationTestInstanceStore
	projection runtimepipeline.DynamicFlowRuntimeReadinessProjection
}

func (s *readinessOwnershipProjectionStore) InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	return s.projection, nil
}

type countedReadinessOwnership struct {
	AgentLifecyclePersistence
	mu     sync.Mutex
	states map[string]RunExecutionOwnership
	calls  map[string]int
	err    error
}

func (p *countedReadinessOwnership) InspectRunExecutionOwnership(_ context.Context, runID string) (RunExecutionOwnership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[runID]++
	return p.states[runID], p.err
}

func TestDynamicReadinessOwnershipIsMemoizedOnlyWithinOneInspection(t *testing.T) {
	ownedRun, foreignRun := uuid.NewString(), uuid.NewString()
	store := &readinessOwnershipProjectionStore{flowActivationTestInstanceStore: &flowActivationTestInstanceStore{}}
	row := func(runID string, n int) runtimepipeline.DynamicFlowRuntimeReadiness {
		return runtimepipeline.DynamicFlowRuntimeReadiness{
			InstancePath: fmt.Sprintf("review/%d", n),
			Plan:         runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: runID},
		}
	}
	for n := 0; n < 128; n++ {
		store.projection.CurrentCompleted = append(store.projection.CurrentCompleted, row(ownedRun, n))
	}
	for n := 0; n < 3; n++ {
		store.projection.CurrentPending = append(store.projection.CurrentPending, row(ownedRun, n+128))
	}
	store.projection.SourceTransitionRequired = append(store.projection.SourceTransitionRequired, row(ownedRun, 131))
	for n := 0; n < 9; n++ {
		store.projection.CurrentCompleted = append(store.projection.CurrentCompleted, row(foreignRun, n))
	}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, store)
	am.roles.StandingRestarts = flowActivationStandingRestarts{ownedRun: runtimestanding.StandingRestartOrdinary}
	owner := &countedReadinessOwnership{
		AgentLifecyclePersistence: am.lifecycle.persistence(),
		states:                    map[string]RunExecutionOwnership{ownedRun: RunExecutionOwned, foreignRun: RunExecutionForeign},
		calls:                     make(map[string]int),
	}
	am.lifecycle.replacePersistence(owner)
	check := func(wantCompleted, wantPending, wantTransition int) {
		t.Helper()
		got, err := am.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), authorActivityTestSourceArtifactFact)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.CurrentCompleted) != wantCompleted || len(got.CurrentPending) != wantPending || len(got.SourceTransitionRequired) != wantTransition {
			t.Fatalf("readiness classification: completed=%d pending=%d transition=%d", len(got.CurrentCompleted), len(got.CurrentPending), len(got.SourceTransitionRequired))
		}
	}
	check(128, 3, 1)
	if owner.calls[ownedRun] != 1 || owner.calls[foreignRun] != 1 {
		t.Fatalf("ownership reads = %#v, want one per run across all projection groups", owner.calls)
	}
	owner.mu.Lock()
	owner.states[ownedRun] = RunExecutionForeign
	owner.mu.Unlock()
	check(0, 0, 0)
	if owner.calls[ownedRun] != 2 || owner.calls[foreignRun] != 2 {
		t.Fatalf("later inspection reused authority: %#v", owner.calls)
	}
	owner.mu.Lock()
	owner.states[ownedRun] = RunExecutionOtherNormalSource
	owner.mu.Unlock()
	check(0, 0, 0)
	owner.mu.Lock()
	owner.states[ownedRun] = 0
	owner.mu.Unlock()
	if _, err := am.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), authorActivityTestSourceArtifactFact); err == nil {
		t.Fatal("invalid ownership disposition was cached or accepted")
	}
	owner.mu.Lock()
	owner.err = errors.New("ownership unavailable")
	owner.mu.Unlock()
	if _, err := am.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), authorActivityTestSourceArtifactFact); !errors.Is(err, owner.err) {
		t.Fatalf("ownership reader failure = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, authorActivityTestSourceArtifactFact); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled inspection = %v", err)
	}
	store.projection = runtimepipeline.DynamicFlowRuntimeReadinessProjection{}
	if _, err := am.InspectDynamicFlowRuntimeReadinessForSource(ctx, authorActivityTestSourceArtifactFact); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled empty inspection = %v", err)
	}
}
