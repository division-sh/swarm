package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type readinessRetryProjectionStore struct {
	*readinessOwnershipProjectionStore
	inspections   int
	inspectionErr error
	afterInspect  func()
	loads         []string
	loadErr       error
}

func (s *readinessRetryProjectionStore) InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	s.inspections++
	if s.afterInspect != nil {
		s.afterInspect()
	}
	return s.projection, s.inspectionErr
}

func (s *readinessRetryProjectionStore) LoadDynamicFlowRuntimeReadiness(_ context.Context, runID string, _ runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	s.loads = append(s.loads, runID)
	return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, s.loadErr
}

func readinessRetryProbe(t *testing.T, projection runtimepipeline.DynamicFlowRuntimeReadinessProjection) (*AgentManager, *readinessRetryProjectionStore, *countedReadinessOwnership) {
	t.Helper()
	store := &readinessRetryProjectionStore{
		readinessOwnershipProjectionStore: &readinessOwnershipProjectionStore{
			flowActivationTestInstanceStore: &flowActivationTestInstanceStore{}, projection: projection,
		},
		loadErr: errors.New("pending reconciliation reached the exact readiness loader"),
	}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, store)
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(testFlowBundle(t, "")))
	owner := &countedReadinessOwnership{
		AgentLifecyclePersistence: am.lifecycle.persistence(),
		states:                    make(map[string]RunExecutionOwnership), calls: make(map[string]int),
	}
	restarts := flowActivationStandingRestarts{}
	for _, cohort := range [][]runtimepipeline.DynamicFlowRuntimeReadiness{projection.CurrentCompleted, projection.CurrentPending, projection.SourceTransitionRequired} {
		for _, row := range cohort {
			owner.states[row.Plan.RunID] = RunExecutionOwned
			restarts[row.Plan.RunID] = runtimestanding.StandingRestartOrdinary
		}
	}
	am.roles.StandingRestarts = restarts
	am.lifecycle.replacePersistence(owner)
	return am, store, owner
}

func TestDynamicReadinessRetryFiltersOnlyPending(t *testing.T) {
	completed := runtimepipeline.DynamicFlowRuntimeReadiness{InstancePath: "completed", Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString()}}
	pending := runtimepipeline.DynamicFlowRuntimeReadiness{InstancePath: "pending", Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString()}}
	transition := runtimepipeline.DynamicFlowRuntimeReadiness{InstancePath: "transition", Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString()}}
	for _, tc := range []struct {
		name       string
		projection runtimepipeline.DynamicFlowRuntimeReadinessProjection
	}{
		{"empty", runtimepipeline.DynamicFlowRuntimeReadinessProjection{}},
		{"completed_only", runtimepipeline.DynamicFlowRuntimeReadinessProjection{CurrentCompleted: []runtimepipeline.DynamicFlowRuntimeReadiness{completed}}},
		{"source_transition_only", runtimepipeline.DynamicFlowRuntimeReadinessProjection{SourceTransitionRequired: []runtimepipeline.DynamicFlowRuntimeReadiness{transition}}},
		{"pending", runtimepipeline.DynamicFlowRuntimeReadinessProjection{CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{pending}}},
		{"mixed", runtimepipeline.DynamicFlowRuntimeReadinessProjection{
			CurrentCompleted: []runtimepipeline.DynamicFlowRuntimeReadiness{completed}, CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{pending},
			SourceTransitionRequired: []runtimepipeline.DynamicFlowRuntimeReadiness{transition},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			am, store, owner := readinessRetryProbe(t, tc.projection)
			err := am.reconcilePendingDynamicFlowRuntimeReadiness(testAuthorActivityContext(context.Background()))
			if len(tc.projection.CurrentPending) == 0 && err != nil || len(tc.projection.CurrentPending) != 0 && !errors.Is(err, store.loadErr) {
				t.Fatalf("retry result = %v", err)
			}
			wantCalls := make(map[string]int)
			var wantLoads []string
			for _, row := range tc.projection.CurrentPending {
				wantCalls[row.Plan.RunID] = 1
				wantLoads = append(wantLoads, row.Plan.RunID)
			}
			if store.inspections != 1 || !reflect.DeepEqual(owner.calls, wantCalls) || !reflect.DeepEqual(store.loads, wantLoads) {
				t.Fatalf("retry: full backend inspections=%d ownership=%v loads=%v; want 1, %v, %v", store.inspections, owner.calls, store.loads, wantCalls, wantLoads)
			}
			owner.calls = make(map[string]int)
			full, err := am.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), authorActivityTestSourceArtifactFact)
			if err != nil {
				t.Fatal(err)
			}
			for _, cohort := range []struct {
				got, want []runtimepipeline.DynamicFlowRuntimeReadiness
			}{
				{full.CurrentCompleted, tc.projection.CurrentCompleted},
				{full.CurrentPending, tc.projection.CurrentPending},
				{full.SourceTransitionRequired, tc.projection.SourceTransitionRequired},
			} {
				if len(cohort.got) != len(cohort.want) || len(cohort.want) != 0 && !reflect.DeepEqual(cohort.got, cohort.want) {
					t.Fatalf("full public inspection changed its cohorts: %+v", full)
				}
			}
			for _, cohort := range [][]runtimepipeline.DynamicFlowRuntimeReadiness{tc.projection.CurrentCompleted, tc.projection.CurrentPending, tc.projection.SourceTransitionRequired} {
				for _, row := range cohort {
					if owner.calls[row.Plan.RunID] != 1 {
						t.Fatalf("public inspection omitted ownership for %s: %v", row.InstancePath, owner.calls)
					}
				}
			}
		})
	}
}

func TestDynamicReadinessRetryPreservesIntegrityAndCancellation(t *testing.T) {
	row := runtimepipeline.DynamicFlowRuntimeReadiness{InstancePath: "completed", Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString()}}
	for _, projection := range []runtimepipeline.DynamicFlowRuntimeReadinessProjection{
		{}, {CurrentCompleted: []runtimepipeline.DynamicFlowRuntimeReadiness{row}},
		{SourceTransitionRequired: []runtimepipeline.DynamicFlowRuntimeReadiness{row}},
		{CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{row}},
	} {
		for _, failure := range []string{"backend_integrity", "canceled_before", "canceled_after_backend", "missing_source", "stale_source"} {
			t.Run(failure+"/"+readinessRetryCohortName(projection), func(t *testing.T) {
				am, store, owner := readinessRetryProbe(t, projection)
				ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
				defer cancel()
				var want error
				switch failure {
				case "backend_integrity":
					want = errors.New("full backend readiness integrity refused")
					store.inspectionErr = want
				case "canceled_before":
					cancel()
					want = context.Canceled
				case "canceled_after_backend":
					store.afterInspect = cancel
					want = context.Canceled
				case "missing_source":
					ctx = context.Background()
				case "stale_source":
					foreign, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("f", 64))
					if err != nil {
						t.Fatal(err)
					}
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, foreign)
					want = errDynamicFlowRuntimeReadinessSourceStale
				}
				err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx)
				if err == nil || want != nil && !errors.Is(err, want) || len(owner.calls) != 0 || len(store.loads) != 0 {
					t.Fatalf("error=%v want=%v ownership=%v loads=%v", err, want, owner.calls, store.loads)
				}
				wantInspections := 1
				if failure == "missing_source" || failure == "stale_source" {
					wantInspections = 0
				}
				if store.inspections != wantInspections {
					t.Fatalf("source/backend admission order: inspections=%d", store.inspections)
				}
			})
		}
	}
}

func readinessRetryCohortName(projection runtimepipeline.DynamicFlowRuntimeReadinessProjection) string {
	switch {
	case len(projection.CurrentCompleted) != 0:
		return "completed"
	case len(projection.SourceTransitionRequired) != 0:
		return "source_transition"
	case len(projection.CurrentPending) != 0:
		return "pending"
	default:
		return "empty"
	}
}

func TestDynamicReadinessRetryRechecksPendingOwnership(t *testing.T) {
	runID := uuid.NewString()
	projection := runtimepipeline.DynamicFlowRuntimeReadinessProjection{CurrentPending: []runtimepipeline.DynamicFlowRuntimeReadiness{{InstancePath: "pending", Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{RunID: runID}}}}
	am, store, owner := readinessRetryProbe(t, projection)
	ctx := testAuthorActivityContext(context.Background())
	if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); !errors.Is(err, store.loadErr) {
		t.Fatal(err)
	}
	for _, disposition := range []RunExecutionOwnership{RunExecutionForeign, RunExecutionOtherNormalSource, 0} {
		owner.states[runID] = disposition
		store.loads = nil
		err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx)
		if len(store.loads) != 0 || disposition == 0 && err == nil || disposition != 0 && err != nil {
			t.Fatalf("disposition=%v err=%v loads=%v", disposition, err, store.loads)
		}
	}
	owner.states[runID] = RunExecutionOwned
	owner.err = errors.New("generation grant revoked")
	if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); !errors.Is(err, owner.err) || len(store.loads) != 0 {
		t.Fatalf("grant refusal=%v loads=%v", err, store.loads)
	}
	if owner.calls[runID] != 5 {
		t.Fatalf("retry reused prior inspection authority: %v", owner.calls)
	}
	owner.err = nil
	am.roles.StandingRestarts = flowActivationStandingRestarts{runID: runtimestanding.StandingRestartSuspended}
	if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); err != nil || len(store.loads) != 0 {
		t.Fatalf("suspended standing work: err=%v loads=%v", err, store.loads)
	}
	standingErr := errors.New("standing disposition unavailable")
	am.roles.StandingRestarts = readinessRetryStandingFailure{standingErr}
	if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); !errors.Is(err, standingErr) || len(store.loads) != 0 {
		t.Fatalf("standing refusal=%v loads=%v", err, store.loads)
	}
}

type readinessRetryStandingFailure struct{ err error }

func (s readinessRetryStandingFailure) StandingRunRestartDisposition(context.Context, string) (runtimestanding.StandingRestartDisposition, error) {
	return runtimestanding.StandingRestartDisposition{}, s.err
}
