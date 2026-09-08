package destructivereset

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCoordinatorReconstructsOnlyAfterKnownSafeBoundary(t *testing.T) {
	failure := errors.New("lost stage acknowledgment")
	for _, test := range []struct {
		name         string
		include, dry bool
		failStage    string
		wantRetain   []bool
		wantError    bool
	}{
		{name: "retained", wantRetain: []bool{true}},
		{name: "deleted", include: true, wantRetain: []bool{false}},
		{name: "dry run", dry: true},
		{name: "planning failure compensation", failStage: "plan", wantRetain: []bool{true}, wantError: true},
		{name: "uncertain quiescence stays fenced", failStage: "quiescence", wantError: true},
		{name: "uncertain cleanup stays fenced", failStage: "cleanup", wantError: true},
		{name: "uncertain container stays fenced", failStage: "containers", wantError: true},
		{name: "partial container stays fenced", failStage: "partial"},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &recordingResetLifecycle{}
			coord := Coordinator{
				Planner: plannerFunc(func(context.Context, Request) (Plan, error) {
					if test.failStage == "plan" {
						return Plan{}, failure
					}
					return Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: test.include}, nil
				}), Locks: &recordingLockManager{acquired: true}, RuntimeContexts: lifecycle,
				Quiescer: quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
					if test.failStage == "quiescence" {
						return QuiescenceResult{}, failure
					}
					return QuiescenceResult{}, nil
				}),
				Cleaner: cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) {
					if test.failStage == "cleanup" {
						return CleanupResult{}, failure
					}
					return CleanupResult{}, nil
				}),
				Containers: containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
					if test.failStage == "containers" {
						return ContainerResetResult{}, failure
					}
					if test.failStage == "partial" {
						return ContainerResetResult{OperationName: DefaultOperationName, Failed: []ContainerStopFailure{{Error: failure.Error()}}}, nil
					}
					return ContainerResetResult{OperationName: DefaultOperationName}, nil
				}),
			}
			installCoordinatorOperationFixture(t, &coord)
			_, err := coord.Execute(context.Background(), Request{OperationID: destructiveResetOperationID, ActorTokenID: "operator", RequestHash: "hash", DryRun: test.dry, IncludeSourceArtifacts: test.include, IncludeSourceArtifactsSet: true})
			if (err != nil) != test.wantError || !reflect.DeepEqual(lifecycle.retained, test.wantRetain) {
				t.Fatalf("err=%v reconstructed=%v, want error=%v reconstructed=%v", err, lifecycle.retained, test.wantError, test.wantRetain)
			}
			wantBegins := 1
			if test.dry {
				wantBegins = 0
			}
			if lifecycle.begins != wantBegins || lifecycle.releases != wantBegins {
				t.Fatalf("lifecycle begin/release = %d/%d, want %d/%d", lifecycle.begins, lifecycle.releases, wantBegins, wantBegins)
			}
		})
	}
}

type recordingResetLifecycle struct {
	begins, releases int
	retained         []bool
}

func (l *recordingResetLifecycle) BeginDestructiveReset(context.Context) (RuntimeReset, error) {
	l.begins++
	return l, nil
}
func (l *recordingResetLifecycle) Complete(_ context.Context, retain bool) error {
	l.retained = append(l.retained, retain)
	return nil
}
func (l *recordingResetLifecycle) Release() { l.releases++ }
