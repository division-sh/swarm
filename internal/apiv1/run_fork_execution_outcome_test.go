package apiv1

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestSelectedRunForkAdapterPreservesOutcomeAndError(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		name := "no_acknowledged_result"
		if acknowledged {
			name = "activation_with_cleanup_error"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("selected fork execution or cleanup failed")
			calls := 0
			executor := SelectedContractRunForkExecutor{
				ExecuteSelectedContractRunFork: func(context.Context, runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
					calls++
					if !acknowledged {
						return runforkexecution.SelectedContractExecutionResult{}, failure
					}
					return runforkexecution.SelectedContractExecutionResult{
						Owner: "selected-contract-owner",
						Materialization: runfork.RunForkMaterialization{
							SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
							ForkRunStatus: "paused", ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
						},
						Activation:         runfork.RunForkActivation{Activated: true, ForkRunStatus: "running", SourceFrozen: true, SourceRunStatus: "forked"},
						ExecutedEventCount: 2,
					}, failure
				},
			}
			result, err := executor.ExecuteRunFork(context.Background(), RunForkExecutionRequest{BundleHash: runForkTestBundleHash})
			if !errors.Is(err, failure) || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			if !acknowledged {
				if result.ForkRunID != "" || result.ForkRunStatus != "" || result.SourceFrozen || result.ExecutedEventCount != 0 {
					t.Fatalf("fabricated execution result: %+v", result)
				}
				return
			}
			if result.Owner != "selected-contract-owner" || result.SourceRunID != runForkTestSourceRunID || result.ForkRunID != runForkTestForkRunID || result.ForkEventID != runForkTestEventID || result.ForkRunStatus != "running" || result.SourceRunStatus != "forked" || !result.SourceFrozen || result.ExecutedEventCount != 2 || result.BundleHash != runForkTestBundleHash {
				t.Fatalf("lost execution result: %+v", result)
			}
		})
	}
}
