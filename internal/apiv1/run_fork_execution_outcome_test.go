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
						Owner: runfork.RunForkSelectedContractExecutionOwner,
						Materialization: runfork.RunForkMaterialization{
							SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
							ForkRunStatus: "paused", ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
						},
						Activation: runfork.RunForkActivation{
							SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
							ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
							Activated: true, ForkRunStatus: "running", SourceFrozen: true, SourceRunStatus: "forked",
						},
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
			if result.Owner != runfork.RunForkSelectedContractExecutionOwner || result.SourceRunID != runForkTestSourceRunID || result.ForkRunID != runForkTestForkRunID || result.ForkEventID != runForkTestEventID || result.ForkRunStatus != "running" || result.SourceRunStatus != "forked" || !result.SourceFrozen || result.ExecutedEventCount != 2 || result.BundleHash != runForkTestBundleHash || !result.activationAcknowledged {
				t.Fatalf("lost execution result: %+v", result)
			}
		})
	}
}

func TestSelectedRunForkAdapterRefusesContradictoryActivationEvidence(t *testing.T) {
	base := runforkexecution.SelectedContractExecutionResult{
		Owner: runfork.RunForkSelectedContractExecutionOwner,
		Materialization: runfork.RunForkMaterialization{
			SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
			ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
		},
		Activation: runfork.RunForkActivation{
			SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
			ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
			Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus,
			SourceRunStatus: runfork.RunForkSourceFrozenStatus, SourceFrozen: true,
		},
	}
	for _, tc := range []struct {
		name   string
		change func(*runforkexecution.SelectedContractExecutionResult)
	}{
		{"missing_ack", func(r *runforkexecution.SelectedContractExecutionResult) { r.Activation.Activated = false }},
		{"foreign_source", func(r *runforkexecution.SelectedContractExecutionResult) {
			r.Activation.SourceRunID = runForkTestForkRunID
		}},
		{"foreign_fork", func(r *runforkexecution.SelectedContractExecutionResult) {
			r.Activation.ForkRunID = runForkTestSourceRunID
		}},
		{"foreign_event", func(r *runforkexecution.SelectedContractExecutionResult) {
			r.Activation.ForkPoint.EventID = runForkTestSourceRunID
		}},
		{"wrong_status", func(r *runforkexecution.SelectedContractExecutionResult) { r.Activation.ForkRunStatus = "paused" }},
		{"wrong_owner", func(r *runforkexecution.SelectedContractExecutionResult) { r.Owner = "foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := base
			tc.change(&result)
			executor := SelectedContractRunForkExecutor{ExecuteSelectedContractRunFork: func(context.Context, runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
				return result, errors.New("postcommit diagnostic")
			}}
			got, err := executor.ExecuteRunFork(context.Background(), RunForkExecutionRequest{
				SourceRunID: runForkTestSourceRunID, ForkEventID: runForkTestEventID,
			})
			if err == nil || got.activationAcknowledged {
				t.Fatalf("contradictory activation admitted: result=%+v error=%v", got, err)
			}
		})
	}
}
