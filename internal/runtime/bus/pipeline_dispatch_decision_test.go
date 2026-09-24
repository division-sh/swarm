package bus

import (
	"errors"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestPipelineDispatchDecisionMatrix(t *testing.T) {
	testErr := errors.New("dispatch failed")
	retryAt := time.Now().UTC().Add(time.Minute)
	tests := []struct {
		name     string
		outcome  runtimepipelineobligation.ExecutionOutcome
		err      error
		queued   bool
		purpose  runtimepipelineobligation.Purpose
		recovery bool
		action   pipelineDispatchAction
		kind     runtimepipelineobligation.DispositionKind
		reason   string
		failed   bool
	}{
		{"committed_after_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposePublication, false, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"committed_recovery_after_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"explicit_retry", runtimepipelineobligation.ReleaseForRetry("busy", nil), testErr, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchRetryRelease, "", "busy", false},
		{"explicit_defer", runtimepipelineobligation.DeferExecution("waiting", retryAt, nil), testErr, false, runtimepipelineobligation.PurposeDecisionRoute, true, pipelineDispatchSettle, runtimepipelineobligation.DispositionDeferred, "waiting", false},
		{"explicit_dead_letter", runtimepipelineobligation.DeadLetterExecution("handler_rejected", nil), testErr, false, runtimepipelineobligation.PurposePublication, false, pipelineDispatchSettle, runtimepipelineobligation.DispositionDeadLetter, "handler_rejected", false},
		{"paused", runtimepipelineobligation.Continue(), ErrRuntimeIngressPaused, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchPending, "", "", false},
		{"blocked", runtimepipelineobligation.Continue(), ErrRunDispatchBlocked, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchPending, "", "", false},
		{"queued", runtimepipelineobligation.Continue(), nil, true, runtimepipelineobligation.PurposePublication, false, pipelineDispatchPending, "", "", false},
		{"publication_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposePublication, false, pipelineDispatchSettle, runtimepipelineobligation.DispositionTerminal, "pipeline_outbox_dispatch_failed", false},
		{"recovery_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchSettle, runtimepipelineobligation.DispositionTerminal, "pipeline_recovery_failed", true},
		{"decision_recovery_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposeDecisionRoute, true, pipelineDispatchSettle, runtimepipelineobligation.DispositionQuarantined, "", true},
		{"decision_processed", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposeDecisionRoute, true, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"committed_decision_processed", runtimepipelineobligation.ExecutionOutcome{Committed: true}, nil, false, runtimepipelineobligation.PurposeDecisionRoute, true, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"committed_decision_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposeDecisionRoute, true, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"ordinary_success", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposePublication, false, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"ordinary_recovery_success", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposeRecovery, true, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyPipelineDispatch(tc.outcome, tc.err, tc.queued, tc.purpose, tc.recovery)
			if got.action != tc.action || got.failedBeforeSettle != tc.failed {
				t.Fatalf("decision = %+v, want action=%v failed=%t", got, tc.action, tc.failed)
			}
			if got.action == pipelineDispatchRetryRelease {
				if got.retry.ReasonCode() != tc.reason {
					t.Fatalf("retry reason = %q, want %q", got.retry.ReasonCode(), tc.reason)
				}
				return
			}
			if got.action != pipelineDispatchSettle {
				return
			}
			if got.disposition.Kind() != tc.kind || (tc.reason != "" && got.disposition.ReasonCode() != tc.reason) {
				t.Fatalf("disposition = %s/%s, want %s/%s", got.disposition.Kind(), got.disposition.ReasonCode(), tc.kind, tc.reason)
			}
		})
	}
}
