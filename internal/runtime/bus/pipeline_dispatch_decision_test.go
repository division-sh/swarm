package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestPipelineDispatchDecisionMatrix(t *testing.T) {
	testErr := errors.New("dispatch failed")
	retryAt := time.Now().UTC().Add(time.Minute)
	tests := []struct {
		name    string
		outcome runtimepipelineobligation.ExecutionOutcome
		err     error
		queued  bool
		purpose runtimepipelineobligation.Purpose
		phase   pipelineDispatchPhase
		action  pipelineDispatchAction
		kind    runtimepipelineobligation.DispositionKind
		reason  string
		failed  bool
	}{
		{"committed_after_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"committed_recovery_after_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"explicit_retry", runtimepipelineobligation.ReleaseForRetry("busy", nil), testErr, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchRetryRelease, "", "busy", false},
		{"explicit_defer", runtimepipelineobligation.DeferExecution("waiting", retryAt, nil), testErr, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchRecoveryFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionDeferred, "waiting", false},
		{"explicit_dead_letter", runtimepipelineobligation.DeadLetterExecution("handler_rejected", nil), testErr, false, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionDeadLetter, "handler_rejected", false},
		{"paused", runtimepipelineobligation.Continue(), ErrRuntimeIngressPaused, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchPending, "", "", false},
		{"blocked", runtimepipelineobligation.Continue(), ErrRunDispatchBlocked, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchPending, "", "", false},
		{"queued", runtimepipelineobligation.Continue(), nil, true, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal, pipelineDispatchPending, "", "", false},
		{"publication_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionTerminal, "pipeline_outbox_dispatch_failed", false},
		{"recovery_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionTerminal, "pipeline_recovery_failed", true},
		{"decision_recovery_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchRecoveryFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionQuarantined, "", true},
		{"decision_processed", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchRecoveryFinal, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"committed_decision_processed", runtimepipelineobligation.ExecutionOutcome{Committed: true}, nil, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchRecoveryFinal, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"committed_decision_cleanup_failure", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchRecoveryFinal, pipelineDispatchMarkDecisionProcessed, "", "", false},
		{"ordinary_success", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposePublication, pipelineDispatchOutboxFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"ordinary_recovery_success", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposeRecovery, pipelineDispatchRecoveryFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"foreground_interceptors_continue", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchContinue, "", "", false},
		{"foreground_committed_cleanup_continues", runtimepipelineobligation.ExecutionOutcome{Committed: true}, testErr, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchContinue, "", "", false},
		{"foreground_interceptors_paused", runtimepipelineobligation.Continue(), ErrRuntimeIngressPaused, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchPending, "", "", false},
		{"foreground_routes_blocked", runtimepipelineobligation.Continue(), ErrRunDispatchBlocked, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishRoutes, pipelineDispatchPending, "", "", false},
		{"foreground_deferred_incomplete", runtimepipelineobligation.Continue(), errAuthoritativeDeliveryIncomplete, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishDeferred, pipelineDispatchPending, "", "", false},
		{"foreground_interceptor_retry", runtimepipelineobligation.ReleaseForRetry("later_not_ready", nil), nil, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchRetryRelease, "", "later_not_ready", false},
		{"foreground_interceptor_dead_letter", runtimepipelineobligation.DeadLetterExecution("handler_rejected", nil), nil, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchSettle, runtimepipelineobligation.DispositionDeadLetter, "handler_rejected", false},
		{"foreground_interceptor_failure", runtimepipelineobligation.Continue(), testErr, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishInterceptors, pipelineDispatchSettle, runtimepipelineobligation.DispositionTerminal, "", false},
		{"foreground_final_success", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposePublication, pipelineDispatchPublishFinal, pipelineDispatchSettle, runtimepipelineobligation.DispositionAcknowledged, "pipeline_persisted", false},
		{"foreground_decision_processed", runtimepipelineobligation.Continue(), nil, false, runtimepipelineobligation.PurposeDecisionRoute, pipelineDispatchPublishFinal, pipelineDispatchMarkDecisionProcessed, "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyPipelineDispatch(tc.outcome, tc.err, tc.queued, tc.purpose, tc.phase)
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

func TestPipelineDispatchInterruptionRetainsOnlyUncommittedUnclassifiedWork(t *testing.T) {
	for _, phase := range []pipelineDispatchPhase{pipelineDispatchPublishInterceptors, pipelineDispatchPublishRoutes, pipelineDispatchPublishDeferred, pipelineDispatchPublishFinal, pipelineDispatchOutboxFinal, pipelineDispatchRecoveryFinal} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded, fmt.Errorf("receiver check: %w", context.DeadlineExceeded), errors.Join(context.Canceled, context.DeadlineExceeded)} {
			for _, purpose := range []runtimepipelineobligation.Purpose{runtimepipelineobligation.PurposePublication, runtimepipelineobligation.PurposeRecovery, runtimepipelineobligation.PurposeDecisionRoute} {
				got := classifyPipelineDispatch(runtimepipelineobligation.Continue(), cause, false, purpose, phase)
				if got.action != pipelineDispatchPending || got.failedBeforeSettle {
					t.Fatalf("phase=%v purpose=%v cause=%v: %+v", phase, purpose, cause, got)
				}
				got = classifyPipelineDispatch(runtimepipelineobligation.ExecutionOutcome{Committed: true}, cause, false, purpose, phase)
				if got.action == pipelineDispatchPending || got.failedBeforeSettle {
					t.Fatalf("acknowledged work became pending: %+v", got)
				}
				for _, refused := range []error{errors.Join(cause, errors.New("wrong owner")), runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "receiver_revoked", "bus", "admit", map[string]any{"action": "delivery"}, cause)} {
					got = classifyPipelineDispatch(runtimepipelineobligation.Continue(), refused, false, purpose, phase)
					if got.action != pipelineDispatchSettle || (got.disposition.Kind() != runtimepipelineobligation.DispositionTerminal && got.disposition.Kind() != runtimepipelineobligation.DispositionQuarantined) {
						t.Fatalf("refusal became retryable: %+v", got)
					}
				}
				outcome := runtimepipelineobligation.DeadLetterExecution("handler_rejected", nil)
				got = classifyPipelineDispatch(outcome, cause, false, purpose, phase)
				if got.action != pipelineDispatchSettle || got.disposition.Kind() != runtimepipelineobligation.DispositionDeadLetter {
					t.Fatalf("explicit disposition erased: %+v", got)
				}
			}
		}
	}
}
