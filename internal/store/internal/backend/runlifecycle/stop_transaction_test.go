package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/lib/pq"
)

type stopSQLiteError int

func (e stopSQLiteError) Error() string { return "test sqlite error" }
func (e stopSQLiteError) Code() int     { return int(e) }

func TestClassifyStopTransactionOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		committed   bool
		cause       error
		uncertain   bool
	}{
		{"constraint", "commit", false, &pq.Error{Code: "23503"}, false},
		{"serialization_rollback", "commit", false, &pq.Error{Code: "40001"}, false},
		{"statement_completion_unknown", "commit", false, &pq.Error{Code: "40003"}, true},
		{"failed_transaction_rollback", "commit", false, pq.ErrInFailedTransaction, false},
		{"sqlite_extended_constraint", "commit", false, stopSQLiteError(787), false},
		{"sqlite_busy_rejection", "commit", false, stopSQLiteError(5), false},
		{"sqlite_locked_rejection", "commit", false, stopSQLiteError(6), false},
		{"canceled_before_commit_admission", "commit", false, context.Canceled, false},
		{"joined_cancellation_before_commit_admission", "commit", false, errors.Join(context.Canceled), false},
		{"cancellation_does_not_erase_lost_ack", "commit", false, errors.Join(context.Canceled, errors.New("lost acknowledgement")), true},
		{"transport", "commit", false, errors.New("lost acknowledgement"), true},
		{"text_is_not_rollback_evidence", "commit", false, errors.New("23503 rollback SQLITE_CONSTRAINT"), true},
		{"statement", "transition", false, errors.New("statement failed"), false},
		{"cleanup_after_success", "commit", true, errors.New("cleanup failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyStopTransactionOutcome(tc.stage, tc.committed, fmt.Errorf("backend: %w", tc.cause))
			failure, ok := runtimefailures.EnvelopeFromError(err)
			class, code := runtimefailures.ClassInternalFailure, "run_stop_failed"
			if tc.uncertain {
				class, code = runtimefailures.ClassOutcomeUncertain, "run_stop_commit_unconfirmed"
			}
			stage := tc.stage
			if tc.committed {
				stage = "post_commit"
			}
			if !ok || !errors.Is(err, tc.cause) || failure.Class != class || failure.Detail.Code != code || failure.Component != "runtime.run_control" || failure.Operation != "stop" || failure.Detail.Attributes["stage"] != stage || failure.Retryable {
				t.Fatalf("classification=%+v cause=%v", failure, err)
			}
		})
	}
}

func TestClassifyStopTransactionOutcomePreservesTypedFailure(t *testing.T) {
	cause := errors.New("backend rollback evidence")
	for _, class := range []runtimefailures.Class{runtimefailures.ClassInternalFailure, runtimefailures.ClassOutcomeUncertain, runtimefailures.ClassLifecycleConflict} {
		typed := runtimefailures.Wrap(class, "backend_result", "backend", "commit", map[string]any{"stage": "backend_commit"}, cause)
		joined := errors.Join(typed, errors.New("cleanup error"))
		if got := classifyStopTransactionOutcome("commit", false, joined); got != joined || !errors.Is(got, cause) {
			t.Fatalf("typed backend result replaced: %v", got)
		}
	}
}
