package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/lib/pq"
)

type stopSQLiteError int

func (e stopSQLiteError) Error() string { return "test sqlite error" }
func (e stopSQLiteError) Code() int     { return int(e) }

func TestClassifyStopTransactionOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		phase     mutationprotocol.Phase
		committed bool
		cause     error
		uncertain bool
	}{
		{"constraint", mutationprotocol.CommitAdmission, false, &pq.Error{Code: "23503"}, false},
		{"serialization_rollback", mutationprotocol.CommitAdmission, false, &pq.Error{Code: "40001"}, false},
		{"statement_completion_unknown", mutationprotocol.CommitAdmission, false, &pq.Error{Code: "40003"}, true},
		{"failed_transaction_rollback", mutationprotocol.CommitAdmission, false, pq.ErrInFailedTransaction, false},
		{"sqlite_extended_constraint", mutationprotocol.CommitAdmission, false, stopSQLiteError(787), false},
		{"sqlite_busy_rejection", mutationprotocol.CommitAdmission, false, stopSQLiteError(5), false},
		{"sqlite_locked_rejection", mutationprotocol.CommitAdmission, false, stopSQLiteError(6), false},
		{"canceled_before_commit_admission", mutationprotocol.CommitAdmission, false, context.Canceled, false},
		{"joined_cancellation_before_commit_admission", mutationprotocol.CommitAdmission, false, errors.Join(context.Canceled), false},
		{"cancellation_does_not_erase_lost_ack", mutationprotocol.CommitAdmission, false, errors.Join(context.Canceled, errors.New("lost acknowledgement")), true},
		{"transport", mutationprotocol.CommitAdmission, false, errors.New("lost acknowledgement"), true},
		{"text_is_not_rollback_evidence", mutationprotocol.CommitAdmission, false, errors.New("23503 rollback SQLITE_CONSTRAINT"), true},
		{"statement", mutationprotocol.DomainWrite, false, errors.New("statement failed"), false},
		{"cleanup_after_success", mutationprotocol.PostCommit, true, errors.New("cleanup failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyStopTransactionOutcome(tc.phase, tc.committed, fmt.Errorf("backend: %w", tc.cause))
			failure, ok := runtimefailures.EnvelopeFromError(err)
			class, code := runtimefailures.ClassInternalFailure, "run_stop_failed"
			if tc.uncertain {
				class, code = runtimefailures.ClassOutcomeUncertain, "run_stop_commit_unconfirmed"
			}
			stage := stopMutationStage(tc.phase)
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
		if got := classifyStopTransactionOutcome(mutationprotocol.CommitAdmission, false, joined); got != joined || !errors.Is(got, cause) {
			t.Fatalf("typed backend result replaced: %v", got)
		}
	}
}
