package runlifecycle

import (
	"context"
	"database/sql"
	"errors"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/lib/pq"
)

func runControlStageFailure(action, stage string, err error) error {
	if action != "stop" {
		return err
	}
	return runcontrol.StopFailure(stage, err)
}

func classifyStopTransactionOutcome(stage string, committed bool, err error) error {
	if err == nil {
		return nil
	}
	if _, typed := runtimefailures.EnvelopeFromError(err); typed {
		return err
	}
	if stage == "commit" && !committed && !stopCommitRejected(err) {
		return runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "run_stop_commit_unconfirmed", "runtime.run_control", "stop", map[string]any{"stage": stage}, err)
	}
	if committed {
		stage = "post_commit"
	}
	return runcontrol.StopFailure(stage, err)
}

func stopCommitRejected(err error) bool {
	// Server-reported integrity/transaction rollback and SQLite constraint
	// refusal are known rejection, unlike loss of the COMMIT acknowledgement.
	if errors.Is(err, pq.ErrInFailedTransaction) || stopCommitAdmissionCanceled(err) {
		return true
	}
	var pg *pq.Error
	if errors.As(err, &pg) && (pg.Code.Class() == "23" || pg.Code.Class() == "40" && pg.Code != "40003") {
		return true
	}
	var sqlite interface{ Code() int }
	if errors.As(err, &sqlite) {
		switch sqlite.Code() & 255 {
		case 5, 6, 19:
			return true
		}
	}
	return false
}

func stopCommitAdmissionCanceled(err error) bool {
	// These transaction owners check cancellation after the mutation callback,
	// before admitting COMMIT. Do not mistake that known refusal for a lost ack,
	// or erase another commit/cleanup fault merely joined with cancellation.
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !stopCommitAdmissionCanceled(cause) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return stopCommitAdmissionCanceled(cause)
	}
	return false
}

func (s *RunLifecyclePostgresOwner) runStopMutationOutcome(ctx context.Context, effects *runforkrevision.Effects, operation func(context.Context, *sql.Tx, *authoractivity.Mutation) error) (bool, error) {
	stage := "transaction_begin"
	committed, err := s.runPrivateAuthorActivityMutationObserved(ctx, effects, operation, &stage)
	return committed, classifyStopTransactionOutcome(stage, committed, err)
}

func (s *RunLifecycleSQLiteOwner) runStopMutationOutcome(ctx context.Context, effects *runforkrevision.Effects, operation func(context.Context, *sql.Tx, *authoractivity.Mutation) error) (bool, error) {
	stage := "transaction_begin"
	committed, err := s.runPrivateAuthorActivityMutationObserved(ctx, "sqlite run stop", effects, operation, &stage)
	return committed, classifyStopTransactionOutcome(stage, committed, err)
}
