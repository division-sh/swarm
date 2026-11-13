package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type fanOutResumeTransaction func(context.Context, func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error)

func (s *fanOutPostgresOwner) ResumeRunFanOut(ctx context.Context, runID string) (fanoutobligation.ResumeResult, error) {
	return resumeRunFanOut(ctx, runID, true, s.grant, s.admission, func(ctx context.Context, intent fanoutobligation.Intent) ([]fanoutobligation.Outcome, error) {
		return validateFanOutResumeInput(ctx, s.backend.ConstructionHandle(), true, s.resourceData, intent)
	}, func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
		outcome := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error { return operation(ctx, tx, attempt) })
		})
		_, acknowledged := outcome.Value()
		return acknowledged, outcome.Err()
	})
}

func (s *fanOutSQLiteOwner) ResumeRunFanOut(ctx context.Context, runID string) (fanoutobligation.ResumeResult, error) {
	return resumeRunFanOut(ctx, runID, false, s.grant, s.admission, func(ctx context.Context, intent fanoutobligation.Intent) ([]fanoutobligation.Outcome, error) {
		return validateFanOutResumeInput(ctx, s.backend.ConstructionHandle(), false, s.resourceData, intent)
	}, func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
		s.mutationMu.Lock()
		defer s.mutationMu.Unlock()
		outcome := mutationprotocol.RunSQLite(ctx, s.backend, "recheck paused fan-out handler", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error { return operation(ctx, tx, attempt) })
		})
		_, acknowledged := outcome.Value()
		return acknowledged, outcome.Err()
	})
}

func resumeRunFanOut(ctx context.Context, runID string, postgres bool, grant startupownership.GrantEvidence, admission FanOutAdmission, validate func(context.Context, fanoutobligation.Intent) ([]fanoutobligation.Outcome, error), transaction fanOutResumeTransaction) (result fanoutobligation.ResumeResult, err error) {
	if grant.SelectedFork != nil {
		return result, errors.New("selected fork handler resume is not admitted")
	}
	// One run-locked inventory only; a subsequently reblocked revision is new work.
	var inventory []fanoutobligation.Intent
	acknowledged, err := transaction(ctx, func(ctx context.Context, tx *sql.Tx, _ *mutationprotocol.Attempt) error {
		var err error
		inventory, err = loadFanOutResumeInventory(ctx, tx, admission, grant, runID)
		return err
	})
	if err != nil || !acknowledged {
		return result, errors.Join(err, unacknowledgedFanOutResume(acknowledged))
	}
	result.Observed, result.Admitted = len(inventory), true
	result.Unresolved = len(inventory)
	var refusals error
	for _, observed := range inventory {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(refusals, err)
		}
		prefix, validationErr := validate(ctx, observed)
		var state fanOutReopenObservation
		acknowledged, err := transaction(ctx, func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) error {
			var err error
			state, err = reopenFanOutHandlerTx(ctx, tx, attempt, postgres, admission, grant, observed, prefix, validationErr)
			return err
		})
		if acknowledged {
			result.Unresolved--
			switch {
			case state.refusal != nil:
				result.Remaining++
				refusals = errors.Join(refusals, state.refusal)
			case state.changed:
				result.Changed++
			case state.reopened:
				result.Reopened++
			}
		} else {
			// Observe current state without retrying an unacknowledged write.
			changed, readErr := observeUnacknowledgedFanOutResume(ctx, postgres, admission, grant, observed, transaction)
			if changed {
				result.Changed++
				result.Unresolved--
			}
			err = errors.Join(err, readErr, unacknowledgedFanOutResume(false))
		}
		if err != nil {
			return result, errors.Join(refusals, err)
		}
	}
	return result, refusals
}

func unacknowledgedFanOutResume(acknowledged bool) error {
	if acknowledged {
		return nil
	}
	return errors.New("fan-out resume transaction was not acknowledged")
}

func observeUnacknowledgedFanOutResume(ctx context.Context, postgres bool, admission FanOutAdmission, grant startupownership.GrantEvidence, observed fanoutobligation.Intent, transaction fanOutResumeTransaction) (bool, error) {
	changed := false
	acknowledged, err := transaction(ctx, func(ctx context.Context, tx *sql.Tx, _ *mutationprotocol.Attempt) error {
		if err := admitOrdinaryFanOutResumeTx(ctx, tx, admission, grant, observed.Request.Key.RunID); err != nil {
			return err
		}
		current, err := lockFanOutResumeIntent(ctx, tx, postgres, observed.Request.Key)
		if err != nil {
			return err
		}
		changed = !sameBlockedFanOutRevision(current, observed)
		return nil
	})
	return acknowledged && changed, errors.Join(err, unacknowledgedFanOutResume(acknowledged))
}
