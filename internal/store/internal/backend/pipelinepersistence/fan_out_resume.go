package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
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
	// The run lock bounds this one inventory. Later blocked revisions are not
	// appended to it, even when another worker reblocks a reopened handler.
	var inventory []fanoutobligation.Intent
	acknowledged, err := transaction(ctx, func(ctx context.Context, tx *sql.Tx, _ *mutationprotocol.Attempt) error {
		ready, err := admitFanOutRun(ctx, tx, admission, grant, runID, true)
		if err != nil || !ready {
			return errors.Join(err, fanoutobligation.ErrStaleClaim)
		}
		if err := runstate.RequireNormalControlTx(ctx, tx, runID, runfork.ControlRunContinue); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND status='blocked' ORDER BY COALESCE(CAST(triggering_delivery_id AS TEXT),''),COALESCE(CAST(deployment_feed_id AS TEXT),''),flow_path,declaration_family,semantic_path`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			intent, err := scanFanOutIntent(rows)
			if err != nil {
				return err
			}
			inventory = append(inventory, intent)
		}
		return errors.Join(rows.Err(), rows.Close())
	})
	if err != nil || !acknowledged {
		if err == nil {
			err = errors.New("fan-out resume inventory was not acknowledged")
		}
		return result, err
	}
	result.Observed = len(inventory)
	result.Admitted = true
	var refusals error
	for _, observed := range inventory {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		prefix, validationErr := validate(ctx, observed)
		changed := false
		reopened := false
		var refused error
		acknowledged, err := transaction(ctx, func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) error {
			ready, err := admitFanOutRun(ctx, tx, admission, grant, runID, true)
			if err != nil || !ready {
				return errors.Join(err, fanoutobligation.ErrStaleClaim)
			}
			if err := runstate.RequireNormalControlTx(ctx, tx, runID, runfork.ControlRunContinue); err != nil {
				return err
			}
			predicate, args := fanOutKeyPredicate("", observed.Request.Key, 1)
			query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE ` + predicate
			if postgres {
				query += ` FOR UPDATE`
			}
			current, err := scanFanOutIntent(tx.QueryRowContext(ctx, query, args...))
			if err != nil {
				return err
			}
			if !sameBlockedFanOutRevision(current, observed) {
				changed = true
				return nil
			}
			if validationErr != nil {
				refused = validationErr
				return nil
			}
			if err := refuseInheritedFanOutResume(ctx, tx, current); err != nil {
				refused = err
				return nil
			}
			if hash, err := current.Request.OriginBundleHash(); err != nil || hash != grant.BundleHash {
				return errors.Join(err, fanoutobligation.ErrStaleClaim)
			}
			if life := current.Request.Capsule.Context.Life; life != nil {
				header, found, err := workflowheader.LoadForRead(ctx, tx, life.RunID, current.Request.Capsule.EntityID, life.FlowInstance)
				if err != nil {
					return err
				}
				if !found || header.Life != life.Life {
					refused = pipeline.InstanceRestarted(flowidentity.RunScopedFlowInstance{RunID: life.RunID, Route: current.Request.Capsule.Route}, life.Life, header.Life)
					return nil
				}
			}
			currentPrefix, err := readFanOutResumePrefix(ctx, tx, current)
			if err != nil {
				refused = err
				return nil
			}
			if !reflect.DeepEqual(currentPrefix, prefix) {
				changed = true
				return nil
			}
			at, err := fanOutAdmissionTime(ctx, tx, postgres, time.Now)
			if err != nil {
				return err
			}
			predicate, args = fanOutKeyPredicate("", observed.Request.Key, 2)
			updated, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET status='open',blocked_reason=NULL,updated_at=$1 WHERE `+predicate+` AND status='blocked' AND claim_owner IS NULL`, append([]any{at}, args...)...)
			if err != nil {
				return err
			}
			count, err := updated.RowsAffected()
			if err != nil || count != 1 {
				return errors.Join(err, fanoutobligation.ErrStaleClaim)
			}
			fact, err := runforkrevision.FanOutIntentFact(observed.Request.Key)
			if err != nil {
				return err
			}
			reopened = true
			return attempt.AddFacts(runID, fact)
		})
		if acknowledged {
			if refused != nil {
				result.Remaining++
				refusals = errors.Join(refusals, refused)
			} else if changed {
				result.Changed++
			} else if reopened {
				result.Reopened++
			}
		}
		if err != nil || !acknowledged {
			if err == nil {
				err = errors.New("fan-out resume transition was not acknowledged")
			}
			if !acknowledged {
				// Resolve only the current state under native admission. Never retry
				// an unacknowledged reopen or infer its success from a local error.
				readChanged := false
				readAcknowledged, readErr := transaction(ctx, func(ctx context.Context, tx *sql.Tx, _ *mutationprotocol.Attempt) error {
					ready, readErr := admitFanOutRun(ctx, tx, admission, grant, runID, true)
					if readErr != nil || !ready {
						return errors.Join(readErr, fanoutobligation.ErrStaleClaim)
					}
					if readErr := runstate.RequireNormalControlTx(ctx, tx, runID, runfork.ControlRunContinue); readErr != nil {
						return readErr
					}
					predicate, args := fanOutKeyPredicate("", observed.Request.Key, 1)
					query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE ` + predicate
					if postgres {
						query += ` FOR UPDATE`
					}
					current, readErr := scanFanOutIntent(tx.QueryRowContext(ctx, query, args...))
					if readErr == nil && !sameBlockedFanOutRevision(current, observed) {
						readChanged = true
					}
					return readErr
				})
				if readAcknowledged && readChanged {
					result.Changed++
				}
				err = errors.Join(err, readErr)
			}
			return result, errors.Join(refusals, err)
		}
	}
	return result, refusals
}

func sameBlockedFanOutRevision(current, observed fanoutobligation.Intent) bool {
	return current.Status == fanoutobligation.StatusBlocked && current.ClaimOwner == "" &&
		current.ClaimGeneration == observed.ClaimGeneration && current.UpdatedAt.Equal(observed.UpdatedAt) &&
		current.Cursor == observed.Cursor && current.BlockedReason == observed.BlockedReason &&
		current.Source == observed.Source && reflect.DeepEqual(current.Request, observed.Request)
}
