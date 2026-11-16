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

type fanOutReopenObservation struct {
	changed, reopened bool
	refusal           error
}

func admitOrdinaryFanOutResumeTx(ctx context.Context, tx *sql.Tx, admission FanOutAdmission, grant startupownership.GrantEvidence, runID string) error {
	ready, err := admitFanOutRun(ctx, tx, admission, grant, runID, true)
	if err != nil || !ready {
		return errors.Join(err, fanoutobligation.ErrStaleClaim)
	}
	return runstate.RequireNormalControlTx(ctx, tx, runID, runfork.ControlRunContinue)
}

func loadFanOutResumeInventory(ctx context.Context, tx *sql.Tx, admission FanOutAdmission, grant startupownership.GrantEvidence, runID string) ([]fanoutobligation.Intent, error) {
	if err := admitOrdinaryFanOutResumeTx(ctx, tx, admission, grant, runID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND status='blocked' ORDER BY COALESCE(CAST(triggering_delivery_id AS TEXT),''),COALESCE(CAST(deployment_feed_id AS TEXT),''),flow_path,declaration_family,semantic_path`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var inventory []fanoutobligation.Intent
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return nil, err
		}
		inventory = append(inventory, intent)
	}
	return inventory, errors.Join(rows.Err(), rows.Close())
}

func lockFanOutResumeIntent(ctx context.Context, tx *sql.Tx, postgres bool, key fanoutobligation.IntentKey) (fanoutobligation.Intent, error) {
	predicate, args := fanOutKeyPredicate("", key, 1)
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE ` + predicate
	if postgres {
		query += ` FOR UPDATE`
	}
	return scanFanOutIntent(tx.QueryRowContext(ctx, query, args...))
}

func reopenFanOutHandlerTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, postgres bool, admission FanOutAdmission, grant startupownership.GrantEvidence, observed fanoutobligation.Intent, prefix []fanoutobligation.Outcome, validationErr error) (state fanOutReopenObservation, err error) {
	if err := admitOrdinaryFanOutResumeTx(ctx, tx, admission, grant, observed.Request.Key.RunID); err != nil {
		return state, err
	}
	current, err := lockFanOutResumeIntent(ctx, tx, postgres, observed.Request.Key)
	if err != nil {
		return state, err
	}
	if !sameBlockedFanOutRevision(current, observed) {
		state.changed = true
		return state, nil
	}
	state.refusal = validationErr
	if state.refusal != nil {
		return state, nil
	}
	if hash, err := current.Request.OriginBundleHash(); err != nil || hash != grant.BundleHash {
		return state, errors.Join(err, fanoutobligation.ErrStaleClaim)
	}
	state.refusal = validateFanOutResumeCurrentLife(ctx, tx, current)
	if state.refusal != nil {
		return state, nil
	}
	currentPrefix, err := readFanOutResumePrefix(ctx, tx, current)
	if err != nil {
		state.refusal = err
		return state, nil
	}
	if !reflect.DeepEqual(currentPrefix, prefix) {
		state.changed = true
		return state, nil
	}
	at, err := fanOutAdmissionTime(ctx, tx, postgres, time.Now)
	if err != nil {
		return state, err
	}
	predicate, args := fanOutKeyPredicate("", observed.Request.Key, 2)
	updated, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET status='open',blocked_reason=NULL,updated_at=$1 WHERE `+predicate+` AND status='blocked' AND claim_owner IS NULL`, append([]any{at}, args...)...)
	if err != nil {
		return state, err
	}
	count, err := updated.RowsAffected()
	if err != nil || count != 1 {
		return state, errors.Join(err, fanoutobligation.ErrStaleClaim)
	}
	fact, err := runforkrevision.FanOutIntentFact(observed.Request.Key)
	if err != nil {
		return state, err
	}
	state.reopened = true
	return state, attempt.AddFacts(observed.Request.Key.RunID, fact)
}

func validateFanOutResumeCurrentLife(ctx context.Context, tx *sql.Tx, intent fanoutobligation.Intent) error {
	if err := refuseInheritedFanOutResume(ctx, tx, intent); err != nil {
		return err
	}
	life := intent.Request.Capsule.Context.Life
	if life == nil {
		return nil
	}
	header, found, err := workflowheader.LoadForRead(ctx, tx, life.RunID, intent.Request.Capsule.EntityID, life.FlowInstance)
	if err != nil {
		return err
	}
	if !found || header.Life != life.Life {
		return pipeline.InstanceRestarted(flowidentity.RunScopedFlowInstance{RunID: life.RunID, Route: intent.Request.Capsule.Route}, life.Life, header.Life)
	}
	return nil
}

func sameBlockedFanOutRevision(current, observed fanoutobligation.Intent) bool {
	return current.Status == fanoutobligation.StatusBlocked && current.ClaimOwner == "" &&
		current.ClaimGeneration == observed.ClaimGeneration && current.UpdatedAt.Equal(observed.UpdatedAt) &&
		current.Cursor == observed.Cursor && current.BlockedReason == observed.BlockedReason &&
		current.Source == observed.Source && reflect.DeepEqual(current.Request, observed.Request)
}
