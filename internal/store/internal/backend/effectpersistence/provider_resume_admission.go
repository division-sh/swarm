package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

func authorizeProviderResumeTx(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, existing existingExternalAttempt) (runtimeeffects.Attempt, bool, error) {
	if authority.Kind != runtimeeffects.AuthorityNormalAgent || authority.ExecutionMode != executionmode.Live || req.Kind != runtimeeffects.KindProviderTurn ||
		req.Origin.Kind != runtimeeffects.CompletionOriginDelivery || req.ProviderRequestJournal == nil ||
		existing.operationState != string(runtimeeffects.StateOutcomeUncertain) || existing.attemptState != string(runtimeeffects.StateOutcomeUncertain) {
		return runtimeeffects.Attempt{}, false, nil
	}
	refuse := func(reason string) (runtimeeffects.Attempt, bool, error) {
		return runtimeeffects.Attempt{}, true, runtimefailures.New(runtimefailures.ClassLifecycleConflict,
			"provider_resume_evidence_refused", "external-effects", "resume_provider_attempt", map[string]any{
				"operation_id": req.OperationID, "attempt_id": existing.attemptID, "reason": reason,
			})
	}
	if !existing.matchesAuthorityIdentity(authority) || !existing.matchesOperationRequest(req) ||
		req.CapabilitySurface == nil || req.CapabilitySurface.ID != existing.capabilitySurfaceID || delivery == nil {
		return refuse("original operation identity or request differs")
	}
	readOnly, err := completionResumeSurfaceAllowsHTTP(req.Adapter, req.CompletionRequest, *req.CapabilitySurface)
	if err != nil || !readOnly {
		return refuse("frozen provider surface is not eligible")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT provider_request_journal FROM runtime_external_effect_attempts WHERE attempt_id=$1 AND operation_id=$2`, existing.attemptID, req.OperationID).Scan(&raw); err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	journal, err := runtimeeffects.DecodeProviderRequestJournal(raw)
	if err != nil {
		return refuse("original request journal is unavailable")
	}
	retained, err := runtimeeffects.EncodeProviderRequestJournal(journal)
	if err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	requested, err := runtimeeffects.EncodeProviderRequestJournal(*req.ProviderRequestJournal)
	if err != nil || !bytes.Equal(retained, requested) {
		return refuse("original physical request journal differs")
	}
	previous, state, err := loadBusinessTurnFirstAttempt(ctx, tx, postgres, existing.attemptID, req.Now)
	if err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	if state != runtimeeffects.StateOutcomeUncertain || previous.OperationID != req.OperationID ||
		previous.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return refuse("predecessor physical attempt differs")
	}
	permit, found, err := loadProviderRecoveryDrain(ctx, tx, previous, postgres, true)
	if err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	if !found || permit.Target != runtimeeffects.ProviderDrainTargetRunning {
		return refuse("predecessor has no settled running takeover drain")
	}
	proved, err := storeagent.ReadProviderResumeTakeoverChainTx(ctx, tx, authority.Normal, previous.Authority.Normal,
		permit.LifecycleOperationID, permit.LifecycleTransitionID, permit.SuccessorEpoch, permit.SuccessorGeneration, !postgres)
	if err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	if !proved {
		return refuse("successor lacks the exact immutable takeover chain")
	}
	if err := delivery.ValidateProviderResumeClaimTx(ctx, tx, previous.Origin.Delivery, req.Origin.Delivery); err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	if err := requireProviderResumeTurnClockTx(ctx, tx, postgres, previous, req.Now); err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	var attempt runtimeeffects.Attempt
	if postgres {
		attempt, _, err = insertExternalRetryAttemptPostgres(ctx, tx, authority, req, existing.attemptOrdinal+1)
	} else {
		attempt, err = insertExternalRetryAttemptSQLiteTx(ctx, tx, authority, req, existing.attemptOrdinal+1)
	}
	if err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	query := `UPDATE runtime_external_effect_attempts SET provider_request_journal=$1,resume_from_attempt_id=$2
		WHERE attempt_id=$3 AND operation_id=$4 AND state='authorized' AND resume_from_attempt_id IS NULL`
	if postgres {
		query = `UPDATE runtime_external_effect_attempts SET provider_request_journal=$1::jsonb,resume_from_attempt_id=$2::uuid
			WHERE attempt_id=$3::uuid AND operation_id=$4::uuid AND state='authorized' AND resume_from_attempt_id IS NULL`
	}
	result, err := tx.ExecContext(ctx, query, string(retained), previous.AttemptID, attempt.AttemptID, req.OperationID)
	if err := requireExternalAttemptTransition(result, err); err != nil {
		return runtimeeffects.Attempt{}, true, err
	}
	return attempt, true, nil
}

func requireProviderResumeTurnClockTx(ctx context.Context, tx *sql.Tx, postgres bool, previous runtimeeffects.Attempt, now time.Time) error {
	turnID, _, err := businessTurnIdentity(previous.Origin)
	if err != nil {
		return err
	}
	if err := requireBusinessTurnBindingTx(ctx, tx, postgres, turnID, previous, now); err != nil {
		return err
	}
	var first sql.NullString
	var rawLaunched any
	if err := tx.QueryRowContext(ctx, `SELECT CAST(first_attempt_id AS TEXT),first_launched_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`, turnID).Scan(&first, &rawLaunched); err != nil {
		return err
	}
	launched, valid, err := sqliteTimeValue(rawLaunched)
	if err != nil || !valid || !first.Valid {
		return fmt.Errorf("provider resume requires its original first-launch clock")
	}
	anchor, _, err := loadBusinessTurnFirstAttempt(ctx, tx, postgres, first.String, now)
	if err != nil {
		return err
	}
	if err := validateBusinessTurnAnchor(anchor, previous); err != nil {
		return err
	}
	physicalLaunch, err := loadExternalEffectLaunchTime(ctx, tx, anchor.AttemptID, anchor.OperationID, postgres)
	if err != nil {
		return err
	}
	if !physicalLaunch.Equal(launched) {
		return fmt.Errorf("provider resume first-launch clock contradicts its original physical attempt")
	}
	return nil
}
