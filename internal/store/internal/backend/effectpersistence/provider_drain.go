package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type providerAttemptDrainCandidate struct {
	AttemptID           string
	OperationID         string
	State               runtimeeffects.State
	OriginDeliveryID    string
	OriginRunID         string
	OriginRouteIdentity string
	OriginClaimToken    string
	OriginClaimVersion  int64
	OriginSubscriber    string
}

type completionSettlementPermitKind uint8

const (
	completionSettlementCurrent completionSettlementPermitKind = iota + 1
	completionSettlementDrained
)

type providerDrainPermit struct {
	DrainID               string
	LifecycleTransitionID string
	Target                runtimeeffects.ProviderDrainTarget
	SuccessorEpoch        int64
	SuccessorGeneration   uint64
	Origin                runtimedelivery.Claim
	ExpiresAt             time.Time
}

type completionSettlementPermit struct {
	Kind  completionSettlementPermitKind
	Drain providerDrainPermit
}

func resolveCompletionSettlementPermitPostgres(
	ctx context.Context,
	tx *sql.Tx,
	attempt runtimeeffects.Attempt,
) (completionSettlementPermit, error) {
	if attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent {
		if err := requireExternalEffectAuthorityPostgres(ctx, tx, attempt.Authority, false); err != nil {
			return completionSettlementPermit{}, err
		}
		return completionSettlementPermit{Kind: completionSettlementCurrent}, nil
	}
	if err := requireExternalEffectRunActivePostgres(ctx, tx, attempt.Authority); err != nil {
		return completionSettlementPermit{}, err
	}
	fields, err := agentIdentityFields(attempt.Authority.Normal.Identity)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	var epoch, generation int64
	var phase string
	err = tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
		  AND agent_route_presence=$4 AND flow_scope_key=$5
		  AND flow_instance_id=$6 AND flow_instance=$7
		FOR UPDATE
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	if epoch == attempt.Authority.Normal.RuntimeEpoch && generation == int64(attempt.Authority.Normal.Generation) && phase == "running" {
		return completionSettlementPermit{Kind: completionSettlementCurrent}, nil
	}
	permit, err := loadProviderDrainPermitPostgres(ctx, tx, attempt)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	if epoch != permit.SuccessorEpoch || generation != int64(permit.SuccessorGeneration) ||
		(permit.Target == runtimeeffects.ProviderDrainTargetRunning && phase != "running") ||
		(permit.Target != runtimeeffects.ProviderDrainTargetRunning && phase != "draining") {
		return completionSettlementPermit{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_successor_mismatch", "external-effects", "settle_completion", map[string]any{"attempt_id": attempt.AttemptID})
	}
	return completionSettlementPermit{Kind: completionSettlementDrained, Drain: permit}, nil
}

func resolveCompletionSettlementPermitSQLite(
	ctx context.Context,
	tx *sql.Tx,
	attempt runtimeeffects.Attempt,
) (completionSettlementPermit, error) {
	if attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent {
		if err := requireExternalEffectAuthoritySQLite(ctx, tx, attempt.Authority, false); err != nil {
			return completionSettlementPermit{}, err
		}
		return completionSettlementPermit{Kind: completionSettlementCurrent}, nil
	}
	if err := requireExternalEffectRunActiveSQLite(ctx, tx, attempt.Authority); err != nil {
		return completionSettlementPermit{}, err
	}
	fields, err := agentIdentityFields(attempt.Authority.Normal.Identity)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	var epoch, generation int64
	var phase string
	err = tx.QueryRowContext(ctx, `
		SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
		FROM agents
		WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=?
		  AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=?
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	if epoch == attempt.Authority.Normal.RuntimeEpoch && generation == int64(attempt.Authority.Normal.Generation) && phase == "running" {
		return completionSettlementPermit{Kind: completionSettlementCurrent}, nil
	}
	permit, err := loadProviderDrainPermitSQLite(ctx, tx, attempt)
	if err != nil {
		return completionSettlementPermit{}, err
	}
	if epoch != permit.SuccessorEpoch || generation != int64(permit.SuccessorGeneration) ||
		(permit.Target == runtimeeffects.ProviderDrainTargetRunning && phase != "running") ||
		(permit.Target != runtimeeffects.ProviderDrainTargetRunning && phase != "draining") {
		return completionSettlementPermit{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_successor_mismatch", "external-effects", "settle_completion", map[string]any{"attempt_id": attempt.AttemptID})
	}
	return completionSettlementPermit{Kind: completionSettlementDrained, Drain: permit}, nil
}

func loadProviderDrainPermitPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (providerDrainPermit, error) {
	var permit providerDrainPermit
	var generation int64
	var target, deliveryID, runID, routeIdentity, claimToken, subscriber string
	var claimVersion int64
	err := tx.QueryRowContext(ctx, `
		SELECT drain_id::text, lifecycle_transition_id::text, target_phase,
		       successor_runtime_epoch, successor_generation,
		       origin_delivery_id::text, origin_run_id::text, origin_route_identity,
		       origin_claim_token::text, origin_claim_version, origin_subscriber_id
		FROM runtime_provider_attempt_drains
		WHERE attempt_id=$1::uuid AND operation_id=$2::uuid
		  AND predecessor_runtime_epoch=$3 AND predecessor_generation=$4
		  AND state='pending' AND expires_at>CURRENT_TIMESTAMP
		FOR UPDATE
	`, attempt.AttemptID, attempt.OperationID, attempt.Authority.Normal.RuntimeEpoch, attempt.Authority.Normal.Generation).Scan(
		&permit.DrainID, &permit.LifecycleTransitionID, &target,
		&permit.SuccessorEpoch, &generation,
		&deliveryID, &runID, &routeIdentity, &claimToken, &claimVersion, &subscriber,
	)
	if err != nil {
		return providerDrainPermit{}, providerDrainPermitError(attempt, err)
	}
	permit.SuccessorGeneration = uint64(generation)
	permit.Target = runtimeeffects.ProviderDrainTarget(target)
	permit.Origin, err = runtimedelivery.AdmitPersistedClaim(deliveryID, runID, routeIdentity, claimToken, claimVersion, runtimedelivery.SubscriberAgent, subscriber)
	if err != nil {
		return providerDrainPermit{}, err
	}
	return permit, validateProviderDrainPermit(attempt, permit)
}

func loadProviderDrainPermitSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (providerDrainPermit, error) {
	var permit providerDrainPermit
	var generation int64
	var target, deliveryID, runID, routeIdentity, claimToken, subscriber string
	var claimVersion int64
	err := tx.QueryRowContext(ctx, `
		SELECT drain_id, lifecycle_transition_id, target_phase,
		       successor_runtime_epoch, successor_generation,
		       origin_delivery_id, origin_run_id, origin_route_identity,
		       origin_claim_token, origin_claim_version, origin_subscriber_id
		FROM runtime_provider_attempt_drains
		WHERE attempt_id=? AND operation_id=?
		  AND predecessor_runtime_epoch=? AND predecessor_generation=?
		  AND state='pending' AND datetime(substr(replace(CAST(expires_at AS TEXT),'T',' '),1,19))>CURRENT_TIMESTAMP
	`, attempt.AttemptID, attempt.OperationID, attempt.Authority.Normal.RuntimeEpoch, attempt.Authority.Normal.Generation).Scan(
		&permit.DrainID, &permit.LifecycleTransitionID, &target,
		&permit.SuccessorEpoch, &generation,
		&deliveryID, &runID, &routeIdentity, &claimToken, &claimVersion, &subscriber,
	)
	if err != nil {
		return providerDrainPermit{}, providerDrainPermitError(attempt, err)
	}
	permit.SuccessorGeneration = uint64(generation)
	permit.Target = runtimeeffects.ProviderDrainTarget(target)
	permit.Origin, err = runtimedelivery.AdmitPersistedClaim(deliveryID, runID, routeIdentity, claimToken, claimVersion, runtimedelivery.SubscriberAgent, subscriber)
	if err != nil {
		return providerDrainPermit{}, err
	}
	return permit, validateProviderDrainPermit(attempt, permit)
}

func providerDrainPermitError(attempt runtimeeffects.Attempt, err error) error {
	if err == sql.ErrNoRows {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_missing", "external-effects", "settle_completion", map[string]any{"attempt_id": attempt.AttemptID})
	}
	return err
}

func validateProviderDrainPermit(attempt runtimeeffects.Attempt, permit providerDrainPermit) error {
	if !permit.Target.Valid() || permit.SuccessorEpoch <= 0 || permit.SuccessorGeneration == 0 {
		return fmt.Errorf("provider attempt drain permit is invalid")
	}
	if !attempt.OriginDelivery.Same(permit.Origin) {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_attempt_drain_origin_mismatch", "external-effects", "settle_completion", map[string]any{"attempt_id": attempt.AttemptID})
	}
	return nil
}

type providerDrainRecoveryResolution struct {
	Kind    completionSettlementPermitKind
	Drain   providerDrainPermit
	Expired bool
}

func resolveProviderDrainRecoveryPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, now time.Time) (providerDrainRecoveryResolution, error) {
	return resolveProviderDrainRecovery(ctx, tx, attempt, now, true)
}

func resolveProviderDrainRecoverySQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, now time.Time) (providerDrainRecoveryResolution, error) {
	return resolveProviderDrainRecovery(ctx, tx, attempt, now, false)
}

func resolveProviderDrainRecovery(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, now time.Time, postgres bool) (providerDrainRecoveryResolution, error) {
	if attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent {
		return providerDrainRecoveryResolution{Kind: completionSettlementCurrent}, nil
	}
	permit, found, err := loadProviderDrainForRecovery(ctx, tx, attempt, postgres)
	if err != nil {
		return providerDrainRecoveryResolution{}, err
	}
	fields, err := agentIdentityFields(attempt.Authority.Normal.Identity)
	if err != nil {
		return providerDrainRecoveryResolution{}, err
	}
	var epoch, generation int64
	var phase string
	if postgres {
		err = tx.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase FROM agents
			WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3 AND agent_route_presence=$4
			  AND flow_scope_key=$5 AND flow_instance_id=$6 AND flow_instance=$7 FOR UPDATE
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase FROM agents
			WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=?
			  AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=?
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&epoch, &generation, &phase)
	}
	if err != nil {
		return providerDrainRecoveryResolution{}, err
	}
	if !found {
		if epoch == attempt.Authority.Normal.RuntimeEpoch && generation == int64(attempt.Authority.Normal.Generation) && phase == "running" {
			return providerDrainRecoveryResolution{Kind: completionSettlementCurrent}, nil
		}
		return providerDrainRecoveryResolution{}, runtimefailures.New(
			runtimefailures.ClassLifecycleConflict,
			"provider_attempt_drain_missing",
			"external-effects",
			"startup_reconcile",
			map[string]any{"attempt_id": attempt.AttemptID},
		)
	}
	if epoch != permit.SuccessorEpoch || generation != int64(permit.SuccessorGeneration) ||
		(permit.Target == runtimeeffects.ProviderDrainTargetRunning && phase != "running") ||
		(permit.Target != runtimeeffects.ProviderDrainTargetRunning && phase != "draining") {
		return providerDrainRecoveryResolution{}, runtimefailures.New(
			runtimefailures.ClassLifecycleConflict,
			"provider_attempt_drain_successor_mismatch",
			"external-effects",
			"startup_reconcile",
			map[string]any{"attempt_id": attempt.AttemptID},
		)
	}
	return providerDrainRecoveryResolution{Kind: completionSettlementDrained, Drain: permit, Expired: !permit.ExpiresAt.After(now.UTC())}, nil
}

func loadProviderDrainForRecovery(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, postgres bool) (providerDrainPermit, bool, error) {
	var permit providerDrainPermit
	var expires conversationForkTimeValue
	var generation int64
	var target, deliveryID, runID, routeIdentity, claimToken, subscriber string
	var claimVersion int64
	var err error
	if postgres {
		err = tx.QueryRowContext(ctx, `
			SELECT drain_id::text,lifecycle_transition_id::text,target_phase,successor_runtime_epoch,successor_generation,
			       origin_delivery_id::text,origin_run_id::text,origin_route_identity,origin_claim_token::text,
			       origin_claim_version,origin_subscriber_id,expires_at
			FROM runtime_provider_attempt_drains
			WHERE attempt_id=$1::uuid AND operation_id=$2::uuid
			  AND predecessor_runtime_epoch=$3 AND predecessor_generation=$4 AND state='pending'
			FOR UPDATE
		`, attempt.AttemptID, attempt.OperationID, attempt.Authority.Normal.RuntimeEpoch, attempt.Authority.Normal.Generation).Scan(
			&permit.DrainID, &permit.LifecycleTransitionID, &target, &permit.SuccessorEpoch, &generation,
			&deliveryID, &runID, &routeIdentity, &claimToken, &claimVersion, &subscriber, &expires,
		)
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT drain_id,lifecycle_transition_id,target_phase,successor_runtime_epoch,successor_generation,
			       origin_delivery_id,origin_run_id,origin_route_identity,origin_claim_token,
			       origin_claim_version,origin_subscriber_id,expires_at
			FROM runtime_provider_attempt_drains
			WHERE attempt_id=? AND operation_id=?
			  AND predecessor_runtime_epoch=? AND predecessor_generation=? AND state='pending'
		`, attempt.AttemptID, attempt.OperationID, attempt.Authority.Normal.RuntimeEpoch, attempt.Authority.Normal.Generation).Scan(
			&permit.DrainID, &permit.LifecycleTransitionID, &target, &permit.SuccessorEpoch, &generation,
			&deliveryID, &runID, &routeIdentity, &claimToken, &claimVersion, &subscriber, &expires,
		)
	}
	if err == sql.ErrNoRows {
		return providerDrainPermit{}, false, nil
	}
	if err != nil {
		return providerDrainPermit{}, false, err
	}
	if !expires.Valid {
		return providerDrainPermit{}, false, fmt.Errorf("provider attempt drain %s has no expiry", permit.DrainID)
	}
	permit.ExpiresAt = expires.Time
	permit.SuccessorGeneration = uint64(generation)
	permit.Target = runtimeeffects.ProviderDrainTarget(target)
	permit.Origin, err = runtimedelivery.AdmitPersistedClaim(deliveryID, runID, routeIdentity, claimToken, claimVersion, runtimedelivery.SubscriberAgent, subscriber)
	if err != nil {
		return providerDrainPermit{}, false, err
	}
	if err := validateProviderDrainPermit(attempt, permit); err != nil {
		return providerDrainPermit{}, false, err
	}
	return permit, true, nil
}

func (s *EffectPostgresOwner) settleProviderDrainTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	attempt runtimeeffects.Attempt,
	settlement runtimeeffects.CompletionSettlement,
	permit providerDrainPermit,
) (*runtimeeffects.ProviderDrainFinalization, error) {
	if err := settleProviderDrainOrigin(ctx, tx, story, settlement, permit.Origin, s.delivery); err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runtime_provider_attempt_drains SET state='settled', settled_at=$2 WHERE drain_id=$1::uuid AND state='pending'`, permit.DrainID, settlement.Now.UTC())
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return nil, err
	}
	return finalizeProviderDrainPostgres(ctx, tx, attempt, permit)
}

func (s *EffectSQLiteOwner) settleProviderDrainTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	attempt runtimeeffects.Attempt,
	settlement runtimeeffects.CompletionSettlement,
	permit providerDrainPermit,
) (*runtimeeffects.ProviderDrainFinalization, error) {
	if err := settleProviderDrainOrigin(ctx, tx, story, settlement, permit.Origin, s.delivery); err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runtime_provider_attempt_drains SET state='settled', settled_at=? WHERE drain_id=? AND state='pending'`, settlement.Now.UTC(), permit.DrainID)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return nil, err
	}
	return finalizeProviderDrainSQLite(ctx, tx, attempt, permit)
}

func settleProviderDrainRecovery(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	attempt runtimeeffects.Attempt,
	settlement runtimeeffects.CompletionSettlement,
	permit providerDrainPermit,
	expired bool,
	postgres bool,
	delivery providerDrainDeliveryOwner,
) (*runtimeeffects.ProviderDrainFinalization, error) {
	if err := settleProviderDrainOriginRecovery(ctx, tx, story, settlement, permit.Origin, delivery); err != nil {
		return nil, err
	}
	state := "settled"
	var failure any
	if expired {
		state = "expired"
		raw, err := json.Marshal(settlement.Settlement.Failure)
		if err != nil {
			return nil, err
		}
		failure = string(raw)
	}
	var result sql.Result
	var err error
	if postgres {
		result, err = tx.ExecContext(ctx, `UPDATE runtime_provider_attempt_drains SET state=$2,failure=$3::jsonb,settled_at=$4 WHERE drain_id=$1::uuid AND state='pending'`, permit.DrainID, state, failure, settlement.Now.UTC())
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE runtime_provider_attempt_drains SET state=?,failure=?,settled_at=? WHERE drain_id=? AND state='pending'`, state, failure, settlement.Now.UTC(), permit.DrainID)
	}
	if err := requireExternalAttemptTransition(result, err); err != nil {
		return nil, err
	}
	if postgres {
		return finalizeProviderDrainPostgres(ctx, tx, attempt, permit)
	}
	return finalizeProviderDrainSQLite(ctx, tx, attempt, permit)
}

func settleProviderDrainOrigin(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	settlement runtimeeffects.CompletionSettlement,
	claim runtimedelivery.Claim,
	delivery providerDrainDeliveryOwner,
) error {
	if delivery == nil {
		return fmt.Errorf("provider-drain delivery owner is not bound")
	}
	duration := time.Duration(0)
	if settlement.AgentTurn != nil && settlement.AgentTurn.LatencyMS > 0 {
		duration = time.Duration(settlement.AgentTurn.LatencyMS) * time.Millisecond
	}
	if settlement.Settlement.State == runtimeeffects.StateSettled {
		return delivery.SettleProviderOriginSuccessTx(ctx, tx, story, claim, nil, duration)
	}
	reason := "provider_attempt_drained_without_confirmed_success"
	if settlement.Settlement.Failure != nil && strings.TrimSpace(settlement.Settlement.Failure.Detail.Code) != "" {
		reason = settlement.Settlement.Failure.Detail.Code
	}
	return delivery.SettleProviderOriginFailureTx(ctx, tx, story, claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter,
		ReasonCode:  reason,
		Failure:     settlement.Settlement.Failure,
		Duration:    duration,
	})
}

func settleProviderDrainOriginRecovery(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	settlement runtimeeffects.CompletionSettlement,
	claim runtimedelivery.Claim,
	delivery providerDrainDeliveryOwner,
) error {
	if delivery == nil {
		return fmt.Errorf("provider-drain delivery owner is not bound")
	}
	if settlement.Settlement.State == runtimeeffects.StateSettled || settlement.Settlement.Failure == nil {
		return fmt.Errorf("provider drain recovery requires a failed or uncertain completion settlement")
	}
	duration := time.Duration(0)
	if settlement.AgentTurn != nil && settlement.AgentTurn.LatencyMS > 0 {
		duration = time.Duration(settlement.AgentTurn.LatencyMS) * time.Millisecond
	}
	reason := "provider_attempt_drained_without_confirmed_success"
	if code := strings.TrimSpace(settlement.Settlement.Failure.Detail.Code); code != "" {
		reason = code
	}
	return delivery.SettleProviderOriginRecoveryFailureTx(ctx, tx, story, claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter,
		ReasonCode:  reason,
		Failure:     settlement.Settlement.Failure,
		Duration:    duration,
	})
}

func finalizeProviderDrainPostgres(
	ctx context.Context,
	tx *sql.Tx,
	attempt runtimeeffects.Attempt,
	permit providerDrainPermit,
) (*runtimeeffects.ProviderDrainFinalization, error) {
	if permit.Target == runtimeeffects.ProviderDrainTargetRunning {
		return nil, nil
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE lifecycle_transition_id=$1::uuid AND state='pending'`, permit.LifecycleTransitionID).Scan(&pending); err != nil {
		return nil, err
	}
	if pending != 0 {
		return nil, nil
	}
	fields, err := agentIdentityFields(attempt.Authority.Normal.Identity)
	if err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agents SET lifecycle_phase=$8
		WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
		  AND agent_route_presence=$4 AND flow_scope_key=$5
		  AND flow_instance_id=$6 AND flow_instance=$7
		  AND lifecycle_runtime_epoch=$9 AND lifecycle_generation=$10 AND lifecycle_phase='draining'
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		string(permit.Target), permit.SuccessorEpoch, permit.SuccessorGeneration)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return nil, err
	}
	return providerDrainFinalization(attempt, permit), nil
}

func finalizeProviderDrainSQLite(
	ctx context.Context,
	tx *sql.Tx,
	attempt runtimeeffects.Attempt,
	permit providerDrainPermit,
) (*runtimeeffects.ProviderDrainFinalization, error) {
	if permit.Target == runtimeeffects.ProviderDrainTargetRunning {
		return nil, nil
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE lifecycle_transition_id=? AND state='pending'`, permit.LifecycleTransitionID).Scan(&pending); err != nil {
		return nil, err
	}
	if pending != 0 {
		return nil, nil
	}
	fields, err := agentIdentityFields(attempt.Authority.Normal.Identity)
	if err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agents SET lifecycle_phase=?
		WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=?
		  AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=?
		  AND lifecycle_runtime_epoch=? AND lifecycle_generation=? AND lifecycle_phase='draining'
	`, string(permit.Target), fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		permit.SuccessorEpoch, permit.SuccessorGeneration)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return nil, err
	}
	return providerDrainFinalization(attempt, permit), nil
}

func providerDrainFinalization(attempt runtimeeffects.Attempt, permit providerDrainPermit) *runtimeeffects.ProviderDrainFinalization {
	return &runtimeeffects.ProviderDrainFinalization{
		Token: runtimeeffects.LifecycleToken{
			RuntimeEpoch: permit.SuccessorEpoch,
			Identity:     attempt.Authority.Normal.Identity,
			AgentID:      attempt.Authority.Normal.AgentID,
			Generation:   permit.SuccessorGeneration,
		},
		Target: permit.Target,
	}
}

func (c providerAttemptDrainCandidate) claim(agentID string) (runtimedelivery.Claim, error) {
	if strings.TrimSpace(c.OriginSubscriber) != strings.TrimSpace(agentID) {
		return runtimedelivery.Claim{}, fmt.Errorf("provider attempt %s origin subscriber does not match lifecycle identity", c.AttemptID)
	}
	return runtimedelivery.AdmitPersistedClaim(
		c.OriginDeliveryID,
		c.OriginRunID,
		c.OriginRouteIdentity,
		c.OriginClaimToken,
		c.OriginClaimVersion,
		runtimedelivery.SubscriberAgent,
		c.OriginSubscriber,
	)
}

func (s *EffectPostgresOwner) CaptureProviderAttemptDrainsPostgresTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	capture runtimeeffects.ProviderAttemptDrainCapture,
) (runtimeeffects.ProviderAttemptDrainCaptureResult, error) {
	if err := capture.Validate(); err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	fields, err := agentIdentityFields(capture.Predecessor.Identity)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT a.attempt_id::text, a.operation_id::text, a.state,
		       COALESCE(a.origin_delivery_id::text,''), COALESCE(a.origin_run_id::text,''),
		       COALESCE(a.origin_route_identity,''), COALESCE(a.origin_claim_token::text,''),
		       COALESCE(a.origin_claim_version,0), COALESCE(a.origin_subscriber_id,'')
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE o.authority_kind='normal_agent' AND o.effect_kind='provider_turn'
		  AND o.agent_id=$1 AND o.agent_name_owner=$2 AND o.agent_name_source=$3
		  AND o.agent_route_presence=$4 AND o.flow_scope_key=$5
		  AND o.flow_instance_id=$6 AND o.flow_instance=$7
		  AND o.runtime_epoch=$8 AND o.generation=$9
		  AND a.state IN ('authorized','launched','response_observed')
		ORDER BY a.attempt_id
		FOR UPDATE OF a,o
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		capture.Predecessor.RuntimeEpoch, capture.Predecessor.Generation)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	candidates, err := scanProviderAttemptDrainCandidates(rows)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	return s.captureProviderAttemptDrains(ctx, tx, story, capture, candidates, true)
}

func (s *EffectSQLiteOwner) CaptureProviderAttemptDrainsSQLiteTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	capture runtimeeffects.ProviderAttemptDrainCapture,
) (runtimeeffects.ProviderAttemptDrainCaptureResult, error) {
	if err := capture.Validate(); err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	fields, err := agentIdentityFields(capture.Predecessor.Identity)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT a.attempt_id, a.operation_id, a.state,
		       COALESCE(a.origin_delivery_id,''), COALESCE(a.origin_run_id,''),
		       COALESCE(a.origin_route_identity,''), COALESCE(a.origin_claim_token,''),
		       COALESCE(a.origin_claim_version,0), COALESCE(a.origin_subscriber_id,'')
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE o.authority_kind='normal_agent' AND o.effect_kind='provider_turn'
		  AND o.agent_id=? AND o.agent_name_owner=? AND o.agent_name_source=?
		  AND o.agent_route_presence=? AND o.flow_scope_key=?
		  AND o.flow_instance_id=? AND o.flow_instance=?
		  AND o.runtime_epoch=? AND o.generation=?
		  AND a.state IN ('authorized','launched','response_observed')
		ORDER BY a.attempt_id
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		capture.Predecessor.RuntimeEpoch, capture.Predecessor.Generation)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	candidates, err := scanProviderAttemptDrainCandidates(rows)
	if err != nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
	}
	return s.captureProviderAttemptDrains(ctx, tx, story, capture, candidates, false)
}

func scanProviderAttemptDrainCandidates(rows *sql.Rows) ([]providerAttemptDrainCandidate, error) {
	defer rows.Close()
	var candidates []providerAttemptDrainCandidate
	for rows.Next() {
		var candidate providerAttemptDrainCandidate
		if err := rows.Scan(
			&candidate.AttemptID,
			&candidate.OperationID,
			&candidate.State,
			&candidate.OriginDeliveryID,
			&candidate.OriginRunID,
			&candidate.OriginRouteIdentity,
			&candidate.OriginClaimToken,
			&candidate.OriginClaimVersion,
			&candidate.OriginSubscriber,
		); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (s *EffectPostgresOwner) captureProviderAttemptDrains(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	capture runtimeeffects.ProviderAttemptDrainCapture,
	candidates []providerAttemptDrainCandidate,
	postgres bool,
) (runtimeeffects.ProviderAttemptDrainCaptureResult, error) {
	return captureProviderAttemptDrains(ctx, tx, story, capture, candidates, postgres, s.delivery)
}

func (s *EffectSQLiteOwner) captureProviderAttemptDrains(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	capture runtimeeffects.ProviderAttemptDrainCapture,
	candidates []providerAttemptDrainCandidate,
	postgres bool,
) (runtimeeffects.ProviderAttemptDrainCaptureResult, error) {
	return captureProviderAttemptDrains(ctx, tx, story, capture, candidates, postgres, s.delivery)
}

func captureProviderAttemptDrains(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	capture runtimeeffects.ProviderAttemptDrainCapture,
	candidates []providerAttemptDrainCandidate,
	postgres bool,
	delivery providerDrainDeliveryOwner,
) (runtimeeffects.ProviderAttemptDrainCaptureResult, error) {
	if len(candidates) != 0 && delivery == nil {
		return runtimeeffects.ProviderAttemptDrainCaptureResult{}, fmt.Errorf("provider-drain delivery owner is not bound")
	}
	result := runtimeeffects.ProviderAttemptDrainCaptureResult{}
	for _, candidate := range candidates {
		claim, err := candidate.claim(capture.Predecessor.AgentID)
		if err != nil {
			return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
		}
		switch candidate.State {
		case runtimeeffects.StateAuthorized:
			if err := abandonPrelaunchProviderAttempt(ctx, tx, story, candidate, claim, capture.CapturedAt, postgres, delivery); err != nil {
				return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
			}
		case runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved:
			if err := insertProviderAttemptDrain(ctx, tx, capture, candidate, claim, postgres); err != nil {
				return runtimeeffects.ProviderAttemptDrainCaptureResult{}, err
			}
			result.Captured++
		default:
			return runtimeeffects.ProviderAttemptDrainCaptureResult{}, fmt.Errorf("provider attempt %s has uncaptured state %q", candidate.AttemptID, candidate.State)
		}
	}
	return result, nil
}

func abandonPrelaunchProviderAttempt(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	candidate providerAttemptDrainCandidate,
	claim runtimedelivery.Claim,
	now time.Time,
	postgres bool,
	delivery providerDrainDeliveryOwner,
) error {
	failureErr := runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"provider_attempt_superseded_before_launch",
		"external-effects",
		"launch_attempt",
		map[string]any{"attempt_id": candidate.AttemptID},
	)
	failure, _ := runtimefailures.EnvelopeFromError(failureErr)
	if err := delivery.SettleProviderOriginFailureTx(ctx, tx, story, claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter,
		ReasonCode:  "provider_attempt_superseded_before_launch",
		Failure:     &failure,
	}); err != nil {
		return fmt.Errorf("settle superseded prelaunch provider origin: %w", err)
	}
	failureJSON, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	var attemptResult, operationResult sql.Result
	if postgres {
		attemptResult, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET state='terminal_failure', failure=$2::jsonb, completed_at=$3, updated_at=$3 WHERE attempt_id=$1::uuid AND state='authorized'`, candidate.AttemptID, string(failureJSON), now.UTC())
		if err == nil {
			operationResult, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='terminal_failure', completed_at=$2, updated_at=$2 WHERE operation_id=$1::uuid AND state='authorized'`, candidate.OperationID, now.UTC())
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, candidate.AttemptID)
		}
	} else {
		attemptResult, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET state='terminal_failure', failure=?, completed_at=?, updated_at=? WHERE attempt_id=? AND state='authorized'`, string(failureJSON), now.UTC(), now.UTC(), candidate.AttemptID)
		if err == nil {
			operationResult, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='terminal_failure', completed_at=?, updated_at=? WHERE operation_id=? AND state='authorized'`, now.UTC(), now.UTC(), candidate.OperationID)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM runtime_effect_budget_reservations WHERE attempt_id=?`, candidate.AttemptID)
		}
	}
	if err != nil {
		return err
	}
	if err := requireExternalAttemptTransition(attemptResult, nil); err != nil {
		return err
	}
	if err := requireExternalAttemptTransition(operationResult, nil); err != nil {
		return err
	}
	source, err := loadExternalEffectStorySource(ctx, tx, candidate.AttemptID, postgres)
	if err != nil {
		return err
	}
	return recordExternalEffectStory(ctx, story, source, runtimeeffects.StateTerminalFailure, &failure, now.UTC())
}

func insertProviderAttemptDrain(
	ctx context.Context,
	tx *sql.Tx,
	capture runtimeeffects.ProviderAttemptDrainCapture,
	candidate providerAttemptDrainCandidate,
	claim runtimedelivery.Claim,
	postgres bool,
) error {
	fields, err := agentIdentityFields(capture.Predecessor.Identity)
	if err != nil {
		return err
	}
	drainID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("provider-drain:"+candidate.AttemptID+":"+capture.LifecycleTransitionID)).String()
	args := []any{
		drainID, candidate.AttemptID, candidate.OperationID, capture.LifecycleOperationID, capture.LifecycleTransitionID,
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		capture.Predecessor.RuntimeEpoch, capture.Predecessor.Generation, capture.SuccessorRuntimeEpoch, capture.SuccessorGeneration, string(capture.Target),
		claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), claim.PersistenceToken(), claim.Version(), string(claim.SubscriberClass()), claim.SubscriberID(),
		capture.CapturedAt.UTC(), capture.ExpiresAt.UTC(),
	}
	if postgres {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO runtime_provider_attempt_drains (
				drain_id, attempt_id, operation_id, lifecycle_operation_id, lifecycle_transition_id,
				agent_id, agent_name_owner, agent_name_source, agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				predecessor_runtime_epoch, predecessor_generation, successor_runtime_epoch, successor_generation, target_phase,
				origin_delivery_id, origin_run_id, origin_route_identity, origin_claim_token, origin_claim_version, origin_subscriber_type, origin_subscriber_id,
				state, captured_at, expires_at
			) VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::uuid,$19::uuid,$20,$21::uuid,$22,$23,$24,'pending',$25,$26)
		`, args...)
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO runtime_provider_attempt_drains (
			drain_id, attempt_id, operation_id, lifecycle_operation_id, lifecycle_transition_id,
			agent_id, agent_name_owner, agent_name_source, agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			predecessor_runtime_epoch, predecessor_generation, successor_runtime_epoch, successor_generation, target_phase,
			origin_delivery_id, origin_run_id, origin_route_identity, origin_claim_token, origin_claim_version, origin_subscriber_type, origin_subscriber_id,
			state, captured_at, expires_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)
	`, args...)
	return err
}
