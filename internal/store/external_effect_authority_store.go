package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func externalEffectAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	if !authority.Valid() {
		return false, nil
	}
	if current, err := externalEffectRunCurrent(ctx, q, authority, storerunlifecycle.DialectPostgres); err != nil || !current {
		return current, err
	}
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		var epoch, generation int64
		var phase string
		err := q.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id=$1`, authority.Normal.AgentID).Scan(&epoch, &generation, &phase)
		if err == sql.ErrNoRows {
			return false, nil
		}
		return err == nil && epoch == authority.Normal.RuntimeEpoch && generation == int64(authority.Normal.Generation) && strings.TrimSpace(phase) == "running", err
	case runtimeeffects.AuthoritySelectedContractFork:
		var current selectedRuntimeAuthorityRow
		err := q.QueryRowContext(ctx, `
			SELECT execution_id::text, fork_run_id::text, generation, admission_fingerprint,
			       container_plan_fingerprint, actor_census_fingerprint, effective_config_fingerprint,
			       state, COALESCE(execution_owner,''), lease_expires_at > CURRENT_TIMESTAMP, fence_generation
			FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid
		`, authority.SelectedFork.ExecutionID).Scan(&current.executionID, &current.forkRunID, &current.generation, &current.admissionFingerprint,
			&current.containerFingerprint, &current.actorFingerprint, &current.configFingerprint, &current.state,
			&current.owner, &current.leaseCurrent, &current.fence)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return selectedRuntimeAuthorityMatches(authority, current), nil
	case runtimeeffects.AuthorityConversationForkChat:
		return forkChatAuthorityCurrentPostgres(ctx, q, authority)
	case runtimeeffects.AuthorityStartupProbe:
		return startupProbeAuthorityCurrentPostgres(ctx, q, authority)
	default:
		return false, nil
	}
}

func externalEffectAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	if !authority.Valid() {
		return false, nil
	}
	if current, err := externalEffectRunCurrent(ctx, q, authority, storerunlifecycle.DialectSQLite); err != nil || !current {
		return current, err
	}
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		var epoch, generation int64
		var phase string
		err := q.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id=?`, authority.Normal.AgentID).Scan(&epoch, &generation, &phase)
		if err == sql.ErrNoRows {
			return false, nil
		}
		return err == nil && epoch == authority.Normal.RuntimeEpoch && generation == int64(authority.Normal.Generation) && strings.TrimSpace(phase) == "running", err
	case runtimeeffects.AuthoritySelectedContractFork:
		var current selectedRuntimeAuthorityRow
		err := q.QueryRowContext(ctx, `
			SELECT execution_id, fork_run_id, generation, admission_fingerprint,
			       container_plan_fingerprint, actor_census_fingerprint, effective_config_fingerprint,
			       state, COALESCE(execution_owner,''), `+sqliteCurrentLeaseSQL+`, fence_generation
			FROM run_fork_selected_contract_runtime_executions WHERE execution_id=?
		`, authority.SelectedFork.ExecutionID).Scan(&current.executionID, &current.forkRunID, &current.generation, &current.admissionFingerprint,
			&current.containerFingerprint, &current.actorFingerprint, &current.configFingerprint, &current.state,
			&current.owner, &current.leaseCurrent, &current.fence)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return selectedRuntimeAuthorityMatches(authority, current), nil
	case runtimeeffects.AuthorityConversationForkChat:
		return forkChatAuthorityCurrentSQLite(ctx, q, authority)
	case runtimeeffects.AuthorityStartupProbe:
		return startupProbeAuthorityCurrentSQLite(ctx, q, authority)
	default:
		return false, nil
	}
}

func requireExternalEffectAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	if err := requireExternalEffectRunActive(ctx, tx, authority, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat && authorize {
		return claimOrValidateForkChatAuthorityPostgres(ctx, tx, authority)
	}
	return requireCurrentExternalEffectAuthorityPostgres(ctx, tx, authority)
}

func requireExternalEffectAuthoritySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	if err := requireExternalEffectRunActive(ctx, tx, authority, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat && authorize {
		return claimOrValidateForkChatAuthoritySQLite(ctx, tx, authority)
	}
	return requireCurrentExternalEffectAuthoritySQLite(ctx, tx, authority)
}

func requireExternalEffectRunActive(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, dialect storerunlifecycle.Dialect) error {
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat || authority.Kind == runtimeeffects.AuthorityStartupProbe {
		return nil
	}
	runID := strings.TrimSpace(authority.SelectedFork.ForkRunID)
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		runID = strings.TrimSpace(authority.Target.RunID)
		if runID == "" {
			var ok bool
			var err error
			runID, ok, err = runtimecurrentstate.RunIDFromContext(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
	}
	return storerunlifecycle.RequireActive(ctx, tx, runID, dialect)
}

func externalEffectRunCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, dialect storerunlifecycle.Dialect) (bool, error) {
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat || authority.Kind == runtimeeffects.AuthorityStartupProbe {
		return true, nil
	}
	runID := strings.TrimSpace(authority.SelectedFork.ForkRunID)
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		runID = strings.TrimSpace(authority.Target.RunID)
		if runID == "" {
			var ok bool
			var err error
			runID, ok, err = runtimecurrentstate.RunIDFromContext(ctx)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, nil
			}
		}
	}
	query := `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id=$1::uuid AND status IN ('running','paused'))`
	if dialect == storerunlifecycle.DialectSQLite {
		query = `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id=? AND status IN ('running','paused'))`
	}
	var active bool
	if err := q.QueryRowContext(ctx, query, runID).Scan(&active); err != nil {
		return false, err
	}
	return active, nil
}

func requireCurrentExternalEffectAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	var res sql.Result
	var err error
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		res, err = tx.ExecContext(ctx, `
			UPDATE agents SET lifecycle_generation=lifecycle_generation
			WHERE agent_id=$1 AND lifecycle_runtime_epoch=$2 AND lifecycle_generation=$3 AND lifecycle_phase='running'
		`, authority.Normal.AgentID, authority.Normal.RuntimeEpoch, authority.Normal.Generation)
	case runtimeeffects.AuthoritySelectedContractFork:
		selected := authority.SelectedFork
		res, err = tx.ExecContext(ctx, `
			UPDATE run_fork_selected_contract_runtime_executions SET updated_at=updated_at
			WHERE execution_id=$1::uuid AND fork_run_id=$2::uuid AND generation=$3
			  AND state='running' AND execution_owner=$4 AND fence_generation=$5
			  AND admission_fingerprint=$6 AND container_plan_fingerprint=$7
			  AND actor_census_fingerprint=$8 AND effective_config_fingerprint=$9
			  AND lease_expires_at>CURRENT_TIMESTAMP
		`, selected.ExecutionID, selected.ForkRunID, selected.Generation, authority.ExecutionOwner, authority.FenceGeneration,
			selected.AdmissionFingerprint, selected.ContainerPlanFingerprint, selected.ActorCensusFingerprint, selected.EffectiveConfigFingerprint)
	case runtimeeffects.AuthorityConversationForkChat:
		forkchat := authority.ForkChat
		res, err = tx.ExecContext(ctx, `
			UPDATE conversation_fork_turns SET updated_at=updated_at
			WHERE fork_turn_id=$1::uuid AND fork_id=$2::uuid AND actor_token_id=$3
			  AND request_occurrence_id=$4::uuid AND request_hash=$5 AND state='executing'
			  AND execution_owner=$6 AND fence_generation=$7 AND lease_expires_at>CURRENT_TIMESTAMP
		`, forkchat.ForkTurnID, forkchat.ForkID, forkchat.ActorTokenID, forkchat.RequestOccurrenceID,
			forkchat.RequestHash, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityStartupProbe:
		startup := authority.StartupProbe
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_startup_authority_facts SET created_at=created_at
			WHERE authority_id=$1::uuid AND state_version=$2 AND state IN ('active','prepared')
			  AND owner_id=$3 AND generation=$4 AND NOT EXISTS (
			    SELECT 1 FROM runtime_startup_authority_facts newer
			    WHERE newer.lease_authority_id=runtime_startup_authority_facts.lease_authority_id
			      AND newer.transition_ordinal>runtime_startup_authority_facts.transition_ordinal
			  )
		`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	default:
		return invalidExternalAuthority(authority, "unsupported_kind")
	}
	return requireCurrentExternalEffectAuthorityMutation(res, err, authority)
}

func requireCurrentExternalEffectAuthoritySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	var res sql.Result
	var err error
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		res, err = tx.ExecContext(ctx, `
			UPDATE agents SET lifecycle_generation=lifecycle_generation
			WHERE agent_id=? AND lifecycle_runtime_epoch=? AND lifecycle_generation=? AND lifecycle_phase='running'
		`, authority.Normal.AgentID, authority.Normal.RuntimeEpoch, authority.Normal.Generation)
	case runtimeeffects.AuthoritySelectedContractFork:
		selected := authority.SelectedFork
		res, err = tx.ExecContext(ctx, `
			UPDATE run_fork_selected_contract_runtime_executions SET updated_at=updated_at
			WHERE execution_id=? AND fork_run_id=? AND generation=?
			  AND state='running' AND execution_owner=? AND fence_generation=?
			  AND admission_fingerprint=? AND container_plan_fingerprint=?
			  AND actor_census_fingerprint=? AND effective_config_fingerprint=?
			  AND `+sqliteCurrentLeaseSQL+`
		`, selected.ExecutionID, selected.ForkRunID, selected.Generation, authority.ExecutionOwner, authority.FenceGeneration,
			selected.AdmissionFingerprint, selected.ContainerPlanFingerprint, selected.ActorCensusFingerprint, selected.EffectiveConfigFingerprint)
	case runtimeeffects.AuthorityConversationForkChat:
		forkchat := authority.ForkChat
		res, err = tx.ExecContext(ctx, `
			UPDATE conversation_fork_turns SET updated_at=updated_at
			WHERE fork_turn_id=? AND fork_id=? AND actor_token_id=?
			  AND request_occurrence_id=? AND request_hash=? AND state='executing'
			  AND execution_owner=? AND fence_generation=? AND `+sqliteCurrentLeaseSQL+`
		`, forkchat.ForkTurnID, forkchat.ForkID, forkchat.ActorTokenID, forkchat.RequestOccurrenceID,
			forkchat.RequestHash, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityStartupProbe:
		startup := authority.StartupProbe
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_startup_authority_facts SET created_at=created_at
			WHERE authority_id=? AND state_version=? AND state IN ('active','prepared')
			  AND owner_id=? AND generation=? AND NOT EXISTS (
			    SELECT 1 FROM runtime_startup_authority_facts newer
			    WHERE newer.lease_authority_id=runtime_startup_authority_facts.lease_authority_id
			      AND newer.transition_ordinal>runtime_startup_authority_facts.transition_ordinal
			  )
		`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	default:
		return invalidExternalAuthority(authority, "unsupported_kind")
	}
	return requireCurrentExternalEffectAuthorityMutation(res, err, authority)
}

func requireCurrentExternalEffectAuthorityMutation(res sql.Result, err error, authority runtimeeffects.Authority) error {
	if err != nil {
		return fmt.Errorf("lock current external effect authority: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm current external effect authority: %w", err)
	}
	if rows != 1 {
		return invalidExternalAuthority(authority, "stale")
	}
	return nil
}

func requireCompletionAuthorityNoLiveAttempts(ctx context.Context, tx *sql.Tx, sqlite bool, authority runtimeeffects.Authority) error {
	var query string
	var coordinate string
	switch authority.Kind {
	case runtimeeffects.AuthoritySelectedContractFork:
		coordinate = authority.SelectedFork.ExecutionID
		query = `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1::uuid AND a.state IN ('authorized','launched','response_observed')`
		if sqlite {
			query = `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=? AND a.state IN ('authorized','launched','response_observed')`
		}
	case runtimeeffects.AuthorityConversationForkChat:
		coordinate = authority.ForkChat.ForkTurnID
		query = `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=$1::uuid AND a.state IN ('authorized','launched','response_observed')`
		if sqlite {
			query = `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=? AND a.state IN ('authorized','launched','response_observed')`
		}
	default:
		return fmt.Errorf("completion authority %q has no container terminalization", authority.Kind)
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, coordinate).Scan(&count); err != nil {
		return fmt.Errorf("count live completion attempts for %s: %w", authority.Kind, err)
	}
	if count != 0 {
		return fmt.Errorf("%s authority has %d non-terminal completion attempts", authority.Kind, count)
	}
	return nil
}

func externalEffectAttemptLeasePostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (time.Time, error) {
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthoritySelectedContractFork:
		var lease time.Time
		err := q.QueryRowContext(ctx, `
			SELECT lease_expires_at
			FROM run_fork_selected_contract_runtime_executions
			WHERE execution_id=$1::uuid AND state='running' AND execution_owner=$2 AND fence_generation=$3
		`, authority.SelectedFork.ExecutionID, authority.ExecutionOwner, authority.FenceGeneration).Scan(&lease)
		if err != nil {
			return time.Time{}, fmt.Errorf("load selected-contract attempt lease: %w", err)
		}
		return lease.UTC(), nil
	case runtimeeffects.AuthorityStartupProbe:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthorityConversationForkChat:
		var lease time.Time
		err := q.QueryRowContext(ctx, `
			SELECT lease_expires_at
			FROM conversation_fork_turns
			WHERE fork_turn_id=$1::uuid AND state='executing' AND execution_owner=$2 AND fence_generation=$3
		`, authority.ForkChat.ForkTurnID, authority.ExecutionOwner, authority.FenceGeneration).Scan(&lease)
		if err != nil {
			return time.Time{}, fmt.Errorf("load forkchat attempt lease: %w", err)
		}
		return lease.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("load attempt lease for unsupported authority kind %q", authority.Kind)
	}
}

func externalEffectAttemptLeaseSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (time.Time, error) {
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthoritySelectedContractFork:
		var lease conversationForkTimeValue
		err := q.QueryRowContext(ctx, `
			SELECT lease_expires_at
			FROM run_fork_selected_contract_runtime_executions
			WHERE execution_id=? AND state='running' AND execution_owner=? AND fence_generation=?
		`, authority.SelectedFork.ExecutionID, authority.ExecutionOwner, authority.FenceGeneration).Scan(&lease)
		if err != nil || !lease.Valid {
			return time.Time{}, fmt.Errorf("load sqlite selected-contract attempt lease: %w", err)
		}
		return lease.Time.UTC(), nil
	case runtimeeffects.AuthorityStartupProbe:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthorityConversationForkChat:
		var lease conversationForkTimeValue
		err := q.QueryRowContext(ctx, `
			SELECT lease_expires_at
			FROM conversation_fork_turns
			WHERE fork_turn_id=? AND state='executing' AND execution_owner=? AND fence_generation=?
		`, authority.ForkChat.ForkTurnID, authority.ExecutionOwner, authority.FenceGeneration).Scan(&lease)
		if err != nil || !lease.Valid {
			return time.Time{}, fmt.Errorf("load sqlite forkchat attempt lease: %w", err)
		}
		return lease.Time.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("load sqlite attempt lease for unsupported authority kind %q", authority.Kind)
	}
}

func startupProbeAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	startup := authority.StartupProbe
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runtime_startup_authority_facts f
		WHERE f.authority_id=$1::uuid AND f.state_version=$2 AND f.state IN ('active','prepared') AND f.owner_id=$3 AND f.generation=$4
		  AND NOT EXISTS (SELECT 1 FROM runtime_startup_authority_facts newer WHERE newer.lease_authority_id=f.lease_authority_id AND newer.transition_ordinal>f.transition_ordinal)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

func startupProbeAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	startup := authority.StartupProbe
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runtime_startup_authority_facts f
		WHERE f.authority_id=? AND f.state_version=? AND f.state IN ('active','prepared') AND f.owner_id=? AND f.generation=?
		  AND NOT EXISTS (SELECT 1 FROM runtime_startup_authority_facts newer WHERE newer.lease_authority_id=f.lease_authority_id AND newer.transition_ordinal>f.transition_ordinal)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

type selectedRuntimeAuthorityRow struct {
	executionID          string
	forkRunID            string
	generation           uint64
	admissionFingerprint string
	containerFingerprint string
	actorFingerprint     string
	configFingerprint    string
	state                string
	owner                string
	leaseCurrent         bool
	fence                uint64
}

func selectedRuntimeAuthorityMatches(authority runtimeeffects.Authority, current selectedRuntimeAuthorityRow) bool {
	selected := authority.SelectedFork
	return strings.TrimSpace(current.executionID) == strings.TrimSpace(selected.ExecutionID) &&
		strings.TrimSpace(current.forkRunID) == strings.TrimSpace(selected.ForkRunID) &&
		current.generation == selected.Generation &&
		current.admissionFingerprint == selected.AdmissionFingerprint &&
		current.containerFingerprint == selected.ContainerPlanFingerprint &&
		current.actorFingerprint == selected.ActorCensusFingerprint &&
		current.configFingerprint == selected.EffectiveConfigFingerprint &&
		current.state == "running" && current.owner == authority.ExecutionOwner && current.fence == authority.FenceGeneration && current.leaseCurrent
}

func forkChatAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	var forkID, actor, occurrence, hash, state, owner string
	var leaseCurrent bool
	var fence uint64
	err := q.QueryRowContext(ctx, `SELECT fork_id::text, actor_token_id, request_occurrence_id::text, request_hash, state, COALESCE(execution_owner,''), lease_expires_at > CURRENT_TIMESTAMP, fence_generation FROM conversation_fork_turns WHERE fork_turn_id=$1::uuid`, authority.ForkChat.ForkTurnID).
		Scan(&forkID, &actor, &occurrence, &hash, &state, &owner, &leaseCurrent, &fence)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return forkChatAuthorityMatches(authority, forkID, actor, occurrence, hash, state, owner, leaseCurrent, fence), nil
}

func forkChatAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	var forkID, actor, occurrence, hash, state, owner string
	var leaseCurrent bool
	var fence uint64
	err := q.QueryRowContext(ctx, `SELECT fork_id, actor_token_id, request_occurrence_id, request_hash, state, COALESCE(execution_owner,''), `+sqliteCurrentLeaseSQL+`, fence_generation FROM conversation_fork_turns WHERE fork_turn_id=?`, authority.ForkChat.ForkTurnID).
		Scan(&forkID, &actor, &occurrence, &hash, &state, &owner, &leaseCurrent, &fence)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return forkChatAuthorityMatches(authority, forkID, actor, occurrence, hash, state, owner, leaseCurrent, fence), nil
}

func forkChatAuthorityMatches(authority runtimeeffects.Authority, forkID, actor, occurrence, hash, state, owner string, leaseCurrent bool, fence uint64) bool {
	forkchat := authority.ForkChat
	return forkID == forkchat.ForkID && actor == forkchat.ActorTokenID && occurrence == forkchat.RequestOccurrenceID && hash == forkchat.RequestHash &&
		state == "executing" && owner == authority.ExecutionOwner && fence == authority.FenceGeneration && leaseCurrent
}

func claimOrValidateForkChatAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	now := time.Now().UTC()
	expires := now.Add(conversationForkChatExecutionLease)
	res, err := tx.ExecContext(ctx, `
		UPDATE conversation_fork_turns
		SET state='executing', lease_expires_at=GREATEST(lease_expires_at,$4), updated_at=$5
		WHERE fork_turn_id=$1::uuid AND fork_id=$6::uuid AND actor_token_id=$7
		  AND request_occurrence_id=$8::uuid AND request_hash=$9 AND state IN ('prepared','executing')
		  AND execution_owner=$2 AND fence_generation=$3 AND lease_expires_at>CURRENT_TIMESTAMP
	`, authority.ForkChat.ForkTurnID, authority.ExecutionOwner, authority.FenceGeneration, expires, now,
		authority.ForkChat.ForkID, authority.ForkChat.ActorTokenID, authority.ForkChat.RequestOccurrenceID, authority.ForkChat.RequestHash)
	if err != nil {
		return fmt.Errorf("claim forkchat authority: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm forkchat authority claim: %w", err)
	}
	if rows != 1 {
		return invalidExternalAuthority(authority, "forkchat_claim_conflict")
	}
	return nil
}

func claimOrValidateForkChatAuthoritySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	now := time.Now().UTC()
	expires := now.Add(conversationForkChatExecutionLease)
	res, err := tx.ExecContext(ctx, `
		UPDATE conversation_fork_turns
		SET state='executing', lease_expires_at=CASE WHEN lease_expires_at>? THEN lease_expires_at ELSE ? END, updated_at=?
		WHERE fork_turn_id=? AND fork_id=? AND actor_token_id=?
		  AND request_occurrence_id=? AND request_hash=? AND state IN ('prepared','executing')
		  AND execution_owner=? AND fence_generation=? AND `+sqliteCurrentLeaseSQL+`
	`, expires, expires, now, authority.ForkChat.ForkTurnID, authority.ForkChat.ForkID, authority.ForkChat.ActorTokenID,
		authority.ForkChat.RequestOccurrenceID, authority.ForkChat.RequestHash, authority.ExecutionOwner,
		authority.FenceGeneration)
	if err != nil {
		return fmt.Errorf("claim sqlite forkchat authority: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm sqlite forkchat authority claim: %w", err)
	}
	if rows != 1 {
		return invalidExternalAuthority(authority, "forkchat_claim_conflict")
	}
	return nil
}

func invalidExternalAuthority(authority runtimeeffects.Authority, reason string) error {
	return runtimefailures.New(runtimefailures.ClassSupersededGeneration, "external_effect_authority_stale", "external-effects", "check_authority", map[string]any{
		"authority_kind": authority.Kind,
		"authority_id":   authority.ID,
		"reason":         strings.TrimSpace(reason),
	})
}
