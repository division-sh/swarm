package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func externalEffectAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	if !authority.Valid() {
		return false, nil
	}
	if current, err := externalEffectRunCurrentPostgres(ctx, q, authority); err != nil || !current {
		return current, err
	}
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		fields, err := agentIdentityFields(authority.Normal.Identity)
		if err != nil {
			return false, err
		}
		var epoch, generation int64
		var phase string
		err = q.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
			FROM agents
			WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
			  AND agent_route_presence=$4 AND flow_scope_key=$5
			  AND flow_instance_id=$6 AND flow_instance=$7 AND run_id=$8::uuid
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, fields.RunID).Scan(&epoch, &generation, &phase)
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
	case runtimeeffects.AuthorityServeRegistration:
		return serveRegistrationAuthorityCurrentPostgres(ctx, q, authority)
	case runtimeeffects.AuthorityChannelConfirmation:
		return channelConfirmationAuthorityCurrentPostgres(ctx, q, authority)
	default:
		return false, nil
	}
}

func ExternalEffectAuthorityCurrentPostgres(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority runtimeeffects.Authority) (bool, error) {
	return externalEffectAuthorityCurrentPostgres(ctx, q, authority)
}

func externalEffectAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	if !authority.Valid() {
		return false, nil
	}
	if current, err := externalEffectRunCurrentSQLite(ctx, q, authority); err != nil || !current {
		return current, err
	}
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		fields, err := agentIdentityFields(authority.Normal.Identity)
		if err != nil {
			return false, err
		}
		var epoch, generation int64
		var phase string
		err = q.QueryRowContext(ctx, `
			SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase
			FROM agents
			WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=?
			  AND agent_route_presence=? AND flow_scope_key=?
			  AND flow_instance_id=? AND flow_instance=? AND run_id=?
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, fields.RunID).Scan(&epoch, &generation, &phase)
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
	case runtimeeffects.AuthorityServeRegistration:
		return serveRegistrationAuthorityCurrentSQLite(ctx, q, authority)
	case runtimeeffects.AuthorityChannelConfirmation:
		return channelConfirmationAuthorityCurrentSQLite(ctx, q, authority)
	default:
		return false, nil
	}
}

func ExternalEffectAuthorityCurrentSQLite(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority runtimeeffects.Authority) (bool, error) {
	return externalEffectAuthorityCurrentSQLite(ctx, q, authority)
}

func requireExternalEffectAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	if err := requireExternalEffectRunActivePostgres(ctx, tx, authority); err != nil {
		return err
	}
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat && authorize {
		return claimOrValidateForkChatAuthorityPostgres(ctx, tx, authority)
	}
	return requireCurrentExternalEffectAuthorityPostgres(ctx, tx, authority)
}

func (s *EffectPostgresOwner) RequireExternalEffectAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	return requireExternalEffectAuthorityPostgres(ctx, tx, authority, authorize)
}

func requireExternalEffectAuthoritySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	if err := requireExternalEffectRunActiveSQLite(ctx, tx, authority); err != nil {
		return err
	}
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat && authorize {
		return claimOrValidateForkChatAuthoritySQLite(ctx, tx, authority)
	}
	return requireCurrentExternalEffectAuthoritySQLite(ctx, tx, authority)
}

func (s *EffectSQLiteOwner) RequireExternalEffectAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, authorize bool) error {
	return requireExternalEffectAuthoritySQLite(ctx, tx, authority, authorize)
}

func externalEffectRunID(ctx context.Context, authority runtimeeffects.Authority) (string, bool, error) {
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat || authority.Kind == runtimeeffects.AuthorityStartupProbe || authority.Kind == runtimeeffects.AuthorityServeRegistration || authority.Kind == runtimeeffects.AuthorityChannelConfirmation {
		return "", false, nil
	}
	runID := strings.TrimSpace(authority.SelectedFork.ForkRunID)
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		runID = strings.TrimSpace(authority.Target.RunID)
		if runID == "" {
			var ok bool
			var err error
			runID, ok, err = runtimecurrentstate.RunIDFromContext(ctx)
			if err != nil {
				return "", false, err
			}
			if !ok {
				return "", false, nil
			}
		}
	}
	return runID, true, nil
}

func requireExternalEffectRunActivePostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	runID, required, err := externalEffectRunID(ctx, authority)
	if err != nil || !required {
		return err
	}
	return requirePostgresRunActive(ctx, tx, runID)
}

func requireExternalEffectRunActiveSQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	runID, required, err := externalEffectRunID(ctx, authority)
	if err != nil || !required {
		return err
	}
	return requireSQLiteRunActive(ctx, tx, runID)
}

func externalEffectRunCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	runID, required, err := externalEffectRunID(ctx, authority)
	if err != nil {
		return false, err
	}
	if !required {
		return true, nil
	}
	err = requirePostgresRunActiveQuery(ctx, q, runID)
	if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) || errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
		return false, nil
	}
	return err == nil, err
}

func externalEffectRunCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	runID, required, err := externalEffectRunID(ctx, authority)
	if err != nil {
		return false, err
	}
	if !required {
		return true, nil
	}
	err = requireSQLiteRunActiveQuery(ctx, q, runID)
	if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) || errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
		return false, nil
	}
	return err == nil, err
}

func requireCurrentExternalEffectAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	var res sql.Result
	var err error
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		fields, fieldsErr := agentIdentityFields(authority.Normal.Identity)
		if fieldsErr != nil {
			return fieldsErr
		}
		res, err = tx.ExecContext(ctx, `
			UPDATE agents SET lifecycle_generation=lifecycle_generation
			WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3
			  AND agent_route_presence=$4 AND flow_scope_key=$5
			  AND flow_instance_id=$6 AND flow_instance=$7
			  AND lifecycle_runtime_epoch=$8 AND lifecycle_generation=$9 AND run_id=$10::uuid AND lifecycle_phase='running'
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			authority.Normal.RuntimeEpoch, authority.Normal.Generation, fields.RunID)
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
			  AND request_occurrence_id=$4::uuid AND request_hash=$5 AND bundle_hash=$6 AND state='executing'
			  AND execution_owner=$7 AND fence_generation=$8 AND lease_expires_at>CURRENT_TIMESTAMP
		`, forkchat.ForkTurnID, forkchat.ForkID, forkchat.ActorTokenID, forkchat.RequestOccurrenceID,
			forkchat.RequestHash, forkchat.BundleHash, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityStartupProbe:
		startup := authority.StartupProbe
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_generation_grants SET created_at=created_at
			WHERE grant_id=$1::uuid AND state_version=$2 AND state IN ('prepared','probe_settled')
			  AND process_owner_id=$3 AND runtime_generation=$4 AND NOT EXISTS (
			    SELECT 1 FROM runtime_generation_grants newer
			    WHERE newer.grant_id=runtime_generation_grants.grant_id
			      AND newer.state_version>runtime_generation_grants.state_version
			  )
		`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityServeRegistration:
		startup := authority.ServeRegistration
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_generation_grants SET created_at=created_at
			WHERE grant_id=$1::uuid AND state_version=$2 AND state='admitted'
			  AND process_owner_id=$3 AND runtime_generation=$4 AND NOT EXISTS (
			    SELECT 1 FROM runtime_generation_grants newer
			    WHERE newer.grant_id=runtime_generation_grants.grant_id
			      AND newer.state_version>runtime_generation_grants.state_version
			  )
			`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityChannelConfirmation:
		confirmation := authority.ChannelConfirmation
		res, err = tx.ExecContext(ctx, `
			UPDATE connected_channel_activations activation SET updated_at=activation.updated_at
			FROM channel_onboarding_operations operation
			WHERE activation.activation_id=$1::uuid AND activation.operation_id=operation.operation_id
			  AND activation.status='current' AND activation.activation_revision=$2
			  AND activation.binding_revision=$3 AND activation.principal_id=$4::uuid
			  AND activation.bundle_hash=$5 AND activation.runtime_instance_id=$6::uuid
			  AND activation.context_publication_generation=$7 AND activation.plan_generation=$8
			  AND operation.operation_id=$9::uuid AND operation.operation_revision=$10
			  AND operation.phase='delivering_confirmation'
			  AND operation.confirmation_operation_id=$11::uuid
			  AND operation.activation_revision=activation.activation_revision
			  AND operation.binding_revision=activation.binding_revision
			  AND operation.runtime_instance_id=activation.runtime_instance_id
		`, confirmation.ActivationID, confirmation.ActivationRevision, confirmation.BindingRevision,
			confirmation.PrincipalID, confirmation.BundleHash, confirmation.RuntimeInstanceID, confirmation.ContextPublicationGeneration,
			confirmation.PlanGeneration.Diagnostic(), confirmation.OnboardingOperationID, confirmation.OnboardingRevision,
			confirmation.EffectOperationID)
	default:
		return invalidExternalAuthority(authority, "unsupported_kind")
	}
	return requireCurrentExternalEffectAuthorityMutation(res, err, authority)
}

func (s *EffectPostgresOwner) RequireCurrentExternalEffectAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	return requireCurrentExternalEffectAuthorityPostgres(ctx, tx, authority)
}

func requireCurrentExternalEffectAuthoritySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	if !authority.Valid() {
		return invalidExternalAuthority(authority, "invalid")
	}
	var res sql.Result
	var err error
	switch authority.Kind {
	case runtimeeffects.AuthorityNormalAgent:
		fields, fieldsErr := agentIdentityFields(authority.Normal.Identity)
		if fieldsErr != nil {
			return fieldsErr
		}
		res, err = tx.ExecContext(ctx, `
			UPDATE agents SET lifecycle_generation=lifecycle_generation
			WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=?
			  AND agent_route_presence=? AND flow_scope_key=?
			  AND flow_instance_id=? AND flow_instance=?
			  AND lifecycle_runtime_epoch=? AND lifecycle_generation=? AND run_id=? AND lifecycle_phase='running'
		`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
			fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			authority.Normal.RuntimeEpoch, authority.Normal.Generation, fields.RunID)
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
			  AND request_occurrence_id=? AND request_hash=? AND bundle_hash=? AND state='executing'
			  AND execution_owner=? AND fence_generation=? AND `+sqliteCurrentLeaseSQL+`
		`, forkchat.ForkTurnID, forkchat.ForkID, forkchat.ActorTokenID, forkchat.RequestOccurrenceID,
			forkchat.RequestHash, forkchat.BundleHash, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityStartupProbe:
		startup := authority.StartupProbe
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_generation_grants SET created_at=created_at
			WHERE grant_id=? AND state_version=? AND state IN ('prepared','probe_settled')
			  AND process_owner_id=? AND runtime_generation=? AND NOT EXISTS (
			    SELECT 1 FROM runtime_generation_grants newer
			    WHERE newer.grant_id=runtime_generation_grants.grant_id
			      AND newer.state_version>runtime_generation_grants.state_version
			  )
		`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityServeRegistration:
		startup := authority.ServeRegistration
		res, err = tx.ExecContext(ctx, `
			UPDATE runtime_generation_grants SET created_at=created_at
			WHERE grant_id=? AND state_version=? AND state='admitted'
			  AND process_owner_id=? AND runtime_generation=? AND NOT EXISTS (
			    SELECT 1 FROM runtime_generation_grants newer
			    WHERE newer.grant_id=runtime_generation_grants.grant_id
			      AND newer.state_version>runtime_generation_grants.state_version
			  )
			`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration)
	case runtimeeffects.AuthorityChannelConfirmation:
		confirmation := authority.ChannelConfirmation
		res, err = tx.ExecContext(ctx, `
			UPDATE connected_channel_activations SET updated_at=updated_at
			WHERE activation_id=? AND status='current' AND activation_revision=?
			  AND binding_revision=? AND principal_id=? AND bundle_hash=?
			  AND runtime_instance_id=? AND context_publication_generation=? AND plan_generation=?
			  AND operation_id=? AND EXISTS (
			    SELECT 1 FROM channel_onboarding_operations operation
			    WHERE operation.operation_id=connected_channel_activations.operation_id
			      AND operation.operation_revision=? AND operation.phase='delivering_confirmation'
			      AND operation.confirmation_operation_id=?
			      AND operation.activation_revision=connected_channel_activations.activation_revision
			      AND operation.binding_revision=connected_channel_activations.binding_revision
			      AND operation.runtime_instance_id=connected_channel_activations.runtime_instance_id
			  )
		`, confirmation.ActivationID, confirmation.ActivationRevision, confirmation.BindingRevision,
			confirmation.PrincipalID, confirmation.BundleHash, confirmation.RuntimeInstanceID, confirmation.ContextPublicationGeneration,
			confirmation.PlanGeneration.Diagnostic(), confirmation.OnboardingOperationID, confirmation.OnboardingRevision,
			confirmation.EffectOperationID)
	default:
		return invalidExternalAuthority(authority, "unsupported_kind")
	}
	return requireCurrentExternalEffectAuthorityMutation(res, err, authority)
}

func (s *EffectSQLiteOwner) RequireCurrentExternalEffectAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	return requireCurrentExternalEffectAuthoritySQLite(ctx, tx, authority)
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

func (s *EffectPostgresOwner) RequireCompletionAuthorityNoLiveAttemptsTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	return requireCompletionAuthorityNoLiveAttempts(ctx, tx, false, authority)
}

func (s *EffectSQLiteOwner) RequireCompletionAuthorityNoLiveAttemptsTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	return requireCompletionAuthorityNoLiveAttempts(ctx, tx, true, authority)
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
	case runtimeeffects.AuthorityServeRegistration:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthorityChannelConfirmation:
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
	case runtimeeffects.AuthorityServeRegistration:
		return authority.LeaseExpiresAt.UTC(), nil
	case runtimeeffects.AuthorityChannelConfirmation:
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
		SELECT COUNT(*) FROM runtime_generation_grants f
		WHERE f.grant_id=$1::uuid AND f.state_version=$2 AND f.state IN ('prepared','probe_settled') AND f.process_owner_id=$3 AND f.runtime_generation=$4
		  AND NOT EXISTS (SELECT 1 FROM runtime_generation_grants newer WHERE newer.grant_id=f.grant_id AND newer.state_version>f.state_version)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

func startupProbeAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	startup := authority.StartupProbe
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runtime_generation_grants f
		WHERE f.grant_id=? AND f.state_version=? AND f.state IN ('prepared','probe_settled') AND f.process_owner_id=? AND f.runtime_generation=?
		  AND NOT EXISTS (SELECT 1 FROM runtime_generation_grants newer WHERE newer.grant_id=f.grant_id AND newer.state_version>f.state_version)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

func serveRegistrationAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	startup := authority.ServeRegistration
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runtime_generation_grants f
		WHERE f.grant_id=$1::uuid AND f.state_version=$2 AND f.state='admitted'
		  AND f.process_owner_id=$3 AND f.runtime_generation=$4
		  AND NOT EXISTS (SELECT 1 FROM runtime_generation_grants newer WHERE newer.grant_id=f.grant_id AND newer.state_version>f.state_version)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

func serveRegistrationAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	startup := authority.ServeRegistration
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM runtime_generation_grants f
		WHERE f.grant_id=? AND f.state_version=? AND f.state='admitted'
		  AND f.process_owner_id=? AND f.runtime_generation=?
		  AND NOT EXISTS (SELECT 1 FROM runtime_generation_grants newer WHERE newer.grant_id=f.grant_id AND newer.state_version>f.state_version)
	`, startup.StartupAuthorityID, startup.StartupStateVersion, authority.ExecutionOwner, authority.FenceGeneration).Scan(&count)
	return count == 1, err
}

func channelConfirmationAuthorityCurrentPostgres(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	confirmation := authority.ChannelConfirmation
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM connected_channel_activations activation
		JOIN channel_onboarding_operations operation ON operation.operation_id=activation.operation_id
		WHERE activation.activation_id=$1::uuid AND activation.status='current'
		  AND activation.activation_revision=$2 AND activation.binding_revision=$3
		  AND activation.principal_id=$4::uuid AND activation.bundle_hash=$5
		  AND activation.runtime_instance_id=$6::uuid
		  AND activation.context_publication_generation=$7 AND activation.plan_generation=$8
		  AND operation.operation_id=$9::uuid AND operation.operation_revision=$10
		  AND operation.phase='delivering_confirmation'
		  AND operation.confirmation_operation_id=$11::uuid
		  AND operation.activation_revision=activation.activation_revision
		  AND operation.binding_revision=activation.binding_revision
		  AND operation.runtime_instance_id=activation.runtime_instance_id
	`, confirmation.ActivationID, confirmation.ActivationRevision, confirmation.BindingRevision,
		confirmation.PrincipalID, confirmation.BundleHash, confirmation.RuntimeInstanceID, confirmation.ContextPublicationGeneration,
		confirmation.PlanGeneration.Diagnostic(), confirmation.OnboardingOperationID, confirmation.OnboardingRevision,
		confirmation.EffectOperationID).Scan(&count)
	return count == 1, err
}

func channelConfirmationAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	confirmation := authority.ChannelConfirmation
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM connected_channel_activations activation
		JOIN channel_onboarding_operations operation ON operation.operation_id=activation.operation_id
		WHERE activation.activation_id=? AND activation.status='current'
		  AND activation.activation_revision=? AND activation.binding_revision=?
		  AND activation.principal_id=? AND activation.bundle_hash=?
		  AND activation.runtime_instance_id=?
		  AND activation.context_publication_generation=? AND activation.plan_generation=?
		  AND operation.operation_id=? AND operation.operation_revision=?
		  AND operation.phase='delivering_confirmation'
		  AND operation.confirmation_operation_id=?
		  AND operation.activation_revision=activation.activation_revision
		  AND operation.binding_revision=activation.binding_revision
		  AND operation.runtime_instance_id=activation.runtime_instance_id
	`, confirmation.ActivationID, confirmation.ActivationRevision, confirmation.BindingRevision,
		confirmation.PrincipalID, confirmation.BundleHash, confirmation.RuntimeInstanceID, confirmation.ContextPublicationGeneration,
		confirmation.PlanGeneration.Diagnostic(), confirmation.OnboardingOperationID, confirmation.OnboardingRevision,
		confirmation.EffectOperationID).Scan(&count)
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
	var forkID, sourceRunID, bundleHash, actor, occurrence, hash, state, owner string
	var leaseCurrent bool
	var fence uint64
	err := q.QueryRowContext(ctx, `
		SELECT t.fork_id::text, f.source_run_id::text, t.bundle_hash, t.actor_token_id,
		       t.request_occurrence_id::text, t.request_hash, t.state,
		       COALESCE(t.execution_owner,''), t.lease_expires_at > CURRENT_TIMESTAMP, t.fence_generation
		FROM conversation_fork_turns t
		JOIN conversation_forks f ON f.fork_id=t.fork_id
		WHERE t.fork_turn_id=$1::uuid
	`, authority.ForkChat.ForkTurnID).Scan(&forkID, &sourceRunID, &bundleHash, &actor, &occurrence, &hash, &state, &owner, &leaseCurrent, &fence)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return forkChatAuthorityMatches(authority, forkID, sourceRunID, bundleHash, actor, occurrence, hash, state, owner, leaseCurrent, fence), nil
}

func forkChatAuthorityCurrentSQLite(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority) (bool, error) {
	var forkID, sourceRunID, bundleHash, actor, occurrence, hash, state, owner string
	var leaseCurrent bool
	var fence uint64
	err := q.QueryRowContext(ctx, `
		SELECT t.fork_id, f.source_run_id, t.bundle_hash, t.actor_token_id,
		       t.request_occurrence_id, t.request_hash, t.state,
		       COALESCE(t.execution_owner,''), `+sqliteCurrentLeaseSQL+`, t.fence_generation
		FROM conversation_fork_turns t
		JOIN conversation_forks f ON f.fork_id=t.fork_id
		WHERE t.fork_turn_id=?
	`, authority.ForkChat.ForkTurnID).Scan(&forkID, &sourceRunID, &bundleHash, &actor, &occurrence, &hash, &state, &owner, &leaseCurrent, &fence)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return forkChatAuthorityMatches(authority, forkID, sourceRunID, bundleHash, actor, occurrence, hash, state, owner, leaseCurrent, fence), nil
}

func forkChatAuthorityMatches(authority runtimeeffects.Authority, forkID, sourceRunID, bundleHash, actor, occurrence, hash, state, owner string, leaseCurrent bool, fence uint64) bool {
	forkchat := authority.ForkChat
	return forkID == forkchat.ForkID && sourceRunID == forkchat.SourceRunID && bundleHash == forkchat.BundleHash && actor == forkchat.ActorTokenID && occurrence == forkchat.RequestOccurrenceID && hash == forkchat.RequestHash &&
		state == "executing" && owner == authority.ExecutionOwner && fence == authority.FenceGeneration && leaseCurrent
}

func claimOrValidateForkChatAuthorityPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	now := time.Now().UTC()
	expires := now.Add(conversationForkChatExecutionLease)
	res, err := tx.ExecContext(ctx, `
		UPDATE conversation_fork_turns
		SET state='executing', lease_expires_at=GREATEST(lease_expires_at,$4), updated_at=$5
		WHERE fork_turn_id=$1::uuid AND fork_id=$6::uuid AND actor_token_id=$7
		  AND request_occurrence_id=$8::uuid AND request_hash=$9 AND bundle_hash=$10 AND state IN ('prepared','executing')
		  AND execution_owner=$2 AND fence_generation=$3 AND lease_expires_at>CURRENT_TIMESTAMP
		  AND EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id=conversation_fork_turns.fork_id AND f.source_run_id=$11::uuid)
	`, authority.ForkChat.ForkTurnID, authority.ExecutionOwner, authority.FenceGeneration, expires, now,
		authority.ForkChat.ForkID, authority.ForkChat.ActorTokenID, authority.ForkChat.RequestOccurrenceID, authority.ForkChat.RequestHash,
		authority.ForkChat.BundleHash, authority.ForkChat.SourceRunID)
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
		  AND request_occurrence_id=? AND request_hash=? AND bundle_hash=? AND state IN ('prepared','executing')
		  AND execution_owner=? AND fence_generation=? AND `+sqliteCurrentLeaseSQL+`
		  AND EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id=conversation_fork_turns.fork_id AND f.source_run_id=?)
	`, expires, expires, now, authority.ForkChat.ForkTurnID, authority.ForkChat.ForkID, authority.ForkChat.ActorTokenID,
		authority.ForkChat.RequestOccurrenceID, authority.ForkChat.RequestHash, authority.ForkChat.BundleHash, authority.ExecutionOwner,
		authority.FenceGeneration, authority.ForkChat.SourceRunID)
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
