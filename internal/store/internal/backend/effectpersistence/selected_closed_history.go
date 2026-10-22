package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
)

// This is evidence-only validation under the caller's retained cleanup fence.
// Closure and a zero live-attempt count alone cannot certify the retained facts.
// Reuse the native evidence decoders; never construct an executable Authority.
func RequireClosedSelectedEffectHistoryTx(ctx context.Context, tx *sql.Tx, execution runfork.SelectedContractRuntimeExecution, bundleHash string, sqlite bool) error {
	if ctx == nil || tx == nil || execution.State != "closed" || execution.ExecutionID == "" || execution.ForkRunID == "" || bundleHash == "" {
		return errors.New("selected effect history requires its exact closed generation and retained transaction")
	}
	if err := RequireSelectedHistoryNoLiveAttemptsTx(ctx, tx, execution.ExecutionID, sqlite); err != nil {
		return err
	}
	if err := requireClosedSelectedChildCoverage(ctx, tx, execution.ForkRunID, sqlite); err != nil {
		return err
	}
	if err := requireClosedSelectedOperations(ctx, tx, execution, bundleHash); err != nil {
		return err
	}
	if err := requireClosedSelectedAttempts(ctx, tx, execution); err != nil {
		return err
	}
	if err := requireClosedSelectedSurfaces(ctx, tx, execution); err != nil {
		return err
	}
	return requireClosedSelectedTurnOrigins(ctx, tx, execution.ForkRunID)
}

func requireClosedSelectedChildCoverage(ctx context.Context, tx *sql.Tx, child string, sqlite bool) error {
	lineageRun, surfaceRun := `o.lineage->>'run_id'`, `s.surface->'authority'->>'run_id'`
	if sqlite {
		lineageRun, surfaceRun = `json_extract(o.lineage,'$.run_id')`, `json_extract(s.surface,'$.authority.run_id')`
	}
	for _, check := range []struct{ name, query string }{
		{"operation projection", `SELECT COUNT(*) FROM runtime_external_effect_operations o
			WHERE (o.agent_run_id=$1 OR ` + lineageRun + `=CAST($1 AS TEXT)) AND NOT EXISTS (
			 SELECT 1 FROM run_fork_selected_contract_runtime_executions e WHERE e.execution_id=o.selected_execution_id
			 AND e.fork_run_id=$1 AND e.state='closed' AND o.authority_kind='selected_contract_fork'
			 AND o.authority_id=CAST(e.execution_id AS TEXT) AND o.generation=e.generation)`},
		{"grant projection", `SELECT COUNT(*) FROM runtime_generation_grants g WHERE g.selected_fork_run_id=$1 AND NOT EXISTS (
			 SELECT 1 FROM run_fork_selected_contract_runtime_executions e WHERE e.execution_id=g.selected_execution_id
			 AND e.fork_run_id=$1 AND e.binding_id=g.selected_binding_id AND e.generation=g.runtime_generation AND e.state='closed')`},
		{"surface projection", `SELECT COUNT(*) FROM managed_agent_capability_surfaces s
			WHERE (s.selected_fork_run_id=$1 OR ` + surfaceRun + `=CAST($1 AS TEXT)) AND NOT EXISTS (
			 SELECT 1 FROM run_fork_selected_contract_runtime_executions e WHERE e.execution_id=s.selected_execution_id
			 AND e.fork_run_id=$1 AND e.state='closed' AND s.execution_kind='selected_contract_fork'
			 AND s.execution_authority_id=CAST(e.execution_id AS TEXT))`},
	} {
		var count int64
		if err := tx.QueryRowContext(ctx, check.query, child).Scan(&count); err != nil {
			return fmt.Errorf("read selected history %s: %w", check.name, err)
		}
		if count != 0 {
			return fmt.Errorf("selected history %s crosses or omits the closed execution cohort", check.name)
		}
	}
	return ctx.Err()
}

func requireClosedSelectedOperations(ctx context.Context, tx *sql.Tx, execution runfork.SelectedContractRuntimeExecution, bundleHash string) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(o.operation_id AS TEXT),o.effect_kind,o.authority_kind,o.authority_id,
		CAST(o.selected_execution_id AS TEXT),o.generation,o.bundle_hash,o.execution_mode,o.state,o.authority_evidence,o.lineage,
		o.created_at,o.updated_at,o.completed_at,
		(SELECT COUNT(*) FROM runtime_external_effect_attempts a WHERE a.operation_id=o.operation_id)
		FROM runtime_external_effect_operations o WHERE o.selected_execution_id=$1
		OR (o.authority_kind='selected_contract_fork' AND o.authority_id=CAST($1 AS TEXT)) ORDER BY o.operation_id`, execution.ExecutionID)
	if err != nil {
		return fmt.Errorf("read closed selected operations: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var id, kind, authorityKind, authorityID, selectedID, bundle, mode, state string
		var generation uint64
		var evidence, lineage []byte
		var created, updated, completed any
		var attempts int64
		if err := rows.Scan(&id, &kind, &authorityKind, &authorityID, &selectedID, &generation, &bundle, &mode, &state, &evidence, &lineage, &created, &updated, &completed, &attempts); err != nil {
			return err
		}
		if id == "" || authorityKind != string(effects.AuthoritySelectedContractFork) || authorityID != execution.ExecutionID ||
			selectedID != execution.ExecutionID || generation != execution.Generation || bundle != bundleHash || attempts == 0 || !closedSelectedEffectState(state) {
			return fmt.Errorf("selected operation %s has incomplete or crossed terminal evidence", id)
		}
		if _, ok := executionmode.Parse(mode); !ok {
			return fmt.Errorf("selected operation %s has invalid execution mode", id)
		}
		if _, err := readClosedSelectedAuthority(evidence, execution, mode, kind == string(effects.KindProviderTurn)); err != nil {
			return fmt.Errorf("selected operation %s: %w", id, err)
		}
		var origin map[string]string
		if err := canonicaljson.DecodeInto(lineage, &origin); err != nil {
			return err
		}
		if origin["run_id"] != execution.ForkRunID {
			return fmt.Errorf("selected operation %s has foreign run lineage", id)
		}
		if err := validateClosedSelectedEffectClocks(created, updated, completed); err != nil {
			return fmt.Errorf("selected operation %s: %w", id, err)
		}
	}
	return rows.Err()
}

func requireClosedSelectedAttempts(ctx context.Context, tx *sql.Tx, execution runfork.SelectedContractRuntimeExecution) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(a.attempt_id AS TEXT),CAST(a.operation_id AS TEXT),o.effect_kind,o.execution_mode,
		a.attempt_ordinal,a.execution_mode,a.generation,a.execution_owner,a.fence_generation,a.authority_evidence,
		a.state,COALESCE(a.failure,'null'),a.authorized_at,a.updated_at,a.completed_at,a.completion_continuation_active
		FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE o.selected_execution_id=$1 ORDER BY a.operation_id,a.attempt_ordinal`, execution.ExecutionID)
	if err != nil {
		return fmt.Errorf("read closed selected attempts: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	ordinals := make(map[string]int64)
	for rows.Next() {
		var id, operationID, kind, operationMode, mode, owner, state string
		var ordinal int64
		var generation, fence uint64
		var evidence, failure []byte
		var authorized, updated, completed any
		var continuation bool
		if err := rows.Scan(&id, &operationID, &kind, &operationMode, &ordinal, &mode, &generation, &owner, &fence, &evidence,
			&state, &failure, &authorized, &updated, &completed, &continuation); err != nil {
			return err
		}
		ordinals[operationID]++
		if id == "" || ordinal != ordinals[operationID] || mode != operationMode || generation != execution.Generation || owner == "" ||
			fence == 0 || fence > execution.FenceGeneration || continuation || !closedSelectedEffectState(state) {
			return fmt.Errorf("selected attempt %s has incomplete, live or crossed evidence", id)
		}
		original, err := readClosedSelectedAuthority(evidence, execution, mode, kind == string(effects.KindProviderTurn))
		if err != nil {
			return fmt.Errorf("selected attempt %s: %w", id, err)
		}
		if original.ExecutionOwner != owner || original.FenceGeneration != fence {
			return fmt.Errorf("selected attempt %s differs from its original physical authority", id)
		}
		if _, _, _, err := decodeExternalEffectStorySettlement(state, failure, completed); err != nil {
			return err
		}
		if err := validateClosedSelectedEffectClocks(authorized, updated, completed); err != nil {
			return fmt.Errorf("selected attempt %s: %w", id, err)
		}
	}
	return rows.Err()
}

func readClosedSelectedAuthority(raw []byte, execution runfork.SelectedContractRuntimeExecution, mode string, provider bool) (completionRecoveryAuthorityEvidence, error) {
	var absent completionRecoveryAuthorityEvidence
	if _, err := canonicaljson.Canonicalize(raw); err != nil {
		return absent, err
	}
	var evidence completionRecoveryAuthorityEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		return absent, err
	}
	for _, coordinate := range []struct{ name, actual, expected string }{
		{"authority kind", evidence.AuthorityKind, string(effects.AuthoritySelectedContractFork)},
		{"authority ID", evidence.AuthorityID, execution.ExecutionID},
		{"execution", evidence.ExecutionID, execution.ExecutionID},
		{"fork run", evidence.ForkRunID, execution.ForkRunID},
		{"mode", evidence.ExecutionMode, mode},
		{"admission", evidence.AdmissionFingerprint, execution.AdmissionFingerprint},
		{"container", evidence.ContainerPlanFingerprint, execution.ContainerPlanFingerprint},
		{"actor census", evidence.ActorCensusFingerprint, execution.ActorCensusFingerprint},
		{"configuration", evidence.EffectiveConfigFingerprint, execution.EffectiveConfigFingerprint},
	} {
		if coordinate.actual != coordinate.expected {
			return absent, fmt.Errorf("selected effect %s differs from its closed generation", coordinate.name)
		}
	}
	if evidence.Generation != execution.Generation || evidence.ExecutionOwner == "" || evidence.FenceGeneration == 0 || evidence.FenceGeneration > execution.FenceGeneration {
		return absent, errors.New("selected effect owner/generation differs from its closed generation")
	}
	if provider {
		if err := evidence.ProviderRequest.Validate(); err != nil {
			return absent, err
		}
		if evidence.UsageTarget.RunID != execution.ForkRunID {
			return absent, errors.New("selected provider target has foreign run coordinates")
		}
	}
	return evidence, nil
}

func requireClosedSelectedSurfaces(ctx context.Context, tx *sql.Tx, execution runfork.SelectedContractRuntimeExecution) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(surface_id AS TEXT),integrity_hash,actor_id,provider,transport,surface,
		execution_kind,execution_authority_id,CAST(run_id AS TEXT),CAST(selected_execution_id AS TEXT),CAST(selected_fork_run_id AS TEXT)
		FROM managed_agent_capability_surfaces WHERE selected_execution_id=$1
		OR (execution_kind='selected_contract_fork' AND execution_authority_id=CAST($1 AS TEXT)) ORDER BY surface_id`, execution.ExecutionID)
	if err != nil {
		return fmt.Errorf("read closed selected capability surfaces: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var id, integrity, actor, provider, transport string
		var raw []byte
		var stored managedcapabilitystore.StoredProjection
		if err := rows.Scan(&id, &integrity, &actor, &provider, &transport, &raw, &stored.ExecutionKind, &stored.ExecutionAuthorityID,
			&stored.RunID, &stored.SelectedExecutionID, &stored.SelectedForkRunID); err != nil {
			return err
		}
		surface, err := managedcapabilitystore.DecodeStored(raw, stored)
		if err != nil {
			return err
		}
		if surface.ID != id || surface.IntegrityHash != integrity || surface.ActorID != actor || surface.Provider != provider || surface.Transport != transport ||
			surface.Authority.ExecutionAuthorityID != execution.ExecutionID || surface.Authority.RunID != execution.ForkRunID {
			return errors.New("selected capability surface differs from its original physical projection")
		}
	}
	return rows.Err()
}

func requireClosedSelectedTurnOrigins(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	checks := []struct{ name, query string }{
		{"logical turn", `SELECT COUNT(*) FROM runtime_agent_turn_lifetimes t
			LEFT JOIN event_deliveries d ON t.origin_kind='delivery' AND d.delivery_id=t.origin_id
			LEFT JOIN agent_directive_operations o ON t.origin_kind='directive' AND o.operation_id=t.origin_id
			WHERE t.run_id=$1 AND ((t.cancel_reason IS NOT NULL AND t.settled_at IS NULL)
			OR (t.origin_kind='delivery' AND (d.delivery_id IS NULL OR d.run_id<>t.run_id
			 OR d.subscriber_type<>'agent' OR d.subscriber_id<>t.agent_id OR d.status NOT IN ('delivered','dead_letter','canceled')))
			OR (t.origin_kind='directive' AND (o.operation_id IS NULL OR o.resolved_run_id<>t.run_id
			 OR o.agent_id<>t.agent_id OR o.flow_instance<>t.flow_instance OR o.state NOT IN ('executed','succeeded','failed','canceled'))))`},
		{"turn anchors", `SELECT COUNT(*) FROM runtime_agent_turn_lifetimes t
			LEFT JOIN runtime_external_effect_attempts a ON a.attempt_id=t.admitted_attempt_id
			LEFT JOIN runtime_external_effect_attempts c ON c.attempt_id=t.current_attempt_id
			LEFT JOIN runtime_external_effect_attempts f ON f.attempt_id=t.first_attempt_id
			WHERE t.run_id=$1 AND (
			(t.admitted_attempt_id IS NOT NULL AND (a.attempt_id IS NULL OR NOT EXISTS (
			 SELECT 1 FROM runtime_external_effect_operations o JOIN run_fork_selected_contract_runtime_executions e
			 ON e.execution_id=o.selected_execution_id WHERE o.operation_id=a.operation_id AND e.fork_run_id=t.run_id)))
			OR (t.current_attempt_id IS NOT NULL AND (c.attempt_id IS NULL OR NOT EXISTS (
			 SELECT 1 FROM runtime_external_effect_operations o JOIN run_fork_selected_contract_runtime_executions e
			 ON e.execution_id=o.selected_execution_id WHERE o.operation_id=c.operation_id AND e.fork_run_id=t.run_id)))
			OR (t.first_attempt_id IS NOT NULL AND (f.attempt_id IS NULL OR NOT EXISTS (
			 SELECT 1 FROM runtime_external_effect_operations o JOIN run_fork_selected_contract_runtime_executions e
			 ON e.execution_id=o.selected_execution_id WHERE o.operation_id=f.operation_id AND e.fork_run_id=t.run_id))))`},
		// Lifecycle predecessor drain grants are normal-agent-only. Their
		// presence on a selected child is contradictory even after settlement.
		{"normal-only provider drain", `SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE run_id=$1`},
		{"completion audit", `SELECT COUNT(*) FROM agent_turns t
			LEFT JOIN runtime_external_effect_attempts a ON a.attempt_id=t.completion_attempt_id
			LEFT JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			LEFT JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id=o.selected_execution_id
			LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=t.capability_surface_id
			WHERE t.run_id=$1 AND (a.attempt_id IS NULL OR o.operation_id IS NULL OR e.execution_id IS NULL OR e.fork_run_id<>t.run_id
			OR a.usage_target_kind IS NULL OR a.usage_target_kind<>'agent_turn' OR a.usage_target_id IS NULL OR a.usage_target_id<>t.turn_id
			OR a.capability_surface_id IS NULL OR a.capability_surface_id<>t.capability_surface_id
			OR s.surface_id IS NULL OR s.selected_execution_id IS NULL OR s.selected_execution_id<>e.execution_id
			OR s.selected_fork_run_id IS NULL OR s.selected_fork_run_id<>t.run_id
			OR t.execution_mode<>a.execution_mode OR t.execution_mode<>o.execution_mode
			OR o.agent_frame_bytes IS NULL OR t.agent_frame_bytes<>o.agent_frame_bytes)`},
	}
	for _, check := range checks {
		var count int64
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&count); err != nil {
			return fmt.Errorf("read closed selected %s evidence: %w", check.name, err)
		}
		if count != 0 {
			return fmt.Errorf("closed selected %s evidence is live, incomplete or crossed", check.name)
		}
	}
	return ctx.Err()
}

func closedSelectedEffectState(state string) bool {
	return state == string(effects.StateSettled) || state == string(effects.StateTerminalFailure) || state == string(effects.StateOutcomeUncertain)
}

func validateClosedSelectedEffectClocks(created, updated, completed any) error {
	start, present, err := sqliteTimeValue(created)
	if err != nil || !present || start.IsZero() {
		return errors.Join(err, errors.New("selected effect has no original admission clock"))
	}
	end, present, err := sqliteTimeValue(completed)
	if err != nil || !present || end.IsZero() || end.Before(start) {
		return errors.Join(err, errors.New("selected effect has no valid terminal clock"))
	}
	last, present, err := sqliteTimeValue(updated)
	if err != nil || !present || last.IsZero() || last.Before(end) {
		return errors.Join(err, errors.New("selected effect has no valid final update clock"))
	}
	return nil
}
