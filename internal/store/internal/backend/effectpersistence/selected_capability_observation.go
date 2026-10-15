package effectpersistence

import (
	"context"
	"database/sql"
)

type SelectedCompletionCapabilityStorage struct {
	AttemptSurfaceID, TurnSurfaceID, Surface, PlanFingerprint, TurnID string
	OperationFrame, TurnFrame                                         []byte
}

func CountSelectedProviderOperationsStorageTx(ctx context.Context, tx *sql.Tx, executionID string, generation uint64) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_operations
		WHERE effect_kind = 'provider_turn'
		  AND authority_kind = 'selected_contract_fork'
		  AND authority_id = $1
		  AND selected_execution_id = $1::uuid
		  AND generation = $2
		  AND authority_evidence->>'execution_id' = $1
		  AND (authority_evidence->>'generation')::bigint = $2
	`, executionID, generation).Scan(&count)
	return count, err
}

func ReadSelectedCompletionCapabilitiesStorageTx(ctx context.Context, tx *sql.Tx, executionID, adapter string, generation uint64, executionOwner, runID string) ([]SelectedCompletionCapabilityStorage, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT a.capability_surface_id::text, t.capability_surface_id::text, s.surface::text,
		       o.capability_plan_fingerprint, t.turn_id::text, o.agent_frame_bytes, t.agent_frame_bytes
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = $2
		  AND a.generation = $3
		  AND a.execution_owner = $4
		  AND a.state = 'settled'
		  AND t.run_id = $5::uuid
		  AND t.agent_id = 'test-agent'
		  AND t.usage_exactness = 'exact'
		ORDER BY t.created_at, t.turn_id
	`, executionID, adapter, generation, executionOwner, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelectedCompletionCapabilityStorage
	for rows.Next() {
		var row SelectedCompletionCapabilityStorage
		if err := rows.Scan(&row.AttemptSurfaceID, &row.TurnSurfaceID, &row.Surface, &row.PlanFingerprint, &row.TurnID, &row.OperationFrame, &row.TurnFrame); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type SelectedClaudeStartupCounts struct{ Surfaces, Attempts int }

func ReadSelectedClaudeStartupCountsStorageTx(ctx context.Context, tx *sql.Tx, preparationID, processAuthorityID string) (SelectedClaudeStartupCounts, error) {
	var out SelectedClaudeStartupCounts
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT s.surface_id), COUNT(DISTINCT a.attempt_id)
		FROM managed_agent_capability_surfaces s
		JOIN runtime_external_effect_attempts a ON a.capability_surface_id = s.surface_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id IS NULL
		  AND o.authority_kind = 'startup_probe'
		  AND o.startup_authority_id = $2
		  AND o.authority_id = CAST(s.authority_id AS TEXT)
		  AND a.adapter = 'claude_cli_startup_probe'
		  AND a.state = 'settled'
		  AND s.execution_kind = 'selected_fork_preparation'
		  AND s.execution_authority_id = $1
		  AND s.run_id IS NULL
		  AND s.actor_id = 'test-agent'
	`, preparationID, processAuthorityID).Scan(&out.Surfaces, &out.Attempts)
	return out, err
}

func ReadSelectedClaudeStartupSurfaceStorageTx(ctx context.Context, tx *sql.Tx, preparationID, processAuthorityID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `
		SELECT s.surface
		FROM managed_agent_capability_surfaces s
		JOIN runtime_external_effect_attempts a ON a.capability_surface_id = s.surface_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id IS NULL
		  AND o.startup_authority_id = $2
		  AND s.execution_authority_id = $1
		  AND a.adapter = 'claude_cli_startup_probe'
		  AND a.state = 'settled'
		  AND s.run_id IS NULL
		  AND s.actor_id = 'test-agent'
	`, preparationID, processAuthorityID).Scan(&out)
	return out, err
}

type SelectedClaudeTurnCapabilityStorage struct{ AttemptSurfaceID, TurnSurfaceID, Surface string }

func ReadSelectedClaudeTurnCapabilityStorageTx(ctx context.Context, tx *sql.Tx, executionID, runID string) (SelectedClaudeTurnCapabilityStorage, error) {
	var out SelectedClaudeTurnCapabilityStorage
	err := tx.QueryRowContext(ctx, `
		SELECT a.capability_surface_id, t.capability_surface_id, s.surface
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		WHERE o.selected_execution_id = $1
		  AND a.adapter = 'claude_cli'
		  AND a.state = 'settled'
		  AND t.run_id = $2
		  AND t.agent_id = 'test-agent'
	`, executionID, runID).Scan(&out.AttemptSurfaceID, &out.TurnSurfaceID, &out.Surface)
	return out, err
}

type SelectedClaudeAttemptDiagnosticStorage struct{ Adapter, State, AttemptSurface, TurnSurface, Failure string }

func ReadSelectedClaudeAttemptDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, executionID string) ([]SelectedClaudeAttemptDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT a.adapter, a.state, COALESCE(CAST(a.capability_surface_id AS TEXT),''),
		       COALESCE(CAST(t.capability_surface_id AS TEXT),''), COALESCE(CAST(t.failure AS TEXT),'')
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		LEFT JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		WHERE o.selected_execution_id = $1
		ORDER BY a.authorized_at
	`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelectedClaudeAttemptDiagnosticStorage
	for rows.Next() {
		var row SelectedClaudeAttemptDiagnosticStorage
		if err := rows.Scan(&row.Adapter, &row.State, &row.AttemptSurface, &row.TurnSurface, &row.Failure); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
