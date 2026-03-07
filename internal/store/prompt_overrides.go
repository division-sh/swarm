package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimemanager "empireai/internal/runtime/manager"
)

func (s *PostgresStore) GetPromptOverride(ctx context.Context, agentID string) (runtimemanager.PromptOverrideRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return runtimemanager.PromptOverrideRecord{}, false, fmt.Errorf("agent_id is required")
	}
	var rec runtimemanager.PromptOverrideRecord
	rec.AgentID = agentID
	err := s.DB.QueryRowContext(ctx, `
		SELECT
			prompt,
			COALESCE(previous_prompt, ''),
			COALESCE(source, ''),
			COALESCE(notes, ''),
			COALESCE(created_at, now()),
			COALESCE(updated_at, now())
		FROM prompt_overrides
		WHERE agent_id = $1
	`, agentID).Scan(
		&rec.Prompt,
		&rec.PreviousPrompt,
		&rec.Source,
		&rec.Notes,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return runtimemanager.PromptOverrideRecord{}, false, nil
		}
		return runtimemanager.PromptOverrideRecord{}, false, fmt.Errorf("get prompt override: %w", err)
	}
	return rec, true, nil
}

func (s *PostgresStore) UpsertPromptOverride(ctx context.Context, rec runtimemanager.PromptOverrideRecord) error {
	rec.AgentID = strings.TrimSpace(rec.AgentID)
	rec.Prompt = strings.TrimSpace(rec.Prompt)
	rec.PreviousPrompt = strings.TrimSpace(rec.PreviousPrompt)
	rec.Source = strings.TrimSpace(rec.Source)
	rec.Notes = strings.TrimSpace(rec.Notes)
	if rec.AgentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	if rec.Prompt == "" {
		return fmt.Errorf("prompt is required")
	}
	if rec.Source == "" {
		rec.Source = "api"
	}
	if _, err := s.DB.ExecContext(ctx, `
		INSERT INTO prompt_overrides (
			agent_id, prompt, previous_prompt, source, notes, created_at, updated_at
		) VALUES ($1, $2, NULLIF($3,''), $4, NULLIF($5,''), now(), now())
		ON CONFLICT (agent_id) DO UPDATE SET
			prompt = EXCLUDED.prompt,
			previous_prompt = EXCLUDED.previous_prompt,
			source = EXCLUDED.source,
			notes = EXCLUDED.notes,
			updated_at = now()
	`, rec.AgentID, rec.Prompt, rec.PreviousPrompt, rec.Source, rec.Notes); err != nil {
		return fmt.Errorf("upsert prompt override: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeletePromptOverride(ctx context.Context, agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM prompt_overrides WHERE agent_id = $1`, agentID); err != nil {
		return fmt.Errorf("delete prompt override: %w", err)
	}
	return nil
}
