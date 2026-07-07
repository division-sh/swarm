package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

func (s *PostgresStore) LoadComputeModuleReplayEvidence(ctx context.Context, runID string) ([]computemodule.ReplayEnvelope, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->'details'->>'action' = $2
		ORDER BY created_at, event_id
	`, runID, computemodule.ReplayEvidenceAction)
	if err != nil {
		return nil, fmt.Errorf("load postgres compute_module replay evidence: %w", err)
	}
	defer rows.Close()
	return scanComputeModuleReplayEvidenceRows(rows)
}

func (s *SQLiteRuntimeStore) LoadComputeModuleReplayEvidence(ctx context.Context, runID string) ([]computemodule.ReplayEnvelope, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = ?
		  AND json_extract(payload, '$.details.action') = ?
		ORDER BY created_at, event_id
	`, runID, computemodule.ReplayEvidenceAction)
	if err != nil {
		return nil, fmt.Errorf("load sqlite compute_module replay evidence: %w", err)
	}
	defer rows.Close()
	return scanComputeModuleReplayEvidenceRows(rows)
}

type replayEvidenceRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanComputeModuleReplayEvidenceRows(rows replayEvidenceRows) ([]computemodule.ReplayEnvelope, error) {
	var out []computemodule.ReplayEnvelope
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan compute_module replay evidence: %w", err)
		}
		envelopes, err := decodeRuntimeLogReplayEvidencePayload(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, envelopes...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read compute_module replay evidence: %w", err)
	}
	return out, nil
}

func decodeRuntimeLogReplayEvidencePayload(raw []byte) ([]computemodule.ReplayEnvelope, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode runtime log payload: %w", err)
	}
	detail, _ := payload["details"].(map[string]any)
	if len(detail) == 0 {
		return nil, nil
	}
	envelopes, err := computemodule.DecodeReplayEvidenceDetail(detail)
	if err != nil {
		return nil, err
	}
	return envelopes, nil
}
