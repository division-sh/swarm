package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

func (s *PostgresStore) LoadComputeModuleReplayEvidenceForExecution(ctx context.Context, runID, eventID, nodeID string) ([]computemodule.ReplayEnvelope, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	runID, eventID, nodeID, err := normalizeComputeModuleReplayEvidenceScope(runID, eventID, nodeID)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->'details'->>'action' = $2
		  AND payload->'details'->>'event_id' = $3
		  AND payload->'details'->>'node_id' = $4
		ORDER BY created_at, event_id
	`, runID, computemodule.ReplayEvidenceAction, eventID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load postgres compute_module replay evidence: %w", err)
	}
	defer rows.Close()
	return scanComputeModuleReplayEvidenceRows(rows)
}

func (s *SQLiteRuntimeStore) LoadComputeModuleReplayEvidenceForExecution(ctx context.Context, runID, eventID, nodeID string) ([]computemodule.ReplayEnvelope, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required")
	}
	runID, eventID, nodeID, err := normalizeComputeModuleReplayEvidenceScope(runID, eventID, nodeID)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = ?
		  AND json_extract(payload, '$.details.action') = ?
		  AND json_extract(payload, '$.details.event_id') = ?
		  AND json_extract(payload, '$.details.node_id') = ?
		ORDER BY created_at, event_id
	`, runID, computemodule.ReplayEvidenceAction, eventID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load sqlite compute_module replay evidence: %w", err)
	}
	defer rows.Close()
	return scanComputeModuleReplayEvidenceRows(rows)
}

func normalizeComputeModuleReplayEvidenceScope(runID, eventID, nodeID string) (string, string, string, error) {
	runID = strings.TrimSpace(runID)
	eventID = strings.TrimSpace(eventID)
	nodeID = strings.TrimSpace(nodeID)
	if runID == "" {
		return "", "", "", fmt.Errorf("run id is required")
	}
	if eventID == "" {
		return "", "", "", fmt.Errorf("event id is required")
	}
	if nodeID == "" {
		return "", "", "", fmt.Errorf("node id is required")
	}
	return runID, eventID, nodeID, nil
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
