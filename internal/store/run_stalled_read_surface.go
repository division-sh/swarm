package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStore) LoadLatestRunFlowInstance(ctx context.Context, runID string) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", nil
	}
	var flowInstance string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM events
		WHERE run_id = $1::uuid
		  AND COALESCE(flow_instance, '') <> ''
		ORDER BY created_at DESC, event_id::text DESC
		LIMIT 1
	`, runID).Scan(&flowInstance)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load latest run flow instance: %w", err)
	}
	return strings.Trim(flowInstance, "/"), nil
}

func (s *PostgresStore) LoadLatestRunNonEscalationProgressAt(ctx context.Context, runID, escalationEventName string) (time.Time, error) {
	if s == nil || s.DB == nil {
		return time.Time{}, fmt.Errorf("postgres store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return time.Time{}, nil
	}
	var raw any
	if err := s.DB.QueryRowContext(ctx, `
		SELECT MAX(created_at)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name <> $2
	`, runID, strings.TrimSpace(escalationEventName)).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("load latest non-escalation progress timestamp: %w", err)
	}
	progressAt, _, err := storeTimeValue(raw)
	if err != nil {
		return time.Time{}, err
	}
	return progressAt, nil
}

func (s *SQLiteRuntimeStore) LoadLatestRunFlowInstance(ctx context.Context, runID string) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("sqlite runtime store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", nil
	}
	var flowInstance string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM events
		WHERE run_id = ?
		  AND COALESCE(flow_instance, '') <> ''
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, runID).Scan(&flowInstance)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load latest run flow instance: %w", err)
	}
	return strings.Trim(flowInstance, "/"), nil
}

func (s *SQLiteRuntimeStore) LoadLatestRunNonEscalationProgressAt(ctx context.Context, runID, escalationEventName string) (time.Time, error) {
	if s == nil || s.DB == nil {
		return time.Time{}, fmt.Errorf("sqlite runtime store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return time.Time{}, nil
	}
	var raw any
	if err := s.DB.QueryRowContext(ctx, `
		SELECT MAX(created_at)
		FROM events
		WHERE run_id = ?
		  AND event_name <> ?
	`, runID, strings.TrimSpace(escalationEventName)).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("load latest non-escalation progress timestamp: %w", err)
	}
	progressAt, _, err := storeTimeValue(raw)
	if err != nil {
		return time.Time{}, err
	}
	return progressAt, nil
}

func storeTimeValue(raw any) (time.Time, bool, error) {
	switch typed := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if typed.IsZero() {
			return time.Time{}, false, nil
		}
		return typed.UTC(), true, nil
	case string:
		return parseStoreTimeString(typed)
	case []byte:
		return parseStoreTimeString(string(typed))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported store time value %T", raw)
	}
}

func parseStoreTimeString(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, format := range formats {
		parsed, err := time.Parse(format, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse store time %q: %w", raw, lastErr)
}
