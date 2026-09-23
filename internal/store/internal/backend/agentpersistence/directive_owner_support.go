package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storefailurecodec "github.com/division-sh/swarm/internal/store/internal/failurecodec"
)

func (s *AgentPostgresOwner) runDirectiveMutation(ctx context.Context, evidence mutationprotocol.Evidence, fn func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, evidence, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error { return fn(ctx, tx, attempt) })
		return struct{}{}, err
	})
	return result.Acknowledged(), result.Err()
}

func (s *AgentSQLiteOwner) runDirectiveMutation(ctx context.Context, label string, evidence mutationprotocol.Evidence, fn func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, label, evidence, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error { return fn(ctx, tx, attempt) })
		return struct{}{}, err
	})
	return result.Acknowledged(), result.Err()
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any { return sqliteNullString(raw) }

func jsonRawMessageValue(raw any) json.RawMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), value...)
	case []byte:
		return json.RawMessage(append([]byte(nil), value...))
	case string:
		return json.RawMessage([]byte(value))
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return encoded
	}
}

var decodeStoredFailure = storefailurecodec.Decode

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseSQLiteTimeString(value)
	case []byte:
		return parseSQLiteTimeString(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseSQLiteTimeString(raw string) (time.Time, bool, error) {
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
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse SQLite time %q: %w", raw, lastErr)
}
