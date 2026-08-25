package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storefailurecodec "github.com/division-sh/swarm/internal/store/internal/failurecodec"
)

func (s *AgentPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, effects *privaterunforkrevision.Effects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizePostgres(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *AgentSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, fn)
}

func (s *AgentSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, effects *privaterunforkrevision.Effects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
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
