package mutationlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

var syntheticRunNamespace = uuid.MustParse("7e7e89e6-0d4f-4eeb-a8a0-99a3e8ec2ef1")

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Record struct {
	EntityID    string
	Field       string
	OldValue    any
	NewValue    any
	WriterType  string
	WriterID    string
	HandlerStep string
}

func Insert(ctx context.Context, db DBTX, rec Record) error {
	if db == nil {
		return nil
	}
	entityID := strings.TrimSpace(rec.EntityID)
	field := strings.TrimSpace(rec.Field)
	writerType := strings.TrimSpace(rec.WriterType)
	writerID := strings.TrimSpace(rec.WriterID)
	if entityID == "" || field == "" || writerType == "" || writerID == "" {
		return nil
	}

	runID := normalizeRunID(runtimecorrelation.RunIDFromContext(ctx))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id)
		VALUES ($1::uuid)
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		return err
	}

	oldValue, err := jsonbArg(rec.OldValue)
	if err != nil {
		return err
	}
	newValue, err := jsonbArg(rec.NewValue)
	if err != nil {
		return err
	}

	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		if parsed := validUUIDString(inbound.ID); parsed != "" {
			causedByEvent = parsed
		}
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4::jsonb, $5::jsonb,
			NULLIF($6, '')::uuid, $7, $8, NULLIF($9, '')
		)
	`, runID, entityID, field, oldValue, newValue, causedByEvent, writerType, writerID, strings.TrimSpace(rec.HandlerStep))
	return err
}

func normalizeRunID(runID string) string {
	if parsed := validUUIDString(runID); parsed != "" {
		return parsed
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return uuid.NewString()
	}
	return uuid.NewSHA1(syntheticRunNamespace, []byte(runID)).String()
}

func validUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func jsonbArg(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	case []byte:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(raw), nil
	}
}
