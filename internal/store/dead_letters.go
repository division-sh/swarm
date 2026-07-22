package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func (s *PostgresStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return s.runAuthorActivityMutation(ctx, "postgres record dead letter", func(txctx context.Context, tx *sql.Tx) error {
		return s.RecordDeadLetterTx(txctx, tx, rec)
	})
}

func (s *PostgresStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return err
	}
	if err := requireActiveRunForEvent(ctx, tx, rec.OriginalEventID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	return s.insertPostgresDeadLetterTx(ctx, tx, rec)
}

func (s *PostgresStore) recordTerminalizedDeliveryDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return err
	}
	return s.insertPostgresDeadLetterTx(ctx, tx, rec)
}

func (s *PostgresStore) insertPostgresDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	source, err := loadDeadLetterAuthorActivitySource(ctx, tx, rec.OriginalEventID, true)
	if err != nil {
		return err
	}
	result, err := runtimedeadletters.InsertTxWithResult(ctx, tx, rec)
	if err != nil {
		return err
	}
	if !result.Inserted {
		return nil
	}
	return recordDeadLetterAuthorActivity(ctx, result.DeadLetterID, rec, source, deadLetterOccurredAt(rec.Timestamp))
}

func (s *SQLiteRuntimeStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return s.RecordDeadLetterTx(ctx, nil, rec)
}

func (s *SQLiteRuntimeStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return s.runAuthorActivityMutation(ctx, "sqlite record dead letter", func(txctx context.Context, tx *sql.Tx) error {
			return s.RecordDeadLetterTx(txctx, tx, rec)
		})
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return err
	}
	if err := requireActiveRunForEvent(ctx, tx, rec.OriginalEventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	return s.insertSQLiteDeadLetterTx(ctx, tx, rec)
}

func (s *SQLiteRuntimeStore) recordTerminalizedDeliveryDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("terminalized delivery dead letter transaction is required")
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return err
	}
	return s.insertSQLiteDeadLetterTx(ctx, tx, rec)
}

func (s *SQLiteRuntimeStore) insertSQLiteDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	rec, createdAt, err := normalizeSQLiteDeadLetterRecord(s, rec)
	if err != nil {
		return err
	}
	source, err := loadDeadLetterAuthorActivitySource(ctx, tx, rec.OriginalEventID, false)
	if err != nil {
		return err
	}
	deadLetterID := uuid.NewString()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, delivery_id, claim_version, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		)
		SELECT
			?,
			?,
			NULLIF(?, ''),
			NULLIF(?, 0),
			COALESCE(NULLIF(?, ''), COALESCE((SELECT e.event_name FROM events e WHERE e.event_id = ?), '')),
			COALESCE(NULLIF(?, 'null'), COALESCE((SELECT e.payload FROM events e WHERE e.event_id = ?), '{}')),
			?,
			COALESCE(NULLIF(?, ''), COALESCE((SELECT NULLIF(e.flow_instance, '') FROM events e WHERE e.event_id = ?), 'runtime')),
			?,
			?,
			?,
			NULLIF(?, ''),
			?
		WHERE NOT EXISTS (
			SELECT 1
			FROM dead_letters dl
			WHERE (NULLIF(?, '') IS NOT NULL AND dl.delivery_id = NULLIF(?, '') AND dl.claim_version = NULLIF(?, 0))
			   OR (NULLIF(?, '') IS NULL AND dl.delivery_id IS NULL AND dl.original_event_id = ?
			       AND dl.failure = ? AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF(?, ''), ''))
		)
	`,
		deadLetterID,
		rec.OriginalEventID,
		rec.DeliveryID,
		rec.ClaimVersion,
		rec.OriginalEvent,
		rec.OriginalEventID,
		string(rec.OriginalPayload),
		rec.OriginalEventID,
		sqliteNullUUID(rec.EntityID),
		rec.FlowInstance,
		rec.OriginalEventID,
		mustFailureJSON(rec.Failure),
		rec.RetryCount,
		rec.ChainDepth,
		rec.HandlerNode,
		createdAt,
		rec.DeliveryID,
		rec.DeliveryID,
		rec.ClaimVersion,
		rec.DeliveryID,
		rec.OriginalEventID,
		mustFailureJSON(rec.Failure),
		rec.HandlerNode,
	)
	if err != nil {
		return fmt.Errorf("insert sqlite dead letter: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	return recordDeadLetterAuthorActivity(ctx, deadLetterID, rec, source, createdAt)
}

type deadLetterAuthorActivitySource struct {
	RunID      string
	EntityID   string
	FlowID     string
	BundleHash string
	EventType  string
}

func loadDeadLetterAuthorActivitySource(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) (deadLetterAuthorActivitySource, error) {
	query := `SELECT COALESCE(CAST(e.run_id AS TEXT), ''), COALESCE(CAST(e.entity_id AS TEXT), ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, ''), e.event_name FROM events e LEFT JOIN runs r ON r.run_id = e.run_id WHERE e.event_id = ?`
	if postgres {
		query = `SELECT COALESCE(e.run_id::text, ''), COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, ''), e.event_name FROM events e LEFT JOIN runs r ON r.run_id = e.run_id WHERE e.event_id = $1::uuid`
	}
	var source deadLetterAuthorActivitySource
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(eventID)).Scan(&source.RunID, &source.EntityID, &source.FlowID, &source.BundleHash, &source.EventType); err != nil {
		return deadLetterAuthorActivitySource{}, fmt.Errorf("load dead letter source event: %w", err)
	}
	return source, nil
}

func recordDeadLetterAuthorActivity(ctx context.Context, deadLetterID string, rec runtimedeadletters.Record, source deadLetterAuthorActivitySource, occurredAt time.Time) error {
	deadLetterID = strings.TrimSpace(deadLetterID)
	if deadLetterID == "" {
		return fmt.Errorf("dead letter author activity requires dead_letter_id")
	}
	currentScope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || strings.TrimSpace(currentScope.RuntimeInstanceID) == "" {
		return fmt.Errorf("dead letter author activity requires exact runtime instance scope")
	}
	occurrenceScope := currentScope
	if strings.TrimSpace(source.BundleHash) != "" {
		occurrenceScope = runtimeauthoractivity.BundleScope(currentScope.RuntimeInstanceID, source.BundleHash)
	} else if currentScope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(currentScope.BundleHash) == "" {
		return fmt.Errorf("dead letter author activity requires persisted run bundle_hash or exact bundle scope")
	}
	retry := rec.RetryCount
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDeadLetterRecorded, Transition: "recorded",
		SourceOwner: "dead_letters", SourceIdentity: deadLetterID, DedupKey: "dead-letter:" + deadLetterID,
		OccurredAt: occurredAt, RunID: source.RunID, EntityID: source.EntityID, FlowID: source.FlowID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "event", SubjectID: strings.TrimSpace(rec.OriginalEventID), EventType: source.EventType,
			RetryCount: &retry, ReasonCode: rec.Failure.Detail.Code, NodeID: strings.TrimSpace(rec.HandlerNode),
		},
		Scope: occurrenceScope, Failure: &rec.Failure,
	})
}

func deadLetterOccurredAt(raw string) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed.UTC()
	}
	return time.Now().UTC()
}

func normalizeSQLiteDeadLetterRecord(s *SQLiteRuntimeStore, rec runtimedeadletters.Record) (runtimedeadletters.Record, time.Time, error) {
	rec.OriginalEventID = strings.TrimSpace(rec.OriginalEventID)
	rec.DeliveryID = strings.TrimSpace(rec.DeliveryID)
	rec.OriginalEvent = strings.TrimSpace(rec.OriginalEvent)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.HandlerNode = strings.TrimSpace(rec.HandlerNode)
	rec.Timestamp = strings.TrimSpace(rec.Timestamp)
	if rec.OriginalEventID == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter original event id is required")
	}
	if _, err := uuid.Parse(rec.OriginalEventID); err != nil {
		return rec, time.Time{}, fmt.Errorf("dead letter original event id must be a uuid: %w", err)
	}
	if (rec.DeliveryID == "") != (rec.ClaimVersion == 0) {
		return rec, time.Time{}, fmt.Errorf("dead letter delivery id and claim version must be supplied together")
	}
	if rec.DeliveryID != "" {
		if _, err := uuid.Parse(rec.DeliveryID); err != nil {
			return rec, time.Time{}, fmt.Errorf("dead letter delivery id must be a uuid: %w", err)
		}
		if rec.ClaimVersion <= 0 {
			return rec, time.Time{}, fmt.Errorf("dead letter claim version must be positive")
		}
	}
	if rec.EntityID != "" {
		if _, err := uuid.Parse(rec.EntityID); err != nil {
			rec.EntityID = ""
		}
	}
	if err := runtimefailures.ValidateEnvelope(rec.Failure); err != nil {
		return rec, time.Time{}, fmt.Errorf("dead letter failure is invalid: %w", err)
	}
	if len(rec.OriginalPayload) == 0 {
		rec.OriginalPayload = json.RawMessage(`{}`)
	}
	if !json.Valid(rec.OriginalPayload) {
		return rec, time.Time{}, fmt.Errorf("dead letter original payload must be valid json")
	}
	if rec.RetryCount < 0 {
		rec.RetryCount = 0
	}
	if rec.ChainDepth < 0 {
		rec.ChainDepth = 0
	}
	createdAt := time.Now().UTC()
	if s != nil {
		createdAt = s.now()
	}
	if rec.Timestamp != "" {
		parsed, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
		if err != nil {
			return rec, time.Time{}, fmt.Errorf("dead letter timestamp must be RFC3339Nano: %w", err)
		}
		createdAt = parsed.UTC()
	}
	return rec, createdAt, nil
}

func mustFailureJSON(envelope runtimefailures.Envelope) string {
	raw, err := runtimefailures.MarshalEnvelope(envelope)
	if err != nil {
		return "null"
	}
	return string(raw)
}
