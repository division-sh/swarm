package delivery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func (s *DeadLetterPostgresOwner) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	var err error
	rec, _, err = normalizeDeadLetterRecord(rec)
	if err != nil {
		return err
	}
	return s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return s.RecordDeadLetterTx(txctx, tx, story, rec, true)
	})
}

func (s *DeadLetterPostgresOwner) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, rec runtimedeadletters.Record, requireActive bool) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var err error
	rec, _, err = normalizeDeadLetterRecord(rec)
	if err != nil {
		return err
	}
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, true)
	if err != nil {
		return err
	}
	if found {
		return requireExactDeadLetterDuplicate(existing, rec)
	}
	if requireActive {
		if err := requireActiveRunForEvent(ctx, tx, rec.OriginalEventID, true); err != nil {
			return err
		}
	}
	return s.insertPostgresDeadLetterTx(ctx, tx, story, rec)
}

func (s *DeadLetterPostgresOwner) insertPostgresDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, rec runtimedeadletters.Record) error {
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, true)
	if err != nil {
		return err
	}
	if found {
		return requireExactDeadLetterDuplicate(existing, rec)
	}
	source, err := loadDeadLetterAuthorActivitySource(ctx, tx, rec.OriginalEventID, true)
	if err != nil {
		return err
	}
	if err := validateDeadLetterSource(rec, source); err != nil {
		return err
	}
	result, err := insertPostgresDeadLetterRecord(ctx, tx, rec)
	if err != nil {
		return err
	}
	if !result.Inserted {
		return nil
	}
	return recordDeadLetterAuthorActivity(ctx, story, result.DeadLetterID, rec, source, deadLetterOccurredAt(rec.Timestamp))
}

func insertPostgresDeadLetterRecord(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) (runtimedeadletters.InsertResult, error) {
	if tx == nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("dead letter transaction is required")
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(rec.Failure)
	if err != nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("dead letter failure is invalid: %w", err)
	}
	deadLetterID := uuid.NewString()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO dead_letters (
			dead_letter_id, original_event_id, delivery_id, claim_version, original_event, original_payload, entity_id, flow_instance,
			failure, retry_count, chain_depth, handler_node, created_at
		)
			SELECT
				$1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, 0),
				$5, $6::jsonb, NULLIF($7, '')::uuid, $8,
				$9::jsonb, $10, $11, NULLIF($12, ''), $13::timestamptz
		WHERE NOT EXISTS (
			SELECT 1 FROM dead_letters dl
			WHERE (NULLIF($3, '') IS NOT NULL AND dl.delivery_id = NULLIF($3, '')::uuid AND dl.claim_version = NULLIF($4, 0))
			   OR (NULLIF($3, '') IS NULL AND dl.delivery_id IS NULL AND dl.original_event_id = $2::uuid
			       AND dl.failure = $9::jsonb AND COALESCE(dl.handler_node, '') = COALESCE(NULLIF($12, ''), ''))
		)
	`, deadLetterID, rec.OriginalEventID, rec.DeliveryID, rec.ClaimVersion, rec.OriginalEvent,
		[]byte(rec.OriginalPayload), rec.EntityID, rec.FlowInstance, failureJSON, rec.RetryCount,
		rec.ChainDepth, rec.HandlerNode, rec.Timestamp)
	if err != nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("insert dead letter: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("read inserted dead letter rows: %w", err)
	}
	if rows > 0 {
		return runtimedeadletters.InsertResult{DeadLetterID: deadLetterID, Inserted: true}, nil
	}
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, true)
	if err != nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("load existing dead letter: %w", err)
	}
	if !found {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("load existing dead letter: duplicate identity was not found")
	}
	if err := requireExactDeadLetterDuplicate(existing, rec); err != nil {
		return runtimedeadletters.InsertResult{}, err
	}
	return runtimedeadletters.InsertResult{DeadLetterID: existing.DeadLetterID}, nil
}

func (s *DeadLetterSQLiteOwner) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	var err error
	rec, _, err = normalizeDeadLetterRecord(rec)
	if err != nil {
		return err
	}
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite record dead letter", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		return s.RecordDeadLetterTx(txctx, tx, story, rec, true)
	})
}

func (s *DeadLetterSQLiteOwner) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, rec runtimedeadletters.Record, requireActive bool) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("dead letter transaction is required")
	}
	var err error
	rec, _, err = normalizeDeadLetterRecord(rec)
	if err != nil {
		return err
	}
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, false)
	if err != nil {
		return err
	}
	if found {
		return requireExactDeadLetterDuplicate(existing, rec)
	}
	if requireActive {
		if err := requireActiveRunForEvent(ctx, tx, rec.OriginalEventID, false); err != nil {
			return err
		}
	}
	return s.insertSQLiteDeadLetterTx(ctx, tx, story, rec)
}

func (s *DeadLetterSQLiteOwner) insertSQLiteDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, rec runtimedeadletters.Record) error {
	rec, createdAt, err := normalizeDeadLetterRecord(rec)
	if err != nil {
		return err
	}
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, false)
	if err != nil {
		return err
	}
	if found {
		return requireExactDeadLetterDuplicate(existing, rec)
	}
	source, err := loadDeadLetterAuthorActivitySource(ctx, tx, rec.OriginalEventID, false)
	if err != nil {
		return err
	}
	if err := validateDeadLetterSource(rec, source); err != nil {
		return err
	}
	insertResult, err := insertSQLiteDeadLetterRecord(ctx, tx, rec, createdAt)
	if err != nil {
		return err
	}
	if !insertResult.Inserted {
		return nil
	}
	return recordDeadLetterAuthorActivity(ctx, story, insertResult.DeadLetterID, rec, source, createdAt)
}

func insertSQLiteDeadLetterRecord(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record, createdAt time.Time) (runtimedeadletters.InsertResult, error) {
	if tx == nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("dead letter transaction is required")
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
				?,
				?,
				?,
				?,
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
		string(rec.OriginalPayload),
		sqliteNullUUID(rec.EntityID),
		rec.FlowInstance,
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
		return runtimedeadletters.InsertResult{}, fmt.Errorf("insert sqlite dead letter: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return runtimedeadletters.InsertResult{}, err
	}
	if inserted {
		return runtimedeadletters.InsertResult{DeadLetterID: deadLetterID, Inserted: true}, nil
	}
	existing, found, err := loadStoredDeadLetterByIdentity(ctx, tx, rec, false)
	if err != nil {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("load existing sqlite dead letter: %w", err)
	}
	if !found {
		return runtimedeadletters.InsertResult{}, fmt.Errorf("load existing sqlite dead letter: duplicate identity was not found")
	}
	if err := requireExactDeadLetterDuplicate(existing, rec); err != nil {
		return runtimedeadletters.InsertResult{}, err
	}
	return runtimedeadletters.InsertResult{DeadLetterID: existing.DeadLetterID}, nil
}

type deadLetterAuthorActivitySource struct {
	RunID      string
	EntityID   string
	FlowID     string
	BundleHash string
	EventType  string
	Payload    []byte
	ChainDepth int
}

func loadDeadLetterAuthorActivitySource(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) (deadLetterAuthorActivitySource, error) {
	query := `SELECT COALESCE(CAST(e.run_id AS TEXT), ''), COALESCE(CAST(e.entity_id AS TEXT), ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, ''), e.event_name, e.payload_bytes, e.chain_depth FROM events e LEFT JOIN runs r ON r.run_id = e.run_id WHERE e.event_id = ?`
	if postgres {
		query = `SELECT COALESCE(e.run_id::text, ''), COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(r.bundle_hash, ''), e.event_name, e.payload_bytes, e.chain_depth FROM events e LEFT JOIN runs r ON r.run_id = e.run_id WHERE e.event_id = $1::uuid`
	}
	var source deadLetterAuthorActivitySource
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(eventID)).Scan(&source.RunID, &source.EntityID, &source.FlowID, &source.BundleHash, &source.EventType, &source.Payload, &source.ChainDepth); err != nil {
		return deadLetterAuthorActivitySource{}, fmt.Errorf("load dead letter source event: %w", err)
	}
	return source, nil
}

func validateDeadLetterSource(rec runtimedeadletters.Record, source deadLetterAuthorActivitySource) error {
	// Pre-delivery diagnostics reference their causal event while describing an
	// attempted child event. Terminal delivery records describe the persisted
	// event itself, so all event-derived facts must match exactly.
	if rec.DeliveryID == "" {
		return nil
	}
	var conflicts []string
	if rec.OriginalEvent != strings.TrimSpace(source.EventType) {
		conflicts = append(conflicts, "original_event")
	}
	if rec.EntityID != strings.TrimSpace(source.EntityID) {
		conflicts = append(conflicts, "entity_id")
	}
	if !bytes.Equal(rec.OriginalPayload, source.Payload) {
		conflicts = append(conflicts, "original_payload")
	}
	if rec.ChainDepth != source.ChainDepth {
		conflicts = append(conflicts, "chain_depth")
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return fmt.Errorf("dead letter source event facts conflict: %s", strings.Join(conflicts, ", "))
	}
	return nil
}

type storedDeadLetter struct {
	DeadLetterID    string
	OriginalEventID string
	DeliveryID      string
	ClaimVersion    int64
	OriginalEvent   string
	OriginalPayload []byte
	EntityID        string
	FlowInstance    string
	Failure         []byte
	RetryCount      int
	ChainDepth      int
	HandlerNode     string
	CreatedAt       time.Time
}

type deadLetterScanner interface {
	Scan(...any) error
}

func loadStoredDeadLetterByIdentity(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record, postgres bool) (storedDeadLetter, bool, error) {
	failureJSON, err := runtimefailures.MarshalEnvelope(rec.Failure)
	if err != nil {
		return storedDeadLetter{}, false, err
	}
	query := `
		SELECT dead_letter_id, original_event_id, COALESCE(delivery_id, ''), COALESCE(claim_version, 0),
		       original_event, original_payload, COALESCE(entity_id, ''), flow_instance,
		       failure, retry_count, chain_depth, COALESCE(handler_node, ''), created_at
		FROM dead_letters
		WHERE (NULLIF(?, '') IS NOT NULL AND delivery_id = NULLIF(?, '') AND claim_version = NULLIF(?, 0))
		   OR (NULLIF(?, '') IS NULL AND delivery_id IS NULL AND original_event_id = ?
		       AND failure = ? AND COALESCE(handler_node, '') = COALESCE(NULLIF(?, ''), ''))`
	args := []any{rec.DeliveryID, rec.DeliveryID, rec.ClaimVersion, rec.DeliveryID, rec.OriginalEventID, string(failureJSON), rec.HandlerNode}
	if postgres {
		query = `
			SELECT dead_letter_id::text, original_event_id::text, COALESCE(delivery_id::text, ''), COALESCE(claim_version, 0),
			       original_event, original_payload::text, COALESCE(entity_id::text, ''), flow_instance,
			       failure::text, retry_count, chain_depth, COALESCE(handler_node, ''), created_at
			FROM dead_letters
			WHERE (NULLIF($1, '') IS NOT NULL AND delivery_id = NULLIF($1, '')::uuid AND claim_version = NULLIF($2, 0))
			   OR (NULLIF($1, '') IS NULL AND delivery_id IS NULL AND original_event_id = $3::uuid
			       AND failure = $4::jsonb AND COALESCE(handler_node, '') = COALESCE(NULLIF($5, ''), ''))`
		args = []any{rec.DeliveryID, rec.ClaimVersion, rec.OriginalEventID, failureJSON, rec.HandlerNode}
	}
	stored, err := scanStoredDeadLetter(tx.QueryRowContext(ctx, query, args...))
	if err == sql.ErrNoRows {
		return storedDeadLetter{}, false, nil
	}
	if err != nil {
		return storedDeadLetter{}, false, err
	}
	return stored, true, nil
}

func scanStoredDeadLetter(row deadLetterScanner) (storedDeadLetter, error) {
	var stored storedDeadLetter
	var createdAt any
	if err := row.Scan(
		&stored.DeadLetterID, &stored.OriginalEventID, &stored.DeliveryID, &stored.ClaimVersion,
		&stored.OriginalEvent, &stored.OriginalPayload, &stored.EntityID, &stored.FlowInstance,
		&stored.Failure, &stored.RetryCount, &stored.ChainDepth, &stored.HandlerNode, &createdAt,
	); err != nil {
		return storedDeadLetter{}, err
	}
	parsed, err := parseDeadLetterTime(createdAt)
	if err != nil {
		return storedDeadLetter{}, err
	}
	stored.CreatedAt = parsed
	return stored, nil
}

func parseDeadLetterTime(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST"} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed.UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("parse stored dead letter time %q", typed)
	case []byte:
		return parseDeadLetterTime(string(typed))
	default:
		return time.Time{}, fmt.Errorf("stored dead letter time has unsupported type %T", value)
	}
}

func requireExactDeadLetterDuplicate(stored storedDeadLetter, rec runtimedeadletters.Record) error {
	failureJSON, err := runtimefailures.MarshalEnvelope(rec.Failure)
	if err != nil {
		return err
	}
	wantTime, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
	if err != nil {
		return err
	}
	var conflicts []string
	checks := []struct {
		field string
		equal bool
	}{
		{"original_event_id", stored.OriginalEventID == rec.OriginalEventID},
		{"delivery_id", stored.DeliveryID == rec.DeliveryID},
		{"claim_version", stored.ClaimVersion == rec.ClaimVersion},
		{"original_event", stored.OriginalEvent == rec.OriginalEvent},
		{"original_payload", canonicalJSONEqual(stored.OriginalPayload, rec.OriginalPayload)},
		{"entity_id", stored.EntityID == rec.EntityID},
		{"flow_instance", stored.FlowInstance == rec.FlowInstance},
		{"failure", canonicalJSONEqual(stored.Failure, failureJSON)},
		{"retry_count", stored.RetryCount == rec.RetryCount},
		{"chain_depth", stored.ChainDepth == rec.ChainDepth},
		{"handler_node", stored.HandlerNode == rec.HandlerNode},
		{"timestamp", stored.CreatedAt.Equal(wantTime.UTC())},
	}
	for _, check := range checks {
		if !check.equal {
			conflicts = append(conflicts, check.field)
		}
	}
	if len(conflicts) > 0 {
		return runtimedeadletters.NewIdentityConflict(stored.DeadLetterID, conflicts)
	}
	return nil
}

func canonicalJSONEqual(left, right []byte) bool {
	leftCanonical, leftErr := runtimecanonicaljson.Canonicalize(left)
	rightCanonical, rightErr := runtimecanonicaljson.Canonicalize(right)
	return leftErr == nil && rightErr == nil && string(leftCanonical) == string(rightCanonical)
}

func recordDeadLetterAuthorActivity(ctx context.Context, story runtimeauthoractivity.Mutation, deadLetterID string, rec runtimedeadletters.Record, source deadLetterAuthorActivitySource, occurredAt time.Time) error {
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
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDeadLetterRecorded, Transition: "recorded",
		SourceOwner: "dead_letters", SourceIdentity: deadLetterID, DedupKey: "dead-letter:" + deadLetterID,
		OccurredAt: occurredAt, RunID: source.RunID, EntityID: source.EntityID, FlowID: source.FlowID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "event", SubjectID: strings.TrimSpace(rec.OriginalEventID), EventType: source.EventType,
			RetryCount: &retry, ReasonCode: rec.Failure.Detail.Code, NodeID: strings.TrimSpace(rec.HandlerNode),
		},
		Scope: occurrenceScope, Failure: &rec.Failure,
	}
	if story != nil {
		return story.Record(ctx, draft)
	}
	return fmt.Errorf("dead letter activity requires private story ownership")
}

func deadLetterOccurredAt(raw string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	return parsed.UTC()
}

func normalizeDeadLetterRecord(rec runtimedeadletters.Record) (runtimedeadletters.Record, time.Time, error) {
	for field, value := range map[string]string{
		"original event id": rec.OriginalEventID,
		"delivery id":       rec.DeliveryID,
		"original event":    rec.OriginalEvent,
		"entity id":         rec.EntityID,
		"handler node":      rec.HandlerNode,
		"timestamp":         rec.Timestamp,
	} {
		if value != strings.TrimSpace(value) {
			return rec, time.Time{}, fmt.Errorf("dead letter %s is not canonical", field)
		}
	}
	if rec.FlowInstance != strings.Trim(strings.TrimSpace(rec.FlowInstance), "/") {
		return rec, time.Time{}, fmt.Errorf("dead letter flow instance is not canonical")
	}
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
			return rec, time.Time{}, fmt.Errorf("dead letter entity id must be a uuid: %w", err)
		}
	}
	if rec.OriginalEvent == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter original event type is required")
	}
	if rec.FlowInstance == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter flow instance is required")
	}
	if err := runtimefailures.ValidateEnvelope(rec.Failure); err != nil {
		return rec, time.Time{}, fmt.Errorf("dead letter failure is invalid: %w", err)
	}
	if len(rec.OriginalPayload) == 0 {
		return rec, time.Time{}, fmt.Errorf("dead letter original payload is required")
	}
	if !json.Valid(rec.OriginalPayload) {
		return rec, time.Time{}, fmt.Errorf("dead letter original payload must be valid json")
	}
	if rec.RetryCount < 0 {
		return rec, time.Time{}, fmt.Errorf("dead letter retry count must be non-negative")
	}
	if rec.ChainDepth < 0 {
		return rec, time.Time{}, fmt.Errorf("dead letter chain depth must be non-negative")
	}
	if rec.Timestamp == "" {
		return rec, time.Time{}, fmt.Errorf("dead letter timestamp is required")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
	if err != nil {
		return rec, time.Time{}, fmt.Errorf("dead letter timestamp must be RFC3339Nano: %w", err)
	}
	createdAt = createdAt.UTC().Truncate(time.Microsecond)
	rec.Timestamp = createdAt.Format(time.RFC3339Nano)
	return rec, createdAt, nil
}

func mustFailureJSON(envelope runtimefailures.Envelope) string {
	raw, err := runtimefailures.MarshalEnvelope(envelope)
	if err != nil {
		return "null"
	}
	return string(raw)
}
