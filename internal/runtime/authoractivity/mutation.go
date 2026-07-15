package authoractivity

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type mutationContextKey struct{}

type mutationState struct {
	tx         *sql.Tx
	dialect    Dialect
	last       int64
	drafts     []Draft
	finalizing bool
}

func Begin(ctx context.Context, tx *sql.Tx, dialect Dialect) (context.Context, error) {
	if tx == nil {
		return nil, fmt.Errorf("author activity transaction is required")
	}
	if existing, ok := stateFromContext(ctx); ok {
		if existing.tx == tx && existing.dialect == dialect {
			return ctx, nil
		}
		// Post-commit work may retain the completed transaction's context. A
		// finalized owner cannot be joined, so a different transaction starts a
		// fresh story while an active owner still fails closed.
		if !existing.finalizing {
			return nil, fmt.Errorf("author activity nested mutation belongs to a different transaction")
		}
	}
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return nil, fmt.Errorf("author activity dialect %q is not supported", dialect)
	}
	state := &mutationState{tx: tx, dialect: dialect, drafts: make([]Draft, 0, 4)}
	if err := state.lock(ctx); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, mutationContextKey{}, state), nil
}

func InMutation(ctx context.Context, tx *sql.Tx) bool {
	state, ok := stateFromContext(ctx)
	return ok && state.tx == tx && !state.finalizing
}

// FinalizedMutation reports whether ctx retains a completed story transaction.
// Post-commit work may inherit that context, but must start a fresh story rather
// than join the retired transaction. A raw transaction never satisfies this
// predicate and therefore continues to fail closed at story-aware boundaries.
func FinalizedMutation(ctx context.Context, tx *sql.Tx) bool {
	state, ok := stateFromContext(ctx)
	return ok && state.tx == tx && state.finalizing
}

func Require(ctx context.Context) error {
	state, ok := stateFromContext(ctx)
	if !ok || state == nil || state.tx == nil || state.finalizing {
		return fmt.Errorf("registered author activity producer requires story-aware mutation context")
	}
	return nil
}

func Record(ctx context.Context, draft Draft) error {
	state, ok := stateFromContext(ctx)
	if !ok || state == nil || state.tx == nil {
		return fmt.Errorf("registered author activity producer requires story-aware mutation context")
	}
	if state.finalizing {
		return fmt.Errorf("author activity producer attempted domain mutation after story finalization")
	}
	draft = cloneDraft(draft)
	if draft.Version == 0 {
		draft.Version = Version
	}
	if draft.OccurrenceID == "" {
		draft.OccurrenceID = uuid.NewString()
	}
	scope, err := scopeForDraft(ctx, draft.Kind, draft.Transition, draft.Scope)
	if err != nil {
		return err
	}
	draft.Scope = scope
	draft.AuthorSafeSummary, err = NormalizeAuthorSafeSummary(draft.AuthorSafeSummary)
	if err != nil {
		return fmt.Errorf("normalize author activity summary: %w", err)
	}
	if err := ValidateDraft(draft); err != nil {
		return err
	}
	state.drafts = append(state.drafts, draft)
	return nil
}

// PersistedOccurredAt returns the immutable timestamp already assigned to a
// deduplicated occurrence. Replay producers use it to reconstruct the exact
// original draft; all other occurrence fields remain subject to strict replay
// equality during Finalize.
func PersistedOccurredAt(ctx context.Context, dedupKey string) (time.Time, bool, error) {
	state, ok := stateFromContext(ctx)
	if !ok || state == nil || state.tx == nil || state.finalizing {
		return time.Time{}, false, fmt.Errorf("author activity mutation context is required")
	}
	dedupKey = strings.TrimSpace(dedupKey)
	if dedupKey == "" {
		return time.Time{}, false, fmt.Errorf("author activity dedup key is required")
	}
	occurrence, found, err := state.loadByDedup(ctx, dedupKey)
	if err != nil {
		return time.Time{}, false, err
	}
	if !found {
		return time.Time{}, false, nil
	}
	return occurrence.OccurredAt.UTC(), true, nil
}

// PersistedAuthorSafeSummary returns only the already-admitted story summary
// for an exact source occurrence. Downstream producers may copy this fact, but
// must never reopen the source payload to reconstruct it.
func PersistedAuthorSafeSummary(ctx context.Context, dedupKey string) (string, bool, error) {
	state, ok := stateFromContext(ctx)
	if !ok || state == nil || state.tx == nil || state.finalizing {
		return "", false, fmt.Errorf("author activity mutation context is required")
	}
	dedupKey = strings.TrimSpace(dedupKey)
	if dedupKey == "" {
		return "", false, fmt.Errorf("author activity dedup key is required")
	}
	occurrence, found, err := state.loadByDedup(ctx, dedupKey)
	if err != nil || !found {
		return "", found, err
	}
	return occurrence.AuthorSafeSummary, true, nil
}

func Finalize(ctx context.Context) error {
	state, ok := stateFromContext(ctx)
	if !ok || state == nil || state.tx == nil {
		return fmt.Errorf("author activity mutation context is required")
	}
	if state.finalizing {
		return fmt.Errorf("author activity mutation already finalized")
	}
	state.finalizing = true
	unique := make([]Draft, 0, len(state.drafts))
	byDedup := make(map[string]Draft, len(state.drafts))
	for _, draft := range state.drafts {
		if previous, exists := byDedup[draft.DedupKey]; exists {
			if !draftsEqual(previous, draft) {
				return fmt.Errorf("author activity conflicting in-transaction replay for dedup key %q", draft.DedupKey)
			}
			continue
		}
		byDedup[draft.DedupKey] = draft
		existing, found, err := state.loadByDedup(ctx, draft.DedupKey)
		if err != nil {
			return err
		}
		if found {
			if !occurrenceMatchesDraft(existing, draft) {
				return fmt.Errorf("author activity conflicting persisted replay for dedup key %q", draft.DedupKey)
			}
			continue
		}
		unique = append(unique, draft)
	}
	if len(unique) == 0 {
		return nil
	}
	first := state.last + 1
	last := state.last + int64(len(unique))
	if err := state.updateLast(ctx, last); err != nil {
		return err
	}
	for i, draft := range unique {
		if err := state.insert(ctx, first+int64(i), draft); err != nil {
			return err
		}
	}
	state.last = last
	return nil
}

func stateFromContext(ctx context.Context) (*mutationState, bool) {
	if ctx == nil {
		return nil, false
	}
	state, ok := ctx.Value(mutationContextKey{}).(*mutationState)
	return state, ok && state != nil
}

func (s *mutationState) lock(ctx context.Context) error {
	switch s.dialect {
	case DialectPostgres:
		if _, err := s.tx.ExecContext(ctx, `INSERT INTO author_activity_order (singleton_id, last_sequence) VALUES (1, 0) ON CONFLICT (singleton_id) DO NOTHING`); err != nil {
			return fmt.Errorf("initialize author activity order: %w", err)
		}
		if err := s.tx.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE`).Scan(&s.last); err != nil {
			return fmt.Errorf("lock author activity order: %w", err)
		}
	case DialectSQLite:
		if _, err := s.tx.ExecContext(ctx, `INSERT OR IGNORE INTO author_activity_order (singleton_id, last_sequence) VALUES (1, 0)`); err != nil {
			return fmt.Errorf("initialize author activity order: %w", err)
		}
		if _, err := s.tx.ExecContext(ctx, `UPDATE author_activity_order SET last_sequence = last_sequence WHERE singleton_id = 1`); err != nil {
			return fmt.Errorf("lock author activity order: %w", err)
		}
		if err := s.tx.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&s.last); err != nil {
			return fmt.Errorf("read author activity order: %w", err)
		}
	}
	return nil
}

func (s *mutationState) updateLast(ctx context.Context, last int64) error {
	query := `UPDATE author_activity_order SET last_sequence = $1 WHERE singleton_id = 1 AND last_sequence = $2`
	args := []any{last, s.last}
	if s.dialect == DialectSQLite {
		query = `UPDATE author_activity_order SET last_sequence = ? WHERE singleton_id = 1 AND last_sequence = ?`
	}
	result, err := s.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("advance author activity order: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read author activity order advancement: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("author activity order changed outside the locked mutation")
	}
	return nil
}

func (s *mutationState) loadByDedup(ctx context.Context, key string) (Occurrence, bool, error) {
	query := occurrenceSelect + ` WHERE dedup_key = $1`
	if s.dialect == DialectSQLite {
		query = occurrenceSelect + ` WHERE dedup_key = ?`
	}
	occurrence, err := scanOccurrence(s.tx.QueryRowContext(ctx, query, key))
	if err == sql.ErrNoRows {
		return Occurrence{}, false, nil
	}
	if err != nil {
		return Occurrence{}, false, fmt.Errorf("read author activity dedup key %q: %w", key, err)
	}
	return occurrence, true, nil
}

func (s *mutationState) insert(ctx context.Context, sequence int64, draft Draft) error {
	projection, err := json.Marshal(draft.Projection)
	if err != nil {
		return fmt.Errorf("marshal author activity projection: %w", err)
	}
	var failure any
	if draft.Failure != nil {
		failureRaw, err := json.Marshal(draft.Failure)
		if err != nil {
			return fmt.Errorf("marshal author activity failure: %w", err)
		}
		failure = string(failureRaw)
	}
	args := []any{draft.OccurrenceID, sequence, string(draft.Kind), draft.Version, draft.Transition, draft.SourceOwner, draft.SourceIdentity, draft.DedupKey, nullable(draft.RunID), nullable(draft.EntityID), nullable(draft.AgentID), nullable(draft.FlowID), string(draft.Scope.Kind), nullable(draft.Scope.RuntimeInstanceID), nullable(draft.Scope.BundleHash), nullable(draft.AuthorSafeSummary), string(projection), failure, draft.OccurredAt.UTC()}
	query := `INSERT INTO author_activity_occurrences (occurrence_id, sequence, kind, version, transition, source_owner, source_identity, dedup_key, run_id, entity_id, agent_id, flow_id, scope_kind, runtime_instance_id, bundle_hash, author_safe_summary, projection, failure, occurred_at) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, '')::uuid, NULLIF($10, '')::uuid, NULLIF($11, ''), NULLIF($12, ''), $13, NULLIF($14, '')::uuid, NULLIF($15, ''), NULLIF($16, ''), $17::jsonb, NULLIF($18, '')::jsonb, $19)`
	if s.dialect == DialectSQLite {
		query = `INSERT INTO author_activity_occurrences (occurrence_id, sequence, kind, version, transition, source_owner, source_identity, dedup_key, run_id, entity_id, agent_id, flow_id, scope_kind, runtime_instance_id, bundle_hash, author_safe_summary, projection, failure, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		args[8] = nullableSQLite(draft.RunID)
		args[9] = nullableSQLite(draft.EntityID)
		args[10] = nullableSQLite(draft.AgentID)
		args[11] = nullableSQLite(draft.FlowID)
		args[13] = nullableSQLite(draft.Scope.RuntimeInstanceID)
		args[14] = nullableSQLite(draft.Scope.BundleHash)
		args[15] = nullableSQLite(draft.AuthorSafeSummary)
	}
	if _, err := s.tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert author activity %s/%s: %w", draft.Kind, draft.Transition, err)
	}
	return nil
}

func occurrenceMatchesDraft(occurrence Occurrence, draft Draft) bool {
	existing := Draft{
		Kind: occurrence.Kind, Version: occurrence.Version, Transition: occurrence.Transition,
		SourceOwner: occurrence.SourceOwner, SourceIdentity: occurrence.SourceIdentity, DedupKey: occurrence.DedupKey,
		OccurredAt: occurrence.OccurredAt, RunID: occurrence.RunID, EntityID: occurrence.EntityID,
		AgentID: occurrence.AgentID, FlowID: occurrence.FlowID, Scope: occurrence.Scope,
		AuthorSafeSummary: occurrence.AuthorSafeSummary, Projection: occurrence.Projection,
		Failure: occurrence.Failure,
	}
	return draftsEqual(existing, draft)
}

func nullable(value string) string { return strings.TrimSpace(value) }

func nullableSQLite(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

const occurrenceSelect = `SELECT CAST(occurrence_id AS TEXT), sequence, kind, version, transition, source_owner, source_identity, dedup_key, COALESCE(CAST(run_id AS TEXT), ''), COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(agent_id, ''), COALESCE(flow_id, ''), scope_kind, COALESCE(CAST(runtime_instance_id AS TEXT), ''), COALESCE(bundle_hash, ''), COALESCE(author_safe_summary, ''), projection, failure, occurred_at FROM author_activity_occurrences`

type rowScanner interface{ Scan(...any) error }

func scanOccurrence(row rowScanner) (Occurrence, error) {
	var occurrence Occurrence
	var projectionRaw []byte
	var failureRaw []byte
	var occurredAtRaw any
	err := row.Scan(&occurrence.OccurrenceID, &occurrence.Sequence, &occurrence.Kind, &occurrence.Version, &occurrence.Transition, &occurrence.SourceOwner, &occurrence.SourceIdentity, &occurrence.DedupKey, &occurrence.RunID, &occurrence.EntityID, &occurrence.AgentID, &occurrence.FlowID, &occurrence.Scope.Kind, &occurrence.Scope.RuntimeInstanceID, &occurrence.Scope.BundleHash, &occurrence.AuthorSafeSummary, &projectionRaw, &failureRaw, &occurredAtRaw)
	if err != nil {
		return Occurrence{}, err
	}
	if err := json.Unmarshal(projectionRaw, &occurrence.Projection); err != nil {
		return Occurrence{}, fmt.Errorf("decode author activity projection: %w", err)
	}
	if len(failureRaw) > 0 && string(failureRaw) != "null" {
		var failure runtimefailures.Envelope
		if err := json.Unmarshal(failureRaw, &failure); err != nil {
			return Occurrence{}, fmt.Errorf("decode author activity failure: %w", err)
		}
		if err := runtimefailures.ValidateEnvelope(failure); err != nil {
			return Occurrence{}, fmt.Errorf("validate author activity failure: %w", err)
		}
		occurrence.Failure = &failure
	}
	occurredAt, err := decodeTime(occurredAtRaw)
	if err != nil {
		return Occurrence{}, fmt.Errorf("decode author activity occurred_at: %w", err)
	}
	occurrence.OccurredAt = occurredAt
	if err := ValidateDraft(Draft{Kind: occurrence.Kind, Version: occurrence.Version, Transition: occurrence.Transition, SourceOwner: occurrence.SourceOwner, SourceIdentity: occurrence.SourceIdentity, DedupKey: occurrence.DedupKey, OccurredAt: occurrence.OccurredAt, Scope: occurrence.Scope, AuthorSafeSummary: occurrence.AuthorSafeSummary, Projection: occurrence.Projection, Failure: occurrence.Failure}); err != nil {
		return Occurrence{}, fmt.Errorf("invalid persisted author activity %s: %w", occurrence.OccurrenceID, err)
	}
	return occurrence, nil
}

func decodeTime(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		return parseStoredTime(value)
	case []byte:
		return parseStoredTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("unsupported time value %T", raw)
	}
}

func parseStoredTime(value string) (time.Time, error) {
	var lastErr error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		parsed, err := time.Parse(layout, strings.TrimSpace(value))
		if err == nil {
			return parsed.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
