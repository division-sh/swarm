package authoractivity

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

// Mutation is private selected-store transaction state. It never crosses a
// domain port and is passed explicitly between adapter helpers rather than
// hidden in context.Context.
type Mutation struct {
	tx        *sql.Tx
	dialect   Dialect
	last      int64
	drafts    []runtimeauthoractivity.Draft
	finalized bool
}

func Begin(ctx context.Context, tx *sql.Tx, dialect Dialect) (*Mutation, error) {
	if tx == nil {
		return nil, fmt.Errorf("author activity transaction is required")
	}
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return nil, fmt.Errorf("author activity dialect %q is not supported", dialect)
	}
	mutation := &Mutation{tx: tx, dialect: dialect, drafts: make([]runtimeauthoractivity.Draft, 0, 4)}
	if err := mutation.lock(ctx); err != nil {
		return nil, err
	}
	return mutation, nil
}

func (m *Mutation) Record(ctx context.Context, draft runtimeauthoractivity.Draft) error {
	if m == nil || m.tx == nil || m.finalized {
		return fmt.Errorf("author activity mutation is not active")
	}
	admitted, err := runtimeauthoractivity.AdmitDraft(ctx, draft)
	if err != nil {
		return err
	}
	m.drafts = append(m.drafts, admitted)
	return nil
}

func (m *Mutation) PersistedOccurredAt(ctx context.Context, dedupKey string) (time.Time, bool, error) {
	if m == nil || m.tx == nil || m.finalized {
		return time.Time{}, false, fmt.Errorf("author activity mutation is not active")
	}
	occurrence, found, err := m.loadByDedup(ctx, dedupKey)
	if err != nil || !found {
		return time.Time{}, found, err
	}
	return occurrence.OccurredAt.UTC(), true, nil
}

func (m *Mutation) PersistedAuthorSafeSummary(ctx context.Context, dedupKey string) (string, bool, error) {
	if m == nil || m.tx == nil || m.finalized {
		return "", false, fmt.Errorf("author activity mutation is not active")
	}
	occurrence, found, err := m.loadByDedup(ctx, dedupKey)
	if err != nil || !found {
		return "", found, err
	}
	return occurrence.AuthorSafeSummary, true, nil
}

func (m *Mutation) Finalize(ctx context.Context) error {
	if m == nil || m.tx == nil {
		return fmt.Errorf("author activity mutation is required")
	}
	if m.finalized {
		return fmt.Errorf("author activity mutation was already finalized")
	}
	m.finalized = true
	unique := make([]runtimeauthoractivity.Draft, 0, len(m.drafts))
	byDedup := make(map[string]runtimeauthoractivity.Draft, len(m.drafts))
	for _, draft := range m.drafts {
		if previous, exists := byDedup[draft.DedupKey]; exists {
			if !runtimeauthoractivity.DraftsEqual(previous, draft) {
				return fmt.Errorf("author activity conflicting in-transaction replay for dedup key %q", draft.DedupKey)
			}
			continue
		}
		byDedup[draft.DedupKey] = draft
		existing, found, err := m.loadByDedup(ctx, draft.DedupKey)
		if err != nil {
			return err
		}
		if found {
			if !runtimeauthoractivity.DraftsEqual(runtimeauthoractivity.DraftFromOccurrence(existing), draft) {
				return fmt.Errorf("author activity conflicting persisted replay for dedup key %q", draft.DedupKey)
			}
			continue
		}
		unique = append(unique, draft)
	}
	if len(unique) == 0 {
		return nil
	}
	first := m.last + 1
	last := m.last + int64(len(unique))
	if err := m.updateLast(ctx, last); err != nil {
		return err
	}
	for index, draft := range unique {
		if err := m.insert(ctx, first+int64(index), draft); err != nil {
			return err
		}
	}
	m.last = last
	return nil
}

func (m *Mutation) lock(ctx context.Context) error {
	switch m.dialect {
	case DialectPostgres:
		if _, err := m.tx.ExecContext(ctx, `INSERT INTO author_activity_order (singleton_id, last_sequence) VALUES (1, 0) ON CONFLICT (singleton_id) DO NOTHING`); err != nil {
			return fmt.Errorf("initialize author activity order: %w", err)
		}
		if err := m.tx.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE`).Scan(&m.last); err != nil {
			return fmt.Errorf("lock author activity order: %w", err)
		}
	case DialectSQLite:
		if _, err := m.tx.ExecContext(ctx, `INSERT OR IGNORE INTO author_activity_order (singleton_id, last_sequence) VALUES (1, 0)`); err != nil {
			return fmt.Errorf("initialize author activity order: %w", err)
		}
		if _, err := m.tx.ExecContext(ctx, `UPDATE author_activity_order SET last_sequence = last_sequence WHERE singleton_id = 1`); err != nil {
			return fmt.Errorf("lock author activity order: %w", err)
		}
		if err := m.tx.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&m.last); err != nil {
			return fmt.Errorf("read author activity order: %w", err)
		}
	}
	return nil
}

func (m *Mutation) updateLast(ctx context.Context, last int64) error {
	query := `UPDATE author_activity_order SET last_sequence = $1 WHERE singleton_id = 1 AND last_sequence = $2`
	if m.dialect == DialectSQLite {
		query = `UPDATE author_activity_order SET last_sequence = ? WHERE singleton_id = 1 AND last_sequence = ?`
	}
	result, err := m.tx.ExecContext(ctx, query, last, m.last)
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

const occurrenceSelect = `SELECT CAST(occurrence_id AS TEXT), sequence, kind, version, transition, source_owner, source_identity, dedup_key, COALESCE(CAST(run_id AS TEXT), ''), COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(agent_id, ''), COALESCE(flow_id, ''), scope_kind, COALESCE(CAST(runtime_instance_id AS TEXT), ''), COALESCE(bundle_hash, ''), COALESCE(author_safe_summary, ''), projection, failure, occurred_at FROM author_activity_occurrences`

func (m *Mutation) loadByDedup(ctx context.Context, key string) (runtimeauthoractivity.Occurrence, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return runtimeauthoractivity.Occurrence{}, false, fmt.Errorf("author activity dedup key is required")
	}
	query := occurrenceSelect + ` WHERE dedup_key = $1`
	if m.dialect == DialectSQLite {
		query = occurrenceSelect + ` WHERE dedup_key = ?`
	}
	occurrence, err := authoractivityadapter.ScanOccurrence(m.tx.QueryRowContext(ctx, query, key))
	if err == sql.ErrNoRows {
		return runtimeauthoractivity.Occurrence{}, false, nil
	}
	if err != nil {
		return runtimeauthoractivity.Occurrence{}, false, fmt.Errorf("read author activity dedup key %q: %w", key, err)
	}
	return occurrence, true, nil
}

func (m *Mutation) insert(ctx context.Context, sequence int64, draft runtimeauthoractivity.Draft) error {
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
	if m.dialect == DialectSQLite {
		query = `INSERT INTO author_activity_occurrences (occurrence_id, sequence, kind, version, transition, source_owner, source_identity, dedup_key, run_id, entity_id, agent_id, flow_id, scope_kind, runtime_instance_id, bundle_hash, author_safe_summary, projection, failure, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		args[8] = nullableSQLite(draft.RunID)
		args[9] = nullableSQLite(draft.EntityID)
		args[10] = nullableSQLite(draft.AgentID)
		args[11] = nullableSQLite(draft.FlowID)
		args[13] = nullableSQLite(draft.Scope.RuntimeInstanceID)
		args[14] = nullableSQLite(draft.Scope.BundleHash)
		args[15] = nullableSQLite(draft.AuthorSafeSummary)
	}
	if _, err := m.tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert author activity %s/%s: %w", draft.Kind, draft.Transition, err)
	}
	return nil
}

func nullable(value string) string { return strings.TrimSpace(value) }

func nullableSQLite(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
