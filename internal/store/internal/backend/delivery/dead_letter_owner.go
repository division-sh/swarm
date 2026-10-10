package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type DeadLetterPostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent func() error
}

type DeadLetterSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	requireCurrent func() error
}

func NewDeadLetterPostgresOwner(backend *postgresbackend.Backend, requireCurrent func() error) (*DeadLetterPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("dead-letter PostgreSQL backend is required")
	}
	if requireCurrent == nil {
		return nil, errors.New("dead-letter PostgreSQL schema owner is required")
	}
	return &DeadLetterPostgresOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func NewDeadLetterSQLiteOwner(backend *sqlitebackend.Backend, requireCurrent func() error) (*DeadLetterSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("dead-letter SQLite backend is required")
	}
	if requireCurrent == nil {
		return nil, errors.New("dead-letter SQLite schema owner is required")
	}
	return &DeadLetterSQLiteOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func (s *DeadLetterPostgresOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("dead-letter PostgreSQL owner is required")
	}
	return s.requireCurrent()
}

func (s *DeadLetterSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("dead-letter SQLite owner is required")
	}
	return s.requireCurrent()
}

func requireActiveRunForEvent(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) error {
	eventID = strings.TrimSpace(eventID)
	if tx == nil || eventID == "" {
		return errors.New("dead-letter active event lookup requires transaction and event_id")
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("require active event run: event %s not found", eventID)
		}
		return fmt.Errorf("require active event run: %w", err)
	}
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if postgres {
		return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
	}
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func sqliteNullUUID(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func rowsAffected(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}
	return rows > 0, nil
}

func CountDeadLettersForOriginalEvent(ctx context.Context, tx *sql.Tx, originalEventID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, originalEventID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountDeadLetterEntityRelationsSince(ctx context.Context, tx *sql.Tx, postgres bool, since time.Time, entityID string) (int, error) {
	query := `SELECT COUNT(*) FROM dead_letters dl
WHERE COALESCE(NULLIF(dl.original_payload->>'entity_id',''),COALESCE(dl.entity_id::text,''))=$1 AND dl.created_at >= $2`
	if !postgres {
		query = `SELECT COUNT(*) FROM dead_letters dl
WHERE COALESCE(NULLIF(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.entity_id,''))=? AND dl.created_at >= ?`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, entityID, since).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

type DeadLetterObservationRow struct {
	OriginalEventID, StoredEntityID, PayloadEntityID, HandlerNode string
}

type TargetFailureDeadLetterStorage struct {
	Reason, TargetContext string
}

func ReadTargetFailureDeadLetterStorage(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (TargetFailureDeadLetterStorage, error) {
	query := `SELECT failure->'detail'->>'code', COALESCE((failure->'detail'->'attributes'->'target')::text, '')
FROM dead_letters
WHERE original_event_id = $1::uuid
  AND failure->>'class' = 'platform.target_unreachable'
  AND handler_node = 'pin_routing'`
	if !postgres {
		query = `SELECT COALESCE(json_extract(failure, '$.detail.code'), ''), COALESCE(json_extract(failure, '$.detail.attributes.target'), '')
FROM dead_letters
WHERE original_event_id = ?
  AND json_extract(failure, '$.class') = 'platform.target_unreachable'
  AND handler_node = 'pin_routing'`
	}
	var out TargetFailureDeadLetterStorage
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&out.Reason, &out.TargetContext); err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

type ChainDepthDeadLetterStorage struct {
	Count, Depth                             int
	HandlerNode, FailureClass, OriginalEvent string
}

func ReadChainDepthDeadLetterStorage(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	query := `SELECT COUNT(*), COALESCE(MAX(dl.chain_depth), 0), COALESCE(MAX(dl.handler_node), ''),
COALESCE(MAX(dl.failure->>'class'), ''), COALESCE(MAX(dl.original_event), '')
FROM dead_letters dl JOIN events source ON source.event_id=dl.original_event_id
WHERE source.run_id=$1::uuid
AND COALESCE(NULLIF(dl.original_payload->>'entity_id',''),COALESCE(dl.entity_id::text,''))=$2
AND dl.failure->>'class'='platform.chain_depth_exceeded'`
	if !postgres {
		query = `SELECT COUNT(*), COALESCE(MAX(dl.chain_depth), 0), COALESCE(MAX(dl.handler_node), ''),
COALESCE(MAX(json_extract(dl.failure,'$.class')), ''), COALESCE(MAX(dl.original_event), '')
FROM dead_letters dl JOIN events source ON source.event_id=dl.original_event_id
WHERE source.run_id=?
AND COALESCE(NULLIF(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.entity_id,''))=?
AND json_extract(dl.failure,'$.class')='platform.chain_depth_exceeded'`
	}
	var out ChainDepthDeadLetterStorage
	if err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&out.Count, &out.Depth, &out.HandlerNode, &out.FailureClass, &out.OriginalEvent); err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func ReadDeadLetterObservationRows(ctx context.Context, tx *sql.Tx, postgres bool) ([]DeadLetterObservationRow, error) {
	query := `SELECT dl.original_event_id::text,COALESCE(dl.entity_id::text,''),
COALESCE(dl.original_payload->>'entity_id',''),COALESCE(dl.handler_node,'')
FROM dead_letters dl ORDER BY dl.created_at,dl.dead_letter_id`
	if !postgres {
		query = `SELECT dl.original_event_id,COALESCE(dl.entity_id,''),
COALESCE(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.handler_node,'')
FROM dead_letters dl ORDER BY dl.created_at,dl.dead_letter_id`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DeadLetterObservationRow{}
	for rows.Next() {
		var row DeadLetterObservationRow
		if err := rows.Scan(&row.OriginalEventID, &row.StoredEntityID, &row.PayloadEntityID, &row.HandlerNode); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
