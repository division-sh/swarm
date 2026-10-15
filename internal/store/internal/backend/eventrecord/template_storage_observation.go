package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

func ReadWorkflowTimerEventIdentityTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventName string) (string, error) {
	query := `SELECT event_id FROM events WHERE run_id = ? AND event_name = ?`
	if postgres {
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var eventID string
	if err := tx.QueryRowContext(ctx, query, runID, eventName).Scan(&eventID); err != nil {
		return "", err
	}
	return eventID, nil
}

func ReadLatestWorkflowTimerRuntimeLogTextTx(ctx context.Context, tx *sql.Tx, postgres bool) (string, error) {
	query := `SELECT CAST(payload AS TEXT) FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if postgres {
		query = `SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	}
	var payload string
	if err := tx.QueryRowContext(ctx, query).Scan(&payload); err != nil {
		return "", err
	}
	return payload, nil
}

func CountGateSourceNamedEventsTx(ctx context.Context, tx *sql.Tx, name, source string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = $1 AND source_event_id = $2`, name, source).Scan(&count)
	return count, err
}

type A2WorkerOutputStorage struct {
	Count   int
	EventID string
}

func ReadA2WorkerOutputsTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, sourceEventID string) (A2WorkerOutputStorage, error) {
	query := `SELECT COUNT(*), COALESCE(MIN(CAST(event_id AS TEXT)), '') FROM events WHERE run_id=? AND source_event_id=? AND event_name='item.completed'`
	if postgres {
		query = `SELECT COUNT(*), COALESCE(MIN(event_id::text), '') FROM events WHERE run_id=$1::uuid AND source_event_id=$2::uuid AND event_name='item.completed'`
	}
	var out A2WorkerOutputStorage
	if err := tx.QueryRowContext(ctx, query, runID, sourceEventID).Scan(&out.Count, &out.EventID); err != nil {
		return A2WorkerOutputStorage{}, err
	}
	return out, nil
}

func CountSourceEventPublicationsTx(ctx context.Context, tx *sql.Tx, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE source_event_id=$1`, sourceEventID).Scan(&count)
	return count, err
}

func ReadSourceEventPublicationIdentityTx(ctx context.Context, tx *sql.Tx, sourceEventID string) (string, error) {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT event_id FROM events WHERE source_event_id=$1`, sourceEventID).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

func ReadSourceNamedChildEventIDsTx(ctx context.Context, tx *sql.Tx, parentID, name string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events
		WHERE source_event_id=$1 AND (event_name=$2 OR event_name LIKE $3) ORDER BY event_id`, parentID, name, "%/"+name)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func ReadGateRunEventNamesTx(ctx context.Context, tx *sql.Tx, run string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name FROM events WHERE run_id = $1 ORDER BY created_at`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func ReadTemplateNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, name string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1`, name).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadTemplateSourceNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, name, source string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1 AND source_event_id=$2`, name, source).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadTemplateEventPayloadTextTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_id=$1`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountTemplateEventIdentityTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, event).Scan(&count)
	return count, err
}

func CountPostgresRunStatusSemanticEventsTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name <> 'platform.runtime_log'`, runID).Scan(&count)
	return count, err
}

func ReadPostgresRecoveryFailedPayloadStorageTx(ctx context.Context, tx *sql.Tx) ([]byte, error) {
	var payload []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_name = 'platform.recovery_failed' ORDER BY created_at DESC LIMIT 1`).Scan(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func ReadPostgresResetPayloadStorageTx(ctx context.Context, tx *sql.Tx) ([]byte, error) {
	var payload []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_name = 'platform.reset' ORDER BY created_at DESC LIMIT 1`).Scan(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// ReadStageLifecycleRuntimeLogPayloadsTx preserves physical payload text, not
// decoded event bytes, for the bounded whole-store failure diagnostic.
func ReadStageLifecycleRuntimeLogPayloadsTx(ctx context.Context, tx *sql.Tx) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 20`)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		out = append(out, payload)
	}
	return out, rows.Err()
}
