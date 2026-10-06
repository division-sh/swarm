package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type NotifyCompletedTurnsStorage struct{ Turns, Instances int }
type NotifyFanOutCursorStorage struct{ Total, Cursor, Owed, Blocked int }
type NotifyFanOutWorkStorage struct {
	Intents, FloorOne, Minimum, Maximum int
	Revisions, Facts                    int64
}

func ReadNotifyCompletedTurnsForTest(ctx context.Context, selected any, runID, agentID string) (NotifyCompletedTurnsStorage, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyCompletedTurnsStorage{}, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyCompletedTurnsStorage{}, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return NotifyCompletedTurnsStorage{}, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out NotifyCompletedTurnsStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT flow_instance) FROM agent_turns WHERE run_id=$1 AND agent_id=$2 AND failure IS NULL`, runID, agentID).Scan(&out.Turns, &out.Instances)
	})
	if err != nil {
		return NotifyCompletedTurnsStorage{}, err
	}
	return out, nil
}

func ReadNotifyLifecycleTransitionCountForTest(ctx context.Context, selected any, agentID, previous, next string) (int, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return 0, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out int
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_transition_facts WHERE agent_id=$1 AND previous_phase=$2 AND next_phase=$3`, agentID, previous, next).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadNotifyLatestEventFailureForTest(ctx context.Context, selected any, eventID string) (string, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return "", fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out string
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM dead_letters WHERE original_event_id=$1 ORDER BY created_at DESC LIMIT 1`, eventID).Scan(&out)
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadNotifyFlowInstanceCountForTest(ctx context.Context, selected any, flow string) (int, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return 0, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out int
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE flow_template=$1`, flow).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadNotifyRunPresenceForTest(ctx context.Context, selected any, runID string) (int, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return 0, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return 0, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out int
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadNotifyFanOutCursorForTest(ctx context.Context, selected any, runID string) (NotifyFanOutCursorStorage, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyFanOutCursorStorage{}, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyFanOutCursorStorage{}, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return NotifyFanOutCursorStorage{}, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out NotifyFanOutCursorStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Total, &out.Cursor, &out.Owed, &out.Blocked)
	})
	if err != nil {
		return NotifyFanOutCursorStorage{}, err
	}
	return out, nil
}

// The original cursor wait checks this only after its progress-success cut.
func ReadNotifyFirstRunFailureForTest(ctx context.Context, selected any, runID string) (string, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return "", fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out string
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(d.failure AS TEXT) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE CAST(e.run_id AS TEXT)=$1 LIMIT 1`, runID).Scan(&out)
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadNotifyFanOutWorkForTest(ctx context.Context, selected any, runID string) (NotifyFanOutWorkStorage, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyFanOutWorkStorage{}, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return NotifyFanOutWorkStorage{}, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		read = owner.backend.RunReadTransaction
	default:
		return NotifyFanOutWorkStorage{}, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var out NotifyFanOutWorkStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1),
			(SELECT COALESCE(SUM(CASE WHEN next_chunk_size=1 THEN 1 ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1),
			(SELECT COALESCE(MIN(next_chunk_size),0) FROM fan_out_intents WHERE run_id=$1),
			(SELECT COALESCE(MAX(next_chunk_size),0) FROM fan_out_intents WHERE run_id=$1)`, runID).Scan(&out.Intents, &out.FloorOne, &out.Minimum, &out.Maximum); err != nil {
			return err
		}
		var err error
		out.Revisions, out.Facts, err = runforkrevision.CountNotifyFanOutRevisionStorageForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return NotifyFanOutWorkStorage{}, err
	}
	return out, nil
}

func ReadNotifyAgentDeliveryStatusForTest(ctx context.Context, selected any, runID, agentID, instance string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id=$2 AND agent_flow_instance_path=$3 ORDER BY created_at DESC,delivery_id DESC LIMIT 1`, runID, agentID, instance).Scan(&out)
	})
	if err != nil {
		return "", err
	}
	return out, nil
}
