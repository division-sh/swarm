package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
)

type MailboxGateInstanceStorage struct{ State, Accumulator, Config string }

func (s *PipelinePostgresOwner) ReadMailboxGateInstanceStorageForTest(ctx context.Context, runID string) (out MailboxGateInstanceStorage, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateInstanceStorageTx(ctx, tx, runID)
		return err
	})
	return
}

func (s *PipelineSQLiteOwner) ReadMailboxGateInstanceStorageForTest(ctx context.Context, runID string) (out MailboxGateInstanceStorage, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateInstanceStorageTx(ctx, tx, runID)
		return err
	})
	return
}

func readMailboxGateInstanceStorageTx(ctx context.Context, tx *sql.Tx, runID string) (out MailboxGateInstanceStorage, err error) {
	err = tx.QueryRowContext(ctx, `SELECT f.current_state, CAST(f.accumulator AS TEXT), CAST(f.config AS TEXT) FROM entity_state e JOIN flow_instances f ON e.run_id=f.run_id AND e.entity_id=f.entity_id AND e.flow_instance=f.instance_path WHERE e.run_id=$1`, runID).Scan(&out.State, &out.Accumulator, &out.Config)
	return
}

func (s *PipelinePostgresOwner) ReadMailboxGateReceiptsStorageForTest(ctx context.Context, runID string) (out []string, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateReceiptsStorageTx(ctx, tx, runID)
		return err
	})
	return
}

func (s *PipelineSQLiteOwner) ReadMailboxGateReceiptsStorageForTest(ctx context.Context, runID string) (out []string, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateReceiptsStorageTx(ctx, tx, runID)
		return err
	})
	return
}

func readMailboxGateReceiptsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id, subscriber_type, subscriber_id, outcome, reason_code FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1) ORDER BY event_id, subscriber_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, kind, subscriber, outcome, reason string
		if err := rows.Scan(&id, &kind, &subscriber, &outcome, &reason); err != nil {
			return nil, err
		}
		out = append(out, id+" "+kind+" "+subscriber+" "+outcome+" "+reason)
	}
	return out, errors.Join(rows.Err(), rows.Close())
}
