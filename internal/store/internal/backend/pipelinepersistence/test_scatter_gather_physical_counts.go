package pipelinepersistence

import (
	"context"
	"database/sql"
)

type ScatterGatherPhysicalCounts struct {
	Entities, Timers, FanOutIntents, DomainEvents int
}

func (s *PipelinePostgresOwner) ReadScatterGatherPhysicalCountsForTest(ctx context.Context, runID string) (ScatterGatherPhysicalCounts, error) {
	var out ScatterGatherPhysicalCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readScatterGatherPhysicalCountsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadScatterGatherPhysicalCountsForTest(ctx context.Context, runID string) (ScatterGatherPhysicalCounts, error) {
	var out ScatterGatherPhysicalCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readScatterGatherPhysicalCountsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	return out, nil
}

func readScatterGatherPhysicalCountsTx(ctx context.Context, tx *sql.Tx, runID string) (ScatterGatherPhysicalCounts, error) {
	var out ScatterGatherPhysicalCounts
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&out.Entities); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id=$1`, runID).Scan(&out.Timers); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.FanOutIntents); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','batch.opened') OR event_name LIKE 'workers/%/item.reported')`, runID).Scan(&out.DomainEvents); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	return out, nil
}
