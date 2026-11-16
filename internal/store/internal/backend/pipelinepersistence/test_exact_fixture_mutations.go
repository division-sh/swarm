package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *PipelinePostgresOwner) CorruptStagedWorkflowTimerForTest(ctx context.Context, runID, activationID string, fault StagedWorkflowTimerFault) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return CorruptStagedWorkflowTimerTxForTest(ctx, tx, runID, activationID, fault)
	})
}

func (s *PipelineSQLiteOwner) CorruptStagedWorkflowTimerForTest(ctx context.Context, runID, activationID string, fault StagedWorkflowTimerFault) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return CorruptStagedWorkflowTimerTxForTest(ctx, tx, runID, activationID, fault)
	})
}

func (s *PipelinePostgresOwner) RemoveForkReceiverFieldCompanionForTest(ctx context.Context, runID, entityID, instance string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return RemoveForkReceiverFieldCompanionTxForTest(ctx, tx, runID, entityID, instance)
	})
}

func (s *PipelineSQLiteOwner) RemoveForkReceiverFieldCompanionForTest(ctx context.Context, runID, entityID, instance string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return RemoveForkReceiverFieldCompanionTxForTest(ctx, tx, runID, entityID, instance)
	})
}

func (s *PipelinePostgresOwner) SetObservabilityReceiptConflictForTest(ctx context.Context, eventID string, conflict ObservabilityReceiptConflict, at time.Time) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return InsertObservabilityReceiptConflictForTest(ctx, tx, true, eventID, conflict, at)
	})
}

func (s *PipelineSQLiteOwner) SetObservabilityReceiptConflictForTest(ctx context.Context, eventID string, conflict ObservabilityReceiptConflict, at time.Time) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return InsertObservabilityReceiptConflictForTest(ctx, tx, false, eventID, conflict, at)
	})
}

func (s *PipelinePostgresOwner) InstallActiveForkReceiverHeaderDoneFaultForTest(ctx context.Context, runID, entityID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return InstallActiveForkReceiverHeaderDoneFaultTx(ctx, tx, runID, entityID)
	})
}

func (s *PipelineSQLiteOwner) InstallActiveForkReceiverHeaderDoneFaultForTest(ctx context.Context, runID, entityID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return InstallActiveForkReceiverHeaderDoneFaultTx(ctx, tx, runID, entityID)
	})
}

func (s *PipelinePostgresOwner) RemoveMutationLogForRollbackProofForTest(ctx context.Context) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents", "entity_mutations"} {
			if _, err := tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
				return fmt.Errorf("remove mutation-log rollback fixture %s: %w", table, err)
			}
		}
		return nil
	})
}

func (s *PipelineSQLiteOwner) RemoveMutationLogForRollbackProofForTest(ctx context.Context) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents", "entity_mutations"} {
			if _, err := tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
				return fmt.Errorf("remove mutation-log rollback fixture %s: %w", table, err)
			}
		}
		return nil
	})
}

func (s *PipelinePostgresOwner) TryInvalidAccumulatorMutationPathForTest(ctx context.Context, run, entity string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations (
			mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at
		) VALUES ('22222222-2222-2222-2222-222222222222',$1,$2,'accumulator','','null','{"bad":true}','platform','test','seed',CURRENT_TIMESTAMP)`, run, entity)
		return err
	})
}

func (s *PipelineSQLiteOwner) TryInvalidAccumulatorMutationPathForTest(ctx context.Context, run, entity string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations (
			mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at
		) VALUES ('22222222-2222-2222-2222-222222222222',$1,$2,'accumulator','','null','{"bad":true}','platform','test','seed',CURRENT_TIMESTAMP)`, run, entity)
		return err
	})
}
