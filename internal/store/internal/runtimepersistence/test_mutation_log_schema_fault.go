package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func RemoveMutationLogForRollbackProofForTest(ctx context.Context, selected any) error {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents", "entity_mutations"} {
			if _, err := tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
				return fmt.Errorf("remove mutation-log rollback fixture %s: %w", table, err)
			}
		}
		return nil
	})
}

func TryInvalidAccumulatorMutationPathForTest(ctx context.Context, selected any, run, entity string) error {
	for _, id := range []string{run, entity} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return err
		}
	}
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations (
			mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at
		) VALUES ('22222222-2222-2222-2222-222222222222',$1,$2,'accumulator','','null','{"bad":true}','platform','test','seed',CURRENT_TIMESTAMP)`, run, entity)
		return err
	})
}
