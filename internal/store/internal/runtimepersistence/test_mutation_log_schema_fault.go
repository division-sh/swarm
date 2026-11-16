package runtimepersistence

import (
	"context"
	"fmt"
)

func RemoveMutationLogForRollbackProofForTest(ctx context.Context, selected any) error {
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.RemoveMutationLogForRollbackProofForTest(ctx)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.RemoveMutationLogForRollbackProofForTest(ctx)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.TryInvalidAccumulatorMutationPathForTest(ctx, run, entity)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.TryInvalidAccumulatorMutationPathForTest(ctx, run, entity)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
