package runtimepersistence

import (
	"context"
	"database/sql"
)

// Preserve the complete table and dependent keys while making the exact native
// writer's named journal unavailable. Restoration never fabricates history.
func HideWorkflowMutationTableForTest(ctx context.Context, selected any, hidden bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	statement := "ALTER TABLE entity_mutations RENAME TO pipeline_hidden_entity_mutations"
	if !hidden {
		statement = "ALTER TABLE pipeline_hidden_entity_mutations RENAME TO entity_mutations"
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}
