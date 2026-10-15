package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
)

func SeedEntityToolHistoricalReadOnlyImportForTest(ctx context.Context, selected any, runID, entityID, eventID string, at time.Time) error {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID, eventID); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("entity-tool historical import requires its exact clock")
	}
	// The original journal and headerless state were separate durable cuts.
	// Failure of the second cut must not undo the committed historical journal.
	if err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return mutationlog.InsertEntityToolHistoricalReadOnlyMutationsTx(ctx, tx, runID, entityID, eventID, at)
	}); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return entitystore.InsertEntityToolHistoricalReadOnlyStateTx(ctx, tx, runID, entityID, at)
	})
}
