package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func InsertEntityToolSiblingAliasFaultForTest(ctx context.Context, selected any, runID, ownerID, siblingID string, fields map[string]any, at time.Time) error {
	if err := validatePostgresPublicationIdentities(selected, runID, ownerID, siblingID); err != nil {
		return err
	}
	if ownerID == siblingID || at.IsZero() {
		return fmt.Errorf("entity-tool sibling alias fault requires a distinct sibling and exact time")
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertEntityToolSiblingAliasFaultTx(ctx, tx, runID, ownerID, siblingID, fields, at)
	})
}

func SetEntityToolPrivateBookkeepingFaultForTest(ctx context.Context, selected any, entityID string) error {
	if err := validatePostgresPublicationIdentities(selected, entityID); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetEntityToolPrivateBookkeepingFaultTx(ctx, tx, entityID)
	})
}

func SetEntityToolUndeclaredFieldFaultForTest(ctx context.Context, selected any, entityID string) error {
	if err := validatePostgresPublicationIdentities(selected, entityID); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetEntityToolUndeclaredFieldFaultTx(ctx, tx, entityID)
	})
}

func SetEntityToolMarginalStateFaultForTest(ctx context.Context, selected any, entityID string) error {
	if err := validatePostgresPublicationIdentities(selected, entityID); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetEntityToolMarginalStateFaultTx(ctx, tx, entityID)
	})
}

func AdvanceEntityToolForkSourceFaultForTest(ctx context.Context, selected any, runID, entityID string, at time.Time) error {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("entity-tool fork-source advance requires its exact clock")
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.AdvanceEntityToolForkSourceFaultTx(ctx, tx, runID, entityID, at)
	})
}
