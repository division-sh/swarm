package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func validateSelectedForkHistoryFixture(selected any, id string, identity agentidentity.Identity, at time.Time) error {
	if _, err := identity.StorageFields(); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("selected source history requires its exact historical clock")
	}
	if err := validateStandaloneStorageEvent(selected, identity.RunID); err != nil {
		return err
	}
	return validateStandaloneStorageEvent(selected, id)
}

func SeedSelectedForkSourceSessionForTest(ctx context.Context, selected any, sessionID string, identity agentidentity.Identity, at time.Time) error {
	if err := validateSelectedForkHistoryFixture(selected, sessionID, identity, at); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return llmpersistence.SeedSelectedForkSourceSessionTx(ctx, tx, sessionID, identity, at)
		})
	})
}

func SeedSelectedForkSourceConversationAuditForTest(ctx context.Context, selected any, auditID, entityID string, identity agentidentity.Identity, at time.Time) error {
	if err := validateSelectedForkHistoryFixture(selected, auditID, identity, at); err != nil {
		return err
	}
	if err := validateStandaloneStorageEvent(selected, entityID); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return llmpersistence.SeedSelectedForkSourceConversationAuditTx(ctx, tx, auditID, entityID, identity, at)
		})
	})
}

func CountForkOrSourceSessionStorageForTest(ctx context.Context, selected any, forkRunID, sourceSessionID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{forkRunID, sourceSessionID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountForkOrSourceSessionStorageTx(ctx, tx, forkRunID, sourceSessionID)
	})
}

func CountForkOrSourceConversationAuditStorageForTest(ctx context.Context, selected any, forkRunID, sourceAuditID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{forkRunID, sourceAuditID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountForkOrSourceConversationAuditStorageTx(ctx, tx, forkRunID, sourceAuditID)
	})
}

func CountForkOrSourceTurnStorageForTest(ctx context.Context, selected any, forkRunID, sourceTurnID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{forkRunID, sourceTurnID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountForkOrSourceTurnStorageTx(ctx, tx, forkRunID, sourceTurnID)
	})
}
