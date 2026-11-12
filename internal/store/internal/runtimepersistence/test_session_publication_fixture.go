package runtimepersistence

import (
	"context"
	"fmt"

	storechannelonboarding "github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
	"github.com/google/uuid"
)

type SessionPublicationLock = storechannelonboarding.SessionPublicationLock

func HoldSessionPublicationOnboardingLockForTest(ctx context.Context, selected any, operationID string) (*SessionPublicationLock, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	if uuid.Validate(operationID) != nil {
		return nil, fmt.Errorf("onboarding lock requires an exact operation")
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.channelOnboardingPostgresOwner.HoldSessionPublicationOnboardingLockForTest(ctx, operationID)
	case *SQLiteRuntimeStore:
		return owner.channelOnboardingSQLiteOwner.HoldSessionPublicationOnboardingLockForTest(ctx, operationID)
	default:
		return nil, fmt.Errorf("unsupported session publication lock owner %T", selected)
	}
}

func SetSessionPublicationInsertFaultForTest(ctx context.Context, selected any, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.SetSessionPublicationInsertFaultForTest(ctx, enabled)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.SetSessionPublicationInsertFaultForTest(ctx, enabled)
	default:
		return fmt.Errorf("unsupported session publication fault owner %T", selected)
	}
}
