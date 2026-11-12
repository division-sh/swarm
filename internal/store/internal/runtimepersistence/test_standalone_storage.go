package runtimepersistence

import (
	"context"
	"fmt"
	"strings"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/google/uuid"
)

type StandaloneRunStorage = runlifecycle.StandaloneRunStorage
type StandaloneAgentDeliveryStorage = delivery.StandaloneAgentDeliveryStorage

func ReadExactAgentDeliveryStatusForTest(ctx context.Context, selected any, eventID, agentID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	if agentID == "" || strings.TrimSpace(agentID) != agentID {
		return "", fmt.Errorf("delivery status evidence requires an exact agent identity")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadExactAgentDeliveryStatusForTest(ctx, eventID, agentID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadExactAgentDeliveryStatusForTest(ctx, eventID, agentID)
	default:
		return "", fmt.Errorf("standalone evidence requires the original native owner")
	}
}

func validateStandaloneStorageEvent(selected any, eventID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return fmt.Errorf("standalone evidence requires an exact canonical event")
	}
	return nil
}

func ReadStandaloneRunStorageForTest(ctx context.Context, selected any, eventID string) (StandaloneRunStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return StandaloneRunStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner.ReadStandaloneRunStorageForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner.ReadStandaloneRunStorageForTest(ctx, eventID)
	default:
		return StandaloneRunStorage{}, fmt.Errorf("standalone evidence requires the original native owner")
	}
}

func ReadStandaloneAgentDeliveryStorageForTest(ctx context.Context, selected any, eventID, agentID string) (StandaloneAgentDeliveryStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return StandaloneAgentDeliveryStorage{}, err
	}
	if agentID == "" || strings.TrimSpace(agentID) != agentID {
		return StandaloneAgentDeliveryStorage{}, fmt.Errorf("standalone evidence requires an exact agent identity")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.ReadStandaloneAgentDeliveryStorageForTest(ctx, eventID, agentID)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.ReadStandaloneAgentDeliveryStorageForTest(ctx, eventID, agentID)
	default:
		return StandaloneAgentDeliveryStorage{}, fmt.Errorf("standalone evidence requires the original native owner")
	}
}

func ReadStandaloneCompletionCandidateForTest(ctx context.Context, selected any, eventID string) (runtimerunlifecycle.Candidate, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner.ReadStandaloneCompletionCandidateForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner.ReadStandaloneCompletionCandidateForTest(ctx, eventID)
	default:
		return runtimerunlifecycle.Candidate{}, fmt.Errorf("standalone evidence requires the original native owner")
	}
}
