package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

func ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx context.Context, selected any, surfaceID, preparationID, actorID string) ([]byte, error) {
	if err := validateStandaloneStorageEvent(selected, surfaceID); err != nil {
		return nil, err
	}
	if err := validateStandaloneStorageEvent(selected, preparationID); err != nil {
		return nil, err
	}
	if actorID == "" || actorID != strings.TrimSpace(actorID) {
		return nil, fmt.Errorf("selected preparation surface requires a canonical actor coordinate")
	}
	var raw []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		raw, err = managedcapabilitystore.ReadRunlessSelectedPreparationSurfaceStorageTx(ctx, tx, surfaceID, preparationID, actorID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func CountGlobalRunStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, runlifecycle.CountGlobalRunStorageTx)
}

func CountGlobalAgentStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, agentpersistence.CountGlobalAgentStorageTx)
}

func CountGlobalSelectedRuntimeExecutionStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, runforkpersistence.CountGlobalSelectedRuntimeExecutionStorageTx)
}

func CountGlobalGenerationGrantStorageForTest(ctx context.Context, selected any) (int, error) {
	return readSelectedPreflightGlobalCount(ctx, selected, generationauthority.CountGlobalGenerationGrantStorageTx)
}

func readSelectedPreflightGlobalCount(ctx context.Context, selected any, read func(context.Context, *sql.Tx) (int, error)) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = read(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}
