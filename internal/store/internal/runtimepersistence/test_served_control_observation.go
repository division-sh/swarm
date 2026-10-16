package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type DirectiveIdentityStorage = agentpersistence.DirectiveIdentityStorage
type DirectiveFailureStorage = agentpersistence.DirectiveFailureStorage
type DirectiveReplayStorage = agentpersistence.DirectiveReplayStorage
type ServedSetupRunStorage = runlifecycle.ServedSetupRunStorage
type ServedSetupEntityStorage = entitystore.ServedSetupEntityStorage
type ServedSetupMutationAuthorityStorage = mutationlog.ServedSetupMutationAuthorityStorage
type ServedSetupConstructorMutationStorage = mutationlog.ServedSetupConstructorMutationStorage

func ReadDirectiveIdentityStorageForTest(ctx context.Context, selected any, key string) (DirectiveIdentityStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DirectiveIdentityStorage, error) {
		return agentpersistence.ReadDirectiveIdentityStorageTx(ctx, tx, postgres, key)
	})
}

func CountKeyedDirectiveOperationsStorageForTest(ctx context.Context, selected any, key string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return agentpersistence.CountKeyedDirectiveOperationsStorageTx(ctx, tx, key)
	})
}

func ReadDirectiveFailureStorageForTest(ctx context.Context, selected any, key string) (DirectiveFailureStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DirectiveFailureStorage, error) {
		return agentpersistence.ReadDirectiveFailureStorageTx(ctx, tx, key)
	})
}

func ReadDirectiveReplayStorageForTest(ctx context.Context, selected any, key string) (DirectiveReplayStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DirectiveReplayStorage, error) {
		return agentpersistence.ReadDirectiveReplayStorageTx(ctx, tx, postgres, key)
	})
}

func ReadDirectivePipelineFailureStorageForTest(ctx context.Context, selected any, key string) ([]byte, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return pipelinepersistence.ReadDirectivePipelineFailureStorageTx(ctx, tx, key)
	})
}

func CountSuccessfulDirectivePipelineReceiptsStorageForTest(ctx context.Context, selected any, eventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountSuccessfulDirectivePipelineReceiptsStorageTx(ctx, tx, eventID)
	})
}

func ReadDirectiveProjectionResourceStorageForTest(ctx context.Context, selected any, key string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return apiidempotency.ReadDirectiveProjectionResourceStorageTx(ctx, tx, key)
	})
}

func ReadServedSetupRunStorageForTest(ctx context.Context, selected any, runID string) (ServedSetupRunStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedSetupRunStorage, error) {
		return runlifecycle.ReadServedSetupRunStorageTx(ctx, tx, runID)
	})
}

func ReadServedSetupEntityStorageForTest(ctx context.Context, selected any, runID, entityID string) (ServedSetupEntityStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedSetupEntityStorage, error) {
		return entitystore.ReadServedSetupEntityStorageTx(ctx, tx, postgres, runID, entityID)
	})
}

func CountAllRunEntityMutationsStorageForTest(ctx context.Context, selected any, runID, entityID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountAllRunEntityMutationsStorageTx(ctx, tx, runID, entityID)
	})
}

func ReadServedSetupMutationAuthoritiesStorageForTest(ctx context.Context, selected any, runID, entityID string) ([]ServedSetupMutationAuthorityStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]ServedSetupMutationAuthorityStorage, error) {
		return mutationlog.ReadServedSetupMutationAuthoritiesStorageTx(ctx, tx, runID, entityID)
	})
}

func ReadServedSetupConstructorMutationsStorageForTest(ctx context.Context, selected any, runID, entityID string) ([]ServedSetupConstructorMutationStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]ServedSetupConstructorMutationStorage, error) {
		return mutationlog.ReadServedSetupConstructorMutationsStorageTx(ctx, tx, runID, entityID)
	})
}
