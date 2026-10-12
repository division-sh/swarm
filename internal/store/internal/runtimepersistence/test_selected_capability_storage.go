package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	budgetspendstore "github.com/division-sh/swarm/internal/store/internal/budgetspend"
)

type SelectedCompletionCapabilityStorage = effectpersistence.SelectedCompletionCapabilityStorage
type SelectedClaudeStartupCounts = effectpersistence.SelectedClaudeStartupCounts
type SelectedClaudeTurnCapabilityStorage = effectpersistence.SelectedClaudeTurnCapabilityStorage
type SelectedClaudeAttemptDiagnosticStorage = effectpersistence.SelectedClaudeAttemptDiagnosticStorage
type SelectedPreparationStorage = runforkpersistence.SelectedPreparationStorage
type SelectedCompletionModelAliasStorage = budgetspendstore.SelectedCompletionModelAliasStorage

func CountSelectedProviderOperationsStorageForTest(ctx context.Context, selected any, executionID string, generation uint64) (int, error) {
	if err := validateSelectedCapabilityPostgresOwner(selected, executionID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountSelectedProviderOperationsStorageTx(ctx, tx, executionID, generation)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadSelectedCompletionCapabilitiesStorageForTest(ctx context.Context, selected any, executionID, adapter string, generation uint64, executionOwner, runID string) ([]SelectedCompletionCapabilityStorage, error) {
	if err := validateSelectedCapabilityPostgresOwner(selected, executionID); err != nil {
		return nil, err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	var out []SelectedCompletionCapabilityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.ReadSelectedCompletionCapabilitiesStorageTx(ctx, tx, executionID, adapter, generation, executionOwner, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSelectedPreparationStorageForTest(ctx context.Context, selected any, executionID string) (SelectedPreparationStorage, error) {
	var out SelectedPreparationStorage
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedPreparationStorageTx(ctx, tx, executionID)
		return err
	})
	if err != nil {
		return SelectedPreparationStorage{}, err
	}
	return out, nil
}

func ReadSelectedClaudeStartupCountsStorageForTest(ctx context.Context, selected any, preparationID, processAuthorityID string) (SelectedClaudeStartupCounts, error) {
	var out SelectedClaudeStartupCounts
	if err := validateSelectedPreparationObservation(selected, preparationID, processAuthorityID); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.ReadSelectedClaudeStartupCountsStorageTx(ctx, tx, preparationID, processAuthorityID)
		return err
	})
	if err != nil {
		return SelectedClaudeStartupCounts{}, err
	}
	return out, nil
}

func ReadSelectedClaudeStartupSurfaceStorageForTest(ctx context.Context, selected any, preparationID, processAuthorityID string) (string, error) {
	if err := validateSelectedPreparationObservation(selected, preparationID, processAuthorityID); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.ReadSelectedClaudeStartupSurfaceStorageTx(ctx, tx, preparationID, processAuthorityID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadSelectedClaudeTurnCapabilityStorageForTest(ctx context.Context, selected any, executionID, runID string) (SelectedClaudeTurnCapabilityStorage, error) {
	var out SelectedClaudeTurnCapabilityStorage
	if err := validateSelectedPreparationObservation(selected, executionID, runID); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.ReadSelectedClaudeTurnCapabilityStorageTx(ctx, tx, executionID, runID)
		return err
	})
	if err != nil {
		return SelectedClaudeTurnCapabilityStorage{}, err
	}
	return out, nil
}

func ReadSelectedClaudeAttemptDiagnosticsStorageForTest(ctx context.Context, selected any, executionID string) ([]SelectedClaudeAttemptDiagnosticStorage, error) {
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return nil, err
	}
	var out []SelectedClaudeAttemptDiagnosticStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.ReadSelectedClaudeAttemptDiagnosticsStorageTx(ctx, tx, executionID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSelectedCompletionModelAliasStorageForTest(ctx context.Context, selected any, executionID, adapter string) (SelectedCompletionModelAliasStorage, error) {
	var out SelectedCompletionModelAliasStorage
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = budgetspendstore.ReadSelectedCompletionModelAliasStorageTx(ctx, tx, executionID, adapter, llmselection.ModelAliasRegular)
		return err
	})
	if err != nil {
		return SelectedCompletionModelAliasStorage{}, err
	}
	return out, nil
}

func validateSelectedCapabilityPostgresOwner(selected any, executionID string) error {
	if _, ok := selected.(*PostgresStore); !ok {
		return fmt.Errorf("selected provider capability storage requires the original postgres owner")
	}
	return validateStandaloneStorageEvent(selected, executionID)
}

func validateSelectedPreparationObservation(selected any, firstID, secondID string) error {
	if err := validateStandaloneStorageEvent(selected, firstID); err != nil {
		return err
	}
	return validateSelectedForkStorageIdentity(secondID)
}
