package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	RunForkSelectedContractBindingOwner = "store.run_fork.selected_contract_binding"

	runForkSelectedContractBindingTable = "run_fork_selected_contract_bindings"
)

type RunForkSelectedContractBindingRequest struct {
	ForkRunID         string
	SourceRunID       string
	ForkEventID       string
	ContractSelection RunForkContractSelection
}

type RunForkSelectedContractBinding struct {
	Owner             string                   `json:"owner"`
	ForkRunID         string                   `json:"fork_run_id"`
	SourceRunID       string                   `json:"source_run_id"`
	ForkEventID       string                   `json:"fork_event_id"`
	ContractSelection RunForkContractSelection `json:"contract_selection"`
	CreatedAt         time.Time                `json:"created_at"`
}

func RequireRunForkSelectedContractBindingCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	_ = caps
	required := map[string][]string{
		runForkSelectedContractBindingTable: {
			"binding_id",
			"fork_run_id",
			"source_run_id",
			"fork_event_id",
			"mode",
			"contracts_root",
			"bundle_hash",
			"workflow_name",
			"workflow_version",
			"created_at",
		},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run fork selected contract binding requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkSelectedContractBindingCapabilities(ctx context.Context) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	return RequireRunForkSelectedContractBindingCapabilities(caps, catalog)
}

func (s *PostgresStore) LoadRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (RunForkSelectedContractBinding, bool, error) {
	if s == nil || s.DB == nil {
		return RunForkSelectedContractBinding{}, false, fmt.Errorf("postgres store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return RunForkSelectedContractBinding{}, false, err
	}
	if !catalog.hasColumns(
		runForkSelectedContractBindingTable,
		"fork_run_id",
		"source_run_id",
		"fork_event_id",
		"mode",
		"contracts_root",
		"bundle_hash",
		"workflow_name",
		"workflow_version",
		"created_at",
	) {
		return RunForkSelectedContractBinding{}, false, nil
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, s.DB, forkRunID)
	if err == sql.ErrNoRows {
		return RunForkSelectedContractBinding{}, false, nil
	}
	if err != nil {
		return RunForkSelectedContractBinding{}, false, err
	}
	return binding, true, nil
}

func (s *PostgresStore) RequireRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (RunForkSelectedContractBinding, error) {
	if err := s.requireRunForkSelectedContractBindingCapabilities(ctx); err != nil {
		return RunForkSelectedContractBinding{}, err
	}
	binding, ok, err := s.LoadRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return RunForkSelectedContractBinding{}, err
	}
	if !ok {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding for fork run %s not found", strings.TrimSpace(forkRunID))
	}
	return binding, nil
}

func insertRunForkSelectedContractBinding(ctx context.Context, tx *sql.Tx, req RunForkSelectedContractBindingRequest, createdAt time.Time) (RunForkSelectedContractBinding, error) {
	if tx == nil {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding transaction is required")
	}
	binding, err := normalizeRunForkSelectedContractBinding(req, createdAt)
	if err != nil {
		return RunForkSelectedContractBinding{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (
			fork_run_id, source_run_id, fork_event_id,
			mode, contracts_root, bundle_hash, workflow_name, workflow_version, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid,
			$4, NULLIF($5, ''), NULLIF($6, ''), $7, $8, $9
		)
	`, binding.ForkRunID, binding.SourceRunID, binding.ForkEventID,
		binding.ContractSelection.Mode,
		binding.ContractSelection.ContractsRoot,
		binding.ContractSelection.BundleHash,
		binding.ContractSelection.WorkflowName,
		binding.ContractSelection.WorkflowVersion,
		binding.CreatedAt); err != nil {
		return RunForkSelectedContractBinding{}, fmt.Errorf("insert selected contract binding: %w", err)
	}
	return binding, nil
}

func loadRunForkSelectedContractBinding(ctx context.Context, querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, forkRunID string) (RunForkSelectedContractBinding, error) {
	var binding RunForkSelectedContractBinding
	var selection RunForkContractSelection
	err := querier.QueryRowContext(ctx, `
		SELECT
			fork_run_id::text,
			source_run_id::text,
			fork_event_id::text,
			mode,
			COALESCE(contracts_root, ''),
			COALESCE(bundle_hash, ''),
			workflow_name,
			workflow_version,
			created_at
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1::uuid
	`, forkRunID).Scan(
		&binding.ForkRunID,
		&binding.SourceRunID,
		&binding.ForkEventID,
		&selection.Mode,
		&selection.ContractsRoot,
		&selection.BundleHash,
		&selection.WorkflowName,
		&selection.WorkflowVersion,
		&binding.CreatedAt,
	)
	if err != nil {
		return RunForkSelectedContractBinding{}, err
	}
	binding.Owner = RunForkSelectedContractBindingOwner
	binding.ContractSelection = selection
	return binding, nil
}

func normalizeRunForkSelectedContractBinding(req RunForkSelectedContractBindingRequest, createdAt time.Time) (RunForkSelectedContractBinding, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding fork run_id must be a UUID: %w", err)
	}
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	if sourceRunID == "" {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires source run_id")
	}
	if _, err := uuid.Parse(sourceRunID); err != nil {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding source run_id must be a UUID: %w", err)
	}
	forkEventID := strings.TrimSpace(req.ForkEventID)
	if forkEventID == "" {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires fork event_id")
	}
	if _, err := uuid.Parse(forkEventID); err != nil {
		return RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding fork event_id must be a UUID: %w", err)
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return RunForkSelectedContractBinding{}, err
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return RunForkSelectedContractBinding{
		Owner:             RunForkSelectedContractBindingOwner,
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       forkEventID,
		ContractSelection: selection,
		CreatedAt:         createdAt.UTC(),
	}, nil
}

func normalizeRunForkSelectedContractSelection(selection RunForkContractSelection) (RunForkContractSelection, error) {
	selection.Mode = strings.TrimSpace(selection.Mode)
	if selection.Mode == "" {
		selection.Mode = RunForkContractSelectionModeSelectedContracts
	}
	selection.ContractsRoot = strings.TrimSpace(selection.ContractsRoot)
	selection.BundleHash = strings.TrimSpace(selection.BundleHash)
	selection.WorkflowName = strings.TrimSpace(selection.WorkflowName)
	selection.WorkflowVersion = strings.TrimSpace(selection.WorkflowVersion)
	switch selection.Mode {
	case RunForkContractSelectionModeSelectedContracts:
		if selection.ContractsRoot == "" {
			return RunForkContractSelection{}, fmt.Errorf("selected contract binding requires contracts_root")
		}
		if selection.BundleHash != "" {
			return RunForkContractSelection{}, fmt.Errorf("selected contract binding selected_contracts mode cannot carry bundle_hash")
		}
	case RunForkContractSelectionModeBundleHash:
		if selection.BundleHash == "" {
			return RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash mode requires bundle_hash")
		}
		if !canonicalBundleHashPattern.MatchString(selection.BundleHash) {
			return RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
		if selection.ContractsRoot != "" {
			return RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash mode cannot carry contracts_root")
		}
	default:
		return RunForkContractSelection{}, fmt.Errorf("selected contract binding requires mode selected_contracts or bundle_hash; got %q", selection.Mode)
	}
	if selection.WorkflowName == "" {
		return RunForkContractSelection{}, fmt.Errorf("selected contract binding requires workflow_name")
	}
	if selection.WorkflowVersion == "" {
		return RunForkContractSelection{}, fmt.Errorf("selected contract binding requires workflow_version")
	}
	return selection, nil
}
