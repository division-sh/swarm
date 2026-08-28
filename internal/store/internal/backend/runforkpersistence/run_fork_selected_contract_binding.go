package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) requireRunForkSelectedContractBindingAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkSQLiteOwner) requireRunForkSelectedContractBindingAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkPostgresOwner) LoadRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("postgres store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, s.backend, forkRunID)
	if err == sql.ErrNoRows {
		return runfork.RunForkSelectedContractBinding{}, false, nil
	}
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, err
	}
	return binding, true, nil
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("sqlite store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, s.backend, forkRunID)
	if err == sql.ErrNoRows {
		return runfork.RunForkSelectedContractBinding{}, false, nil
	}
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, false, err
	}
	return binding, true, nil
}

func (s *RunForkPostgresOwner) RequireRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, error) {
	if err := s.requireRunForkSelectedContractBindingAccess(); err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	binding, ok, err := s.LoadRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	if !ok {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding for fork run %s not found", strings.TrimSpace(forkRunID))
	}
	return binding, nil
}

func (s *RunForkSQLiteOwner) RequireRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, error) {
	if err := s.requireRunForkSelectedContractBindingAccess(); err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	binding, ok, err := s.LoadRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	if !ok {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding for fork run %s not found", strings.TrimSpace(forkRunID))
	}
	return binding, nil
}

func insertRunForkSelectedContractBinding(ctx context.Context, tx *sql.Tx, req runfork.RunForkSelectedContractBindingRequest, createdAt time.Time) (runfork.RunForkSelectedContractBinding, error) {
	if tx == nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding transaction is required")
	}
	binding, err := normalizeRunForkSelectedContractBinding(req, createdAt)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings (
			fork_run_id, source_run_id, fork_event_id,
			mode, contracts_root, bundle_hash, workflow_name, workflow_version, created_at
		)
		VALUES (
			$1, $2, $3,
			$4, NULLIF($5, ''), NULLIF($6, ''), $7, $8, $9
		)
	`, binding.ForkRunID, binding.SourceRunID, binding.ForkEventID,
		binding.ContractSelection.Mode,
		binding.ContractSelection.ContractsRoot,
		binding.ContractSelection.BundleHash,
		binding.ContractSelection.WorkflowName,
		binding.ContractSelection.WorkflowVersion,
		binding.CreatedAt); err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("insert selected contract binding: %w", err)
	}
	return binding, nil
}

func loadRunForkSelectedContractBinding(ctx context.Context, querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, forkRunID string) (runfork.RunForkSelectedContractBinding, error) {
	var binding runfork.RunForkSelectedContractBinding
	var selection runfork.RunForkContractSelection
	var createdAt any
	err := querier.QueryRowContext(ctx, `
		SELECT
			CAST(fork_run_id AS TEXT),
			CAST(source_run_id AS TEXT),
			CAST(fork_event_id AS TEXT),
			mode,
			COALESCE(contracts_root, ''),
			COALESCE(bundle_hash, ''),
			workflow_name,
			workflow_version,
			created_at
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1
	`, forkRunID).Scan(
		&binding.ForkRunID,
		&binding.SourceRunID,
		&binding.ForkEventID,
		&selection.Mode,
		&selection.ContractsRoot,
		&selection.BundleHash,
		&selection.WorkflowName,
		&selection.WorkflowVersion,
		&createdAt,
	)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	parsedCreatedAt, ok, err := sqliteTimeValue(createdAt)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("decode selected contract binding created_at: %w", err)
	}
	if !ok {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding created_at is required")
	}
	binding.Owner = runfork.RunForkSelectedContractBindingOwner
	binding.ContractSelection = selection
	binding.CreatedAt = parsedCreatedAt
	return binding, nil
}

func normalizeRunForkSelectedContractBinding(req runfork.RunForkSelectedContractBindingRequest, createdAt time.Time) (runfork.RunForkSelectedContractBinding, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding fork run_id must be a UUID: %w", err)
	}
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	if sourceRunID == "" {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires source run_id")
	}
	if _, err := uuid.Parse(sourceRunID); err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding source run_id must be a UUID: %w", err)
	}
	forkEventID := strings.TrimSpace(req.ForkEventID)
	if forkEventID == "" {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding requires fork event_id")
	}
	if _, err := uuid.Parse(forkEventID); err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding fork event_id must be a UUID: %w", err)
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	createdAt = createdAt.UTC().Round(time.Microsecond)
	return runfork.RunForkSelectedContractBinding{
		Owner:             runfork.RunForkSelectedContractBindingOwner,
		ForkRunID:         forkRunID,
		SourceRunID:       sourceRunID,
		ForkEventID:       forkEventID,
		ContractSelection: selection,
		CreatedAt:         createdAt,
	}, nil
}

func NormalizeRunForkSelectedContractBinding(req runfork.RunForkSelectedContractBindingRequest, createdAt time.Time) (runfork.RunForkSelectedContractBinding, error) {
	return normalizeRunForkSelectedContractBinding(req, createdAt)
}

func normalizeRunForkSelectedContractSelection(selection runfork.RunForkContractSelection) (runfork.RunForkContractSelection, error) {
	selection.Mode = strings.TrimSpace(selection.Mode)
	if selection.Mode == "" {
		selection.Mode = runfork.RunForkContractSelectionModeSelectedContracts
	}
	selection.ContractsRoot = strings.TrimSpace(selection.ContractsRoot)
	selection.BundleHash = strings.TrimSpace(selection.BundleHash)
	selection.WorkflowName = strings.TrimSpace(selection.WorkflowName)
	selection.WorkflowVersion = strings.TrimSpace(selection.WorkflowVersion)
	switch selection.Mode {
	case runfork.RunForkContractSelectionModeSelectedContracts:
		if selection.ContractsRoot == "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding requires contracts_root")
		}
		if selection.BundleHash != "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding selected_contracts mode cannot carry bundle_hash")
		}
	case runfork.RunForkContractSelectionModeBundleHash:
		if selection.BundleHash == "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash mode requires bundle_hash")
		}
		if !bundleidentity.IsCanonicalHash(selection.BundleHash) {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
		if selection.ContractsRoot != "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding bundle_hash mode cannot carry contracts_root")
		}
	default:
		return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding requires mode selected_contracts or bundle_hash; got %q", selection.Mode)
	}
	if selection.WorkflowName == "" {
		return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding requires workflow_name")
	}
	if selection.WorkflowVersion == "" {
		return runfork.RunForkContractSelection{}, fmt.Errorf("selected contract binding requires workflow_version")
	}
	return selection, nil
}
