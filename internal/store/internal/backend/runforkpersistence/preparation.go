package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

func proveSelectedExecutionPreparationTx(ctx context.Context, tx *sql.Tx, issued runfork.SelectedContractRuntimeExecution, sqlite bool) error {
	var raw []byte
	var fingerprint string
	if err := tx.QueryRowContext(ctx, `SELECT preparation_binding, preparation_fingerprint
		FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1 AND fork_run_id=$2 AND generation=$3`,
		issued.ExecutionID, issued.ForkRunID, issued.Generation).Scan(&raw, &fingerprint); err != nil {
		return fmt.Errorf("read selected execution preparation: %w", err)
	}
	var binding runfork.SelectedForkPreparationBinding
	if err := canonicaljson.DecodeInto(raw, &binding); err != nil {
		return fmt.Errorf("decode selected execution preparation: %w", err)
	}
	actual, err := binding.Fingerprint()
	if err != nil {
		return err
	}
	if actual != fingerprint || actual != issued.PreparationFingerprint || binding.ForkRunID != issued.ForkRunID ||
		binding.SourceRunID != issued.SourceRunID || binding.ForkEventID != issued.ForkEventID ||
		binding.DeclarationPlanFingerprint != issued.DeclarationPlanFingerprint {
		return fmt.Errorf("selected execution preparation differs from issued authority")
	}
	return proveSelectedPreparationForMutationTx(ctx, tx, binding.SelectedForkPreparation, sqlite)
}

func proveSelectedPreparationForMutationTx(ctx context.Context, tx *sql.Tx, preparation runfork.SelectedForkPreparation, sqlite bool) error {
	if err := preparation.Validate(); err != nil {
		return err
	}
	current, err := storestartup.PreparedProcessCurrent(ctx, tx, preparation.Coordinates, preparation.ProcessGeneration, sqlite, true)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("selected preparation process is no longer current")
	}
	return managedcapabilitystore.ProveSelectedPreparationReceiptsTx(ctx, tx, preparation, sqlite)
}
