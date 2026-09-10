package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/google/uuid"
)

type selectedRecoveryTxOwner interface {
	MarkTerminalTx(context.Context, *sql.Tx, authoractivity.Mutation, *runforkrevision.Effects, runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
	RecoverSelectedForkEffectsTx(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects, string, runtimeeffects.RecoveryRequest) (runtimeeffects.RecoverySummary, error)
}

func (s *RunForkPostgresOwner) ListSelectedForkRecoveryEntries(ctx context.Context) ([]runfork.SelectedForkRecoveryEntry, error) {
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return listSelectedForkRecoveryEntries(ctx, tx)
}

func (s *RunForkSQLiteOwner) ListSelectedForkRecoveryEntries(ctx context.Context) ([]runfork.SelectedForkRecoveryEntry, error) {
	var entries []runfork.SelectedForkRecoveryEntry
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		entries, err = listSelectedForkRecoveryEntries(txctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func listSelectedForkRecoveryEntries(ctx context.Context, tx *sql.Tx) ([]runfork.SelectedForkRecoveryEntry, error) {
	rows, err := tx.QueryContext(ctx, `SELECT b.fork_run_id,r.bundle_hash FROM run_fork_selected_contract_bindings b JOIN runs r ON r.run_id=b.fork_run_id ORDER BY b.fork_run_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []runfork.SelectedForkRecoveryEntry
	for rows.Next() {
		var e runfork.SelectedForkRecoveryEntry
		if err := rows.Scan(&e.Binding.ForkRunID, &e.BundleHash); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].Binding, err = loadRunForkSelectedContractBinding(ctx, tx, entries[i].Binding.ForkRunID)
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func (s *RunForkPostgresOwner) RecoverSelectedFork(ctx context.Context, req runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	if err := req.Validate(); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	defer tx.Rollback()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	if err := admitSelectedRecoveryTx(ctx, tx, req, false); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	snapshot, err := s.LoadSnapshotTx(ctx, tx, req.Entry.Binding.ForkRunID, true)
	if err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	effects := runforkrevision.NewEffects()
	result, err := recoverSelectedForkTx(ctx, tx, s, story, effects, snapshot, req, false)
	if err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	if err := commitRunForkAuthorActivityTransaction(ctx, tx, story, effects); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	return result, nil
}

func (s *RunForkSQLiteOwner) RecoverSelectedFork(ctx context.Context, req runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	if err := req.Validate(); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	var result runfork.SelectedForkRecoveryResult
	err := s.runRuntimeMutation(ctx, "recover selected fork", func(ctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := admitSelectedRecoveryTx(ctx, tx, req, true); err != nil {
			return err
		}
		snapshot, err := s.LoadSnapshotTx(ctx, tx, req.Entry.Binding.ForkRunID)
		if err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		result, err = recoverSelectedForkTx(ctx, tx, s, story, effects, snapshot, req, true)
		if err != nil {
			return err
		}
		if _, err := runforkrevision.FinalizeSQLite(ctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(ctx)
	})
	if err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	return result, nil
}

func admitSelectedRecoveryTx(ctx context.Context, tx *sql.Tx, req runcontrol.SelectedForkRecoveryRequest, sqlite bool) error {
	if _, err := authoractivity.BundleScopeForSource(ctx, req.Entry.BundleHash); err != nil {
		return err
	}
	current, err := storestartup.ProcessAuthorityCurrent(ctx, tx, req.Process, sqlite, true)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("selected recovery process is not current")
	}
	return nil
}

func recoverSelectedForkTx(ctx context.Context, tx *sql.Tx, owner selectedRecoveryTxOwner, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, snapshot runlifecycle.Snapshot, req runcontrol.SelectedForkRecoveryRequest, sqlite bool) (runfork.SelectedForkRecoveryResult, error) {
	runID := req.Entry.Binding.ForkRunID
	result := runfork.SelectedForkRecoveryResult{RunID: runID}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, runID)
	if err != nil {
		return result, err
	}
	if binding.Owner != req.Entry.Binding.Owner || binding.BindingID != req.Entry.Binding.BindingID ||
		binding.ForkRunID != req.Entry.Binding.ForkRunID || binding.SourceRunID != req.Entry.Binding.SourceRunID ||
		binding.ForkEventID != req.Entry.Binding.ForkEventID || binding.ContractSelection != req.Entry.Binding.ContractSelection ||
		!binding.CreatedAt.Equal(req.Entry.Binding.CreatedAt) || snapshot.BundleHash != req.Entry.BundleHash {
		return result, fmt.Errorf("selected recovery binding changed")
	}
	query := `SELECT execution_id,state,preparation_binding,preparation_fingerprint,failure,
		source_run_id,binding_id,fork_event_id,generation,fence_generation,executable_coordinate_fingerprint,
		admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,effective_config_fingerprint,
		declaration_plan_fingerprint,declaration_plan
		FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var state, fingerprint string
	var raw, failureRaw, declarationRaw []byte
	var bindingID string
	execution := runfork.SelectedContractRuntimeExecution{ForkRunID: runID}
	err = tx.QueryRowContext(ctx, query, runID).Scan(&result.ExecutionID, &state, &raw, &fingerprint, &failureRaw,
		&execution.SourceRunID, &bindingID, &execution.ForkEventID, &execution.Generation, &execution.FenceGeneration,
		&execution.ExecutableCoordinateFingerprint, &execution.AdmissionFingerprint, &execution.ContainerPlanFingerprint,
		&execution.ActorCensusFingerprint, &execution.EffectiveConfigFingerprint, &execution.DeclarationPlanFingerprint, &declarationRaw)
	if err == sql.ErrNoRows {
		result.Disposition = runfork.SelectedForkRecoveryStaged
		if snapshot.State.Terminal() {
			result.Disposition = runfork.SelectedForkRecoveryTerminal
		}
		if snapshot.State == runlifecycle.StateRunning {
			return result, fmt.Errorf("selected running fork lacks execution evidence")
		}
		return result, nil
	}
	if err != nil {
		return result, err
	}
	var preparation runfork.SelectedForkPreparationBinding
	if err := canonicaljson.DecodeInto(raw, &preparation); err != nil {
		return result, err
	}
	actual, err := preparation.Fingerprint()
	if err != nil {
		return result, err
	}
	if actual != fingerprint || preparation.ForkRunID != runID || preparation.SourceRunID != binding.SourceRunID || preparation.ForkEventID != binding.ForkEventID || preparation.Coordinates.BundleHash != snapshot.BundleHash {
		return result, fmt.Errorf("selected recovery preparation conflicts with binding")
	}
	execution.PreparationFingerprint = fingerprint
	coordinate, err := execution.CoordinateFingerprint()
	if err != nil {
		return result, err
	}
	id, idErr := uuid.Parse(result.ExecutionID)
	if idErr != nil || id == uuid.Nil || id.String() != result.ExecutionID || execution.Generation == 0 || execution.FenceGeneration == 0 ||
		bindingID != binding.BindingID || execution.SourceRunID != binding.SourceRunID || execution.ForkEventID != binding.ForkEventID ||
		coordinate != execution.ExecutableCoordinateFingerprint || execution.DeclarationPlanFingerprint != preparation.DeclarationPlanFingerprint {
		return result, fmt.Errorf("selected recovery execution conflicts with preparation")
	}
	var declarations agenttopology.SelectedDeclarationPlan
	if err := canonicaljson.DecodeInto(declarationRaw, &declarations); err != nil {
		return result, err
	}
	if err := declarations.Validate(); err != nil {
		return result, err
	}
	if declarations.Revision != execution.DeclarationPlanFingerprint || declarations.BundleHash != snapshot.BundleHash {
		return result, fmt.Errorf("selected recovery declarations conflict with preparation")
	}
	current, err := storestartup.PreparedProcessCurrent(ctx, tx, preparation.Coordinates, preparation.ProcessGeneration, sqlite, false)
	if err != nil {
		return result, err
	}
	if current {
		result.Disposition = runfork.SelectedForkRecoveryCurrent
		return result, nil
	}
	predecessor, err := storestartup.PreparedProcessPredecessor(ctx, tx, preparation.Coordinates, preparation.ProcessGeneration, req.Process, sqlite)
	if err != nil {
		return result, err
	}
	if !predecessor {
		return result, fmt.Errorf("selected recovery preparation is not a recorded predecessor")
	}
	var failure *failures.Envelope
	if len(failureRaw) != 0 && string(failureRaw) != "null" {
		envelope, err := failures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return result, err
		}
		failure = &envelope
	}
	if state == "failed" && failure == nil {
		return result, fmt.Errorf("failed selected execution lacks failure evidence")
	}
	if (snapshot.State.Terminal() && state == "closed") || ((state == "closed" || state == "quiesced") && failure == nil) {
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1 AND a.state IN ('authorized','launched','response_observed'))`, result.ExecutionID).Scan(&active); err != nil {
			return result, err
		}
		if active {
			return result, fmt.Errorf("closed selected execution retains unsettled effects")
		}
		if snapshot.State.Terminal() {
			result.Disposition = runfork.SelectedForkRecoveryTerminal
			return result, nil
		}
	}
	switch state {
	case "quiesced", "closed":
		if failure == nil {
			result.Disposition = runfork.SelectedForkRecoveryControlOnly
			if snapshot.State.Terminal() {
				result.Disposition = runfork.SelectedForkRecoveryTerminal
			}
			return result, nil
		}
	case "prepared", "running", "failed":
	default:
		return result, fmt.Errorf("selected recovery state is invalid: %s", state)
	}
	if failure == nil {
		class, code := failures.ClassLifecycleConflict, "selected_recovery_prelaunch_abandoned"
		if state != "prepared" {
			class, code = failures.ClassOutcomeUncertain, "selected_recovery_outcome_unconfirmed"
		}
		envelope, ok := failures.EnvelopeFromError(failures.New(class, code, "selected-fork", "startup_recovery", map[string]any{"execution_id": result.ExecutionID}))
		if !ok {
			return result, fmt.Errorf("selected recovery failure construction failed")
		}
		failure = &envelope
	}
	failureRaw, err = json.Marshal(failure)
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed',fence_generation=fence_generation+1,lease_expires_at=NULL,failure=$2,terminal_at=$3,updated_at=$3 WHERE execution_id=$1`, result.ExecutionID, string(failureRaw), req.Effects.Now()); err != nil {
		return result, err
	}
	result.Effects, err = owner.RecoverSelectedForkEffectsTx(ctx, tx, story, effects, result.ExecutionID, req.Effects)
	if err != nil {
		return result, err
	}
	if !snapshot.State.Terminal() {
		if _, _, err := owner.MarkTerminalTx(ctx, tx, story, effects, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateFailed, Failure: failure, EndedAt: req.Effects.Now()}); err != nil {
			return result, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed' WHERE execution_id=$1 AND state='failed'`, result.ExecutionID); err != nil {
		return result, err
	}
	result.Disposition = runfork.SelectedForkRecoveryFailed
	return result, nil
}
