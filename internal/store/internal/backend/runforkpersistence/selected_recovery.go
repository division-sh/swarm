package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/google/uuid"
)

type selectedRecoveryTxOwner interface {
	MarkTerminalTx(context.Context, *mutationprotocol.Attempt, runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
	RecoverSelectedForkEffectsTx(context.Context, *mutationprotocol.Attempt, string, runtimeeffects.RecoveryRequest) (runtimeeffects.RecoverySummary, error)
}

func (s *RunForkPostgresOwner) ListSelectedForkRecoveryEntries(ctx context.Context) ([]runfork.SelectedForkRecoveryEntry, error) {
	var entries []runfork.SelectedForkRecoveryEntry
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		entries, err = listSelectedForkRecoveryEntries(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
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
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runfork.SelectedForkRecoveryResult, error) {
		var recovered runfork.SelectedForkRecoveryResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := admitSelectedRecoveryTx(txctx, tx, req, false); err != nil {
				return err
			}
			snapshot, err := s.LoadSnapshotTx(txctx, tx, req.Entry.Binding.ForkRunID, true)
			if err != nil {
				return err
			}
			recovered, err = recoverSelectedForkTx(txctx, tx, s, attempt, snapshot, req, false)
			return err
		})
		return recovered, err
	})
	if !result.Acknowledged() {
		return runfork.SelectedForkRecoveryResult{}, result.Err()
	}
	recovered, _ := result.Value()
	return recovered, result.Err()
}

func (s *RunForkSQLiteOwner) RecoverSelectedFork(ctx context.Context, req runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	if err := req.Validate(); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.SelectedForkRecoveryResult{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "recover selected fork", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runfork.SelectedForkRecoveryResult, error) {
		var recovered runfork.SelectedForkRecoveryResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := admitSelectedRecoveryTx(txctx, tx, req, true); err != nil {
				return err
			}
			snapshot, err := s.LoadSnapshotTx(txctx, tx, req.Entry.Binding.ForkRunID)
			if err != nil {
				return err
			}
			recovered, err = recoverSelectedForkTx(txctx, tx, s, attempt, snapshot, req, true)
			return err
		})
		return recovered, err
	})
	if !result.Acknowledged() {
		return runfork.SelectedForkRecoveryResult{}, result.Err()
	}
	recovered, _ := result.Value()
	return recovered, result.Err()
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

func recoverSelectedForkTx(ctx context.Context, tx *sql.Tx, owner selectedRecoveryTxOwner, attempt *mutationprotocol.Attempt, snapshot runlifecycle.Snapshot, req runcontrol.SelectedForkRecoveryRequest, sqlite bool) (runfork.SelectedForkRecoveryResult, error) {
	runID := req.Entry.Binding.ForkRunID
	result := runfork.SelectedForkRecoveryResult{RunID: runID}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, runID)
	if err != nil {
		return result, err
	}
	if binding.Owner != req.Entry.Binding.Owner || binding.BindingID != req.Entry.Binding.BindingID ||
		binding.ForkRunID != req.Entry.Binding.ForkRunID || binding.SourceRunID != req.Entry.Binding.SourceRunID ||
		binding.ForkPoint.Kind != req.Entry.Binding.ForkPoint.Kind ||
		binding.ForkPoint.Revision != req.Entry.Binding.ForkPoint.Revision ||
		binding.ForkPoint.EventID != req.Entry.Binding.ForkPoint.EventID ||
		binding.ContractSelection != req.Entry.Binding.ContractSelection ||
		!binding.CreatedAt.Equal(req.Entry.Binding.CreatedAt) || snapshot.BundleHash != req.Entry.BundleHash {
		return result, fmt.Errorf("selected recovery binding changed")
	}
	wantOrigin, err := runlifecycle.ForkMaterializationRunOrigin(binding.SourceRunID, binding.ForkPoint.Kind, binding.ForkPoint.Revision, binding.ForkPoint.EventID)
	if err != nil {
		return result, err
	}
	if !snapshot.Origin.Equal(wantOrigin) {
		return result, fmt.Errorf("selected recovery fork origin differs from its fixed binding")
	}
	query := `SELECT execution_id,state,preparation_binding,preparation_fingerprint,failure,
		source_run_id,binding_id,fork_point_kind,fork_revision,COALESCE(CAST(fork_event_id AS TEXT),''),generation,fence_generation,executable_coordinate_fingerprint,
		admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,effective_config_fingerprint,
		declaration_plan_fingerprint,declaration_plan
		FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var state, fingerprint string
	var raw, failureRaw, declarationRaw []byte
	var bindingID, pointKind string
	var forkRevision int64
	execution := runfork.SelectedContractRuntimeExecution{ForkRunID: runID}
	err = tx.QueryRowContext(ctx, query, runID).Scan(&result.ExecutionID, &state, &raw, &fingerprint, &failureRaw,
		&execution.SourceRunID, &bindingID, &pointKind, &forkRevision, &execution.ForkEventID, &execution.Generation, &execution.FenceGeneration,
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
	execution.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointKind(pointKind), Revision: forkRevision, EventID: execution.ForkEventID}
	if err := execution.ForkPoint.Validate(); err != nil {
		return result, fmt.Errorf("selected recovery execution point: %w", err)
	}
	var preparation runfork.SelectedForkPreparationBinding
	if err := canonicaljson.DecodeInto(raw, &preparation); err != nil {
		return result, err
	}
	actual, err := preparation.Fingerprint()
	if err != nil {
		return result, err
	}
	if actual != fingerprint || preparation.ForkRunID != runID || preparation.SourceRunID != binding.SourceRunID ||
		!sameSelectedForkPointIdentity(preparation.ForkPoint, binding.ForkPoint) || preparation.ForkEventID != binding.ForkEventID ||
		preparation.Coordinates.BundleHash != snapshot.BundleHash {
		return result, fmt.Errorf("selected recovery preparation conflicts with binding")
	}
	execution.PreparationFingerprint = fingerprint
	coordinate, err := execution.CoordinateFingerprint()
	if err != nil {
		return result, err
	}
	id, idErr := uuid.Parse(result.ExecutionID)
	if idErr != nil || id == uuid.Nil || id.String() != result.ExecutionID || execution.Generation == 0 || execution.FenceGeneration == 0 ||
		bindingID != binding.BindingID || execution.SourceRunID != binding.SourceRunID ||
		!sameSelectedForkPointIdentity(execution.ForkPoint, binding.ForkPoint) ||
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
	var failure *failures.Envelope
	if len(failureRaw) != 0 && string(failureRaw) != "null" {
		envelope, err := failures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return result, err
		}
		failure = &envelope
	}
	if state == "closed" && failure == nil {
		activated, err := selectedForkActivatedOperationTx(ctx, tx, runID, binding.BindingID, binding.ForkPoint, !sqlite)
		if err != nil {
			return result, err
		}
		if activated {
			active, err := selectedForkActiveEffectsTx(ctx, tx, result.ExecutionID)
			if err != nil {
				return result, err
			}
			if active {
				return result, fmt.Errorf("closed selected execution retains unsettled effects")
			}
			result.Disposition = runfork.SelectedForkRecoveryControlOnly
			if snapshot.State.Terminal() {
				result.Disposition = runfork.SelectedForkRecoveryTerminal
			}
			return result, nil
		}
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
	if binding.ForkPoint.Kind == runfork.RunForkPointDeploymentRevision && !snapshot.State.Terminal() {
		resume, err := selectedFiniteFeedRecoveryOperationTx(ctx, tx, binding, snapshot.BundleHash, !sqlite)
		if err != nil {
			return result, err
		}
		if resume != nil && snapshot.State != runlifecycle.StatePaused {
			return result, fmt.Errorf("selected finite-feed recovery requires paused child, got %s", snapshot.State)
		}
		var pins []durabledata.Pin
		var topologies []runfork.RunForkSelectedContractAgentTopology
		if resume != nil {
			pins, err = selectedFiniteFeedRecoveryPinsTx(ctx, tx, binding.ForkRunID, snapshot.BundleHash)
			if err != nil {
				return result, err
			}
			topologies, err = selectedFiniteFeedRecoveryTopologiesTx(ctx, tx, binding.ForkRunID, snapshot.BundleHash)
			if err != nil {
				return result, err
			}
		}
		if resume != nil && state == "quiesced" && failure == nil {
			if err := requireSelectedDeploymentDrainedTx(ctx, tx, runID); err != nil {
				return result, fmt.Errorf("quiesced selected feed has unfinished work: %w", err)
			}
			result.Disposition = runfork.SelectedForkRecoveryActivateFiniteFeed
			result.Resume = &runfork.SelectedForkFiniteFeedResume{Operation: resume.Request, ForkRunStatus: string(snapshot.State), Pins: pins, AgentTopologies: topologies}
			return result, nil
		}
		if resume != nil && (state == "prepared" || state == "running" || state == "closed") && failure == nil {
			if state != "closed" {
				res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions
					SET state='closed',fence_generation=fence_generation+1,lease_expires_at=NULL,terminal_at=$2,updated_at=$2
					WHERE execution_id=$1 AND state=$3 AND failure IS NULL`, result.ExecutionID, req.Effects.Now(), state)
				if err := requireExactlyOneMutation(res, err, "fence selected finite-feed predecessor"); err != nil {
					return result, err
				}
			}
			result.Disposition = runfork.SelectedForkRecoveryResumeFiniteFeed
			result.Resume = &runfork.SelectedForkFiniteFeedResume{Operation: resume.Request, ForkRunStatus: string(snapshot.State), Pins: pins, AgentTopologies: topologies}
			return result, nil
		}
	}
	if state == "failed" && failure == nil {
		return result, fmt.Errorf("failed selected execution lacks failure evidence")
	}
	if (snapshot.State.Terminal() && state == "closed") || ((state == "closed" || state == "quiesced") && failure == nil) {
		active, err := selectedForkActiveEffectsTx(ctx, tx, result.ExecutionID)
		if err != nil {
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
	result.Effects, err = owner.RecoverSelectedForkEffectsTx(ctx, attempt, result.ExecutionID, req.Effects)
	if err != nil {
		return result, err
	}
	if binding.ForkPoint.Kind == runfork.RunForkPointDeploymentRevision {
		if err := settleInterruptedSelectedFiniteFeedOperationTx(ctx, tx, binding, snapshot.BundleHash, *failure,
			result.Effects.OutcomeUncertain != 0 || failure.Class == failures.ClassOutcomeUncertain, req.Effects.Now(), !sqlite); err != nil {
			return result, err
		}
	}
	if !snapshot.State.Terminal() {
		if _, _, err := owner.MarkTerminalTx(ctx, attempt, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateFailed, Failure: failure, EndedAt: req.Effects.Now()}); err != nil {
			return result, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed' WHERE execution_id=$1 AND state='failed'`, result.ExecutionID); err != nil {
		return result, err
	}
	result.Disposition = runfork.SelectedForkRecoveryFailed
	return result, nil
}

func selectedFiniteFeedRecoveryPinsTx(ctx context.Context, tx *sql.Tx, forkRunID, bundleHash string) ([]durabledata.Pin, error) {
	rows, err := tx.QueryContext(ctx, `SELECT p.run_id,r.status,p.flow_path,p.event_name,p.schema_digest,p.version_id,p.selection,
		i.source_resource_version_id,i.deployment_schema_digest,i.bundle_hash
		FROM resource_version_pins p JOIN runs r ON r.run_id=p.run_id
		LEFT JOIN fan_out_intents i ON i.run_id=p.run_id AND i.origin_kind='deployment'
			AND i.source_resource_flow_path=p.flow_path AND i.source_resource_event_name=p.event_name
		WHERE p.run_id=$1 ORDER BY p.flow_path,p.event_name`, forkRunID)
	if err != nil {
		return nil, fmt.Errorf("load selected finite-feed pins: %w", err)
	}
	defer rows.Close()
	var pins []durabledata.Pin
	seen := make(map[durabledata.DeclarationRef]struct{})
	for rows.Next() {
		var pin durabledata.Pin
		var version, schema, feedBundle sql.NullString
		if err := rows.Scan(&pin.RunID, &pin.RunState, &pin.Declaration.FlowPath, &pin.Declaration.EventName,
			&pin.SchemaDigest, &pin.VersionID, &pin.Selection, &version, &schema, &feedBundle); err != nil {
			return nil, fmt.Errorf("decode selected finite-feed pin: %w", err)
		}
		if pin.RunID != forkRunID || pin.Validate() != nil || !version.Valid || version.String != string(pin.VersionID) ||
			!schema.Valid || schema.String != string(pin.SchemaDigest) || !feedBundle.Valid || feedBundle.String != bundleHash {
			return nil, fmt.Errorf("selected finite-feed pin conflicts with fork run")
		}
		if _, duplicate := seen[pin.Declaration]; duplicate {
			return nil, fmt.Errorf("selected finite-feed pin has duplicate feed declaration %s", pin.Declaration.Key())
		}
		seen[pin.Declaration] = struct{}{}
		pins = append(pins, pin)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read selected finite-feed pins: %w", err)
	}
	var feeds int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, forkRunID).Scan(&feeds); err != nil {
		return nil, fmt.Errorf("count selected finite feeds for pins: %w", err)
	}
	if feeds == 0 || feeds != len(pins) {
		return nil, fmt.Errorf("selected finite-feed pin count %d conflicts with %d admitted feeds", len(pins), feeds)
	}
	return pins, nil
}

func selectedFiniteFeedRecoveryTopologiesTx(ctx context.Context, tx *sql.Tx, forkRunID, bundleHash string) ([]runfork.RunForkSelectedContractAgentTopology, error) {
	rows, err := tx.QueryContext(ctx, `SELECT f.instance_path,f.flow_template,f.mode,r.plan
		FROM flow_instances f LEFT JOIN flow_instance_runtime_readiness r
			ON r.run_id=f.run_id AND r.instance_path=f.instance_path
		WHERE f.run_id=$1 ORDER BY f.instance_path`, forkRunID)
	if err != nil {
		return nil, fmt.Errorf("load selected finite-feed workflow readiness: %w", err)
	}
	defer rows.Close()
	var topologies []runfork.RunForkSelectedContractAgentTopology
	seen := make(map[string]struct{})
	for rows.Next() {
		var path, template, mode string
		var raw []byte
		if err := rows.Scan(&path, &template, &mode, &raw); err != nil {
			return nil, fmt.Errorf("decode selected finite-feed workflow readiness: %w", err)
		}
		if path == "" || template == "" {
			return nil, fmt.Errorf("selected finite-feed workflow has incomplete identity")
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, fmt.Errorf("selected finite-feed workflow %s appears more than once", path)
		}
		seen[path] = struct{}{}
		switch mode {
		case "static":
			if len(raw) != 0 {
				return nil, fmt.Errorf("static selected finite-feed workflow %s has template readiness", path)
			}
		case "template":
			if len(raw) == 0 {
				return nil, fmt.Errorf("template selected finite-feed workflow %s lacks readiness", path)
			}
			var plan runtimepipeline.DynamicFlowRuntimeReadinessPlan
			if err := canonicaljson.DecodeInto(raw, &plan); err != nil {
				return nil, fmt.Errorf("decode selected finite-feed readiness %s: %w", path, err)
			}
			normalized, err := plan.Normalized()
			if err != nil {
				return nil, fmt.Errorf("validate selected finite-feed readiness %s: %w", path, err)
			}
			encoded, err := json.Marshal(normalized)
			if err != nil || !workflowCommitJSONEqual(raw, encoded) || normalized.RunID != forkRunID ||
				normalized.BundleHash != bundleHash || normalized.Identity.InstancePath != path || normalized.Identity.TemplateID != template {
				return nil, fmt.Errorf("selected finite-feed readiness %s conflicts with committed workflow", path)
			}
			fingerprint, err := canonicaljson.Hash(normalized)
			if err != nil {
				return nil, fmt.Errorf("hash selected finite-feed readiness %s: %w", path, err)
			}
			admission, err := agenttopology.FlowReadinessAdmission(forkRunID, path, fingerprint)
			if err != nil {
				return nil, fmt.Errorf("admit selected finite-feed readiness %s: %w", path, err)
			}
			for _, agent := range normalized.Agents {
				topologies = append(topologies, runfork.RunForkSelectedContractAgentTopology{Identity: agent.Identity, Admission: admission})
			}
		default:
			return nil, fmt.Errorf("selected finite-feed workflow %s has unsupported mode %s", path, mode)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read selected finite-feed workflow readiness: %w", err)
	}
	var orphaned int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instance_runtime_readiness r
		LEFT JOIN flow_instances f ON f.run_id=r.run_id AND f.instance_path=r.instance_path
		WHERE r.run_id=$1 AND f.run_id IS NULL`, forkRunID).Scan(&orphaned); err != nil {
		return nil, fmt.Errorf("count orphaned selected finite-feed readiness: %w", err)
	}
	if orphaned != 0 {
		return nil, fmt.Errorf("selected finite-feed recovery has %d orphaned readiness plans", orphaned)
	}
	return topologies, nil
}

func settleInterruptedSelectedFiniteFeedOperationTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, bundleHash string, failure failures.Envelope, uncertain bool, now time.Time, postgres bool) error {
	if binding.ForkPoint.Kind != runfork.RunForkPointDeploymentRevision {
		return fmt.Errorf("interrupted finite-feed settlement requires deployment revision")
	}
	operation, err := selectedForkOperationForRecoveryTx(ctx, tx, binding, bundleHash, postgres)
	if err != nil {
		return err
	}
	if operation.Status != runfork.ForkOperationMaterialized {
		return fmt.Errorf("interrupted finite-feed operation is %s, not materialized", operation.Status)
	}
	code := strings.TrimSpace(failure.Detail.Code)
	if code == "" {
		return fmt.Errorf("interrupted finite-feed failure has no typed code")
	}
	terminal := runfork.ForkOperationFailure{Code: code, Reason: failure.Message}
	if err := terminal.Validate(); err != nil {
		return err
	}
	raw, err := canonicaljson.Bytes(terminal)
	if err != nil {
		return err
	}
	status := runfork.ForkOperationFailed
	if uncertain {
		status = runfork.ForkOperationUncertain
	}
	query := `UPDATE run_fork_operations SET state=$2, failure_json=$3, updated_at=$4
		WHERE operation_id=$1 AND fork_run_id=$5 AND selected_binding_id=$6 AND state='materialized'`
	if postgres {
		query = strings.Replace(query, "failure_json=$3", "failure_json=$3::jsonb", 1)
	}
	mutation, err := tx.ExecContext(ctx, query, operation.Request.OperationID, status, string(raw), now.UTC(), binding.ForkRunID, binding.BindingID)
	if err != nil {
		return err
	}
	return requireExactlyOneMutation(mutation, nil, "settle interrupted finite-feed operation")
}

// A finite feed is resumable only from its permanent operation and exact
// committed feed checkpoint. External effects remain under their existing
// uncertain/failed recovery policy, never under a replaying successor.
func selectedFiniteFeedRecoveryOperationTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, bundleHash string, postgres bool) (*runfork.ForkOperationRecord, error) {
	if binding.ForkPoint.Kind != runfork.RunForkPointDeploymentRevision {
		return nil, nil
	}
	operation, err := selectedForkOperationForRecoveryTx(ctx, tx, binding, bundleHash, postgres)
	if err != nil {
		return nil, err
	}
	if operation.Status != runfork.ForkOperationMaterialized {
		return nil, nil
	}
	var feeds, unsafeEffects int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, binding.ForkRunID).Scan(&feeds); err != nil {
		return nil, fmt.Errorf("read selected recovery feed checkpoint: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations o WHERE o.selected_execution_id IN
		(SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)
		AND (o.state IS NULL OR o.state<>'settled'
			OR NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a WHERE a.operation_id=o.operation_id)
			OR EXISTS (SELECT 1 FROM runtime_external_effect_attempts a WHERE a.operation_id=o.operation_id AND (a.state IS NULL OR a.state<>'settled')))`,
		binding.ForkRunID).Scan(&unsafeEffects); err != nil {
		return nil, fmt.Errorf("read selected recovery effect inventory: %w", err)
	}
	if feeds == 0 || unsafeEffects != 0 {
		return nil, nil
	}
	return &operation, nil
}

func selectedForkActiveEffectsTx(ctx context.Context, tx *sql.Tx, executionID string) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1 AND a.state IN ('authorized','launched','response_observed'))`, executionID).Scan(&active)
	return active, err
}

func sameSelectedForkPointIdentity(a, b runfork.RunForkPoint) bool {
	return a.Kind == b.Kind && a.Revision == b.Revision && a.EventID == b.EventID
}

func selectedForkOperationForRecoveryTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, bundleHash string, postgres bool) (runfork.ForkOperationRecord, error) {
	var operationID string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(operation_id AS TEXT) FROM run_fork_operations WHERE fork_run_id=$1`, binding.ForkRunID).Scan(&operationID); err != nil {
		return runfork.ForkOperationRecord{}, fmt.Errorf("load permanent selected fork operation: %w", err)
	}
	record, found, err := loadForkOperationByIDTx(ctx, tx, operationID, postgres)
	if err != nil {
		return runfork.ForkOperationRecord{}, err
	}
	if !found || record.ForkRunID != binding.ForkRunID || record.BindingID != binding.BindingID ||
		record.Request.SourceRunID != binding.SourceRunID || record.Request.TargetBundleHash != bundleHash ||
		record.Request.ResolvedPoint == nil || !sameSelectedForkPointIdentity(*record.Request.ResolvedPoint, binding.ForkPoint) ||
		record.Request.ForkEventID != binding.ForkEventID || record.Request.ContractSelection != binding.ContractSelection {
		return runfork.ForkOperationRecord{}, fmt.Errorf("permanent selected fork operation conflicts with exact binding")
	}
	return record, nil
}

func selectedForkActivatedOperationTx(ctx context.Context, tx *sql.Tx, forkRunID, bindingID string, point runfork.RunForkPoint, postgres bool) (bool, error) {
	var operationID string
	err := tx.QueryRowContext(ctx, `SELECT CAST(operation_id AS TEXT) FROM run_fork_operations WHERE fork_run_id=$1`, forkRunID).Scan(&operationID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	record, found, err := loadForkOperationByIDTx(ctx, tx, operationID, postgres)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("selected recovery operation disappeared during exact lookup")
	}
	if record.ForkRunID != forkRunID || record.BindingID != bindingID || record.Request.ResolvedPoint == nil ||
		record.Request.ResolvedPoint.Kind != point.Kind || record.Request.ResolvedPoint.Revision != point.Revision ||
		record.Request.ResolvedPoint.EventID != point.EventID {
		return false, fmt.Errorf("selected recovery operation conflicts with exact binding")
	}
	return record.Status == runfork.ForkOperationActivated, nil
}
