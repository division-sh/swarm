package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// AuthorizeRetainedGrantLifecycleTx runs inside the retained session's named
// lifecycle transaction, so selected execution fencing cannot race the write.
func AuthorizeRetainedGrantLifecycleTx(ctx context.Context, tx *sql.Tx, req manager.AgentLifecycleTransition, sqlite bool) error {
	query := `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`
	var raw []byte
	if err := tx.QueryRowContext(ctx, query, req.ProcessBinding.GenerationGrantID).Scan(&raw); err != nil {
		return fmt.Errorf("load lifecycle generation grant: %w", err)
	}
	var evidence startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &evidence); err != nil {
		return err
	}
	if err := evidence.Validate(); err != nil {
		return err
	}
	expected := manager.ProcessExecutionBinding{
		ProcessAuthorityID: evidence.ProcessAuthorityID, ProcessOwnerID: evidence.ProcessOwnerID,
		ProcessBootID: evidence.ProcessBootID, GenerationGrantID: evidence.GrantID,
		BundleHash: evidence.BundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID,
		RuntimeGeneration: evidence.RuntimeGeneration,
	}
	if req.ProcessBinding != expected || evidence.State == startupownership.GrantRetired {
		return errors.New("lifecycle generation grant is retired or differs from the retained process binding")
	}
	if evidence.SelectedFork == nil {
		return nil
	}
	if req.Identity.RunID != evidence.SelectedFork.ForkRunID {
		return errors.New("selected-fork grant cannot mutate another run")
	}
	return ProveSelectedForkGenerationGrantTx(ctx, tx, evidence, sqlite)
}

// ProveSelectedForkGenerationGrantTx joins the execution with its immutable
// binding and run coordinate. These three facts collectively authorize the
// grant; a matching bundle or readiness projection alone does not.
func ProveSelectedForkGenerationGrantTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, sqlite bool) error {
	if err := evidence.Validate(); err != nil {
		return err
	}
	if tx == nil || evidence.SelectedFork == nil || evidence.State == startupownership.GrantRetired {
		return errors.New("selected-fork grant proof requires a transaction and live selected evidence")
	}
	binding := evidence.SelectedFork
	query := `SELECT execution.binding_id, execution.fork_run_id,
		execution.generation, execution.fence_generation, execution.execution_owner,
		execution.admission_fingerprint, execution.container_plan_fingerprint,
		execution.actor_census_fingerprint, execution.effective_config_fingerprint,
		run.bundle_hash
		FROM run_fork_selected_contract_runtime_executions AS execution
		JOIN run_fork_selected_contract_bindings AS binding ON binding.binding_id = execution.binding_id
		JOIN runs AS run ON run.run_id = binding.fork_run_id
		WHERE execution.execution_id = $1
		AND execution.fork_run_id = binding.fork_run_id
		AND execution.source_run_id = binding.source_run_id
		AND execution.fork_event_id = binding.fork_event_id
		AND execution.state = 'running'
		AND (binding.mode = 'selected_contracts' OR (binding.mode = 'bundle_hash' AND binding.bundle_hash = run.bundle_hash))`
	args := []any{binding.ExecutionID}
	if sqlite {
		query += ` AND execution.lease_expires_at > $2`
		args = append(args, time.Now().UTC())
	} else {
		query += ` AND execution.lease_expires_at > CURRENT_TIMESTAMP FOR UPDATE OF execution, binding, run`
	}
	actual := startupownership.SelectedForkGrantBinding{ExecutionID: binding.ExecutionID}
	var bundleHash string
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&actual.BindingID, &actual.ForkRunID, &actual.ExecutionGeneration,
		&actual.FenceGeneration, &actual.ExecutionOwner, &actual.AdmissionFingerprint,
		&actual.ContainerPlanFingerprint, &actual.ActorCensusFingerprint,
		&actual.EffectiveConfigFingerprint, &bundleHash)
	if err == sql.ErrNoRows {
		return errors.New("selected-fork grant execution is absent, expired, terminal or inconsistent with its binding")
	}
	if err != nil {
		return fmt.Errorf("prove selected-fork grant execution: %w", err)
	}
	if actual != *binding || bundleHash != evidence.BundleHash {
		return errors.New("selected-fork grant binding, execution fence or fingerprints changed")
	}
	return nil
}
