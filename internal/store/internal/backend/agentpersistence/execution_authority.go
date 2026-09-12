package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/google/uuid"
)

// AuthorizeGenerationMutationTx revalidates exact current grant authority while
// holding the store mutation fence, whether the connection is retained or pooled.
func AuthorizeGenerationMutationTx(ctx context.Context, tx *sql.Tx, req manager.AgentLifecycleTransition, sqlite bool) error {
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
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
	expected := processBindingForGrant(evidence)
	if req.ProcessBinding != expected || evidence.State == startupownership.GrantRetired {
		return errors.New("lifecycle generation grant is retired or differs from the retained process binding")
	}
	ownership, err := inspectRunExecutionOwnershipTx(ctx, tx, evidence, req.Identity.RunID, sqlite)
	if err != nil {
		return err
	}
	if ownership != manager.RunExecutionOwned {
		// Complete-source-set reconciliation may terminalize a removed ordinary
		// declaration across source coordinates, never acquire its execution.
		// The topology owner below must still prove absence from the current plan.
		if ownership == manager.RunExecutionOtherNormalSource && evidence.SelectedFork == nil && req.Agent == nil && req.TargetPhase == manager.AgentLifecycleTerminated &&
			req.Topology.Authority.Kind == agenttopology.AuthorityStaticDeclarationPlan &&
			(req.OperationKind == "source_set_retire" || req.OperationKind == "process_takeover") {
			return nil
		}
		return fmt.Errorf("%w: %s", manager.ErrRunExecutionNotOwned, req.Identity.RunID)
	}
	return nil
}

func processBindingForGrant(evidence startupownership.GrantEvidence) manager.ProcessExecutionBinding {
	return manager.ProcessExecutionBinding{
		ProcessAuthorityID: evidence.ProcessAuthorityID, ProcessOwnerID: evidence.ProcessOwnerID,
		ProcessBootID: evidence.ProcessBootID, GenerationGrantID: evidence.GrantID,
		BundleHash: evidence.BundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID,
		RuntimeGeneration: evidence.RuntimeGeneration,
	}
}

// InspectRunExecutionOwnershipTx verifies current grant evidence and classifies
// the durable run binding in the same snapshot. A selected binding reserves the
// child at materialization commit, before an execution or grant exists.
func InspectRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	if tx == nil {
		return 0, errors.New("run execution ownership requires a transaction")
	}
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return 0, err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&raw); err != nil {
		return 0, fmt.Errorf("load run execution generation grant: %w", err)
	}
	var current startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &current); err != nil {
		return 0, err
	}
	actual, err := canonicaljson.Bytes(current)
	if err != nil {
		return 0, err
	}
	expected, err := canonicaljson.Bytes(evidence)
	if err != nil {
		return 0, err
	}
	if string(actual) != string(expected) {
		return 0, errors.New("run execution generation grant is no longer current")
	}
	return inspectRunExecutionOwnershipTx(ctx, tx, evidence, runID, sqlite)
}

func inspectRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	if err := evidence.Validate(); err != nil {
		return 0, err
	}
	if evidence.State == startupownership.GrantRetired {
		return 0, errors.New("run execution generation grant is retired")
	}
	hash, bindingID, err := loadRunExecutionBindingTx(ctx, tx, runID, sqlite)
	if err != nil {
		return 0, err
	}
	if evidence.SelectedFork == nil {
		if bindingID.Valid {
			return manager.RunExecutionForeign, nil
		}
		if hash != evidence.BundleHash {
			return manager.RunExecutionOtherNormalSource, nil
		}
		return manager.RunExecutionOwned, nil
	}
	if err := ProveSelectedForkGenerationGrantTx(ctx, tx, evidence, sqlite); err != nil {
		return 0, err
	}
	if runID != evidence.SelectedFork.ForkRunID || !bindingID.Valid || bindingID.String != evidence.SelectedFork.BindingID || hash != evidence.BundleHash {
		return manager.RunExecutionForeign, nil
	}
	return manager.RunExecutionOwned, nil
}

func loadRunExecutionBindingTx(ctx context.Context, tx *sql.Tx, runID string, sqlite bool) (string, sql.NullString, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return "", sql.NullString{}, errors.New("run execution ownership requires a canonical nonzero run UUID")
	}
	query := `SELECT run.bundle_hash, binding.binding_id
		FROM runs AS run
		LEFT JOIN run_fork_selected_contract_bindings AS binding ON binding.fork_run_id=run.run_id
		WHERE run.run_id=$1`
	if !sqlite {
		query += ` FOR UPDATE OF run`
	}
	var hash string
	var bindingID sql.NullString
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&hash, &bindingID); err != nil {
		return "", sql.NullString{}, fmt.Errorf("load run execution binding: %w", err)
	}
	if err := bundleidentity.ValidateCanonicalHash(hash); err != nil {
		return "", sql.NullString{}, err
	}
	if bindingID.Valid {
		id, err := uuid.Parse(bindingID.String)
		if err != nil || id == uuid.Nil || id.String() != bindingID.String {
			return "", sql.NullString{}, errors.New("run execution binding contains an invalid binding UUID")
		}
	}
	return hash, bindingID, nil
}

// ProveSelectedForkGenerationGrantTx joins the execution with its immutable
// binding and run coordinate. These three facts collectively authorize the
// grant; a matching bundle or readiness projection alone does not.
func ProveSelectedForkGenerationGrantTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, sqlite bool) error {
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
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
		execution.declaration_plan_fingerprint, execution.declaration_plan,
		execution.preparation_fingerprint, execution.preparation_binding,
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
	var declarationRaw []byte
	var preparationRaw []byte
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&actual.BindingID, &actual.ForkRunID, &actual.ExecutionGeneration,
		&actual.FenceGeneration, &actual.ExecutionOwner, &actual.AdmissionFingerprint,
		&actual.ContainerPlanFingerprint, &actual.ActorCensusFingerprint,
		&actual.EffectiveConfigFingerprint, &actual.DeclarationPlanFingerprint, &declarationRaw,
		&actual.PreparationFingerprint, &preparationRaw, &bundleHash)
	if err == sql.ErrNoRows {
		return errors.New("selected-fork grant execution is absent, expired, terminal or inconsistent with its binding")
	}
	if err != nil {
		return fmt.Errorf("prove selected-fork grant execution: %w", err)
	}
	if actual != *binding || bundleHash != evidence.BundleHash {
		return errors.New("selected-fork grant binding, execution fence or fingerprints changed")
	}
	var declarations agenttopology.SelectedDeclarationPlan
	if err := canonicaljson.DecodeInto(declarationRaw, &declarations); err != nil {
		return err
	}
	if err := declarations.Validate(); err != nil {
		return err
	}
	if declarations.Revision != actual.DeclarationPlanFingerprint || declarations.BundleHash != bundleHash {
		return errors.New("selected-fork declaration plan differs from grant and target source")
	}
	var preparation runfork.SelectedForkPreparationBinding
	if err := canonicaljson.DecodeInto(preparationRaw, &preparation); err != nil {
		return err
	}
	fingerprint, err := preparation.Fingerprint()
	if err != nil {
		return err
	}
	if fingerprint != actual.PreparationFingerprint || preparation.ForkRunID != actual.ForkRunID ||
		preparation.Coordinates.BundleHash != evidence.BundleHash ||
		preparation.Coordinates.ProcessAuthorityID != evidence.ProcessAuthorityID ||
		preparation.Coordinates.ProcessOwnerID != evidence.ProcessOwnerID ||
		preparation.Coordinates.ProcessBootID != evidence.ProcessBootID {
		return errors.New("selected generation differs from prepared process or binding")
	}
	if err := managedcapabilitystore.ProveSelectedPreparationReceiptsTx(ctx, tx, preparation.SelectedForkPreparation, sqlite); err != nil {
		return err
	}
	if evidence.State != startupownership.GrantPrepared {
		if err := preparation.ValidateSurfaceIDs(evidence.ProbeSurfaceIDs); err != nil {
			return err
		}
	} else if len(evidence.ProbeSurfaceIDs) != 0 {
		return errors.New("issued selected generation cannot pre-settle probe receipts")
	}
	return nil
}
