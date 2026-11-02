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
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
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
	ownership, err := inspectGenerationMutationRunOwnershipTx(ctx, tx, evidence, req, sqlite)
	if err != nil {
		return err
	}
	if ownership != manager.RunExecutionOwned {
		if evidence.SelectedFork == nil && req.OperationKind == "self_release" && req.Agent == nil &&
			req.ExpectedPhase == manager.AgentLifecycleRunning && req.TargetPhase == manager.AgentLifecycleRegistered && req.RunMode == manager.AgentRunModeStopped &&
			req.TargetEpoch == req.ExpectedEpoch && req.TargetGeneration == req.ExpectedGeneration && req.Subordinate.Action == sessions.LifecycleMutationNone {
			retained, err := sourceadmission.ObserveRetainedLiveHandoff(ctx, tx, req.Identity.RunID, evidence.BundleHash)
			if err != nil {
				return err
			}
			if retained {
				return generationauthority.RequireAdmittedOrdinaryGrantTx(ctx, tx, evidence, evidence.BundleHash, sqlite)
			}
		}
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

func inspectGenerationMutationRunOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, req manager.AgentLifecycleTransition, sqlite bool) (manager.RunExecutionOwnership, error) {
	var ownership manager.RunExecutionOwnership
	var err error
	if (evidence.State == startupownership.GrantPrepared || evidence.State == startupownership.GrantProbeSettled) && evidence.SelectedFork == nil && req.OperationKind == "process_takeover" &&
		req.Agent == nil && req.Subordinate.Action == sessions.LifecycleMutationNone && req.TargetPhase == req.ExpectedPhase &&
		req.TargetEpoch == req.ExpectedEpoch+1 && req.TargetGeneration == req.ExpectedGeneration+1 {
		ownership, err = inspectPreparedRunTakeoverTx(ctx, tx, evidence, req.Identity.RunID, sqlite)
		if err == nil && ownership == manager.RunExecutionOwned {
			previous, _, exists, readErr := loadAgentLifecycleState(ctx, tx, req.Identity, sqlite, true)
			if readErr != nil {
				return 0, readErr
			}
			if !exists || previous.RunMode != req.RunMode || previous.ConfigRevision != req.ConfigRevision || !previous.Topology.Equal(req.Topology) {
				return 0, lifecycleConflict(req, previous, exists)
			}
		}
	} else {
		ownership, err = inspectRunExecutionOwnershipTx(ctx, tx, evidence, req.Identity.RunID, sqlite)
	}
	return ownership, err
}

// AuthorizeDynamicFlowActivationTx admits forward progress only under the
// exact currently admitted generation and run binding in the mutation TX.
func AuthorizeDynamicFlowActivationTx(ctx context.Context, tx *sql.Tx, binding runtimeprocessbinding.Binding, runID string, sqlite bool) error {
	return authorizeDynamicFlowGrantTx(ctx, tx, binding, runID, sqlite, flowGrantExecution)
}

// AuthorizeDynamicFlowTakeoverPreparationTx permits only a non-executable
// lifecycle rebind before the replacement grant is admitted for execution.
func AuthorizeDynamicFlowTakeoverPreparationTx(ctx context.Context, tx *sql.Tx, binding runtimeprocessbinding.Binding, runID string, sqlite bool) error {
	return authorizeDynamicFlowGrantTx(ctx, tx, binding, runID, sqlite, flowGrantTakeoverPreparation)
}

// AuthorizeDynamicFlowTopologyPreparationTx admits only pre-run agent
// registration under the current ordinary grant, before execution admission.
func AuthorizeDynamicFlowTopologyPreparationTx(ctx context.Context, tx *sql.Tx, binding runtimeprocessbinding.Binding, runID string, sqlite bool) error {
	return authorizeDynamicFlowGrantTx(ctx, tx, binding, runID, sqlite, flowGrantTopologyPreparation)
}

type flowGrantUse uint8

const (
	flowGrantExecution flowGrantUse = iota + 1
	flowGrantTakeoverPreparation
	flowGrantTopologyPreparation
)

func authorizeDynamicFlowGrantTx(ctx context.Context, tx *sql.Tx, binding runtimeprocessbinding.Binding, runID string, sqlite bool, use flowGrantUse) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, binding.GenerationGrantID).Scan(&raw); err != nil {
		if err == context.Canceled {
			return err
		}
		return fmt.Errorf("load flow activation generation grant: %w", err)
	}
	var evidence startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &evidence); err != nil {
		return err
	}
	if err := evidence.Validate(); err != nil {
		return err
	}
	if !binding.Equal(processBindingForGrant(evidence)) {
		return errors.New("flow activation requires the exact generation grant binding")
	}
	if err := proveGrantProcessTx(ctx, tx, evidence, sqlite, true); err != nil {
		return err
	}
	if err := requireDynamicFlowGrantUse(evidence, use); err != nil {
		return err
	}
	if evidence.SelectedFork == nil {
		query := `SELECT revision FROM agent_topology_source_set_head WHERE singleton_id=1`
		if !sqlite {
			query += ` FOR UPDATE`
		}
		var revision string
		if err := tx.QueryRowContext(ctx, query).Scan(&revision); err != nil {
			if err == context.Canceled {
				return err
			}
			return fmt.Errorf("load flow activation source-set head: %w", err)
		}
		if revision != evidence.SourceSetRevision {
			return errors.New("flow activation generation grant source-set revision is not current")
		}
	}
	var ownership manager.RunExecutionOwnership
	var err error
	if use == flowGrantTakeoverPreparation && (evidence.State == startupownership.GrantPrepared || evidence.State == startupownership.GrantProbeSettled) {
		ownership, err = inspectPreparedRunTakeoverTx(ctx, tx, evidence, runID, sqlite)
	} else {
		ownership, err = inspectRunExecutionOwnershipTx(ctx, tx, evidence, runID, sqlite)
	}
	if err != nil {
		return err
	}
	if ownership != manager.RunExecutionOwned {
		return fmt.Errorf("%w: %s", manager.ErrRunExecutionNotOwned, runID)
	}
	return nil
}

func requireDynamicFlowGrantUse(evidence startupownership.GrantEvidence, use flowGrantUse) error {
	switch use {
	case flowGrantExecution:
		if evidence.State != startupownership.GrantAdmitted {
			return errors.New("flow activation requires the exact admitted generation grant")
		}
	case flowGrantTakeoverPreparation:
		if evidence.SelectedFork != nil || (evidence.State != startupownership.GrantPrepared && evidence.State != startupownership.GrantProbeSettled && evidence.State != startupownership.GrantAdmitted) {
			return errors.New("flow readiness process takeover requires a live ordinary generation grant")
		}
	case flowGrantTopologyPreparation:
		if evidence.SelectedFork != nil || (evidence.State != startupownership.GrantPrepared && evidence.State != startupownership.GrantProbeSettled) {
			return errors.New("flow topology preparation requires a current pre-admission ordinary generation grant")
		}
	default:
		return errors.New("unknown flow generation grant use")
	}
	return nil
}

// Startup takeover rebinds an existing cell without admitting its execution.
func inspectPreparedRunTakeoverTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	if (evidence.State != startupownership.GrantPrepared && evidence.State != startupownership.GrantProbeSettled) || evidence.SelectedFork != nil {
		return 0, errors.New("startup lifecycle takeover requires its ordinary preparation grant")
	}
	if err := generationauthority.RequireCurrentGrantTx(ctx, tx, evidence, sqlite); err != nil {
		return 0, err
	}
	if err := proveGrantProcessTx(ctx, tx, evidence, sqlite, true); err != nil {
		return 0, err
	}
	plan, present, err := generationauthority.ReadSourceSetTx(ctx, tx, sqlite, true)
	if err != nil {
		return 0, err
	}
	root, rootPresent, err := correlation.RootAdmissionFromContext(ctx)
	if err != nil || !rootPresent || root.BundleHash() != evidence.BundleHash {
		return 0, errors.Join(err, errors.New("startup lifecycle takeover requires its exact admitted root"))
	}
	coordinate, err := agenttopology.NewSourceCoordinate(root)
	if err != nil {
		return 0, err
	}
	if !present || plan.Revision != evidence.SourceSetRevision || !generationauthority.SourceSetContains(plan, coordinate) {
		return 0, errors.New("startup lifecycle takeover source set is not current")
	}
	hash, binding, err := loadRunExecutionBindingModeTx(ctx, tx, runID, sqlite, true)
	if err != nil {
		return 0, err
	}
	if binding.Valid && hash == evidence.BundleHash {
		retained, err := sourceadmission.ObserveRetainedLiveHandoff(ctx, tx, runID, hash)
		if err != nil || !retained {
			return manager.RunExecutionForeign, err
		}
		return manager.RunExecutionOwned, nil
	}
	return classifyOrdinaryRunSource(hash, binding, evidence.BundleHash), nil
}

// FlowActivationPredecessorIsFromAnotherProcessTx permits reconstruction after
// process replacement, never same-process replacement of unsettled resources.
// The caller must first authorize the current binding in this transaction.
func FlowActivationPredecessorIsFromAnotherProcessTx(ctx context.Context, tx *sql.Tx, predecessorGrantID string, current runtimeprocessbinding.Binding) (bool, error) {
	if err := current.Validate(); err != nil {
		return false, err
	}
	if predecessorGrantID == current.GenerationGrantID {
		return false, nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, predecessorGrantID).Scan(&raw); err != nil {
		return false, fmt.Errorf("load predecessor flow activation grant: %w", err)
	}
	var predecessor startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &predecessor); err != nil {
		return false, err
	}
	if err := predecessor.Validate(); err != nil {
		return false, err
	}
	return predecessor.ProcessAuthorityID != current.ProcessAuthorityID || predecessor.ProcessBootID != current.ProcessBootID, nil
}

// VerifyFlowActivationRetirementBindingTx retains exact settlement authority
// for a retired grant without permitting new forward progress under it.
func VerifyFlowActivationRetirementBindingTx(ctx context.Context, tx *sql.Tx, retired runtimeprocessbinding.Binding) error {
	if err := retired.Validate(); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, retired.GenerationGrantID).Scan(&raw); err != nil {
		if err == context.Canceled {
			return err
		}
		return fmt.Errorf("load retiring flow activation grant: %w", err)
	}
	var retiredGrant startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &retiredGrant); err != nil {
		return err
	}
	if err := retiredGrant.Validate(); err != nil {
		return err
	}
	if !retired.Equal(processBindingForGrant(retiredGrant)) {
		return errors.New("flow activation retirement binding differs from its generation grant")
	}
	return nil
}

// A grant stamp identifies the process which installed the attachment. It is
// evidence, not attachment identity; write admission still checks the live grant.
func VerifyFlowActivationAttemptProcessTx(ctx context.Context, tx *sql.Tx, stamp string, binding runtimeprocessbinding.Binding) error {
	if err := VerifyFlowActivationRetirementBindingTx(ctx, tx, binding); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, stamp).Scan(&raw); err != nil {
		if err == context.Canceled {
			return err
		}
		return fmt.Errorf("load attachment process evidence: %w", err)
	}
	var evidence startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &evidence); err != nil {
		return err
	}
	if err := evidence.Validate(); err != nil {
		return err
	}
	if !processBindingForGrant(evidence).SameProcessExecution(binding) {
		return errors.New("flow attachment belongs to a different process execution binding")
	}
	return nil
}

// FlowActivationRetirementHasForeignSuccessorTx permits a joined predecessor
// to finish after takeover without treating an invented binding as ownership.
func FlowActivationRetirementHasForeignSuccessorTx(ctx context.Context, tx *sql.Tx, successorGrantID string, retired runtimeprocessbinding.Binding) (bool, error) {
	if err := VerifyFlowActivationRetirementBindingTx(ctx, tx, retired); err != nil {
		return false, err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, successorGrantID).Scan(&raw); err != nil {
		return false, fmt.Errorf("load successor flow activation grant: %w", err)
	}
	var successor startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &successor); err != nil {
		return false, err
	}
	if err := successor.Validate(); err != nil {
		return false, err
	}
	return successor.ProcessAuthorityID != retired.ProcessAuthorityID || successor.ProcessBootID != retired.ProcessBootID, nil
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
	return inspectCurrentRunExecutionOwnershipTx(ctx, tx, evidence, runID, sqlite, true)
}

// ObserveOrdinaryRunExecutionOwnershipTx uses the same exact-grant and run
// classification as mutation admission, without acquiring mutation locks. The
// caller must hold a read snapshot; this result does not authorize mutation.
func ObserveOrdinaryRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	if evidence.SelectedFork != nil {
		return 0, errors.New("ordinary run observation cannot inspect selected-fork authority")
	}
	return inspectCurrentRunExecutionOwnershipTx(ctx, tx, evidence, runID, sqlite, false)
}

// Selected control preflight observes the same exact grant and process without
// taking mutation locks. The eventual writer still calls the locked inspector.
func ObserveRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	return inspectCurrentRunExecutionOwnershipTx(ctx, tx, evidence, runID, sqlite, false)
}

func inspectCurrentRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite, lock bool) (manager.RunExecutionOwnership, error) {
	if tx == nil {
		return 0, errors.New("run execution ownership requires a transaction")
	}
	if lock {
		if err := generationauthority.RequireCurrentGrantTx(ctx, tx, evidence, sqlite); err != nil {
			return 0, err
		}
	} else if err := generationauthority.ObserveCurrentGrantTx(ctx, tx, evidence); err != nil {
		return 0, err
	}
	return inspectRunExecutionOwnershipModeTx(ctx, tx, evidence, runID, sqlite, lock)
}

func inspectRunExecutionOwnershipTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite bool) (manager.RunExecutionOwnership, error) {
	return inspectRunExecutionOwnershipModeTx(ctx, tx, evidence, runID, sqlite, true)
}

func inspectRunExecutionOwnershipModeTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, runID string, sqlite, lock bool) (manager.RunExecutionOwnership, error) {
	if err := evidence.Validate(); err != nil {
		return 0, err
	}
	if evidence.State == startupownership.GrantRetired {
		return 0, errors.New("run execution generation grant is retired")
	}
	if err := proveGrantProcessTx(ctx, tx, evidence, sqlite, lock); err != nil {
		return 0, err
	}
	hash, bindingID, err := loadRunExecutionBindingModeTx(ctx, tx, runID, sqlite, lock)
	if err != nil {
		return 0, err
	}
	if evidence.SelectedFork == nil {
		if bindingID.Valid && hash == evidence.BundleHash {
			applicable, err := sourceadmission.ObserveLiveHandoff(ctx, tx, runID, hash)
			if err != nil || !applicable {
				return manager.RunExecutionForeign, err
			}
			if lock {
				if err := generationauthority.RequireAdmittedOrdinaryGrantTx(ctx, tx, evidence, hash, sqlite); err != nil {
					return 0, err
				}
			} else if evidence.State != startupownership.GrantAdmitted {
				return manager.RunExecutionForeign, nil
			}
			return manager.RunExecutionOwned, nil
		}
		return classifyOrdinaryRunSource(hash, bindingID, evidence.BundleHash), nil
	}
	if lock {
		if err := ProveSelectedForkGenerationGrantTx(ctx, tx, evidence, sqlite); err != nil {
			return 0, err
		}
	} else {
		current, err := ObserveSelectedForkGenerationGrantTx(ctx, tx, evidence, sqlite)
		if err != nil {
			return 0, err
		}
		if !current {
			return manager.RunExecutionForeign, nil
		}
	}
	if runID != evidence.SelectedFork.ForkRunID || !bindingID.Valid || bindingID.String != evidence.SelectedFork.BindingID || hash != evidence.BundleHash {
		return manager.RunExecutionForeign, nil
	}
	return manager.RunExecutionOwned, nil
}

func proveGrantProcessTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, sqlite, lock bool) error {
	var current bool
	var err error
	if lock {
		current, err = generationauthority.ProcessCoordinatesCurrentTx(ctx, tx, evidence.ProcessCoordinates(), sqlite)
	} else {
		current, err = generationauthority.ProcessCoordinatesCurrent(ctx, tx, evidence.ProcessCoordinates(), sqlite)
	}
	if err != nil {
		return err
	}
	if !current {
		return errors.New("generation grant process is no longer current")
	}
	return nil
}

func classifyOrdinaryRunSource(hash string, bindingID sql.NullString, sourceHash string) manager.RunExecutionOwnership {
	if bindingID.Valid {
		return manager.RunExecutionForeign
	}
	if hash != sourceHash {
		return manager.RunExecutionOtherNormalSource
	}
	return manager.RunExecutionOwned
}

// ObserveOrdinaryRunSource shares durable binding classification with grant
// admission. It proves only source applicability, never process possession or
// execution ownership; selected bindings require an exact committed live handoff.
func (s *AgentPostgresOwner) ObserveOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, errors.New("ordinary run source observation requires the selected PostgreSQL reader")
	}
	return observeOrdinaryRunSource(ctx, source, runID, false, s.backend.RunReadTransaction)
}

func (s *AgentSQLiteOwner) ObserveOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, errors.New("ordinary run source observation requires the selected SQLite reader")
	}
	return observeOrdinaryRunSource(ctx, source, runID, true, s.backend.RunReadTransaction)
}

func observeOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string, sqlite bool, read func(context.Context, func(context.Context, *sql.Tx) error) error) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	var matches bool
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		matches, err = ObserveOrdinaryRunSourceTx(ctx, tx, source, runID, sqlite)
		return err
	})
	return matches && err == nil, err
}

// ObserveOrdinaryRunSourceTx shares the caller's read snapshot. It supplies no
// execution admission, even when the retained handoff names an ordinary source.
func ObserveOrdinaryRunSourceTx(ctx context.Context, tx *sql.Tx, source correlation.SourceArtifactFact, runID string, sqlite bool) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	hash, binding, err := loadRunExecutionBindingModeTx(ctx, tx, runID, sqlite, false)
	if err != nil {
		return false, err
	}
	if binding.Valid && hash == source.BundleHash() {
		return sourceadmission.ObserveRetainedLiveHandoff(ctx, tx, runID, hash)
	}
	return classifyOrdinaryRunSource(hash, binding, source.BundleHash()) == manager.RunExecutionOwned, ctx.Err()
}

func loadRunExecutionBindingModeTx(ctx context.Context, tx *sql.Tx, runID string, sqlite, lock bool) (string, sql.NullString, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return "", sql.NullString{}, errors.New("run execution ownership requires a canonical nonzero run UUID")
	}
	query := `SELECT run.bundle_hash, binding.binding_id
		FROM runs AS run
		LEFT JOIN run_fork_selected_contract_bindings AS binding ON binding.fork_run_id=run.run_id
		WHERE run.run_id=$1`
	if !sqlite && lock {
		// This proof does not mutate the run or upgrade shared admission.
		query += ` FOR SHARE OF run`
	}
	var hash string
	var bindingID sql.NullString
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&hash, &bindingID); err != nil {
		if err == context.Canceled {
			return "", sql.NullString{}, err
		}
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
	// Revision-only timer admission holds the run before its execution row.
	// Do not let the joined proof take those rows in the reverse order.
	hash, bindingID, err := loadRunExecutionBindingModeTx(ctx, tx, binding.ForkRunID, sqlite, true)
	if err != nil {
		return err
	}
	if hash != evidence.BundleHash || !bindingID.Valid || bindingID.String != binding.BindingID {
		return errors.New("selected-fork generation grant run binding changed")
	}
	row, err := loadSelectedForkGrantExecutionTx(ctx, tx, *binding, sqlite)
	if err != nil {
		return err
	}
	actual, bundleHash := row.Binding, row.BundleHash
	declarationRaw, preparationRaw := row.Declaration, row.Preparation
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

type selectedForkGrantExecutionRow struct {
	Binding     startupownership.SelectedForkGrantBinding
	BundleHash  string
	Declaration []byte
	Preparation []byte
}

func loadSelectedForkGrantExecutionTx(ctx context.Context, tx *sql.Tx, binding startupownership.SelectedForkGrantBinding, sqlite bool) (selectedForkGrantExecutionRow, error) {
	row, err := loadSelectedForkGrantExecutionModeTx(ctx, tx, binding, sqlite, true, false)
	if errors.Is(err, sql.ErrNoRows) {
		return selectedForkGrantExecutionRow{}, errors.New("selected-fork grant execution is absent, expired, terminal or inconsistent with its binding")
	}
	return row, err
}

// ObserveSelectedForkGenerationGrantTx reports current serving eligibility,
// not mutation authority. Claim admission still validates and locks the full
// grant, preparation receipts, process, executor and run in its own attempt.
func ObserveSelectedForkGenerationGrantTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, sqlite bool) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	if tx == nil || evidence.SelectedFork == nil || evidence.State != startupownership.GrantAdmitted {
		return false, errors.New("selected serving observation requires an admitted grant and read transaction")
	}
	row, err := loadSelectedForkGrantExecutionModeTx(ctx, tx, *evidence.SelectedFork, sqlite, false, false)
	if errors.Is(err, sql.ErrNoRows) {
		materialized, phaseErr := sourceadmission.ObserveMaterializedSelectedGrant(ctx, tx, evidence)
		if phaseErr != nil || !materialized {
			return false, phaseErr
		}
		row, err = loadSelectedForkGrantExecutionModeTx(ctx, tx, *evidence.SelectedFork, sqlite, false, true)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
	}
	return err == nil && row.Binding == *evidence.SelectedFork && row.BundleHash == evidence.BundleHash, err
}

func loadSelectedForkGrantExecutionModeTx(ctx context.Context, tx *sql.Tx, binding startupownership.SelectedForkGrantBinding, sqlite, lock, materialized bool) (selectedForkGrantExecutionRow, error) {
	query := `SELECT execution.binding_id, execution.fork_run_id,
		execution.generation, execution.fence_generation, execution.execution_owner,
		execution.admission_fingerprint, execution.container_plan_fingerprint,
		execution.actor_census_fingerprint, execution.effective_config_fingerprint,
		execution.declaration_plan_fingerprint, execution.declaration_plan,
		execution.preparation_fingerprint, execution.preparation_binding,
		run.bundle_hash,
		EXISTS (SELECT 1 FROM run_fork_selected_contract_execution_live_parents AS parents
		 WHERE parents.execution_id=execution.execution_id AND parents.binding_id=execution.binding_id
		 AND parents.fork_run_id=execution.fork_run_id AND parents.source_run_id=execution.source_run_id
		 AND parents.fork_point_kind=execution.fork_point_kind AND parents.fork_revision=execution.fork_revision
		 AND parents.generation=execution.generation
		 AND (parents.fork_event_id=execution.fork_event_id
		      OR (parents.fork_event_id IS NULL AND execution.fork_event_id IS NULL)))
		FROM run_fork_selected_contract_runtime_executions AS execution
		JOIN run_fork_selected_contract_bindings AS binding ON binding.binding_id = execution.binding_id
		JOIN runs AS run ON run.run_id = binding.fork_run_id
		WHERE execution.execution_id = $1
		AND execution.fork_run_id = binding.fork_run_id
		AND execution.source_run_id = binding.source_run_id
		AND execution.fork_point_kind = binding.fork_point_kind
		AND execution.fork_revision = binding.fork_revision
		AND (execution.fork_event_id = binding.fork_event_id
		     OR (execution.fork_event_id IS NULL AND binding.fork_event_id IS NULL))
		AND execution.state = 'running'
		AND (binding.mode = 'selected_contracts' OR (binding.mode = 'bundle_hash' AND binding.bundle_hash = run.bundle_hash))`
	args := []any{binding.ExecutionID}
	if !lock && !materialized {
		query += ` AND run.status = 'running'`
	} else if !lock {
		query += ` AND run.status = 'paused'`
	}
	if sqlite {
		query += ` AND execution.lease_expires_at > $2`
		args = append(args, time.Now().UTC())
	} else {
		query += ` AND execution.lease_expires_at > CURRENT_TIMESTAMP`
		if lock {
			query += ` FOR SHARE OF execution, binding, run`
		}
	}
	row := selectedForkGrantExecutionRow{Binding: startupownership.SelectedForkGrantBinding{ExecutionID: binding.ExecutionID}}
	var liveParentsMatch bool
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&row.Binding.BindingID, &row.Binding.ForkRunID, &row.Binding.ExecutionGeneration,
		&row.Binding.FenceGeneration, &row.Binding.ExecutionOwner, &row.Binding.AdmissionFingerprint,
		&row.Binding.ContainerPlanFingerprint, &row.Binding.ActorCensusFingerprint,
		&row.Binding.EffectiveConfigFingerprint, &row.Binding.DeclarationPlanFingerprint, &row.Declaration,
		&row.Binding.PreparationFingerprint, &row.Preparation, &row.BundleHash, &liveParentsMatch)
	if err != nil {
		return selectedForkGrantExecutionRow{}, fmt.Errorf("prove selected-fork grant execution: %w", err)
	}
	if !liveParentsMatch {
		return selectedForkGrantExecutionRow{}, errors.New("selected-fork grant execution lacks its exact live-parent association")
	}
	return row, nil
}
