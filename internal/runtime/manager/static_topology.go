package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	semanticview "github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

// CompileStaticTopologyDesiredAgents compiles declaration-owned durable
// topology through the manager's configured execution-selection owner. The
// resulting revisions therefore describe the exact configs persistence will
// admit, rather than unresolved authored declarations.
func (am *AgentManager) CompileStaticTopologyDesiredAgents(source semanticview.Source, coordinate runtimeagenttopology.SourceCoordinate) ([]runtimeagenttopology.DesiredAgent, error) {
	if err := coordinate.Validate(); err != nil {
		return nil, err
	}
	records, err := am.resolvedStaticTopologyRecords(source)
	if err != nil {
		return nil, err
	}
	byIdentity := make(map[string]runtimeagenttopology.DesiredAgent, len(records))
	for _, rec := range records {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return nil, err
		}
		desired := runtimeagenttopology.DesiredAgent{Identity: identity, Source: coordinate, ConfigRevision: revision}
		key, err := desired.Key()
		if err != nil {
			return nil, err
		}
		if previous, ok := byIdentity[key]; ok && previous != desired {
			return nil, fmt.Errorf("static declaration %s compiles to conflicting desired records", identity.Description())
		}
		byIdentity[key] = desired
	}
	out := make([]runtimeagenttopology.DesiredAgent, 0, len(byIdentity))
	for _, desired := range byIdentity {
		out = append(out, desired)
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := out[i].Key()
		right, _ := out[j].Key()
		return left < right
	})
	return out, nil
}

// PrepareStaticTopologyForStartup settles predecessor effect authority and
// linearizes the declaration diff without constructing process-local execution.
func (am *AgentManager) PrepareStaticTopologyForStartup(ctx context.Context, source semanticview.Source) error {
	if am == nil || source == nil || am.store == nil || am.lifecycle == nil || am.lifecycle.persistence() == nil {
		return nil
	}
	if err := am.reconcileExternalEffectsForStartup(ctx); err != nil {
		return err
	}
	admission, err := am.staticTopologyAdmission()
	if err != nil {
		return err
	}
	static := admission.Authority.Static
	completePlan, err := am.completeStaticSourceSet()
	if err != nil {
		return err
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: static.BundleHash, BundleSource: static.BundleSource}
	records, err := am.resolvedStaticTopologyRecords(source)
	if err != nil {
		return err
	}
	for i := range records {
		records[i].Topology = admission
	}
	desiredAgents, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		return err
	}
	if err := verifySourceSetDesiredAgents(completePlan, coordinate, desiredAgents); err != nil {
		return err
	}
	desiredByKey := make(map[string]PersistedAgent, len(records))
	for _, rec := range records {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		key, err := identity.Fingerprint()
		if err != nil {
			return err
		}
		desiredByKey[key] = rec
	}
	if len(desiredByKey) != len(desiredAgents) {
		return errors.New("static topology plan and materialization record census differ")
	}

	persisted, err := am.store.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents for static topology reconciliation: %w", err)
	}
	seen := make(map[string]struct{}, len(desiredByKey))
	for _, current := range persisted {
		identity, err := current.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if current.Topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan || current.Topology.Authority.Static == nil {
			continue
		}
		if current.LifecyclePhase == AgentLifecycleTerminated {
			continue
		}
		owner := current.Topology.Authority.Static
		ownerSource := runtimeagenttopology.SourceCoordinate{BundleHash: owner.BundleHash, BundleSource: owner.BundleSource}
		if !sourceSetContainsCoordinate(completePlan, ownerSource) {
			if err := am.retireRemovedStaticTopology(ctx, current, admission); err != nil {
				return err
			}
			continue
		}
		if owner.BundleHash != static.BundleHash || owner.BundleSource != static.BundleSource {
			continue
		}
		key, err := identity.Fingerprint()
		if err != nil {
			return err
		}
		desired, present := desiredByKey[key]
		if !present {
			if err := am.commitStaticTopologyReconciliation(ctx, current, nil, admission); err != nil {
				return err
			}
			continue
		}
		seen[key] = struct{}{}
		currentRevision, err := lifecycleConfigRevision(current)
		if err != nil {
			return err
		}
		desiredRevision, err := lifecycleConfigRevision(desired)
		if err != nil {
			return err
		}
		if currentRevision == desiredRevision && current.Topology.Equal(admission) && current.LifecyclePhase != AgentLifecycleFailed {
			continue
		}
		if err := am.commitStaticTopologyReconciliation(ctx, current, &desired, admission); err != nil {
			return err
		}
	}

	for key, desired := range desiredByKey {
		if _, ok := seen[key]; ok {
			continue
		}
		identity, _ := desired.Config.ConcreteIdentity()
		if am.roles.LifecycleState == nil {
			return errors.New("static topology reconciliation requires lifecycle state readback")
		}
		state, exists, err := am.roles.LifecycleState.LoadAgentLifecycleState(ctx, identity)
		if err != nil {
			return err
		}
		if exists {
			current := PersistedAgent{
				Config: desired.Config, LifecycleEpoch: state.RuntimeEpoch, LifecycleGeneration: state.Generation,
				LifecyclePhase: state.Phase, LifecycleRunMode: state.RunMode, Topology: state.Topology,
				ProcessBinding: state.ProcessBinding,
			}
			if err := am.commitStaticTopologyReconciliation(ctx, current, &desired, admission); err != nil {
				return err
			}
			continue
		}
		if err := am.commitStaticTopologyReconciliation(ctx, PersistedAgent{}, &desired, admission); err != nil {
			return err
		}
	}
	return nil
}

func (am *AgentManager) retireRemovedStaticTopology(
	ctx context.Context,
	current PersistedAgent,
	admission runtimeagenttopology.Admission,
) error {
	store := am.lifecycle.persistence()
	provider, ok := store.(processExecutionBindingProvider)
	if !ok {
		return errors.New("static topology source removal requires process execution binding")
	}
	target, err := provider.ProcessExecutionBinding()
	if err != nil {
		return err
	}
	identity, err := current.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	subordinate, planHash, err := normalizedLifecycleSubordinate(reconfigureSessionMutationPlan(current.Config, current.Config))
	if err != nil {
		return err
	}
	identityKey, err := identity.Fingerprint()
	if err != nil {
		return err
	}
	revision, err := lifecycleConfigRevision(current)
	if err != nil {
		return err
	}
	if current.LifecycleGeneration == ^uint64(0) {
		return fmt.Errorf("static topology retirement generation exhausted for %s", identity.Description())
	}
	operationKind := "source_set_retire"
	if !sameProcessExecutionOwner(current.ProcessBinding, target) {
		operationKind = "process_takeover"
	}
	operationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-static-source-retire-v1", operationKind, target.GenerationGrantID, identityKey,
		current.ProcessBinding.GenerationGrantID, fmt.Sprint(current.LifecycleEpoch),
		fmt.Sprint(current.LifecycleGeneration), string(current.LifecyclePhase),
	}, "\x00"))).String()
	requestHash := lifecycleRequestHashForIdentity(
		identity, admission, operationKind, revision, planHash,
	)
	targetEpoch := runtimebus.CurrentRuntimeEpoch()
	if targetEpoch <= 0 {
		targetEpoch = 1
	}
	result, err := store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
		OperationID: operationID, OperationKind: operationKind, RequestHash: requestHash,
		Identity: identity, AgentID: identity.AgentID(), Trigger: operationKind,
		ExpectedEpoch: current.LifecycleEpoch, ExpectedGeneration: current.LifecycleGeneration,
		ExpectedPhase: current.LifecyclePhase, TargetEpoch: targetEpoch,
		TargetGeneration: current.LifecycleGeneration + 1, TargetPhase: AgentLifecycleTerminated,
		ConfigRevision: revision, RunMode: current.LifecycleRunMode,
		Subordinate: subordinate, Topology: admission, Now: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("retire removed static topology %s: %w", identity.Description(), err)
	}
	if !result.ProcessBinding.Equal(target) || result.RuntimeEpoch != targetEpoch ||
		result.Generation != current.LifecycleGeneration+1 || result.Phase != AgentLifecycleTerminated {
		return fmt.Errorf("retire removed static topology %s returned conflicting lifecycle evidence", identity.Description())
	}
	return nil
}

// HydrateStaticTopologyForStartup constructs process-local execution only
// after every selected-store lifecycle cell has been rebound.
func (am *AgentManager) HydrateStaticTopologyForStartup(ctx context.Context) error {
	if am == nil || am.store == nil {
		return nil
	}
	return am.hydratePersistedAgentExecutions(ctx)
}

// ReconcileStaticTopologyForStartup preserves the single-runtime call surface
// while keeping preparation and hydration independently orderable by serve.
func (am *AgentManager) ReconcileStaticTopologyForStartup(ctx context.Context, source semanticview.Source) error {
	if err := am.PrepareStaticTopologyForStartup(ctx, source); err != nil {
		return err
	}
	return am.HydrateStaticTopologyForStartup(ctx)
}

type staticTopologySourceSetBinding struct {
	identity    runtimeagentidentity.Identity
	identityKey string
	cell        *agentLifecycleCell
	epoch       int64
	generation  uint64
	phase       AgentLifecyclePhase
	runMode     AgentRunMode
	revision    string
}

// PreparedStaticTopologySourceSetRebind holds every static lifecycle operation
// lock across the selected-store source-set mutation.
type PreparedStaticTopologySourceSetRebind struct {
	mu        sync.Mutex
	manager   *AgentManager
	admission runtimeagenttopology.Admission
	plan      runtimeagenttopology.SourceSetPlan
	bindings  []staticTopologySourceSetBinding
	locked    []*agentLifecycleCell
	done      bool
}

func (am *AgentManager) PrepareStaticTopologySourceSetRebind(
	admission runtimeagenttopology.Admission,
	plan runtimeagenttopology.SourceSetPlan,
	source semanticview.Source,
) (*PreparedStaticTopologySourceSetRebind, error) {
	if am == nil || am.lifecycle == nil || source == nil {
		return nil, errors.New("static topology source-set rebind requires manager and semantic source")
	}
	if err := admission.Validate(); err != nil {
		return nil, fmt.Errorf("static topology source-set rebind admission: %w", err)
	}
	if admission.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan ||
		admission.Authority.Static == nil || admission.Lifetime != runtimeagenttopology.LifetimeDurableManaged {
		return nil, errors.New("static topology source-set rebind requires durable static declaration authority")
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("static topology source-set rebind plan: %w", err)
	}
	static := admission.Authority.Static
	if static.SourceSetRevision != plan.Revision {
		return nil, errors.New("static topology source-set rebind admission differs from complete source-set plan")
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: static.BundleHash, BundleSource: static.BundleSource}
	records, err := am.resolvedStaticTopologyRecords(source)
	if err != nil {
		return nil, err
	}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		return nil, err
	}
	if err := verifySourceSetDesiredAgents(plan, coordinate, desired); err != nil {
		return nil, err
	}
	prepared := &PreparedStaticTopologySourceSetRebind{
		manager: am, admission: admission, plan: plan,
		bindings: make([]staticTopologySourceSetBinding, 0, len(records)),
		locked:   make([]*agentLifecycleCell, 0, len(records)),
	}
	am.lifecycle.executionPublishMu.Lock()
	releaseOnError := true
	defer func() {
		if releaseOnError {
			prepared.release()
		}
	}()

	expected := make(map[runtimeagentidentity.Identity]string, len(records))
	for _, rec := range records {
		identity, identityErr := rec.Config.ConcreteIdentity()
		if identityErr != nil {
			return nil, identityErr
		}
		revision, revisionErr := lifecycleConfigRevision(rec)
		if revisionErr != nil {
			return nil, revisionErr
		}
		expected[identity] = revision
		cell, lockErr := am.lifecycle.lockIdentityTopologyOperation(identity)
		if lockErr != nil {
			return nil, fmt.Errorf("lock static topology source-set rebind for %s: %w", identity.Description(), lockErr)
		}
		prepared.locked = append(prepared.locked, cell)
	}

	am.lifecycle.mu.Lock()
	if am.lifecycle.phase == runtimeLifecycleShuttingDown || am.lifecycle.phase == runtimeLifecycleResetting {
		am.lifecycle.mu.Unlock()
		return nil, errors.New("static topology source-set rebind conflicts with manager retirement")
	}
	for identity, cell := range am.lifecycle.cells {
		owner := cell.topology.Authority.Static
		if cell.topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan || owner == nil ||
			owner.BundleHash != coordinate.BundleHash || owner.BundleSource != coordinate.BundleSource {
			continue
		}
		if _, ok := expected[identity]; !ok && cell.phase != AgentLifecycleTerminated {
			am.lifecycle.mu.Unlock()
			return nil, fmt.Errorf("static topology source-set rebind omitted live declaration %s", identity.Description())
		}
	}
	for identity, revision := range expected {
		cell := am.lifecycle.cells[identity]
		if cell == nil || cell.phase == AgentLifecycleTerminated {
			am.lifecycle.mu.Unlock()
			return nil, fmt.Errorf("static topology source-set rebind requires live declaration %s", identity.Description())
		}
		owner := cell.topology.Authority.Static
		if cell.topology.Authority.Kind != runtimeagenttopology.AuthorityStaticDeclarationPlan || owner == nil ||
			owner.BundleHash != coordinate.BundleHash || owner.BundleSource != coordinate.BundleSource {
			am.lifecycle.mu.Unlock()
			return nil, fmt.Errorf("static topology source-set rebind found foreign authority for %s", identity.Description())
		}
		if cell.configRevision != revision {
			am.lifecycle.mu.Unlock()
			return nil, fmt.Errorf("static topology source-set rebind config changed for %s", identity.Description())
		}
		identityKey, fingerprintErr := identity.Fingerprint()
		if fingerprintErr != nil {
			am.lifecycle.mu.Unlock()
			return nil, fingerprintErr
		}
		prepared.bindings = append(prepared.bindings, staticTopologySourceSetBinding{
			identity: identity, identityKey: identityKey, cell: cell, epoch: cell.epoch, generation: cell.generation,
			phase: cell.phase, runMode: cell.runMode, revision: revision,
		})
	}
	am.lifecycle.mu.Unlock()
	sort.Slice(prepared.bindings, func(i, j int) bool {
		return prepared.bindings[i].identityKey < prepared.bindings[j].identityKey
	})
	releaseOnError = false
	return prepared, nil
}

func (p *PreparedStaticTopologySourceSetRebind) release() {
	if p == nil || p.manager == nil {
		return
	}
	for i := len(p.locked) - 1; i >= 0; i-- {
		p.locked[i].opMu.Unlock()
	}
	p.locked = nil
	p.manager.lifecycle.executionPublishMu.Unlock()
}

func (p *PreparedStaticTopologySourceSetRebind) Abort() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	p.release()
}

func (p *PreparedStaticTopologySourceSetRebind) Commit(ctx context.Context, store AgentLifecyclePersistence, operationScopeID string) (commitErr error) {
	if p == nil || p.manager == nil {
		return errors.New("prepared static topology source-set rebind requires manager and persistence")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return errors.New("prepared static topology source-set rebind is already settled")
	}
	p.done = true
	defer p.release()
	if store == nil {
		return errors.New("prepared static topology source-set rebind requires persistence")
	}
	if _, err := uuid.Parse(strings.TrimSpace(operationScopeID)); err != nil {
		return fmt.Errorf("static topology source-set rebind operation scope must be a UUID: %w", err)
	}

	subordinate, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		return err
	}
	for _, item := range p.bindings {
		operationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
			"agent-static-source-set-rebind-v1", strings.TrimSpace(operationScopeID), p.plan.Revision, item.identityKey,
			fmt.Sprint(item.epoch), fmt.Sprint(item.generation), string(item.phase), item.revision,
		}, "\x00"))).String()
		requestHash := lifecycleRequestHashForIdentity(item.identity, p.admission, "source_set_rebind", item.revision, planHash)
		result, commitErr := store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: "source_set_rebind", RequestHash: requestHash,
			Identity: item.identity, AgentID: item.identity.AgentID(), Trigger: "source_set_rebind",
			ExpectedEpoch: item.epoch, ExpectedGeneration: item.generation, ExpectedPhase: item.phase,
			TargetEpoch: item.epoch, TargetGeneration: item.generation, TargetPhase: item.phase,
			ConfigRevision: item.revision, RunMode: item.runMode, Subordinate: subordinate,
			Topology: p.admission, Now: time.Now().UTC(),
		})
		if commitErr != nil {
			return fmt.Errorf("rebind static topology for %s: %w", item.identity.Description(), commitErr)
		}
		if result.Identity.Normalize() != item.identity || result.RuntimeEpoch != item.epoch ||
			result.Generation != item.generation || result.Phase != item.phase || result.ConfigRevision != item.revision {
			return fmt.Errorf("rebind static topology for %s returned conflicting lifecycle evidence", item.identity.Description())
		}
		if err := result.ProcessBinding.Validate(); err != nil {
			return fmt.Errorf("rebind static topology for %s returned invalid process binding: %w", item.identity.Description(), err)
		}
		item.cell.processBinding = result.ProcessBinding
	}

	p.manager.lifecycle.replacePersistence(store)
	p.manager.lifecycle.mu.Lock()
	for _, item := range p.bindings {
		cell := p.manager.lifecycle.cells[item.identity]
		if cell != item.cell || cell.epoch != item.epoch || cell.generation != item.generation || cell.phase != item.phase {
			p.manager.lifecycle.mu.Unlock()
			return fmt.Errorf("static topology source-set rebind projection changed for %s", item.identity.Description())
		}
		cell.topology = p.admission
	}
	p.manager.lifecycle.mu.Unlock()
	p.manager.mu.Lock()
	p.manager.staticTopology = p.admission
	p.manager.staticSourceSet = p.plan
	p.manager.mu.Unlock()
	return nil
}

func verifySourceSetDesiredAgents(completePlan runtimeagenttopology.SourceSetPlan, coordinate runtimeagenttopology.SourceCoordinate, actual []runtimeagenttopology.DesiredAgent) error {
	coordinate = coordinate.Normalize()
	expectedByKey := make(map[string]runtimeagenttopology.DesiredAgent)
	for _, desired := range completePlan.Agents {
		if desired.Source.Normalize() != coordinate {
			continue
		}
		key, err := desired.Key()
		if err != nil {
			return err
		}
		expectedByKey[key] = desired
	}
	if len(expectedByKey) != len(actual) {
		return fmt.Errorf(
			"static declaration topology differs from complete source-set plan: planned=%d declared=%d",
			len(expectedByKey), len(actual),
		)
	}
	for _, desired := range actual {
		key, err := desired.Key()
		if err != nil {
			return err
		}
		expected, ok := expectedByKey[key]
		if !ok || expected.Source.Normalize() != desired.Source.Normalize() || expected.ConfigRevision != desired.ConfigRevision {
			return fmt.Errorf("static declaration %s differs from complete source-set plan", desired.Identity.Description())
		}
	}
	return nil
}

func (am *AgentManager) hydratePersistedAgentExecutions(ctx context.Context) error {
	am.mu.Lock()
	if am.startupAgentsHydrated {
		am.mu.Unlock()
		return errors.New("startup agent executions were already hydrated")
	}
	am.mu.Unlock()
	agents, err := am.store.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("reload reconciled agents: %w", err)
	}
	admission, err := am.staticTopologyAdmission()
	if err != nil {
		return err
	}
	sort.SliceStable(agents, func(i, j int) bool { return agents[i].StartedAt.Before(agents[j].StartedAt) })
	for _, rec := range agents {
		if !rec.Topology.Equal(admission) {
			continue
		}
		if strings.TrimSpace(rec.Config.ID) == "" {
			return errors.New("persisted agent has no concrete identity")
		}
		if err := am.spawnAgentInternal(ctx, rec, false); err != nil {
			return fmt.Errorf("hydrate reconciled agent %s: %w", rec.Config.ID, err)
		}
	}
	am.mu.Lock()
	am.startupAgentsHydrated = true
	am.mu.Unlock()
	return nil
}

func sourceSetContainsCoordinate(plan runtimeagenttopology.SourceSetPlan, coordinate runtimeagenttopology.SourceCoordinate) bool {
	coordinate = coordinate.Normalize()
	for _, candidate := range plan.Sources {
		if candidate.Normalize() == coordinate {
			return true
		}
	}
	return false
}

func (am *AgentManager) resolvedStaticTopologyRecords(source semanticview.Source) ([]PersistedAgent, error) {
	ordinary, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	required, err := StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	byIdentity := map[string]PersistedAgent{}
	for _, rec := range append(ordinary, required...) {
		if err := am.resolveAgentModel(&rec.Config); err != nil {
			return nil, err
		}
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		key, err := identity.Fingerprint()
		if err != nil {
			return nil, err
		}
		if previous, ok := byIdentity[key]; ok {
			left, _ := lifecycleConfigRevision(previous)
			right, _ := lifecycleConfigRevision(rec)
			if left != right {
				return nil, fmt.Errorf("static declaration %s has conflicting materializations", identity.Description())
			}
			continue
		}
		byIdentity[key] = rec
	}
	out := make([]PersistedAgent, 0, len(byIdentity))
	for _, rec := range byIdentity {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := out[i].Config.ConcreteIdentity()
		right, _ := out[j].Config.ConcreteIdentity()
		leftKey, _ := left.Fingerprint()
		rightKey, _ := right.Fingerprint()
		return leftKey < rightKey
	})
	return out, nil
}

func desiredAgentsFromRecords(records []PersistedAgent, coordinate runtimeagenttopology.SourceCoordinate) ([]runtimeagenttopology.DesiredAgent, error) {
	if err := coordinate.Validate(); err != nil {
		return nil, err
	}
	out := make([]runtimeagenttopology.DesiredAgent, 0, len(records))
	for _, rec := range records {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return nil, err
		}
		out = append(out, runtimeagenttopology.DesiredAgent{
			Identity: identity, Source: coordinate, ConfigRevision: revision,
		})
	}
	return out, nil
}

func (am *AgentManager) commitStaticTopologyReconciliation(ctx context.Context, current PersistedAgent, desired *PersistedAgent, admission runtimeagenttopology.Admission) error {
	store := am.lifecycle.persistence()
	if am.lifecycle == nil || store == nil {
		return errors.New("static topology reconciliation requires generation lifecycle persistence")
	}
	var identitySource PersistedAgent
	if desired != nil {
		identitySource = *desired
	} else {
		identitySource = current
	}
	identity, err := identitySource.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	operationKind := "reconfigure"
	expectedEpoch := current.LifecycleEpoch
	expectedGeneration := current.LifecycleGeneration
	expectedPhase := current.LifecyclePhase
	if expectedGeneration == 0 && expectedPhase == "" {
		operationKind = "spawn"
	}
	configRevision, err := lifecycleConfigRevision(identitySource)
	if err != nil {
		return err
	}
	targetPhase := AgentLifecycleRegistered
	targetAgent := desired
	plan := runtimesessions.LifecycleMutationPlan{Action: runtimesessions.LifecycleMutationNone}
	if desired == nil {
		operationKind = "teardown"
		targetPhase = AgentLifecycleTerminated
		targetAgent = nil
		plan = reconfigureSessionMutationPlan(current.Config, current.Config)
	} else if expectedGeneration > 0 {
		plan = reconfigureSessionMutationPlan(current.Config, desired.Config)
	}
	if expectedGeneration > 0 && !current.ProcessBinding.IsZero() {
		provider, ok := store.(processExecutionBindingProvider)
		if !ok {
			return errors.New("static topology reconciliation requires process execution binding")
		}
		target, bindingErr := provider.ProcessExecutionBinding()
		if bindingErr != nil {
			return bindingErr
		}
		if !current.ProcessBinding.Equal(target) {
			operationKind = "process_takeover"
			if sameProcessExecutionOwner(current.ProcessBinding, target) {
				operationKind = "source_set_rebind"
			}
		}
	}
	targetEpoch := runtimebus.CurrentRuntimeEpoch()
	if targetEpoch <= 0 {
		targetEpoch = 1
	}
	targetGeneration := expectedGeneration + 1
	operationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-static-topology-reconcile-v1", admission.Authority.Static.SourceSetRevision, operationKind,
		identity.Description(), fmt.Sprint(expectedEpoch), fmt.Sprint(expectedGeneration), string(expectedPhase),
		string(targetPhase), configRevision,
	}, "\x00"))).String()
	requestHash := lifecycleRequestHashForIdentity(identity, admission, operationKind, configRevision, string(targetPhase))
	_, err = store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
		OperationID: operationID, OperationKind: operationKind, RequestHash: requestHash,
		Identity: identity, AgentID: identity.AgentID(), Trigger: "source_reconcile",
		ExpectedEpoch: expectedEpoch, ExpectedGeneration: expectedGeneration, ExpectedPhase: expectedPhase,
		TargetEpoch: targetEpoch, TargetGeneration: targetGeneration, TargetPhase: targetPhase,
		ConfigRevision: configRevision, RunMode: AgentRunModeStopped, Agent: targetAgent,
		Subordinate: plan, Topology: admission, Now: time.Now().UTC(),
	})
	return err
}

func sameProcessExecutionOwner(left, right ProcessExecutionBinding) bool {
	return left.ProcessAuthorityID == right.ProcessAuthorityID &&
		left.ProcessOwnerID == right.ProcessOwnerID &&
		left.ProcessBootID == right.ProcessBootID
}
