package manager

// The existing agent and route owners install, verify and retire exact-attempt resources. Their receipts cannot substitute for durable readiness progress.

import (
	"context"
	"errors"
	"fmt"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"sort"
	"strings"
)

func (am *AgentManager) publishPersistedDynamicFlowRoute(ctx context.Context, req runtimebus.FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (runtimebus.FlowRoutePublicationHandle, error) {
	publisher := am.roles.RouteRestorer
	if publisher == nil {
		return nil, fmt.Errorf("event bus does not support process publication for persisted flow-instance route %s", req.Identity.Route.InstancePath)
	}
	return publisher.PublishPersistedFlowInstanceRouteForAttempt(ctx, req, attempt)
}

func (am *AgentManager) retireFlowRouteAttempt(identity runtimeflowidentity.RunScopedFlowInstance, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, publication runtimebus.FlowRoutePublicationHandle) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("retire flow route attempt %s: %v", attempt.ID(), recovered)
		}
	}()
	if err := attempt.Validate(); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.RunID != attempt.RunID() || identity.Route.InstancePath != attempt.InstancePath() {
		return errors.New("flow retirement identity differs from activation attempt")
	}
	if publication != nil {
		return publication.Retire()
	}
	if am.roles.RouteRestorer == nil {
		return errors.New("exact flow route attempt retirement owner is required")
	}
	return am.roles.RouteRestorer.RetireFlowInstanceRouteForAttempt(identity, attempt)
}

func (am *AgentManager) loadDynamicFlowPersistedAgents(
	ctx context.Context,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
) (map[runtimeagentidentity.Identity]PersistedAgent, error) {
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return nil, err
	}
	persistedByIdentity := map[runtimeagentidentity.Identity]PersistedAgent{}
	if am.store == nil {
		return persistedByIdentity, nil
	}
	persisted, err := am.store.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range persisted {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, err
		}
		if flowIdentity.MatchesAgentRoute(identity) {
			if _, exists := persistedByIdentity[identity]; exists {
				return nil, fmt.Errorf("duplicate persisted agent identity %s", identity.Description())
			}
			persistedByIdentity[identity] = rec
		}
	}
	return persistedByIdentity, nil
}

func (am *AgentManager) reconcileDynamicFlowAgentSet(
	ctx context.Context,
	source semanticview.Source,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
	expected []PersistedAgent,
	persisted map[runtimeagentidentity.Identity]PersistedAgent,
	topologyAuthority runtimeagenttopology.Admission,
	retireRemoved bool,
) error {
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return err
	}
	if retireRemoved {
		expectedIdentities := make(map[runtimeagentidentity.Identity]struct{}, len(expected))
		for _, rec := range expected {
			identity, err := rec.Config.ConcreteIdentity()
			if err != nil {
				return err
			}
			expectedIdentities[identity] = struct{}{}
		}
		removedIdentities := make(map[runtimeagentidentity.Identity]struct{})
		for identity := range persisted {
			if _, ok := expectedIdentities[identity]; !ok {
				removedIdentities[identity] = struct{}{}
			}
		}
		for _, cfg := range am.ListAgentConfigs() {
			identity, err := cfg.ConcreteIdentity()
			if err != nil {
				return err
			}
			if !flowIdentity.MatchesAgentRoute(identity) {
				continue
			}
			if _, ok := expectedIdentities[identity]; !ok {
				removedIdentities[identity] = struct{}{}
			}
		}
		orderedRemoved := make([]runtimeagentidentity.Identity, 0, len(removedIdentities))
		for identity := range removedIdentities {
			orderedRemoved = append(orderedRemoved, identity)
		}
		sort.Slice(orderedRemoved, func(i, j int) bool {
			return runtimeagentidentity.Less(orderedRemoved[i], orderedRemoved[j])
		})
		for _, identity := range orderedRemoved {
			if _, live := am.getAgentConfigIdentity(identity); !live {
				stored, found := persisted[identity]
				if !found {
					return fmt.Errorf(
						"removed dynamic flow agent %s has neither process nor durable lifecycle owner",
						identity.Description(),
					)
				}
				if err := am.adoptPersistedAgentLifecycleOnly(ctx, stored); err != nil {
					return fmt.Errorf("adopt removed dynamic flow agent %s: %w", identity.Description(), err)
				}
			}
			if err := am.teardownIdentityWithTopology(ctx, identity, "teardown", &topologyAuthority); err != nil {
				return fmt.Errorf("retire removed dynamic flow agent %s: %w", identity.Description(), err)
			}
			delete(persisted, identity)
		}
	}
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		rec.Topology = topologyAuthority
		if err := am.reconcileDynamicFlowAgent(ctx, source, rec, persisted[identity], &topologyAuthority); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent %s: %w", identity.Description(), err)
		}
	}
	return nil
}

func (am *AgentManager) reconcileDynamicFlowAgent(
	ctx context.Context,
	source semanticview.Source,
	rec PersistedAgent,
	persisted PersistedAgent,
	topology *runtimeagenttopology.Admission,
) error {
	if topology == nil || !rec.Topology.Equal(*topology) {
		return errors.New("dynamic flow agent requires the exact readiness topology admission")
	}
	if err := rec.Topology.Validate(); err != nil {
		return fmt.Errorf("expected dynamic flow agent topology: %w", err)
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	expectedRevision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	existing, present := am.getAgentConfigIdentity(identity)
	actualRevision := ""
	actualTopology := runtimeagenttopology.Admission{}
	if present {
		actualRevision, err = lifecycleConfigRevision(PersistedAgent{Config: existing})
		if err != nil {
			return err
		}
		state, found := am.lifecycle.stateByIdentity(identity)
		if !found {
			return fmt.Errorf("agent %s is process-ready without lifecycle state", identity.Description())
		}
		actualTopology = state.Topology
	}
	persistedIdentity := runtimeagentidentity.Identity{}
	persistedRevision := ""
	if strings.TrimSpace(persisted.Config.ID) != "" {
		persistedIdentity, err = persisted.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if err := persisted.Topology.Validate(); err != nil {
			return fmt.Errorf("persisted dynamic flow agent %s topology: %w", identity.Description(), err)
		}
		persistedRevision, err = lifecycleConfigRevision(persisted)
		if err != nil {
			return err
		}
		if persistedIdentity != identity {
			return fmt.Errorf("agent %s persisted identity changed", identity.Description())
		}
	}
	if present && persistedIdentity.IsZero() {
		return fmt.Errorf("agent %s is process-ready without durable registration", identity.Description())
	}
	if present && (actualRevision != persistedRevision || !actualTopology.Equal(persisted.Topology)) {
		return fmt.Errorf("agent %s process and durable lifecycle facts disagree", identity.Description())
	}
	if !present && !persistedIdentity.IsZero() {
		adopt := am.adoptPersistedAgentForLifecycle
		if persistedRevision != expectedRevision || !persisted.Topology.Equal(rec.Topology) {
			adopt = func(ctx context.Context, _ semanticview.Source, rec PersistedAgent) error {
				return am.adoptPersistedAgentLifecycleOnly(ctx, rec)
			}
		}
		if err := adopt(ctx, source, persisted); err != nil {
			return fmt.Errorf("adopt persisted agent %s: %w", identity.Description(), err)
		}
		present = true
		actualRevision = persistedRevision
		actualTopology = persisted.Topology
	}
	if present {
		if actualRevision == expectedRevision && actualTopology.Equal(rec.Topology) {
			_, err := am.ensureExecutableAgentLifecycle(ctx, identity)
			return err
		}
		if err := am.reconfigureAgentIdentityExactWithTopology(ctx, source, identity, rec.Config, topology); err != nil {
			return err
		}
		_, err := am.ensureExecutableAgentLifecycle(ctx, identity)
		return err
	}
	return am.spawnAgentInternalForSourceWithTopology(ctx, rec, true, source, topology)
}

// DynamicFlowAgentTopologyAdmission derives the one topology admission owned by
// an exact durable readiness plan.
func DynamicFlowAgentTopologyAdmission(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (runtimeagenttopology.Admission, error) {
	fingerprint, err := plan.Hash()
	if err != nil {
		return runtimeagenttopology.Admission{}, err
	}
	return runtimeagenttopology.FlowReadinessAdmission(
		strings.TrimSpace(plan.RunID),
		strings.Trim(strings.TrimSpace(plan.Identity.InstancePath), "/"),
		fingerprint,
	)
}

func (am *AgentManager) verifyDynamicFlowAgents(
	ctx context.Context,
	flowIdentity runtimeflowidentity.RunScopedFlowInstance,
	expected []PersistedAgent,
	topology runtimeagenttopology.Admission,
) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("verify dynamic flow topology admission: %w", err)
	}
	flowIdentity = flowIdentity.Normalize()
	if err := flowIdentity.Validate(); err != nil {
		return err
	}
	expectedByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(expected))
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		expectedByIdentity[identity] = rec
	}
	persistedByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(expected))
	if am.store == nil {
		if len(expectedByIdentity) != 0 {
			return fmt.Errorf("declared agents require durable manager persistence")
		}
	} else {
		persisted, err := am.store.LoadAgents(ctx)
		if err != nil {
			return err
		}
		for _, rec := range persisted {
			identity, err := rec.Config.ConcreteIdentity()
			if err != nil {
				return err
			}
			if flowIdentity.MatchesAgentRoute(identity) {
				persistedByIdentity[identity] = rec
			}
		}
	}
	processByIdentity := make(map[runtimeagentidentity.Identity]models.AgentConfig, len(expected))
	for _, cfg := range am.ListAgentConfigs() {
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			return err
		}
		if flowIdentity.MatchesAgentRoute(identity) {
			processByIdentity[identity] = cfg
		}
	}
	if len(persistedByIdentity) != len(expectedByIdentity) || len(processByIdentity) != len(expectedByIdentity) {
		return fmt.Errorf(
			"declared agent set mismatch: expected=%d persisted=%d process=%d",
			len(expectedByIdentity),
			len(persistedByIdentity),
			len(processByIdentity),
		)
	}
	for _, rec := range expected {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		expectedRevision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		process, ok := processByIdentity[identity]
		if !ok {
			return fmt.Errorf("declared agent %s is not process-ready", identity.Description())
		}
		processRevision, err := lifecycleConfigRevision(PersistedAgent{Config: process})
		if err != nil {
			return err
		}
		stored, ok := persistedByIdentity[identity]
		if !ok {
			return fmt.Errorf("declared agent %s is not durably registered", identity.Description())
		}
		storedRevision, err := lifecycleConfigRevision(stored)
		if err != nil {
			return err
		}
		readiness, err := am.lifecycle.executableReadinessByIdentity(identity)
		if err != nil {
			return fmt.Errorf("declared agent %s executable readiness: %w", identity.Description(), err)
		}
		if processRevision != expectedRevision || storedRevision != expectedRevision ||
			!readiness.State.Topology.Equal(topology) || !stored.Topology.Equal(topology) {
			return fmt.Errorf(
				"declared agent %s readiness facts mismatch: expected_revision=%s process_revision=%s stored_revision=%s process_topology_equal=%t stored_topology_equal=%t",
				identity.Description(), expectedRevision, processRevision, storedRevision,
				readiness.State.Topology.Equal(topology), stored.Topology.Equal(topology),
			)
		}
	}
	return nil
}

func verifyDynamicFlowAgentExpectations(actual []PersistedAgent, expected []runtimepipeline.DynamicFlowRuntimeAgentExpectation) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("declared agent count changed: expected=%d actual=%d", len(expected), len(actual))
	}
	actualByIdentity := make(map[runtimeagentidentity.Identity]PersistedAgent, len(actual))
	for _, rec := range actual {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		if _, exists := actualByIdentity[identity]; exists {
			return fmt.Errorf("duplicate declared agent %s", identity.Description())
		}
		actualByIdentity[identity] = rec
	}
	for _, item := range expected {
		rec, ok := actualByIdentity[item.Identity]
		if !ok {
			return fmt.Errorf("declared agent topology missing %s", item.Identity.Description())
		}
		if rec.Config.EntityID != item.EntityID {
			return fmt.Errorf("declared agent entity ownership changed at %s", item.Identity.Description())
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		if revision != item.ConfigRevision {
			return fmt.Errorf("declared agent topology changed at %s: expected_revision=%s actual_revision=%s", item.Identity.Description(), item.ConfigRevision, revision)
		}
	}
	return nil
}

func (am *AgentManager) verifyDynamicFlowRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	verifier := am.roles.RouteVerifier
	if verifier == nil || !verifier.HasFlowInstanceRoute(identity) {
		return fmt.Errorf("dynamic flow route %s is not process-ready", identity.Key())
	}
	if err := verifier.VerifyFlowInstanceRoute(ctx, identity); err != nil {
		return fmt.Errorf("verify dynamic flow route %s: %w", identity.Key(), err)
	}
	return nil
}
