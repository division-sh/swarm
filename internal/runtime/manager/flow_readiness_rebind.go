package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// One instance's attachment and all of its existing actors change stamps in
// one transaction. The receipt, resources and phase remain the same attempt.
type FlowReadinessSourceSetRebindRequest struct {
	Attempt     runtimepipeline.DynamicFlowRuntimeActivationAttempt
	PlanHash    string
	Transitions []AgentLifecycleTransition
}

type FlowReadinessSourceSetRebindResult struct {
	Attempt     runtimepipeline.DynamicFlowRuntimeActivationAttempt
	Transitions []AgentLifecycleTransitionResult
}

type FlowReadinessSourceSetRebindPersistence interface {
	RebindFlowReadinessSourceSet(context.Context, FlowReadinessSourceSetRebindRequest) (FlowReadinessSourceSetRebindResult, error)
}

func (p *PreparedDurableTopologySourceSetRebind) rebindFlowReadiness(ctx context.Context, store AgentLifecyclePersistence, requests []AgentLifecycleTransition, target ProcessExecutionBinding) (map[int]AgentLifecycleTransitionResult, map[dynamicFlowRuntimeReadinessKey]runtimepipeline.DynamicFlowRuntimeActivationAttempt, error) {
	groups := make(map[dynamicFlowRuntimeReadinessKey]FlowReadinessSourceSetRebindRequest)
	indices := make(map[dynamicFlowRuntimeReadinessKey][]int)
	for index, req := range requests {
		if req.Topology.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan {
			continue
		}
		owner := req.Topology.Authority.Readiness
		key := dynamicFlowRuntimeReadinessKey{runID: owner.RunID, instancePath: owner.InstancePath}
		group, found := groups[key]
		if !found {
			var err error
			group.Attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(owner.AttemptID, owner.RunID, owner.InstancePath, p.bindings[index].currentBinding)
			if err != nil {
				return nil, nil, err
			}
			group.PlanHash = owner.PlanFingerprint
		} else if group.Attempt.ID() != owner.AttemptID || group.PlanHash != owner.PlanFingerprint {
			return nil, nil, errors.New("source-set rebind found conflicting instance attachment evidence")
		}
		group.Transitions = append(group.Transitions, req)
		groups[key] = group
		indices[key] = append(indices[key], index)
	}
	if err := p.includeActiveFlowReadinessRebinds(groups); err != nil {
		return nil, nil, err
	}
	results := make(map[int]AgentLifecycleTransitionResult)
	attempts := make(map[dynamicFlowRuntimeReadinessKey]runtimepipeline.DynamicFlowRuntimeActivationAttempt)
	if len(groups) == 0 {
		return results, attempts, nil
	}
	owner, ok := store.(FlowReadinessSourceSetRebindPersistence)
	if !ok {
		return nil, nil, errors.New("source-set refresh requires the atomic flow readiness rebind owner")
	}
	keys := make([]dynamicFlowRuntimeReadinessKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].runID+"\x00"+keys[i].instancePath < keys[j].runID+"\x00"+keys[j].instancePath
	})
	for _, key := range keys {
		group := groups[key]
		result, err := owner.RebindFlowReadinessSourceSet(ctx, group)
		if err != nil {
			return nil, nil, fmt.Errorf("rebind flow attachment %s: %w", key.instancePath, err)
		}
		if result.Attempt.ID() != group.Attempt.ID() || result.Attempt.RunID() != key.runID || result.Attempt.InstancePath() != key.instancePath || !result.Attempt.ProcessBinding().Equal(target) || len(result.Transitions) != len(group.Transitions) {
			return nil, nil, errors.New("flow readiness rebind returned conflicting instance evidence")
		}
		for index, result := range result.Transitions {
			results[indices[key][index]] = result
		}
		attempts[key] = result.Attempt
	}
	return results, attempts, nil
}

func (p *PreparedDurableTopologySourceSetRebind) includeActiveFlowReadinessRebinds(groups map[dynamicFlowRuntimeReadinessKey]FlowReadinessSourceSetRebindRequest) error {
	p.manager.dynamicFlowReadinessMu.Lock()
	defer p.manager.dynamicFlowReadinessMu.Unlock()
	for key, active := range p.manager.dynamicFlowActiveAttempts {
		if active.pending != nil || active.retiring || active.retirementKind != 0 {
			return errors.New("source-set refresh retains unresolved flow admission or retirement")
		}
		group, exists := groups[key]
		if !exists {
			groups[key] = FlowReadinessSourceSetRebindRequest{Attempt: active.receipt, PlanHash: active.planHash}
			continue
		}
		if group.Attempt.ID() != active.receipt.ID() || group.PlanHash != active.planHash {
			return errors.New("source-set refresh actor census differs from its attachment owner")
		}
	}
	return nil
}
