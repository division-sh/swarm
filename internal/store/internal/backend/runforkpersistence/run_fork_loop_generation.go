package runforkpersistence

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

type runForkGateActivationBinding struct {
	Source gateruntime.Activation
	Fork   gateruntime.Activation
}

type RunForkGateActivationBinding = runForkGateActivationBinding

func forkAttemptGenerationState(raw map[string]any, forkRunID, entityID string) (map[string]any, error) {
	if _, ok := raw[loopruntime.BucketKey]; !ok {
		return cloneForkLoopState(raw), nil
	}
	structured := make(map[string]any, len(raw))
	for key, value := range raw {
		if _, ok := value.(map[string]any); ok {
			structured[key] = value
		}
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, structured)
	if err != nil {
		return nil, err
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, err
	}
	replacements := map[string]attemptgeneration.Generation{}
	for _, activation := range activations {
		old := activation.Generation()
		forked, err := loopruntime.Fork(activation, forkRunID, entityID)
		if err != nil {
			return nil, err
		}
		replacements[old.RevisionID] = forked.Generation()
		if err := loopruntime.Store(carrier.StateBuckets, forked); err != nil {
			return nil, err
		}
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, err
	}
	for _, activation := range joins {
		replacement, ok := replacements[activation.Generation().RevisionID]
		if !ok {
			continue
		}
		if err := joinruntime.ReplaceGeneration(carrier.StateBuckets, activation, replacement); err != nil {
			return nil, err
		}
	}
	for _, nodeBucket := range carrier.StateBuckets {
		accumulators, _ := nodeBucket["handler_accumulators"].(map[string]any)
		for key, value := range accumulators {
			for _, activation := range activations {
				oldGeneration := activation.Generation()
				replacement := replacements[oldGeneration.RevisionID]
				oldSuffix, newSuffix := oldGeneration.KeySuffix(), replacement.KeySuffix()
				if oldSuffix == "" || !strings.Contains(key, oldSuffix) {
					continue
				}
				delete(accumulators, key)
				accumulators[strings.Replace(key, oldSuffix, newSuffix, 1)] = value
				break
			}
		}
	}
	out := cloneForkLoopState(raw)
	for key, value := range carrier.PersistedStateBuckets() {
		out[key] = value
	}
	return out, nil
}

func ForkAttemptGenerationState(raw map[string]any, forkRunID, entityID string) (map[string]any, error) {
	return forkAttemptGenerationState(raw, forkRunID, entityID)
}

func forkGateActivationState(raw map[string]any, forkRunID, flowInstance, entityID string) (map[string]any, []runForkGateActivationBinding, error) {
	if _, ok := raw[gateruntime.BucketKey]; !ok {
		return cloneForkLoopState(raw), nil, nil
	}
	structured := make(map[string]any, len(raw))
	for key, value := range raw {
		if _, ok := value.(map[string]any); ok {
			structured[key] = value
		}
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, structured)
	if err != nil {
		return nil, nil, err
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, nil, err
	}
	bindings := make([]runForkGateActivationBinding, 0, len(activations))
	for _, source := range activations {
		forked, err := gateruntime.New(forkRunID, flowInstance, entityID, source.FlowID, source.Stage, source.DecisionID, source.BundleHash, source.RoutesJSON, source.StartedByEvent, source.OpenedAt)
		if err != nil {
			return nil, nil, err
		}
		switch source.Status {
		case gateruntime.StatusOpen:
		case gateruntime.StatusDecisionCommitted:
			if err := forked.CommitDecision(source.DecisionEventID, source.UpdatedAt); err != nil {
				return nil, nil, err
			}
		case gateruntime.StatusRouted:
			if err := forked.CommitDecision(source.DecisionEventID, source.UpdatedAt); err != nil {
				return nil, nil, err
			}
			if err := forked.Route(source.DecisionEventID, source.UpdatedAt); err != nil {
				return nil, nil, err
			}
		case gateruntime.StatusSuperseded:
			if !forked.Supersede(source.SupersededReason, source.UpdatedAt) {
				return nil, nil, fmt.Errorf("fork gate activation %s could not preserve supersession", source.ActivationID)
			}
		default:
			return nil, nil, fmt.Errorf("fork gate activation %s has unsupported status %s", source.ActivationID, source.Status)
		}
		if err := gateruntime.Store(carrier.StateBuckets, forked); err != nil {
			return nil, nil, err
		}
		bindings = append(bindings, runForkGateActivationBinding{Source: source, Fork: forked})
	}
	out := cloneForkLoopState(raw)
	for key, value := range carrier.PersistedStateBuckets() {
		out[key] = value
	}
	return out, bindings, nil
}

func ForkGateActivationState(raw map[string]any, forkRunID, flowInstance, entityID string) (map[string]any, []RunForkGateActivationBinding, error) {
	return forkGateActivationState(raw, forkRunID, flowInstance, entityID)
}

func cloneForkLoopState(raw map[string]any) map[string]any {
	out := make(map[string]any, len(raw))
	for key, value := range raw {
		out[key] = value
	}
	return out
}
