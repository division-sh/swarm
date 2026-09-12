package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
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

func projectRunForkAttemptGenerationState(raw map[string]any, forkRunID, entityID string) (map[string]any, *loopruntime.ForkCorrespondence, error) {
	out, err := cloneForkLoopState(raw)
	if err != nil {
		return nil, nil, err
	}
	structured := make(map[string]any, len(out))
	for key, value := range out {
		if _, ok := value.(map[string]any); ok {
			if key == "" || key != strings.TrimSpace(key) {
				return nil, nil, fmt.Errorf("fork state bucket key %q is not canonical", key)
			}
			structured[key] = value
		} else if key == loopruntime.BucketKey {
			return nil, nil, fmt.Errorf("fork loop bucket must be an object")
		}
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, structured)
	if err != nil {
		return nil, nil, err
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, nil, err
	}
	correspondence, err := loopruntime.NewForkCorrespondence(activations, forkRunID, entityID)
	if err != nil {
		return nil, nil, err
	}
	for _, activation := range activations {
		if _, ok := carrier.StateBuckets[loopruntime.BucketKey][activation.Key()]; !ok {
			return nil, nil, fmt.Errorf("fork loop activation key contradicts its identity")
		}
	}
	for _, forked := range correspondence.ProjectedActivations() {
		if err := loopruntime.Store(carrier.StateBuckets, forked); err != nil {
			return nil, nil, err
		}
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, nil, err
	}
	for _, activation := range joins {
		if activation.Generation() == (attemptgeneration.Generation{}) {
			continue
		}
		source, err := correspondence.AdmitSource(activation.Generation())
		if err != nil {
			return nil, nil, fmt.Errorf("fork join generation: %w", err)
		}
		child, err := correspondence.Bind(source)
		if err != nil {
			return nil, nil, err
		}
		if err := joinruntime.ReplaceGeneration(carrier.StateBuckets, activation, child.Generation()); err != nil {
			return nil, nil, err
		}
	}
	for _, nodeBucket := range carrier.StateBuckets {
		value, exists := nodeBucket["handler_accumulators"]
		if !exists {
			continue
		}
		accumulators, ok := value.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("fork accumulator bucket must be an object")
		}
		projected := make(map[string]any, len(accumulators))
		for key, value := range accumulators {
			bucket, ok := timeridentity.ParseAccumulatorBucketKey(key)
			if !ok {
				return nil, nil, fmt.Errorf("fork accumulator key %q is not canonical", key)
			}
			childKey := key
			if bucket.Generation != (attemptgeneration.Generation{}) {
				source, err := correspondence.AdmitSourceKey(bucket.Generation)
				if err != nil {
					return nil, nil, fmt.Errorf("fork accumulator generation: %w", err)
				}
				child, err := correspondence.Bind(source)
				if err != nil {
					return nil, nil, err
				}
				bucket.Generation = child.Generation()
				childKey = bucket.Key()
				if _, occupied := accumulators[childKey]; occupied {
					return nil, nil, fmt.Errorf("fork accumulator destination is already occupied")
				}
			}
			if _, occupied := projected[childKey]; occupied {
				return nil, nil, fmt.Errorf("fork accumulator projections collide")
			}
			projected[childKey] = value
		}
		nodeBucket["handler_accumulators"] = projected
	}
	for key, value := range carrier.PersistedStateBuckets() {
		out[key] = value
	}
	return out, correspondence, nil
}

func forkGateActivationState(raw map[string]any, forkRunID, flowInstance, entityID string) (map[string]any, []runForkGateActivationBinding, error) {
	out, err := cloneForkLoopState(raw)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := out[gateruntime.BucketKey]; !ok {
		return out, nil, nil
	}
	structured := make(map[string]any, len(out))
	for key, value := range out {
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
	for key, value := range carrier.PersistedStateBuckets() {
		out[key] = value
	}
	return out, bindings, nil
}

func ForkGateActivationState(raw map[string]any, forkRunID, flowInstance, entityID string) (map[string]any, []RunForkGateActivationBinding, error) {
	return forkGateActivationState(raw, forkRunID, flowInstance, entityID)
}

func cloneForkLoopState(raw map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(raw))
	for key, value := range raw {
		cloned, err := cloneForkStateValue(value)
		if err != nil {
			return nil, fmt.Errorf("fork state %q: %w", key, err)
		}
		out[key] = cloned
	}
	return out, nil
}

func cloneForkStateValue(value any) (any, error) {
	switch v := value.(type) {
	case nil, bool, string, json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v, nil
	case map[string]any:
		if v == nil {
			return map[string]any(nil), nil
		}
		return cloneForkLoopState(v)
	case []any:
		if v == nil {
			return []any(nil), nil
		}
		out := make([]any, len(v))
		for i, item := range v {
			cloned, err := cloneForkStateValue(item)
			if err != nil {
				return nil, err
			}
			out[i] = cloned
		}
		return out, nil
	case json.RawMessage:
		return append(json.RawMessage(nil), v...), nil
	default:
		return nil, fmt.Errorf("unsupported persisted fork state value %T", value)
	}
}
