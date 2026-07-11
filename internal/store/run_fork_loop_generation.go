package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

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
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, structured)
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
		replacement, ok := replacements[activation.Generation.RevisionID]
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

func forkAttemptGenerationTimer(row runForkTimerReconstructionRow, forkRunID string) (runForkTimerReconstructionRow, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(row.FirePayload, &payload); err != nil {
		return row, err
	}
	handle, ok := timeridentity.ParseTimerHandle(payload)
	if !ok || !handle.Generation.Valid() {
		return row, nil
	}
	forked, err := loopruntime.ForkGeneration(handle.Generation, forkRunID, row.EntityID)
	if err != nil {
		return row, fmt.Errorf("fork timer loop generation: %w", err)
	}
	handle.Generation = forked
	metadata := handle.PayloadMetadata()
	for key, value := range payload {
		if key == "timer_handle" || key == handle.Generation.RevisionField {
			continue
		}
		metadata[key] = value
	}
	metadata[forked.RevisionField] = forked.RevisionID
	raw, err := json.Marshal(metadata)
	if err != nil {
		return row, err
	}
	row.TimerName = handle.TaskID()
	row.FirePayload = raw
	return row, nil
}

func cloneForkLoopState(raw map[string]any) map[string]any {
	out := make(map[string]any, len(raw))
	for key, value := range raw {
		out[key] = value
	}
	return out
}
