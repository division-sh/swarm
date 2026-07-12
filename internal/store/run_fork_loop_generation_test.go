package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func TestForkAttemptGenerationRemintsActivationAndTimerIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)
	activation, err := loopruntime.New("source-run", "entity-1", "validation", "revision", "revision_id", "event-1", "drafting", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	join, err := joinruntime.NewActivation("review", "review", "review-node", "review.result", "", []string{"a"}, now, now.Add(time.Hour), "join-timeout", "platform.join_timeout", activation.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if err := joinruntime.Store(buckets, join); err != nil {
		t.Fatal(err)
	}
	accumulatorRef := timeridentity.NewAccumulatorBucketRefForGeneration("review-node", "review.result", "", activation.Generation())
	buckets["review-node"]["handler_accumulators"] = map[string]any{accumulatorRef.Key(): map[string]any{"count": 1}}
	raw := runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()
	forkedRaw, err := forkAttemptGenerationState(raw, "fork-run", "entity-1")
	if err != nil {
		t.Fatal(err)
	}
	forkedCarrier, err := runtimeengine.StateCarrierFromPersisted(nil, forkedRaw)
	if err != nil {
		t.Fatal(err)
	}
	forked, found, err := loopruntime.Load(forkedCarrier.StateBuckets, "validation", "revision")
	if err != nil || !found {
		t.Fatalf("forked activation = found %v err %v", found, err)
	}
	if forked.Attempt != activation.Attempt || forked.MaxAttempts != activation.MaxAttempts || forked.ActivationID == activation.ActivationID || forked.RevisionID == activation.RevisionID {
		t.Fatalf("forked activation = %#v, source %#v", forked, activation)
	}
	forkedJoins, err := joinruntime.List(forkedCarrier.StateBuckets)
	if err != nil || len(forkedJoins) != 1 || !forkedJoins[0].Generation.Equal(forked.Generation()) || forkedJoins[0].Key() == join.Key() {
		t.Fatalf("forked joins = %#v err=%v", forkedJoins, err)
	}
	forkedAccumulators, _ := forkedCarrier.StateBuckets["review-node"]["handler_accumulators"].(map[string]any)
	if len(forkedAccumulators) != 1 {
		t.Fatalf("forked accumulators = %#v", forkedAccumulators)
	}
	for key := range forkedAccumulators {
		if key == accumulatorRef.Key() || !strings.Contains(key, forked.Generation().KeySuffix()) {
			t.Fatalf("forked accumulator key = %q, want fork generation", key)
		}
	}

	handle := timeridentity.WorkflowTimerHandle("review.expiry")
	handle.Generation = activation.Generation()
	payload, err := json.Marshal(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	row := runForkTimerReconstructionRow{EntityID: "entity-1", TimerName: handle.TaskID(), FirePayload: payload}
	forkedRow, err := forkAttemptGenerationTimer(row, "fork-run")
	if err != nil {
		t.Fatal(err)
	}
	var forkedPayload map[string]any
	if err := json.Unmarshal(forkedRow.FirePayload, &forkedPayload); err != nil {
		t.Fatal(err)
	}
	forkedHandle, ok := timeridentity.ParseTimerHandle(forkedPayload)
	if !ok || !forkedHandle.Generation.Equal(forked.Generation()) || forkedRow.TimerName == row.TimerName {
		t.Fatalf("forked timer = row %#v handle %#v", forkedRow, forkedHandle)
	}
}

func TestForkGateActivationStateRemintsAuthorityIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	source, err := gateruntime.New("source-run", "launch/review", "entity-1", "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:"+strings.Repeat("a", 64), "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := gateruntime.Store(buckets, source); err != nil {
		t.Fatal(err)
	}
	raw := runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()
	forkedRaw, bindings, err := forkGateActivationState(raw, "fork-run", "launch/review", "entity-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Source.ActivationID != source.ActivationID {
		t.Fatalf("bindings = %#v", bindings)
	}
	forkedCarrier, err := runtimeengine.StateCarrierFromPersisted(nil, forkedRaw)
	if err != nil {
		t.Fatal(err)
	}
	forked, found, err := gateruntime.Load(forkedCarrier.StateBuckets, "launch", "launch_review")
	if err != nil || !found {
		t.Fatalf("forked gate = %#v found=%v err=%v", forked, found, err)
	}
	if forked.ActivationID == source.ActivationID || forked.CardID == source.CardID || forked.Status != gateruntime.StatusOpen {
		t.Fatalf("forked gate retained source authority: fork=%#v source=%#v", forked, source)
	}
}
