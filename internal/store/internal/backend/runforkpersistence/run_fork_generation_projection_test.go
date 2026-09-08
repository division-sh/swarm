package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func projectionState(t *testing.T) (map[string]any, loopruntime.Activation, timeridentity.AccumulatorBucketRef) {
	t.Helper()
	a, err := loopruntime.New("source", "entity", "", "review", "revision", "start", "draft", 4, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "collector")
	if err != nil {
		t.Fatal(err)
	}
	bucket := timeridentity.NewAccumulatorBucketRefForGeneration(node, "review.result", "window", a.Generation())
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, a); err != nil {
		t.Fatal(err)
	}
	buckets[node.Key()] = map[string]any{"handler_accumulators": map[string]any{bucket.Key(): map[string]any{"nested": []any{map[string]any{"value": "original"}}}}}
	return runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets(), a, bucket
}

func forkAttemptGenerationState(raw map[string]any, runID, entityID string) (map[string]any, error) {
	state, _, err := projectRunForkAttemptGenerationState(raw, runID, entityID)
	return state, err
}

func projectionJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestForkGenerationProjectionSourceIsolation(t *testing.T) {
	raw, _, bucket := projectionState(t)
	before := projectionJSON(t, raw)
	child, err := forkAttemptGenerationState(raw, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	if projectionJSON(t, raw) != before {
		t.Fatal("projection mutated its source")
	}
	acc := child[bucket.Node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
	for _, value := range acc {
		value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = "child"
	}
	if projectionJSON(t, raw) != before {
		t.Fatal("projected child aliases its source")
	}
	second, err := forkAttemptGenerationState(raw, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(second, child) {
		t.Fatal("source retained child mutation")
	}
}

func TestForkGenerationProjectionHistoricalAccumulator(t *testing.T) {
	raw, a, bucket := projectionState(t)
	old := a.Generation()
	if _, err := a.Repeat("draft", "repeat", time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := loopruntime.Store(carrier.StateBuckets, a); err != nil {
		t.Fatal(err)
	}
	if err := a.Close("done", "close", time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	if err := loopruntime.Store(carrier.StateBuckets, a); err != nil {
		t.Fatal(err)
	}
	raw = carrier.PersistedStateBuckets()
	before := projectionJSON(t, raw)
	child, err := forkAttemptGenerationState(raw, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	want, err := loopruntime.ForkGeneration(old, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	bucket.Generation = want
	acc := child[bucket.Node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
	if len(acc) != 1 || acc[bucket.Key()] == nil {
		t.Fatal("historical accumulator did not follow exact historical pair")
	}
	if projectionJSON(t, raw) != before {
		t.Fatal("historical projection mutated source")
	}
}

func TestForkGenerationProjectionHostileEvidence(t *testing.T) {
	for _, kind := range []string{"missing_loop", "malformed_suffix", "duplicate_loop", "foreign_reference", "occupied_destination"} {
		t.Run(kind, func(t *testing.T) {
			raw, a, bucket := projectionState(t)
			acc := raw[bucket.Node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
			switch kind {
			case "missing_loop":
				delete(raw, loopruntime.BucketKey)
			case "malformed_suffix":
				delete(acc, bucket.Key())
				bucket.Generation = attemptgeneration.Generation{}
				acc[bucket.Key()+"@generation=invalid"] = map[string]any{"value": true}
			case "duplicate_loop":
				raw[loopruntime.BucketKey].(map[string]any)["duplicate"] = raw[loopruntime.BucketKey].(map[string]any)[a.Key()]
			case "foreign_reference":
				delete(acc, bucket.Key())
				bucket.Generation.FlowID = "other"
				acc[bucket.Key()] = map[string]any{"value": true}
			case "occupied_destination":
				child, err := loopruntime.ForkGeneration(a.Generation(), "child", "entity")
				if err != nil {
					t.Fatal(err)
				}
				bucket.Generation = child
				acc[bucket.Key()] = map[string]any{"value": true}
			}
			before := projectionJSON(t, raw)
			if _, err := forkAttemptGenerationState(raw, "child", "entity"); err == nil {
				t.Fatal("accepted hostile dependent generation evidence")
			}
			if projectionJSON(t, raw) != before {
				t.Fatal("failed projection mutated source")
			}
		})
	}
}

func TestForkGenerationAccumulatorBusinessSuffixIsNotLineage(t *testing.T) {
	raw, a, bucket := projectionState(t)
	acc := raw[bucket.Node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
	delete(acc, bucket.Key())
	bucket.EventType = "business-" + a.Generation().KeySuffix()
	bucket.Window = "window-" + a.Generation().KeySuffix()
	bucket.Generation = attemptgeneration.Generation{}
	acc[bucket.Key()] = map[string]any{"value": "unchanged"}
	want := projectionJSON(t, acc)
	before := projectionJSON(t, raw)
	child, err := forkAttemptGenerationState(raw, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	got := child[bucket.Node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
	if projectionJSON(t, got) != want {
		t.Fatal("rewrote a business reference containing revision-like text")
	}
	if projectionJSON(t, raw) != before {
		t.Fatal("mutated business source")
	}
}

func TestForkGenerationProjectionNoLoopDetached(t *testing.T) {
	raw := map[string]any{"business": map[string]any{"nested": []any{map[string]any{"value": json.Number("9007199254740993")}}}}
	before := projectionJSON(t, raw)
	child, err := forkAttemptGenerationState(raw, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(raw, child) {
		t.Fatal("non-loop projection changed value types")
	}
	child["business"].(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = "child"
	if projectionJSON(t, raw) != before {
		t.Fatal("no-loop projection aliases source")
	}
}
