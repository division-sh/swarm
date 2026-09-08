package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func TestRemintRunForkPayloadPreservesOrdinaryBusinessFields(t *testing.T) {
	raw := json.RawMessage(`{"entity_id":"source-run","source_run_id":"source-run","source_event_id":"source-event","parent_event_id":"parent-event","flow_instance":"business/flow","loop_generation":{"business":"not-an-activity"},"opaque_revision":"old","large":9007199254740993,"decimal":1.2300e+09}`)
	activation, err := loopruntime.New("source-run", "source-entity", "producer", "revision", "opaque_revision", "source-event", "pending", 3, time.Unix(1700003100, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	generation, err := loopruntime.ForkGeneration(activation.Generation(), "child-run", "source-entity")
	if err != nil {
		t.Fatal(err)
	}
	sourceGeneration, err := json.Marshal(activation.Generation())
	if err != nil {
		t.Fatal(err)
	}
	for _, businessLoop := range []struct {
		name string
		raw  json.RawMessage
	}{
		{name: "arbitrary_loop_value", raw: raw},
		{name: "generation_shaped_business_value", raw: bytes.Replace(raw, []byte(`{"business":"not-an-activity"}`), sourceGeneration, 1)},
	} {
		t.Run(businessLoop.name, func(t *testing.T) {
			raw := businessLoop.raw
			for _, state := range []string{"zero", "declared_revision"} {
				t.Run(state, func(t *testing.T) {
					var generations []attemptgeneration.Generation
					if state == "declared_revision" {
						generations = []attemptgeneration.Generation{generation}
					}
					got, err := remintRunForkPayload(raw, generations)
					if err != nil {
						t.Fatal(err)
					}
					if state == "zero" && !bytes.Equal(got, raw) {
						t.Fatalf("ordinary business payload changed without generation authority: %s", got)
					}
					var wantFields, gotFields map[string]json.RawMessage
					if err := json.Unmarshal(raw, &wantFields); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(got, &gotFields); err != nil {
						t.Fatal(err)
					}
					if state == "declared_revision" {
						wantFields["opaque_revision"], _ = json.Marshal(generation.RevisionID)
					}
					for key, want := range wantFields {
						if !bytes.Equal(gotFields[key], want) {
							t.Errorf("ordinary field %s changed without its own declaration: got=%s want=%s", key, gotFields[key], want)
						}
					}
				})
			}
		})
	}
}
