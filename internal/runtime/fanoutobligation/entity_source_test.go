package fanoutobligation

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func Test2293EntitySourceCaptureIsDetachedTransientAndRequired(t *testing.T) {
	r := a2SourceProjectionRequest(t)
	r.Capsule.EntityID = r.Key.RunID
	r.Source = SourceRef{Kind: SourceEntityField, RunID: r.Key.RunID, EntityID: r.Capsule.EntityID, Field: "items"}
	r.Capsule.SourceProjection.ItemsFrom = "entity.items"
	var err error
	r.PlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CapturedEntitySource(); err == nil {
		t.Fatal("missing capture admitted")
	}
	value := map[string]any{"a": []any{1}, "b": []any{2}, "c": []any{3}}
	r, err = r.CaptureEntitySource(value)
	if err != nil {
		t.Fatal(err)
	}
	value["a"].([]any)[0] = 99
	first, err := r.CapturedEntitySource()
	if err != nil {
		t.Fatal(err)
	}
	first.(map[string]any)["a"] = []any{99}
	second, err := r.CapturedEntitySource()
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal("capture lent mutable storage")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var retained IntentRequest
	if err := json.Unmarshal(raw, &retained); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.CapturedEntitySource(); err == nil {
		t.Fatal("wire retained duplicate source content")
	}
	if err := retained.Validate(); err != nil {
		t.Fatalf("retained request must validate without transient capture: %v", err)
	}
	for _, bad := range []any{nil, []any{}, map[string]any{"a": []any{1}}, map[string]any{"a": []any{"wrong"}, "b": []any{2}, "c": []any{3}}} {
		if _, err := r.CaptureEntitySource(bad); err == nil {
			t.Fatalf("invalid capture admitted: %v", bad)
		}
	}
	r.Source.Kind = SourceEventPayloadField
	if _, err := r.CaptureEntitySource(value); err == nil {
		t.Fatal("payload intent admitted entity capture")
	}
}
