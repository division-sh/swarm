package joinruntime

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestA2PersistedJoinProjectionPreservesDomainBoundary(t *testing.T) {
	arm, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "entry", attemptgeneration.Generation{}), []string{"a"}, nil, time.Unix(100, 0), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, arm); err != nil {
		t.Fatal(err)
	}
	raw := map[string]any{
		"total": float64(7), "list": []any{1}, "opaque": map[string]any{"handler_joins": "business"},
		"handler_joins_extra": nil,
	}
	for key, value := range buckets {
		raw[key] = value
	}
	got, err := PersistedBuckets(raw)
	if err != nil || !reflect.DeepEqual(got, buckets) {
		t.Fatalf("join projection = %#v, want %#v: %v", got, buckets, err)
	}
	joins, err := List(got)
	if err != nil || len(joins) != 1 || !joins[0].JoinRef().Equal(arm.JoinRef()) || len(raw) != 5 || raw["total"] != float64(7) {
		t.Fatalf("projection lost or reinterpreted evidence: %#v %v", raw, err)
	}
}

func TestA2PersistedJoinProjectionRejectsOwnedCorruption(t *testing.T) {
	owned := joinNodeBucketKey(identitytest.RootNode(t, "node"))
	for _, hostile := range []struct {
		name, key string
		value     any
	}{
		{"scalar", owned, float64(7)},
		{"null", owned, nil},
		{"nil_object", owned, map[string]any(nil)},
		{"missing_map", owned, map[string]any{}},
		{"null_map", owned, map[string]any{"handler_joins": nil}},
		{"scalar_map", owned, map[string]any{"handler_joins": "corrupt"}},
		{"activation", owned, map[string]any{"handler_joins": map[string]any{"wrong": map[string]any{}}}},
		{"node", "handler_joins:bad node", map[string]any{"handler_joins": map[string]any{}}},
		{"empty_node", "handler_joins:", map[string]any{"handler_joins": map[string]any{}}},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			raw := map[string]any{hostile.key: hostile.value, "total": float64(7)}
			before := make(map[string]any, len(raw))
			for key, value := range raw {
				before[key] = value
			}
			if got, err := PersistedBuckets(raw); err == nil || got != nil {
				t.Fatalf("accepted corrupt join evidence: %#v %v", got, err)
			}
			if !reflect.DeepEqual(raw, before) {
				t.Fatal("refusal changed persisted evidence")
			}
		})
	}
}
