package joinruntime

import (
	"reflect"
	"testing"
	"time"
)

func TestActivationOrdersResultsByMembershipAndClassifiesDuplicates(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation("line_items", "awaiting", "node", "item.done", "dispatch-1", []string{"a", "b"}, now, now.Add(time.Hour), "task", "platform.join_timeout")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := activation.Add("b", map[string]any{"score": 2}); err != nil || got != AddAccepted {
		t.Fatalf("add b = %q, %v", got, err)
	}
	if got, err := activation.Add("a", map[string]any{"score": 1}); err != nil || got != AddAccepted {
		t.Fatalf("add a = %q, %v", got, err)
	}
	if got, want := activation.Results(), []any{map[string]any{"score": float64(1)}, map[string]any{"score": float64(2)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %#v, want membership order %#v", got, want)
	}
	if got, err := activation.Add("a", map[string]any{"score": 1}); err != nil || got != AddExactDuplicate {
		t.Fatalf("exact duplicate = %q, %v", got, err)
	}
	if got, err := activation.Add("a", map[string]any{"score": 9}); err != nil || got != AddConflictingDuplicate {
		t.Fatalf("conflicting duplicate = %q, %v", got, err)
	}
	if got, err := activation.Add("c", map[string]any{"score": 3}); err != nil || got != AddUnexpected {
		t.Fatalf("unexpected member = %q, %v", got, err)
	}
}

func TestActivationPersistsThroughTypedStateBuckets(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation("join", "awaiting", "node", "item.done", "", []string{}, now, now.Add(time.Hour), "task", "platform.join_timeout")
	if err != nil {
		t.Fatal(err)
	}
	activation.Close(CloseReasonComplete, true, false)
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := Load(buckets, "node", activation.Key())
	if err != nil || !ok {
		t.Fatalf("load = %#v, %v, %v", loaded, ok, err)
	}
	if !reflect.DeepEqual(loaded, activation) {
		t.Fatalf("round trip = %#v, want %#v", loaded, activation)
	}
}

func TestNewActivationRejectsInvalidMembership(t *testing.T) {
	now := time.Now().UTC()
	for _, members := range [][]string{{""}, {"a", "a"}} {
		if _, err := NewActivation("join", "awaiting", "node", "item.done", "", members, now, now.Add(time.Hour), "task", "platform.join_timeout"); err == nil {
			t.Fatalf("members %#v accepted", members)
		}
	}
}
