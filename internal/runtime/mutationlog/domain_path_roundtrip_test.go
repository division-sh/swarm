package mutationlog

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMutationDomainPathChronologicalFold(t *testing.T) {
	for _, domain := range []Domain{DomainAccumulator, DomainGate, DomainBookkeeping} {
		t.Run(string(domain), func(t *testing.T) {
			records := []ProjectionMutation{
				{Domain: DomainLifecycleState, NewValue: "pending"},
				{Domain: domain, Path: "a.b", NewValue: map[string]any{"version": "old"}},
				{Domain: domain, Path: "a", NewValue: map[string]any{"b": "independent"}},
				{Domain: DomainAuthoredField, Path: "a.b", NewValue: "authored"},
				{Domain: domain, Path: "a.b", NewValue: json.Number("9007199254740993")},
				{Domain: DomainAuthoredField, Path: "a.c", NewValue: "retained"},
				{Domain: DomainAuthoredField, Path: "a.b", NewValue: nil},
				{Domain: DomainLifecycleState, NewValue: "completed"},
			}
			got, err := ReconstructEntityStateProjection(records)
			if err != nil {
				t.Fatal(err)
			}
			var atomic map[string]any
			switch domain {
			case DomainAccumulator:
				atomic = got.Accumulator
			case DomainGate:
				atomic = got.Gates
			case DomainBookkeeping:
				atomic = got.Bookkeeping
			}
			want := map[string]any{"a.b": json.Number("9007199254740993"), "a": map[string]any{"b": "independent"}}
			if got.CurrentState != "completed" || !reflect.DeepEqual(got.Fields, map[string]any{"a": map[string]any{"c": "retained"}}) || !reflect.DeepEqual(atomic, want) {
				t.Fatalf("domain interpretation/chronology changed: %#v", got)
			}
			if err := ApplyEntityStateProjectionMutation(&got, domain, "a.b", nil); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(atomic, map[string]any{"a": map[string]any{"b": "independent"}}) {
				t.Fatalf("atomic removal changed neighboring identity: %#v", atomic)
			}
			if err := ApplyEntityStateProjectionMutation(&got, domain, "a", nil); err != nil || len(atomic) != 0 {
				t.Fatalf("clear: %#v %v", atomic, err)
			}
		})
	}
}
