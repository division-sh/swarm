package mutationlog

import (
	"reflect"
	"testing"
)

func TestQ10PartialDomainDiffAndReconstructionAbsence(t *testing.T) {
	t.Run("data_only", func(t *testing.T) {
		state, err := ReconstructEntityStateProjection([]ProjectionMutation{
			{Domain: DomainAuthoredField, Path: "current_state", NewValue: "unknown"},
			{Domain: DomainAuthoredField, Path: "gates", NewValue: map[string]any{}},
		})
		if err != nil || state.CurrentState != nil || state.Gates != nil || state.Bookkeeping != nil || state.Accumulator != nil {
			t.Fatalf("data-only history invented lifecycle: state=%#v err=%v", state, err)
		}
		if state.Fields["current_state"] != "unknown" {
			t.Fatalf("authored stage-like field was lost: %#v", state.Fields)
		}
	})
	t.Run("empty_import", func(t *testing.T) {
		state, err := ReconstructEntityStateProjection(nil)
		if err != nil || state.CurrentState != nil || state.Gates != nil || state.Bookkeeping != nil || state.Accumulator != nil || state.Fields == nil {
			t.Fatalf("empty history invented lifecycle: state=%#v err=%v", state, err)
		}
	})
	t.Run("birth_preserves_prior_domain_entries", func(t *testing.T) {
		state, err := ReconstructEntityStateProjection([]ProjectionMutation{
			{Domain: DomainGate, Path: "accepted", NewValue: true},
			{Domain: DomainLifecycleState, NewValue: "unknown"},
		})
		if err != nil || state.CurrentState == nil || *state.CurrentState != "unknown" || state.Gates["accepted"] != true || state.Bookkeeping == nil || state.Accumulator == nil {
			t.Fatalf("birth lost domain evidence: state=%#v err=%v", state, err)
		}
	})
	t.Run("partial_delta", func(t *testing.T) {
		before := EntityStateProjection{Accumulator: map[string]any{"gate": "open"}}
		after := EntityStateProjection{Accumulator: map[string]any{}}
		rows, err := BuildEntityStateDiffRecords("entity", before, after, Writer{Type: "platform", ID: "gate", HandlerStep: "supersede"})
		if err != nil || len(rows) != 1 || rows[0].Domain != DomainAccumulator || rows[0].Path != "gate" || rows[0].NewValue != nil {
			t.Fatalf("partial delta invented lifecycle: rows=%#v err=%v", rows, err)
		}
	})
	t.Run("unchanged_present_state", func(t *testing.T) {
		before := EntityStateProjection{CurrentState: PresentLifecycleState("unknown")}
		after := EntityStateProjection{CurrentState: PresentLifecycleState("unknown")}
		rows, err := BuildEntityStateDiffRecords("entity", before, after, Writer{Type: "platform", ID: "node"})
		if err != nil || !reflect.DeepEqual(rows, []Record{}) {
			t.Fatalf("pointer identity became a transition: rows=%#v err=%v", rows, err)
		}
	})
	for _, value := range []any{nil, "", " ", 0, false} {
		t.Run("invalid_birth", func(t *testing.T) {
			if _, err := ReconstructEntityStateProjection([]ProjectionMutation{{Domain: DomainLifecycleState, NewValue: value}}); err == nil {
				t.Fatalf("invalid lifecycle birth admitted: %#v", value)
			}
		})
	}
}
