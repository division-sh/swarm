package workflowlifecycle

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestWorkflowLifecycleEffectConstructionMatrix(t *testing.T) {
	occurredAt := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)

	t.Run("initial entry", func(t *testing.T) {
		effect, err := NewInitialEntry(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "waiting", executionmode.Mock, occurredAt)
		if err != nil {
			t.Fatalf("NewInitialEntry: %v", err)
		}
		if effect.Kind() != KindInitialEntry || effect.EntityID().String() != "entity-1" || effect.ExecutionMode() != executionmode.Mock ||
			effect.InitialStage() != "waiting" || !effect.OccurredAt().Equal(occurredAt) {
			t.Fatalf("initial effect = %#v", effect)
		}
		if _, ok := effect.Transition(); ok {
			t.Fatal("initial entry unexpectedly carried a transition")
		}
	})

	t.Run("accepted event without transition", func(t *testing.T) {
		effect, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "event-1", "review.noted", executionmode.Live, occurredAt, nil)
		if err != nil {
			t.Fatalf("NewAcceptedEvent: %v", err)
		}
		if effect.Kind() != KindAcceptedEvent || effect.EntityID().String() != "entity-1" || effect.EventID() != "event-1" || effect.ExecutionMode() != executionmode.Live ||
			effect.EventType() != "review.noted" || !effect.OccurredAt().Equal(occurredAt) {
			t.Fatalf("accepted event effect = %#v", effect)
		}
		if _, ok := effect.Transition(); ok {
			t.Fatal("event-only effect unexpectedly carried a transition")
		}
	})

	t.Run("accepted event with complete transition", func(t *testing.T) {
		transition, err := NewCompiledTransition(effectTestCompiledTransition(t), handlerselection.NotApplicable(), []string{"review_allowed"})
		if err != nil {
			t.Fatalf("NewCompiledTransition: %v", err)
		}
		effect, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "event-1", "review.approved", executionmode.Live, occurredAt, &transition)
		if err != nil {
			t.Fatalf("NewAcceptedEvent: %v", err)
		}
		got, ok := effect.Transition()
		if !ok || got.From() != "waiting" || got.To() != "approved" || got.ID() != transition.ID() || !strings.HasPrefix(got.ID(), "transition:") {
			t.Fatalf("accepted transition = %#v ok=%v", got, ok)
		}
	})

	for _, test := range []struct {
		name string
		make func() error
	}{
		{
			name: "initial missing instance",
			make: func() error {
				_, err := NewInitialEntry(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.EntityID(""), "waiting", executionmode.Live, occurredAt)
				return err
			},
		},
		{
			name: "initial missing stage",
			make: func() error {
				_, err := NewInitialEntry(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "", executionmode.Live, occurredAt)
				return err
			},
		},
		{
			name: "initial missing occurrence time",
			make: func() error {
				_, err := NewInitialEntry(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "waiting", executionmode.Live, time.Time{})
				return err
			},
		},
		{
			name: "initial missing execution mode",
			make: func() error {
				_, err := NewInitialEntry(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "waiting", "", occurredAt)
				return err
			},
		},
		{
			name: "accepted event missing identity",
			make: func() error {
				_, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "", "review.noted", executionmode.Live, occurredAt, nil)
				return err
			},
		},
		{
			name: "accepted event missing type",
			make: func() error {
				_, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "event-1", "", executionmode.Live, occurredAt, nil)
				return err
			},
		},
		{
			name: "accepted event missing occurrence time",
			make: func() error {
				_, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "event-1", "review.noted", executionmode.Live, time.Time{}, nil)
				return err
			},
		},
		{
			name: "transition missing source",
			make: func() error {
				value := Transition{from: "", to: "approved"}
				return value.Validate()
			},
		},
		{
			name: "transition missing target",
			make: func() error {
				value := Transition{from: "waiting", to: ""}
				return value.Validate()
			},
		},
		{
			name: "transition missing identity",
			make: func() error {
				_, err := NewCompiledTransition(contracts.CompiledTransition{}, handlerselection.NotApplicable(), nil)
				return err
			},
		},
		{
			name: "transition does not change state",
			make: func() error {
				value := Transition{from: "waiting", to: "waiting"}
				return value.Validate()
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if err := test.make(); err == nil {
				t.Fatal("invalid lifecycle effect was constructible")
			}
		})
	}
}

func effectTestCompiledTransition(t *testing.T) contracts.CompiledTransition {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration("flow/one", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	graph := contracts.BuildWorkflowStageTopology("flow/one", "waiting", []string{"waiting", "approved"}, []string{"approved"}, []contracts.HandlerTransitionSemantic{{Node: node, EventType: "review.approved", AdvancesTo: "approved"}}, nil, nil)
	compiled, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{Node: node, HandlerEvent: "review.approved", AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler}, "waiting", "approved")
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestTransitionEvidenceRoundTripRejectsTampering(t *testing.T) {
	guards := []string{"review_allowed"}
	transition, err := NewCompiledTransition(effectTestCompiledTransition(t), handlerselection.NotApplicable(), guards)
	if err != nil {
		t.Fatal(err)
	}
	guards[0] = "mutated"
	raw, err := json.Marshal(transition)
	if err != nil {
		t.Fatal(err)
	}
	var hydrated Transition
	if err := json.Unmarshal(raw, &hydrated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transition, hydrated) || hydrated.GuardsEvaluated()[0] != "review_allowed" {
		t.Fatalf("transition evidence changed on round trip: %s", raw)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"identity", transition.ID(), "legacy_waiting-approved"},
		{"source", `"from":"waiting"`, `"from":"other"`},
		{"target", `"to":"approved"`, `"to":"other"`},
		{"flow", `"Flow":"flow/one"`, `"Flow":"flow/two"`},
		{"handler", `"HandlerEvent":"review.approved"`, `"HandlerEvent":"review.rejected"`},
		{"guards", "review_allowed", "never_evaluated"},
		{"selection", `"selection_disposition":"not_applicable"`, `"selection_disposition":"selected"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := strings.Replace(string(raw), tc.old, tc.replacement, 1)
			if mutated == string(raw) {
				t.Fatal("tampering did not change the fixture")
			}
			if err := json.Unmarshal([]byte(mutated), &hydrated); err == nil {
				t.Fatalf("tampered evidence accepted: %s", mutated)
			}
			if hydrated.ID() != transition.ID() {
				t.Fatal("failed hydration changed the destination")
			}
		})
	}
}
