package workflowlifecycle

import (
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
		transition, err := NewTransition("waiting", "approved", "waiting-approved")
		if err != nil {
			t.Fatalf("NewTransition: %v", err)
		}
		effect, err := NewAcceptedEvent(runtimeflowidentity.RouteForInstancePath("flow/one"), identity.NormalizeEntityID("entity-1"), "event-1", "review.approved", executionmode.Live, occurredAt, &transition)
		if err != nil {
			t.Fatalf("NewAcceptedEvent: %v", err)
		}
		got, ok := effect.Transition()
		if !ok || got.From() != "waiting" || got.To() != "approved" || got.ID() != "waiting-approved" {
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
				_, err := NewTransition("", "approved", "waiting-approved")
				return err
			},
		},
		{
			name: "transition missing target",
			make: func() error {
				_, err := NewTransition("waiting", "", "waiting-approved")
				return err
			},
		},
		{
			name: "transition missing identity",
			make: func() error {
				_, err := NewTransition("waiting", "approved", "")
				return err
			},
		},
		{
			name: "transition does not change state",
			make: func() error {
				_, err := NewTransition("waiting", "waiting", "waiting-waiting")
				return err
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
