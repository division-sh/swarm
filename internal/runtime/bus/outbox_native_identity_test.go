package bus_test

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

func TestNativeOutboxConflictingImmutableSourcesRefuseWithoutPartialEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variation := range []string{"payload", "time", "run", "source", "route"} {
			t.Run(backend+"/"+variation, func(t *testing.T) {
				f := newNativeOutboxFixture(t, backend)
				original := f.intent("native-immutable-conflict")
				original.Recipients = nil
				candidate := conflictingNativeOutboxSource(original.Event, variation)
				result, err := f.commit([]engine.EmitIntent{original, {Event: candidate}})
				if err == nil || result.Committed || f.eventCount(t) != 0 {
					t.Fatalf("conflicting %s escaped: result=%+v err=%v", variation, result, err)
				}
				if variation != "run" && !errors.Is(err, events.ErrEventIdentityConflict) {
					t.Fatalf("conflict was not complete immutable identity refusal: %v", err)
				}
				assertNativeOutboxClaimReleased(t, f, original.Event.ID())
			})
		}
	}
}

func conflictingNativeOutboxSource(event events.Event, variation string) events.Event {
	payload, at, run, source, envelope := event.Payload(), event.CreatedAt(), event.RunID(), event.SourceAgent(), event.Envelope()
	switch variation {
	case "payload":
		payload = []byte(`{"value":"changed"}`)
	case "time":
		at = at.Add(time.Second)
	case "run":
		run = eventtest.UUID("foreign-run")
		envelope.Source = eventtest.RootRoutingSource(run).Route()
	case "source":
		source = "different-source"
	case "route":
		envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run})
	default:
		panic("unknown immutable source control")
	}
	return eventtest.ExistingRunRootIngress(event.ID(), event.Type(), source, event.TaskID(), payload, event.ChainDepth(), run, envelope, at)
}
