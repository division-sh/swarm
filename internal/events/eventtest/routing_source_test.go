package eventtest

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestExplicitChildSourceFixturesPreserveAdmittedAuthority(t *testing.T) {
	entityID := uuid.NewString()
	for _, source := range []events.RoutingSource{
		RootRoutingSource(entityID),
		StaticFlowRoutingSource("review", "review", entityID),
		ConcreteTemplateRoutingSource("review", "review/instance", entityID),
	} {
		for _, namedProducer := range []bool{false, true} {
			t.Run(source.Kind().StorageCode()+"/named="+strconv.FormatBool(namedProducer), func(t *testing.T) {
				lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
				envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route())
				envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "other", FlowInstance: "other/child", EntityID: uuid.NewString()})
				var event events.Event
				if namedProducer {
					event = ChildForProducerWithRoutingSource(uuid.NewString(), "review.ready", Producer(events.EventProducerPlatform, "workflow"), "", []byte(`{}`), 1, lineage, envelope, source, time.Now().UTC())
				} else {
					event = ChildWithLineageAndRoutingSource(uuid.NewString(), "review.ready", "reviewer", "", []byte(`{}`), 1, lineage, envelope, source, time.Now().UTC())
				}
				if !reflect.DeepEqual(event.RoutingSource(), source) || !reflect.DeepEqual(event.Envelope(), envelope) {
					t.Fatalf("fixture replaced source or recipient: source=%#v envelope=%#v", event.RoutingSource(), event.Envelope())
				}
			})
		}
	}
}

func TestExplicitSourceFixtureRejectsContradictoryEnvelope(t *testing.T) {
	for _, namedProducer := range []bool{false, true} {
		t.Run("named="+strconv.FormatBool(namedProducer), func(t *testing.T) {
			source := RootRoutingSource(uuid.NewString())
			envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: uuid.NewString()})
			lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
			defer func() {
				if failure := recover(); failure == nil || !strings.Contains(fmt.Sprint(failure), "event envelope source does not match typed routing source") {
					t.Fatalf("contradiction must reach canonical event validation: %v", failure)
				}
			}()
			if namedProducer {
				ChildForProducerWithRoutingSource(uuid.NewString(), "review.ready", Producer(events.EventProducerPlatform, "workflow"), "", []byte(`{}`), 1, lineage, envelope, source, time.Now().UTC())
			} else {
				ChildWithLineageAndRoutingSource(uuid.NewString(), "review.ready", "reviewer", "", []byte(`{}`), 1, lineage, envelope, source, time.Now().UTC())
			}
		})
	}
}

func TestExplicitIngressAndControlFixturesPreserveSource(t *testing.T) {
	entityID, runID := uuid.NewString(), uuid.NewString()
	// This is not an inferable ingress envelope. Only the supplied admitted
	// ingress authority may decide its projection, never the receiver shape.
	envelope := events.EventEnvelope{Source: events.RouteIdentity{EntityID: entityID}}
	source, err := events.NewExternalIngressRoutingSource("review", entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []events.Event{
		RunCreatingRootIngressWithRoutingSource(uuid.NewString(), "review.requested", "provider", "", []byte(`{}`), 0, runID, "", envelope, source, time.Now().UTC()),
		ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "review.requested", "provider", "", []byte(`{}`), 0, runID, envelope, source, time.Now().UTC()),
	} {
		if !reflect.DeepEqual(event.RoutingSource(), source) || !event.Envelope().Source.Empty() {
			t.Fatal("ingress fixture changed admitted source or retained an execution source envelope")
		}
	}
	control, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	event := RuntimeControlWithRoutingSource(uuid.NewString(), "review.timer", "workflow", "", []byte(`{}`), 0, runID, "", events.EnvelopeForSourceRoute(events.EventEnvelope{}, control.Route()), control, time.Now().UTC())
	if !reflect.DeepEqual(event.RoutingSource(), control) {
		t.Fatal("control fixture changed admitted authority")
	}
}
