package runfork

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func TestInputPublicationDoesNotReplaceOtherPublicationOwners(t *testing.T) {
	run, id, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
	external, err := events.NewExternalIngressRoutingSource("child", entity, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	root, err := events.NewRootRoutingSource(entity)
	if err != nil {
		t.Fatal(err)
	}
	static, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: entity})
	if err != nil {
		t.Fatal(err)
	}
	template := eventtest.ConcreteTemplateRoutingSource("child", "child/one", entity)
	control, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: entity})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		source events.RoutingSource
		want   bool
	}{
		{"api_absence", events.NoRoutingSource(), true}, {"external_ingress", external, true},
		{"declared_root", root, false}, {"static_producer", static, false}, {"template_producer", template, false},
		{"flow_control", control, false}, {"platform_control", events.NewPlatformControlRoutingSource(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := eventtest.ExistingRunRootIngressWithRoutingSource(id, "work.first", "source", "", []byte(`{}`), 0, run, events.EventEnvelope{}, tc.source, time.Now().UTC())
			event, err = eventtest.AdmitPayload(event, ".", "work.first")
			if err != nil {
				t.Fatal(err)
			}
			input, err := InputPublicationFromEvent(event)
			if err != nil {
				t.Fatal(err)
			}
			if _, present := input.Event(); present != tc.want {
				t.Fatalf("input=%v want=%v", present, tc.want)
			}
		})
	}
	event := eventtest.OperatorInjected(id, "work.first", "operator", "", []byte(`{}`), 0, run, nil, events.EventEnvelope{}, time.Now().UTC())
	if _, err := InputPublicationFromEvent(event); err == nil {
		t.Fatal("input without persisted schema accepted")
	}
}
