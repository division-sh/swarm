package eventfixture

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestBindPayloadRetainsExactSourceSchemaCoordinate(t *testing.T) {
	runID := eventtest.UUID("payload-root-run")
	entityID := eventtest.UUID("payload-provider-entity")
	provider, err := events.NewExternalIngressRoutingSource("provider", entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		source events.RoutingSource
		flow   string
	}{
		{"absent", events.NoRoutingSource(), ""},
		{"root", eventtest.RootRoutingSource(runID), "."},
		{"provider", provider, "provider"},
		{"private", eventtest.StaticFlowRoutingSource("child", "child", entityID), "child"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(tc.name), "work.ready", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, tc.source, time.Now().UTC())
			bound, err := BindPayload(event)
			if err != nil {
				t.Fatal(err)
			}
			admission, ok := bound.PayloadAdmission()
			if !ok || admission.Binding().FlowID() != tc.flow || bound.RoutingSource() != tc.source {
				t.Fatalf("source/schema coordinate changed: %+v", bound)
			}
			if _, err := BindPayload(bound); err != nil {
				t.Fatalf("identical fixture binding changed: %v", err)
			}
		})
	}
}
