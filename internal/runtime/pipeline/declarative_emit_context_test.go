package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func TestPublicationIdentityResolvesAgainstProducerRoute(t *testing.T) {
	cases := []struct {
		name          string
		mode          string
		eventType     string
		producerRoute events.RouteIdentity
		want          string
	}{
		{
			mode:      "template",
			name:      "template instance local success event",
			eventType: "work.completed",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker/inst-1",
				EntityID:     "ent-worker",
			},
			want: "worker/inst-1/work.completed",
		},
		{
			mode:      "static",
			name:      "static service local failure event",
			eventType: "work.rejected",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker",
				EntityID:     "ent-worker",
			},
			want: "worker/work.rejected",
		},
		{
			mode:      "template",
			name:      "declaration reference projects exact instance",
			eventType: "worker/work.completed",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker/inst-1",
				EntityID:     "ent-worker",
			},
			want: "worker/inst-1/work.completed",
		},
		{
			mode:      "static",
			name:      "static manual prefix stays service scoped",
			eventType: "worker/work.rejected",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker",
				EntityID:     "ent-worker",
			},
			want: "worker/work.rejected",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runtimepinrouting.AdmitPublicationIdentity("worker", tc.eventType, mustPublicationRoutingSource(t, tc.mode, tc.producerRoute))
			if err != nil || string(got) != tc.want {
				t.Fatalf("publication = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func mustPublicationRoutingSource(t testing.TB, mode string, route events.RouteIdentity) events.RoutingSource {
	t.Helper()
	var (
		source events.RoutingSource
		err    error
	)
	if mode == "template" {
		source, err = events.NewConcreteTemplateInstanceRoutingSource(route)
	} else {
		source, err = events.NewStaticFlowRoutingSource(route)
	}
	if err != nil {
		t.Fatalf("construct %s publication routing source: %v", mode, err)
	}
	return source
}
