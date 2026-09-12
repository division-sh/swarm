package semanticview

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func TestAdmitFlowOwnedAgentSubscriptionsCanonicalizesSameScopeExactAndPattern(t *testing.T) {
	admission, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "reviewer",
		FlowPath:      "review/inst-1",
		LocalEvents:   map[string]struct{}{"task.ready": {}, "task.done": {}},
		Subscriptions: []string{"task.ready", "task.*", "review/inst-1/task.done"},
	})
	if err != nil {
		t.Fatalf("AdmitFlowOwnedAgentSubscriptions: %v", err)
	}
	want := []string{"review/inst-1/task.*", "review/inst-1/task.done", "review/inst-1/task.ready"}
	wantPersisted := []string{"review/inst-1/task.done", "review/inst-1/task.ready", "task.*"}
	if got := admission.PersistedSubscriptions(); !reflect.DeepEqual(got, wantPersisted) {
		t.Fatalf("persisted subscriptions = %#v, want %#v", got, wantPersisted)
	}
	if got := admission.RoutePatterns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("route patterns = %#v, want %#v", got, want)
	}
}

func TestAdmitFlowOwnedAgentSubscriptionsRejectsForeignExactAndPattern(t *testing.T) {
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			_, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
				AgentID:       "reviewer",
				FlowPath:      "review/inst-1",
				Subscriptions: []string{subscription},
			})
			if err == nil || (!strings.Contains(err.Error(), "cannot cross a flow boundary") && !strings.Contains(err.Error(), "connect in the nearest common ancestor schema.yaml")) {
				t.Fatalf("error = %v, want cross-boundary rejection", err)
			}
		})
	}
}

func TestAgentSubscriptionRecoveryRetainsExactScope(t *testing.T) {
	for _, path := range []string{"", "review", "review/instance-a", "left/parent-a/child/child-b"} {
		t.Run(path, func(t *testing.T) {
			req := FlowOwnedAgentSubscriptionRequest{
				AgentID: "reviewer", FlowPath: path,
				LocalEvents: map[string]struct{}{"task.ready": {}}, Subscriptions: []string{"task.*", "task.ready"},
			}
			initial, err := AdmitFlowOwnedAgentSubscriptions(nil, req)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				req.Subscriptions = initial.PersistedSubscriptions()
				recovered, err := AdmitFlowOwnedAgentSubscriptions(nil, req)
				if err != nil {
					t.Fatalf("recovery %d: %v", i, err)
				}
				if !reflect.DeepEqual(initial.RoutePatterns(), recovered.RoutePatterns()) || !reflect.DeepEqual(initial.PersistedSubscriptions(), recovered.PersistedSubscriptions()) {
					t.Fatalf("recovery changed admission: %+v -> %+v", initial, recovered)
				}
				for _, pattern := range recovered.RoutePatterns() {
					if eventidentity.MatchPattern(pattern, "sibling/instance/task.ready") || eventidentity.MatchPattern(pattern, path+"/descendant/task.ready") {
						t.Fatalf("route %q crosses its scope %q", pattern, path)
					}
				}
				initial = recovered
			}
		})
	}
}

func TestAdmitFlowOwnedAgentSubscriptionsRejectsRootCrossFlowWildcard(t *testing.T) {
	_, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "root-observer",
		LocalEvents:   map[string]struct{}{"task.ready": {}},
		Subscriptions: []string{"task.ready", "**/task.done"},
	})
	if err == nil || !strings.Contains(err.Error(), "connect in the nearest common ancestor schema.yaml") {
		t.Fatalf("error = %v, want connect teaching rejection", err)
	}
}

func TestFlowOwnedAgentSubscriptionAdmissionCarrierOnlyRetainsIdentityWithoutRoutes(t *testing.T) {
	admission, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "selected-agent",
		LocalEvents:   map[string]struct{}{"task.ready": {}},
		Subscriptions: []string{"task.ready"},
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier := admission.CarrierOnly()
	if !carrier.ValidForAgent("selected-agent") || len(carrier.RoutePatterns()) != 0 {
		t.Fatalf("carrier admission = %#v routes = %#v", carrier, carrier.RoutePatterns())
	}
}
