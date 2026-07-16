package semanticview

import (
	"reflect"
	"strings"
	"testing"
)

func TestAdmitFlowOwnedAgentSubscriptionsCanonicalizesSameScopeExactAndPattern(t *testing.T) {
	admission, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "reviewer",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*", "review/inst-1/task.done"},
	})
	if err != nil {
		t.Fatalf("AdmitFlowOwnedAgentSubscriptions: %v", err)
	}
	want := []string{"review/inst-1/task.*", "review/inst-1/task.done", "review/inst-1/task.ready"}
	if got := admission.PersistedSubscriptions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted subscriptions = %#v, want %#v", got, want)
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
			if err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("error = %v, want cross-boundary rejection", err)
			}
		})
	}
}

func TestAdmitFlowOwnedAgentSubscriptionsPreservesRootExactAndNonImportWildcard(t *testing.T) {
	admission, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "root-observer",
		Subscriptions: []string{"task.ready", "**/task.done"},
	})
	if err != nil {
		t.Fatalf("AdmitFlowOwnedAgentSubscriptions: %v", err)
	}
	want := []string{"**/task.done", "task.ready"}
	if got := admission.RoutePatterns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("route patterns = %#v, want %#v", got, want)
	}
}

func TestFlowOwnedAgentSubscriptionAdmissionCarrierOnlyRetainsIdentityWithoutRoutes(t *testing.T) {
	admission, err := AdmitFlowOwnedAgentSubscriptions(nil, FlowOwnedAgentSubscriptionRequest{
		AgentID:       "selected-agent",
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
