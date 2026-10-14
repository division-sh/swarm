package semanticview

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestS6AgentSubscriptionsRefuseUnanchoredControlOrigins(t *testing.T) {
	for _, authored := range []string{"platform.boot", "platform.runtime_log", "event.replayed", "*", "**", "platform.*", "event.*", "*.boot", "*.runtime_log", "*.replayed"} {
		t.Run(authored, func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: AuthoredSubscriptionConsumerAgent, ConsumerID: "listener", FlowID: ".", Authored: authored,
				LocalEvents: map[string]struct{}{"platform.boot": {}, "platform.runtime_log": {}, "event.replayed": {}},
			})
			if admission.Admitted() || admission.Failure() != AuthoredSubscriptionFailureUnanchoredAgentOrigin || len(admission.RoutePatterns()) != 0 || !strings.Contains(admission.Message(), "activity-order anchor") {
				t.Fatalf("unanchored origin gained agent admission: class=%s failure=%s patterns=%v message=%q", admission.Class(), admission.Failure(), admission.RoutePatterns(), admission.Message())
			}
		})
	}
}

func TestS6CompiledAgentControlSubscriptionAdmission(t *testing.T) {
	source := loadLifecycleStaticSource(t, canonicalrouting.CopySelectedForkReadiness(t, 1, "node"))
	for _, authored := range []string{"platform.boot", "platform.runtime_log", "event.replayed", "*", "platform.*", "event.*"} {
		admission, err := AdmitFlowOwnedAgentSubscriptions(source, FlowOwnedAgentSubscriptionRequest{
			AgentID: "root-listener", FlowID: ".", Subscriptions: []string{authored},
		})
		if err == nil || !strings.Contains(err.Error(), "activity-order anchor") || admission.ValidForAgent("root-listener") || len(admission.RoutePatterns()) != 0 {
			t.Fatalf("compiled source admitted unanchored origin %s: %+v err=%v", authored, admission, err)
		}
	}
	for _, authored := range []string{"platform.reset", "platform.agent_started"} {
		admission, err := AdmitFlowOwnedAgentSubscriptions(source, FlowOwnedAgentSubscriptionRequest{
			AgentID: "root-listener", FlowID: ".", Subscriptions: []string{authored},
		})
		if err != nil || !admission.ValidForAgent("root-listener") {
			t.Fatalf("compiled anchored control changed: %+v err=%v", admission, err)
		}
	}
	for _, authored := range []string{"platform.boot", "platform.runtime_log", "event.replayed", "*"} {
		admission := ClassifyAuthoredSubscription(source, AuthoredSubscriptionRequest{
			ConsumerKind: AuthoredSubscriptionConsumerNode, ConsumerID: "system", FlowID: ".", Authored: authored,
		})
		if !admission.Admitted() {
			t.Fatalf("compiled system consumer changed: %s", admission.Message())
		}
	}
}

func TestS6RootDotCannotBypassAgentControlAdmission(t *testing.T) {
	for _, authored := range []string{"platform.boot", "platform.runtime_log", "event.replayed", "*", "platform.*", "event.*"} {
		admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
			ConsumerKind: AuthoredSubscriptionConsumerAgent, ConsumerID: "root-listener", FlowID: ".", FlowPath: ".", Authored: authored,
			LocalEvents: map[string]struct{}{authored: {}},
		})
		if admission.Admitted() || admission.Failure() != AuthoredSubscriptionFailureUnanchoredAgentOrigin || len(admission.RoutePatternsAt(".")) != 0 {
			t.Fatalf("root execution projection bypassed control refusal for %s: %+v", authored, admission)
		}
	}
}

func TestS6ControlSubscriptionRefusalPreservesOtherConsumersAndScopes(t *testing.T) {
	for _, kind := range []AuthoredSubscriptionConsumerKind{AuthoredSubscriptionConsumerNode, AuthoredSubscriptionConsumerTimer} {
		for _, authored := range []string{"platform.boot", "platform.runtime_log", "event.replayed"} {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: kind, ConsumerID: "system", FlowID: ".", Authored: authored, LocalEvents: map[string]struct{}{authored: {}},
			})
			if !admission.Admitted() || !admission.Matches(authored) {
				t.Fatalf("%s control changed: %+v", kind, admission)
			}
		}
	}
	for _, test := range []struct{ path, authored string }{
		{"", "platform.agent_started"}, {"", "platform.reset"}, {"", "task.*"},
		{"child", "*"}, {"child", "platform.*"}, {"child", "platform.boot"},
		{"child/one", "event.*"}, {"child/one", "event.replayed"},
	} {
		t.Run(test.path+"/"+test.authored, func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: AuthoredSubscriptionConsumerAgent, ConsumerID: "listener", FlowPath: test.path, Authored: test.authored,
				LocalEvents: map[string]struct{}{test.authored: {}},
			})
			if !admission.Admitted() {
				t.Fatalf("unrelated/private/anchored admission changed: %s", admission.Message())
			}
			want := test.authored
			if test.path != "" {
				want = test.path + "/" + want
			}
			if got := admission.RoutePatterns(); len(got) != 1 || got[0] != want {
				t.Fatalf("route scope changed: got=%v want=%s", got, want)
			}
		})
	}
}
