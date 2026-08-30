package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestClassifyAuthoredSubscriptionExactAdmissionMatrix(t *testing.T) {
	tests := []struct {
		name      string
		kind      AuthoredSubscriptionConsumerKind
		authored  string
		wantClass AuthoredSubscriptionAdmissionClass
		wantFail  AuthoredSubscriptionFailure
	}{
		{name: "node local", kind: AuthoredSubscriptionConsumerNode, authored: "task.done", wantClass: AuthoredSubscriptionLocalExact},
		{name: "node same scope qualified", kind: AuthoredSubscriptionConsumerNode, authored: "child/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "node unresolved qualified", kind: AuthoredSubscriptionConsumerNode, authored: "missing/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "node descendant qualified", kind: AuthoredSubscriptionConsumerNode, authored: "child/grandchild/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "node full uri", kind: AuthoredSubscriptionConsumerNode, authored: "swarm://child/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "agent local", kind: AuthoredSubscriptionConsumerAgent, authored: "task.done", wantClass: AuthoredSubscriptionLocalExact},
		{name: "agent same scope qualified", kind: AuthoredSubscriptionConsumerAgent, authored: "child/task.done", wantClass: AuthoredSubscriptionSameScopeAgentExact},
		{name: "agent unresolved qualified", kind: AuthoredSubscriptionConsumerAgent, authored: "missing/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "agent descendant qualified", kind: AuthoredSubscriptionConsumerAgent, authored: "child/grandchild/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "agent full uri", kind: AuthoredSubscriptionConsumerAgent, authored: "swarm://child/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "timer local", kind: AuthoredSubscriptionConsumerTimer, authored: "task.done", wantClass: AuthoredSubscriptionLocalExact},
		{name: "timer qualified", kind: AuthoredSubscriptionConsumerTimer, authored: "child/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "timer full uri", kind: AuthoredSubscriptionConsumerTimer, authored: "swarm://child/task.done", wantFail: AuthoredSubscriptionFailureQualifiedExact},
		{name: "timer wildcard", kind: AuthoredSubscriptionConsumerTimer, authored: "task.*", wantFail: AuthoredSubscriptionFailureTimerPatternForbidden},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: tc.kind,
				ConsumerID:   "consumer",
				FlowID:       "child",
				FlowPath:     "child",
				Authored:     tc.authored,
			})
			if admission.Class() != tc.wantClass || admission.Failure() != tc.wantFail {
				t.Fatalf("admission = class %q failure %q message %q, want class %q failure %q", admission.Class(), admission.Failure(), admission.Message(), tc.wantClass, tc.wantFail)
			}
			if got := admission.Admitted(); got != (tc.wantFail == "") {
				t.Fatalf("Admitted() = %t, want %t", got, tc.wantFail == "")
			}
		})
	}
}

func TestClassifyAuthoredSubscriptionScopesNonRootNodePatterns(t *testing.T) {
	for _, authored := range []string{"task.*", "*.done", "*", "missing.*", "*/task.done", "**/task.done"} {
		t.Run(authored, func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: AuthoredSubscriptionConsumerNode,
				ConsumerID:   "listener",
				FlowID:       "child",
				FlowPath:     "child",
				LocalEvents:  map[string]struct{}{"task.done": {}},
				Authored:     authored,
			})
			want := "child/" + authored
			if !admission.Admitted() || admission.Class() != AuthoredSubscriptionLocalPattern {
				t.Fatalf("admission = class %q failure %q, want admitted local pattern", admission.Class(), admission.Failure())
			}
			if got := admission.RoutePatterns(); len(got) != 1 || got[0] != want {
				t.Fatalf("route patterns = %#v, want %#v", got, []string{want})
			}
		})
	}

	root := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   "root-listener",
		Authored:     "*.done",
	})
	if got := root.RoutePatterns(); len(got) != 1 || got[0] != "*.done" {
		t.Fatalf("root route patterns = %#v, want existing root wildcard behavior", got)
	}
}

func TestAuthoredSubscriptionAdmissionMatchesOnlyAuthorizedPatternProjection(t *testing.T) {
	admission := AuthoredSubscriptionAdmission{
		authored:      "producer/**/task.done",
		routePatterns: []string{"worker/**/task.done"},
		class:         AuthoredSubscriptionImportedPattern,
	}
	if admission.Matches("producer/task.done") {
		t.Fatal("raw authored wildcard bypassed the authorized route projection")
	}
	if !admission.Matches("worker/instance-1/task.done") {
		t.Fatal("authorized route projection did not match its admitted event")
	}
}

func TestAuthoredSubscriptionAdmissionMatchesOwnFlowInputWithoutOpeningSiblingExact(t *testing.T) {
	local := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   "listener",
		FlowPath:     "receiver",
		InputEvents:  []string{"task.ready"},
		Authored:     "task.ready",
	})
	if !local.MatchesReceiverInput("receiver/task.ready", "receiver", []string{"task.ready"}) {
		t.Fatal("admitted receiver-local exact did not match its own-flow input event")
	}
	invalid := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   "listener",
		FlowPath:     "receiver",
		InputEvents:  []string{"task.ready"},
		Authored:     "producer/task.ready",
	})
	if invalid.MatchesReceiverInput("producer/task.ready", "receiver", []string{"task.ready"}) {
		t.Fatal("invalid qualified exact bypassed admission through receiver input localization")
	}
	wildcard := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   "listener",
		FlowPath:     "receiver",
		InputEvents:  []string{"task.ready"},
		Authored:     "*",
	})
	if !wildcard.MatchesReceiverInput("receiver/task.ready", "receiver", []string{"task.ready"}) {
		t.Fatal("wildcard did not match an event localized through a declared receiver input")
	}
	if wildcard.MatchesReceiverInput("producer/task.other", "receiver", []string{"task.ready"}) {
		t.Fatal("wildcard matched an event that was not localized through a declared receiver input")
	}
}

func TestResolveNodeSubscriptionHandlerPrioritizesExactBeforeWildcard(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path:   "child",
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.completed": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {
				ID: "listener",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"*.completed":    {},
					"task.completed": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	source := Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &root.Children[0]},
		},
	})

	resolved := ResolveExecutableNodeSubscriptionHandler(source, identitytest.FlowNode(t, "child", "listener"), "child/task.completed")
	if !resolved.Matched || resolved.HandlerEventKey != "task.completed" {
		t.Fatalf("resolved handler = %#v, want exact task.completed before wildcard", resolved)
	}
}

func TestResolveFlowNodeSubscriptionHandlerRejectsBareSubscriptionAsExecutableHandler(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path:   "child",
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.requested": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {ID: "listener", SubscribesTo: []string{"task.requested"}},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	source := Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &root.Children[0]},
		},
	})

	resolved := ResolveExecutableNodeSubscriptionHandler(source, identitytest.FlowNode(t, "child", "listener"), "child/task.requested")
	if resolved.Matched {
		t.Fatalf("resolved handler = %#v, want routing-only subscription to remain non-executable", resolved)
	}
}

func TestResolveNodeSubscriptionHandlerScopesLocalWildcardToOwnerFlow(t *testing.T) {
	for _, authored := range []string{"task.*", "*"} {
		t.Run(authored, func(t *testing.T) {
			child := runtimecontracts.FlowContractView{
				Path:   "child",
				Paths:  runtimecontracts.FlowContractPaths{FlowPath: "child"},
				Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					"listener": {ID: "listener", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{authored: {}}},
				},
			}
			sibling := runtimecontracts.FlowContractView{
				Path:   "sibling",
				Paths:  runtimecontracts.FlowContractPaths{FlowPath: "sibling"},
				Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
			}
			root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child, sibling}}
			source := Wrap(&runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
				Root: &root,
				ByID: map[string]*runtimecontracts.FlowContractView{
					"child":   &root.Children[0],
					"sibling": &root.Children[1],
				},
			}})

			node := identitytest.FlowNode(t, "child", "listener")
			if got := ResolveExecutableNodeSubscriptionHandler(source, node, "child/task.done"); !got.Matched || got.HandlerEventKey != authored {
				t.Fatalf("local resolution = %#v, want handler %q", got, authored)
			}
			if got := ResolveExecutableNodeSubscriptionHandler(source, node, "sibling/task.done"); got.Matched {
				t.Fatalf("sibling resolution = %#v, want no cross-scope handler", got)
			}
		})
	}
}
