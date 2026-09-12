package semanticview

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestAdmittedSubscriptionExecutionProjection(t *testing.T) {
	for _, tc := range []struct {
		name, declarationPath, executionPath, authored, want string
		kind                                                 AuthoredSubscriptionConsumerKind
	}{
		{"root", "", "", "task.done", "task.done", AuthoredSubscriptionConsumerAgent},
		{"static", "left/child", "left/child", "task.done", "left/child/task.done", AuthoredSubscriptionConsumerAgent},
		{"template local", "left/child", "left/child/a", "task.done", "left/child/a/task.done", AuthoredSubscriptionConsumerAgent},
		{"template qualified", "left/child", "left/child/b", "left/child/task.done", "left/child/b/task.done", AuthoredSubscriptionConsumerAgent},
		{"nested template", "left/child", "left/a/child/b", "left/child/task.done", "left/a/child/b/task.done", AuthoredSubscriptionConsumerAgent},
		{"node", "left/child", "left/child/a", "task.done", "left/child/a/task.done", AuthoredSubscriptionConsumerNode},
		{"timer", "left/child", "left/child/a", "task.done", "left/child/a/task.done", AuthoredSubscriptionConsumerTimer},
		{"pattern", "left/child", "left/child/a", "task.*", "left/child/a/task.*", AuthoredSubscriptionConsumerAgent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: tc.kind, ConsumerID: "listener", FlowPath: tc.declarationPath,
				Authored: tc.authored, LocalEvents: map[string]struct{}{"task.done": {}},
			})
			if !a.Admitted() {
				t.Fatal(a.Message())
			}
			before := a.RoutePatterns()
			got := a.RoutePatternsAt(tc.executionPath)
			if !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("routes=%v want=%s", got, tc.want)
			}
			got[0] = "foreign/forged"
			if !reflect.DeepEqual(a.RoutePatternsAt(tc.executionPath), []string{tc.want}) || !reflect.DeepEqual(before, a.RoutePatterns()) {
				t.Fatal("execution projection mutated declaration or aliases a prior result")
			}
		})
	}
	for _, raw := range []string{"foreign/task.done", "left/child/a/task.done", "left/child/task.*", "missing"} {
		a := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
			ConsumerKind: AuthoredSubscriptionConsumerAgent, FlowPath: "left/child", Authored: raw,
			LocalEvents: map[string]struct{}{"task.done": {}},
		})
		if a.Admitted() || len(a.RoutePatternsAt("left/child/a")) != 0 {
			t.Fatalf("invalid declaration gained route authority: %s", raw)
		}
	}
	if got := (AuthoredSubscriptionAdmission{}).RoutePatternsAt("left/child/a"); len(got) != 0 {
		t.Fatalf("zero admission routes=%v", got)
	}
}

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
				LocalEvents:  map[string]struct{}{"task.done": {}},
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

func TestClassifyAuthoredSubscriptionRejectsUndeclaredReceiverExactForEveryConsumerKind(t *testing.T) {
	for _, kind := range []AuthoredSubscriptionConsumerKind{
		AuthoredSubscriptionConsumerNode,
		AuthoredSubscriptionConsumerAgent,
		AuthoredSubscriptionConsumerTimer,
	} {
		t.Run(string(kind), func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: kind,
				ConsumerID:   "listener",
				FlowID:       "child",
				FlowPath:     "child",
				LocalEvents:  map[string]struct{}{"child.ready": {}},
				Authored:     "root.started",
			})
			if admission.Admitted() || admission.Failure() != AuthoredSubscriptionFailureReceiverEventMissing {
				t.Fatalf("admission = class %q failure %q message %q", admission.Class(), admission.Failure(), admission.Message())
			}
			for _, want := range []string{"receiver-local event", "input pin", "nearest common ancestor schema.yaml"} {
				if !strings.Contains(admission.Message(), want) {
					t.Fatalf("message = %q, want %q", admission.Message(), want)
				}
			}
		})
	}
}

func TestClassifyAuthoredSubscriptionScopesNonRootNodePatterns(t *testing.T) {
	for _, authored := range []string{"task.*", "*.done", "*", "missing.*"} {
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
	for _, authored := range []string{"*/task.done", "**/task.done", "child/task.*"} {
		t.Run("reject_"+authored, func(t *testing.T) {
			admission := ClassifyAuthoredSubscription(nil, AuthoredSubscriptionRequest{
				ConsumerKind: AuthoredSubscriptionConsumerNode,
				ConsumerID:   "listener",
				FlowID:       "child",
				FlowPath:     "child",
				Authored:     authored,
			})
			if admission.Admitted() || admission.Failure() != AuthoredSubscriptionFailurePatternUnauthorized {
				t.Fatalf("admission = class %q failure %q, want pattern_unauthorized", admission.Class(), admission.Failure())
			}
			if !strings.Contains(admission.Message(), "connect in the nearest common ancestor schema.yaml") {
				t.Fatalf("message = %q, want connect teaching error", admission.Message())
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
			"listener": {SubscribesTo: []string{"task.requested"}},
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
					"listener": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{authored: {}}},
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
