package semanticview

import (
	"reflect"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestA2UntilConsumerUsesCompiledEventWithoutSyntheticHandler(t *testing.T) {
	for _, hasRealHandler := range []bool{false, true} {
		t.Run(map[bool]string{false: "closure_only", true: "ordinary_handler_preserved"}[hasRealHandler], func(t *testing.T) {
			node, err := identity.AdmitExecutableNodeDeclaration(".", "collector")
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			spec := rc.JoinSpec{Stage: "waiting", Members: rc.JoinMembersSpec{Count: &count, By: "payload.member"}, Output: "payload.result", Until: "closed", OnCompleteFound: true, OnComplete: rc.HandlerRuleEntry{AdvancesTo: "done"}}
			handlers := map[string]rc.SystemNodeEventHandler{"arrived": {Join: &spec}}
			if hasRealHandler {
				handlers["closed"] = rc.SystemNodeEventHandler{CreateEntity: true, AdvancesTo: "wrong", Emit: rc.EmitSpec{Event: "arrived"}}
			}
			bundle := &rc.WorkflowContractBundle{
				RootSchema: &rc.FlowSchemaDocument{},
				Nodes:      map[string]rc.SystemNodeContract{"collector": {EventHandlers: handlers}},
				Events:     map[string]rc.EventCatalogEntry{"arrived": {Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"member": {Type: "text"}, "result": {Type: "integer"}}}}, "closed": {}},
			}
			root := &rc.FlowContractView{Path: ".", Paths: rc.FlowContractPaths{FlowPath: "."}, Schema: *bundle.RootSchema, Nodes: bundle.Nodes, Events: bundle.Events}
			bundle.FlowTree = rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{".": root}, ByPath: map[string]*rc.FlowContractView{".": root}}
			if err := rc.CompileWorkflowSemantics(bundle); err != nil {
				t.Fatal(err)
			}
			plan, err := bundle.CompileWorkflowJoinPlan(node, "arrived", handlers["arrived"])
			if err != nil {
				t.Fatal(err)
			}
			if plan.UntilEvent == "" {
				t.Fatal("closure event was not compiled")
			}
			bundle.Semantics.Joins = []rc.WorkflowJoinPlan{plan}
			source := Wrap(bundle)
			before := len(source.ExecutableNodeEventHandlers(node))
			census := BuildAuthoredEventEndpointCensus(source)
			closures, authored := 0, 0
			for _, consumer := range census.Consumers() {
				if consumer.Node != node || consumer.Event.EventKey() != plan.UntilEvent {
					continue
				}
				if consumer.Kind == EventEndpointNodeHandler {
					authored++
				}
				if consumer.Site == "join.until" {
					closures++
					if consumer.Kind != EventEndpointNodeGenerated || consumer.HandlerEvent != "arrived" || consumer.SourceLocation != "event_handlers.arrived.join.until" {
						t.Fatalf("closure mislabeled as authored handler: %#v", consumer)
					}
				}
			}
			if closures != 1 || authored != map[bool]int{false: 0, true: 1}[hasRealHandler] {
				t.Fatalf("closure=%d real-handler=%d", closures, authored)
			}
			resolution := ResolveExecutableNodeSubscriptionHandler(source, node, plan.UntilEvent)
			if !resolution.Matched || !resolution.Admission.Admitted() || resolution.HandlerEventKey != plan.UntilEvent || len(resolution.Handler.JoinUntilPlans) != 1 {
				t.Fatalf("closure carrier not resolved: %#v", resolution)
			}
			carrier := resolution.Handler
			carrier.JoinUntilPlans = nil
			want := rc.SystemNodeEventHandler{}
			if hasRealHandler {
				want = source.ExecutableNodeEventHandlers(node)["closed"]
			}
			if !reflect.DeepEqual(carrier, want) {
				t.Fatalf("closure changed ordinary handler operations: got=%#v want=%#v", carrier, want)
			}
			if resolution.Handler.JoinUntilPlans[0].HandlerEvent != "arrived" {
				t.Fatal("closure lost arrival declaration ownership")
			}
			if !reflect.DeepEqual(ExecutableNodeEffectiveSubscriptions(source, node), []string{"arrived", "closed", "platform.join_complete"}) {
				t.Fatal("compiled closure not subscribed")
			}
			if len(source.ExecutableNodeEventHandlers(node)) != before {
				t.Fatal("census manufactured a closure handler")
			}
		})
	}
}

func TestA2UntilCarrierPreservesMultipleExactDeclarationsAndScope(t *testing.T) {
	count := 1
	join := rc.JoinSpec{Stage: "waiting", Members: rc.JoinMembersSpec{Count: &count, By: "payload.member"}, Output: "payload.result", Until: "closed", OnCompleteFound: true, OnComplete: rc.HandlerRuleEntry{AdvancesTo: "done"}}
	first, second := join, join
	first.ID, second.ID = "first", "second"
	handlers := map[string]rc.SystemNodeEventHandler{
		"arrived": {Join: &first}, "other.arrived": {Join: &second},
		"closed": {Description: "ordinary action", AdvancesTo: "next"},
	}
	event := rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"member": {Type: "text"}, "result": {Type: "integer"}}}}
	child := rc.FlowContractView{Path: "child", Paths: rc.FlowContractPaths{FlowPath: "child"},
		Nodes:  map[string]rc.SystemNodeContract{"collector": {EventHandlers: handlers}},
		Events: map[string]rc.EventCatalogEntry{"arrived": event, "other.arrived": event, "closed": {}},
	}
	sibling := rc.FlowContractView{Path: "sibling", Paths: rc.FlowContractPaths{FlowPath: "sibling"}, Events: map[string]rc.EventCatalogEntry{"closed": {}}}
	root := &rc.FlowContractView{Children: []rc.FlowContractView{child, sibling}}
	bundle := &rc.WorkflowContractBundle{FlowTree: rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{"child": &root.Children[0], "sibling": &root.Children[1]}, ByPath: map[string]*rc.FlowContractView{"child": &root.Children[0], "sibling": &root.Children[1]}}}
	if err := rc.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, "child", "collector")
	source := Wrap(bundle)
	for _, eventType := range []string{"closed", "child/closed"} {
		resolution := ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
		if !resolution.Matched || resolution.HandlerEventKey != "closed" || resolution.Handler.AdvancesTo != "next" || len(resolution.Handler.JoinUntilPlans) != 2 {
			t.Fatalf("qualified/local ordinary handler and closure not preserved: %#v", resolution)
		}
		for _, plan := range resolution.Handler.JoinUntilPlans {
			if plan.Node != node || plan.UntilEvent != "child/closed" {
				t.Fatal("closure borrowed a foreign declaration/event")
			}
			if err := rc.ValidateJoinHandlerIsolation(rc.SystemNodeEventHandler{Join: &plan.Spec, JoinUntilPlans: resolution.Handler.JoinUntilPlans}); err != nil {
				t.Fatalf("internal metadata invalidated isolated join: %v", err)
			}
		}
		*resolution.Handler.JoinUntilPlans[0].Spec.Members.Count = 99
		if *source.WorkflowJoins()[0].Spec.Members.Count != 1 {
			t.Fatal("carrier mutated compiler plan")
		}
	}
	if len(WorkflowJoinUntilPlansForEvent(source, node, "closed")) != 0 || len(WorkflowJoinUntilPlansForEvent(source, node, "child/closed")) != 2 {
		t.Fatal("direct projection guessed a local or mismatched event")
	}
	for _, eventType := range []string{"sibling/closed", "child/closed.extra", "child/arrived"} {
		if got := ResolveExecutableNodeSubscriptionHandler(source, node, eventType); len(got.Handler.JoinUntilPlans) != 0 {
			t.Fatalf("nonclosure event borrowed closure plans: %#v", got)
		}
	}
	if len(WorkflowJoinUntilPlansForEvent(source, identitytest.FlowNode(t, "sibling", "collector"), "child/closed")) != 0 {
		t.Fatal("closure borrowed sibling node identity")
	}
	closures, ordinary := 0, 0
	for _, consumer := range BuildAuthoredEventEndpointCensus(source).Consumers() {
		if consumer.Node == node && consumer.Event.EventKey() == "child/closed" {
			if consumer.Site == "join.until" {
				closures++
			}
			if consumer.Kind == EventEndpointNodeHandler {
				ordinary++
			}
		}
	}
	if closures != 2 || ordinary != 1 {
		t.Fatalf("census lost distinct closure/ordinary consumers: %d/%d", closures, ordinary)
	}
}

func TestA2JoinCollectionViewDetachesAdmittedMapProjectionM09M42(t *testing.T) {
	node, err := identity.AdmitExecutableNodeDeclaration(".", "collector")
	if err != nil {
		t.Fatal(err)
	}
	p, err := rc.AdmitCollectionProjection(rc.CatalogTypeReference{Type: "map[text][integer]"})
	if err != nil {
		t.Fatal(err)
	}
	bundle := &rc.WorkflowContractBundle{Semantics: rc.WorkflowSemanticView{Joins: []rc.WorkflowJoinPlan{{Node: node, HandlerEvent: "arrived", Mode: rc.WorkflowJoinModeArrival, MembersCollectionProjection: p, UntilEvent: "closed"}}}}
	source := Wrap(bundle)
	plan := source.WorkflowJoins()[0]
	items, err := plan.MembersCollectionProjection.Project(map[string]any{"z": []any{1, 1}, " a ": []any{2}, "a": []any{3}})
	if err != nil || !reflect.DeepEqual(items, []any{" a ", "a", "z"}) {
		t.Fatalf("join view did not consume canonical map keys: %#v %v", items, err)
	}
	plan.MembersCollectionProjection.Type.Value.Element.Kind = rc.CatalogTypeText
	if source.WorkflowJoins()[0].MembersCollectionProjection.Type.Value.Element.Kind != rc.CatalogTypeInteger {
		t.Fatal("join view exposed compiler-owned projection")
	}
	if source.WorkflowJoins()[0].UntilEvent != "closed" {
		t.Fatal("view changed compiled closure identity")
	}
}

func TestA2UntilCarrierPreservesAnotherJoinsArrivalHandler(t *testing.T) {
	node := identitytest.RootNode(t, "collector")
	count := 1
	first := rc.JoinSpec{ID: "first", Stage: "waiting", Members: rc.JoinMembersSpec{Count: &count, By: "payload.member"}, Output: "payload.result", Until: "closed", OnCompleteFound: true, OnComplete: rc.HandlerRuleEntry{AdvancesTo: "done"}}
	second := first
	second.ID, second.Until = "second", "later"
	event := rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{"member": {Type: "text"}, "result": {Type: "integer"}}}}
	bundle := &rc.WorkflowContractBundle{RootSchema: &rc.FlowSchemaDocument{},
		Nodes:  map[string]rc.SystemNodeContract{"collector": {EventHandlers: map[string]rc.SystemNodeEventHandler{"arrived": {Join: &first}, "closed": {Join: &second}}}},
		Events: map[string]rc.EventCatalogEntry{"arrived": event, "closed": event, "later": {}},
	}
	root := &rc.FlowContractView{Path: ".", Paths: rc.FlowContractPaths{FlowPath: "."}, Schema: *bundle.RootSchema, Nodes: bundle.Nodes, Events: bundle.Events}
	bundle.FlowTree = rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{".": root}, ByPath: map[string]*rc.FlowContractView{".": root}}
	if err := rc.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)
	resolution := ResolveExecutableNodeSubscriptionHandler(source, node, "closed")
	if !resolution.Matched || resolution.HandlerEventKey != "closed" || resolution.Handler.Join == nil || resolution.Handler.Join.EffectiveID() != "second" || len(resolution.Handler.JoinUntilPlans) != 1 || resolution.Handler.JoinUntilPlans[0].Spec.EffectiveID() != "first" {
		t.Fatal("closure transport replaced another join's lawful arrival handler")
	}
	if err := rc.ValidateJoinHandlerIsolation(resolution.Handler); err != nil {
		t.Fatalf("internal closure metadata created an authored isolation conflict: %v", err)
	}
	closures, ordinary := 0, 0
	for _, consumer := range BuildAuthoredEventEndpointCensus(source).Consumers() {
		if consumer.Node == node && consumer.Event.EventKey() == "closed" {
			if consumer.Site == "join.until" {
				closures++
			}
			if consumer.Kind == EventEndpointNodeHandler && consumer.HandlerEvent == "closed" {
				ordinary++
			}
		}
	}
	if closures != 1 || ordinary != 1 {
		t.Fatalf("lawful closure/member consumer overlap lost: %d/%d", closures, ordinary)
	}
}
