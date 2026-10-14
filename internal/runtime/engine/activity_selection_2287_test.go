package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func activitySelectionSource2287(t *testing.T, flow string, handler c.SystemNodeEventHandler) (semanticview.Source, identity.ExecutableNode, c.SystemNodeEventHandler) {
	t.Helper()
	node := identitytest.ExecutableNode(t, flow, "scanner")
	qualified, err := completeSemanticFixtureHandlerRuleIdentity(node, "source.requested", handler)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &c.WorkflowContractBundle{
		Tools: sourceWithActivityTool().ToolEntries(),
		Nodes: map[string]c.SystemNodeContract{"scanner": {ExecutionType: "system_node", EventHandlers: map[string]c.SystemNodeEventHandler{"source.requested": qualified}}},
		Events: map[string]c.EventCatalogEntry{"source.requested": {Payload: c.EventPayloadSpec{
			Properties: map[string]c.EventFieldSpec{"url": {Type: "text"}, "member": {Type: "text"}, "result": {Type: "text"}}, Required: []string{"url"},
		}}},
	}
	source := sourceWithFixtureStages(mustCompileEngineFlowSource(flow, bundle), flow, "pending", "pending", "completed", "expired")
	return source, node, qualified
}

func assertHandlerActivityIntentCatalog2287(t *testing.T, source semanticview.Source, node identity.ExecutableNode, handler c.SystemNodeEventHandler, intent ActivityIntent) {
	t.Helper()
	id := handler.Activity.ID
	if id == "" {
		id = "scanner_source_requested_source_scrape"
	}
	base := id
	if node.FlowPath() != "." {
		base = node.FlowPath() + "/" + id
	}
	want := c.ActivityResultEvents{ActivityID: id, SuccessEvent: base + ".succeeded", FailureEvent: base + ".failed", RevisionRequested: base + ".revision_requested", Rejected: base + ".rejected"}
	got := c.ActivityResultEvents{ActivityID: intent.ActivityID, SuccessEvent: intent.SuccessEvent, FailureEvent: intent.FailureEvent, RevisionRequested: intent.RevisionEvent, Rejected: intent.RejectedEvent}
	if got != want {
		t.Fatalf("handler intent disagrees with literal four-name expectation: %#v, want %#v", got, want)
	}
	sites := c.ActivitySitesForNode(node, map[string]c.SystemNodeEventHandler{"source.requested": handler})
	if len(sites) != 1 || sites[0].Kind != c.ActivitySiteHandler || sites[0].RuleIndex != -1 {
		t.Fatalf("catalog borrowed selected-row ownership: %#v", sites)
	}
	owned, err := c.ActivityResultEventsForSite(sites[0])
	if err != nil || owned != want {
		t.Fatalf("catalog site owner disagrees with intent: %#v, %v", owned, err)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("missing compiled catalog")
	}
	entries, schemas := bundle.GeneratedActivityEventEntries(), bundle.GeneratedActivityEventSchemas()
	for _, name := range []string{want.SuccessEvent, want.FailureEvent} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("compiled catalog lost %s", name)
		}
		if _, ok := schemas[name]; !ok {
			t.Fatalf("compiled schema lost %s", name)
		}
	}
	if value, ok := intent.Input.Lookup("url"); !ok || value.Interface() != "https://example.com" {
		t.Fatalf("handler input changed: %#v, %t", value.Interface(), ok)
	}
}

func TestExecutor_HandlerActivityCompletionSelectionKeepsCatalogOwnership(t *testing.T) {
	for _, flow := range []string{".", "research"} {
		for _, selected := range []bool{false, true} {
			for _, id := range []string{"", "explicit_fetch"} {
				t.Run(fmt.Sprintf("%s/selected_%t/explicit_%t", flow, selected, id != ""), func(t *testing.T) {
					condition := "false"
					if selected {
						condition = ""
					}
					source, node, handler := activitySelectionSource2287(t, flow, c.SystemNodeEventHandler{
						Activity:   c.ActivitySpec{ID: id, Tool: "source_scrape", Input: map[string]c.ExpressionValue{"url": c.RefExpression("payload.url")}},
						OnComplete: []c.HandlerRuleEntry{{Condition: condition}},
					})
					order := []string{}
					repo, writer := &orderedStateRepo{order: &order}, &orderedActivityWriter{order: &order}
					exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: repo, MutationOwner: composedMutationOwner{state: repo, activities: writer, order: &order}, Locker: orderedLocker{order: &order}}, stubEvaluator{bools: map[string]bool{"false": false}})
					if err != nil {
						t.Fatal(err)
					}
					result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
						EntityID: "entity-1", Node: node, HandlerEventKey: "source.requested", Handler: handler,
						Event: eventtest.RunCreatingRootIngress("evt-completion", "source.requested", "", "task-1", json.RawMessage(`{"url":"https://example.com"}`), 0, "run-1", "", events.EventEnvelope{}, time.Time{}),
						State: testStateSnapshot("pending", nil, nil, nil),
					})
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(order, []string{"lock", "tx", "save", "activity_intents"}) || len(writer.intents) != 1 || len(result.ActivityIntents) != 1 {
						t.Fatalf("handler completion lost ordered singleton intent: %v, %#v", order, result)
					}
					assertHandlerActivityIntentCatalog2287(t, source, node, handler, writer.intents[0])
					fact := requireResolvedSelection(t, result.HandlerRuleSelection)
					if fact.Context() != handlerselection.ContextOnComplete {
						t.Fatalf("completion selection context = %s", fact.Context())
					}
					if selected {
						ref, _ := handler.OnComplete[0].DeclarationIdentity()
						if fact.Disposition() != handlerselection.DispositionSelected || fact.Ref() != ref || ref.Flow().String() != flow || ref.SemanticPath() != `nodes["scanner"].handlers["source.requested"].on_complete[0]` || fact.DisplayLabel() != "on_complete[0]" {
							t.Fatalf("handler activity lost selected completion identity: %#v", fact)
						}
					} else if fact.Disposition() != handlerselection.DispositionNoMatch || fact.Ref().Valid() {
						t.Fatalf("unselected completion manufactured identity: %#v", fact)
					}
				})
			}
		}
	}
}

func TestExecutor_JoinOutcomeDoesNotInventActivityOwnership(t *testing.T) {
	for _, flow := range []string{".", "research"} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deadline_%t", flow, deadline), func(t *testing.T) {
				count := 1
				source, node, handler := activitySelectionSource2287(t, flow, c.SystemNodeEventHandler{
					Join: &c.JoinSpec{Stage: "pending", Members: c.JoinMembersSpec{Count: &count, By: "payload.member"}, Output: "payload.result",
						OnCompleteFound: true, OnComplete: c.HandlerRuleEntry{AdvancesTo: "completed"},
						OnDeadlineFound: true, OnDeadline: c.HandlerRuleEntry{AdvancesTo: "expired"}, Deadline: &c.JoinDeadlineSpec{After: "1h", From: c.JoinDeadlineFromStageEntry},
					},
				})
				row, rowSource, wantContext := &handler.Join.OnComplete, handlerRuleSourceJoinOnComplete, handlerselection.ContextJoinComplete
				if deadline {
					row, rowSource, wantContext = &handler.Join.OnDeadline, handlerRuleSourceJoinTimeout, handlerselection.ContextJoinTimeout
				}
				frame := executionFrame{ctx: context.Background(), req: ExecutionRequest{
					EntityID: "entity-1", Node: node, ExecutionFlowID: identity.NormalizeFlowID(flow), HandlerEventKey: "source.requested", Handler: handler,
					Event: eventtest.RunCreatingRootIngress("evt-join", "source.requested", "", "task-1", json.RawMessage(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Time{}),
				}, state: ExecutionState{State: testStateSnapshot("pending", nil, nil, nil)}, rule: row, ruleSource: rowSource, ruleIndex: 0}
				exec := &Executor{deps: RuntimeDependencies{Source: source}}
				if err := exec.applyRule(&frame, row); err != nil {
					t.Fatal(err)
				}
				before := frame.result.HandlerRuleSelection
				if err := exec.stepActivity(&frame); err != nil {
					t.Fatal(err)
				}
				if len(frame.result.ActivityIntents) != 0 || !frame.result.HandlerRuleSelection.Equal(before) {
					t.Fatalf("activity lost exact join-outcome selection: %#v", frame.result)
				}
				fact := requireResolvedSelection(t, frame.result.HandlerRuleSelection)
				ref, _ := row.DeclarationIdentity()
				wantDisplay := "join.on_complete"
				if deadline {
					wantDisplay = "join.on_deadline"
				}
				if fact.Context() != wantContext || fact.Disposition() != handlerselection.DispositionSelected || fact.Ref() != ref || ref.Flow().String() != flow || fact.DisplayLabel() != wantDisplay {
					t.Fatalf("activity borrowed foreign join-outcome identity: %#v", fact)
				}
				bundle, _ := semanticview.Bundle(source)
				if len(bundle.GeneratedActivityEventEntries()) != 0 || len(bundle.GeneratedActivityEventSchemas()) != 0 {
					t.Fatal("supported join outcome invented a generated activity catalog")
				}
				for _, id := range []string{"", "explicit_fetch"} {
					invalid := handler
					invalid.Activity = c.ActivitySpec{ID: id, Tool: "source_scrape"}
					if err := c.ValidateJoinHandlerIsolation(invalid); err == nil || !strings.Contains(err.Error(), "cannot be combined with handler fields activity") {
						t.Fatalf("join plus handler activity lost exact admission refusal: %v", err)
					}
				}
			})
		}
	}
}

func TestExecutor_SelectionPresentationKeepsFullAdmittedText(t *testing.T) {
	predicate := strings.Repeat("payload.url != '' &&\n", 80) + "payload.url != ''"
	admitted, err := loadNodeHandlerFixture("on_complete:\n  - condition: |-\n      " + strings.ReplaceAll(predicate, "\n", "\n      ") + "\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, flow := range []string{".", "research"} {
		t.Run(flow, func(t *testing.T) {
			admitted.Activity = c.ActivitySpec{Tool: "source_scrape", Input: map[string]c.ExpressionValue{"url": c.RefExpression("payload.url")}}
			source, node, handler := activitySelectionSource2287(t, flow, admitted)
			order := []string{}
			repo, writer := &orderedStateRepo{order: &order}, &orderedActivityWriter{order: &order}
			exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: repo, MutationOwner: composedMutationOwner{state: repo, activities: writer, order: &order}, Locker: orderedLocker{order: &order}}, stubEvaluator{bools: map[string]bool{predicate: true}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", Node: node, HandlerEventKey: "source.requested", Handler: handler,
				Event: eventtest.RunCreatingRootIngress("evt-full-predicate", "source.requested", "", "task-1", json.RawMessage(`{"url":"https://example.com"}`), 0, "run-1", "", events.EventEnvelope{}, time.Time{}),
				State: testStateSnapshot("pending", nil, nil, nil),
			})
			if err != nil {
				t.Fatal(err)
			}
			fact := requireResolvedSelection(t, result.HandlerRuleSelection)
			ref, _ := handler.OnComplete[0].DeclarationIdentity()
			if fact.Context() != handlerselection.ContextOnComplete || fact.Disposition() != handlerselection.DispositionSelected || fact.Ref() != ref || fact.DisplayLabel() != "on_complete[0]: condition "+predicate {
				t.Fatalf("selection lost positional identity or full admitted predicate: %#v", fact)
			}
			readback, err := handlerselection.Hydrate(string(fact.Context()), string(fact.Disposition()), ref.Flow().String(), ref.Family(), ref.SemanticPath(), fact.DisplayLabel())
			if err != nil || !readback.Equal(fact) {
				t.Fatalf("selection readback changed the immutable full-text fact: %#v, %v", readback, err)
			}
			if len(writer.intents) != 1 || !reflect.DeepEqual(order, []string{"lock", "tx", "save", "activity_intents"}) {
				t.Fatalf("presentation changed handler effects: %v, %#v", order, writer.intents)
			}
			assertHandlerActivityIntentCatalog2287(t, source, node, handler, writer.intents[0])
		})
	}
}
