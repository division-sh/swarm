package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestW2RejectsInvalidCompiledPinAndPermissionInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bundle *WorkflowContractBundle
		want   string
	}{
		{
			name: "root duplicate read",
			bundle: &WorkflowContractBundle{RootSchema: &FlowSchemaDocument{Pins: FlowPins{
				Inputs: FlowInputPins{Reads: []string{"entity.status", "entity.status"}},
			}}},
			want: "compile root input entity permissions",
		},
		{
			name: "flow inexact write",
			bundle: &WorkflowContractBundle{FlowSchemas: map[string]FlowSchemaDocument{
				"worker": {Pins: FlowPins{Outputs: FlowOutputPins{Writes: []string{" entity.status"}}}},
			}},
			want: "compile flow worker output entity permissions",
		},
		{
			name: "flow duplicate input pin",
			bundle: &WorkflowContractBundle{FlowSchemas: map[string]FlowSchemaDocument{
				"worker": {Pins: FlowPins{Inputs: FlowInputPins{EventPins: []FlowInputEventPin{
					{Event: "work.requested"}, {Event: "work.requested"},
				}}}},
			}},
			want: "input pin event \"work.requested\" is declared more than once",
		},
		{
			name: "root duplicate output pin",
			bundle: &WorkflowContractBundle{RootSchema: &FlowSchemaDocument{Pins: FlowPins{
				Outputs: FlowOutputPins{EventPins: []FlowOutputEventPin{
					{Event: "work.completed"}, {Event: "work.completed"},
				}},
			}}},
			want: "output pin event \"work.completed\" is declared more than once",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CompileWorkflowSemantics(tc.bundle)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileWorkflowSemantics error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestWorkflowSemanticsRuleActionUsesHandlerAdvancesToFallback(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"review_node": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"expense.submitted": {
						AdvancesTo: "awaiting_review",
						Rules: []HandlerRuleEntry{
							{
								ID:        "needs-human",
								Condition: "payload.amount > 100",
								Action:    ActionSpec{ID: "request_review"},
							},
							{
								ID:        "auto-approve",
								Condition: "else",
							},
						},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	transitions := bundle.DerivedHandlerTransitions()
	if len(transitions) != 1 {
		t.Fatalf("handler transitions = %#v", transitions)
	}
	carriers := HandlerTransitionAdvanceCarriers(transitions[0])
	if len(carriers) != 1 || carriers[0].Kind != HandlerAdvanceCarrierHandler || carriers[0].AdvancesTo != "awaiting_review" {
		t.Fatalf("inherited target must remain handler-owned, not a fabricated rule advance: %#v", carriers)
	}
	rules := transitions[0].Rules
	if len(rules) != 2 || rules[0].Action.ID != "request_review" || rules[1].Action.ID != "" {
		t.Fatalf("handler owner lost rule actions: %#v", rules)
	}
}

func TestWorkflowSemanticsJoinPlanPreservesDeclaringResultCatalog(t *testing.T) {
	spec := JoinSpec{Output: "payload.result"}
	bundle := &WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{Types: map[string]NamedTypeDecl{
			"JoinResult": {Fields: map[string]TypeFieldSpec{"value": {Type: "text"}}},
		}},
		Events: map[string]EventCatalogEntry{
			"item.completed": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"result": {Type: "JoinResult"}}}},
		},
		Nodes: map[string]SystemNodeContract{
			"join-node": {EventHandlers: map[string]SystemNodeEventHandler{"item.completed": {Join: &spec}}},
		},
		scopedNodes: map[string]SystemNodeContract{
			contractScopeKey(ContractItemSource{FlowPath: ".", Family: "nodes"}, "join-node"): {EventHandlers: map[string]SystemNodeEventHandler{"item.completed": {Join: &spec}}},
		},
		scopedNodeSources: map[string]ContractItemSource{
			contractScopeKey(ContractItemSource{FlowPath: ".", Family: "nodes"}, "join-node"): {FlowPath: ".", Family: "nodes", File: "nodes.yaml"},
		},
	}

	populateWorkflowSemantics(bundle)

	joins := bundle.WorkflowJoins()
	if len(joins) != 1 || joins[0].ResultType.Type != "JoinResult" {
		t.Fatalf("join result type = %#v", joins)
	}
	resolved, err := joins[0].ResultType.Resolve()
	if err != nil || resolved.Kind != CatalogTypeObject || resolved.Name != "JoinResult" {
		t.Fatalf("resolved join result = %#v, %v", resolved, err)
	}
	delete(bundle.RootTypes.Types, "JoinResult")
	if _, ok := joins[0].ResultType.NamedFields("JoinResult"); !ok {
		t.Fatal("join plan did not retain the declaring type catalog")
	}
}

func TestWorkflowSemanticsDerivesTopLevelCompletionTransitions(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"fan-in-node": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"component.scaffolded": {
						OnComplete: []HandlerRuleEntry{{
							ID:         "top-complete",
							Condition:  "true",
							AdvancesTo: "top_review",
						}},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	transitions := bundle.DerivedHandlerTransitions()
	if len(transitions) != 1 {
		t.Fatalf("handler transitions = %#v", transitions)
	}
	carriers := HandlerTransitionAdvanceCarriers(transitions[0])
	if len(carriers) != 1 || carriers[0].Kind != HandlerAdvanceCarrierOnComplete || carriers[0].AdvancesTo != "top_review" || carriers[0].Rule.ID != "top-complete" {
		t.Fatalf("top-level on_complete carrier = %#v", carriers)
	}
}

func TestStreamAccumulatorDerivesNoIntrinsicTimeoutSubscriptionOrTransition(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"collector": {
				SubscribesTo: []string{"item.arrived"},
				EventHandlers: map[string]SystemNodeEventHandler{
					"item.arrived": {
						Accumulate: &AccumulateSpec{Into: "items", From: "payload"},
						AdvancesTo: "processed",
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)
	collector := identitytest.RootNode(t, "collector")
	if got := bundle.Semantics.EffectiveNodes[collector.Key()].RuntimeSubscriptions; !reflect.DeepEqual(got, []string{"item.arrived"}) {
		t.Fatalf("stream accumulator subscriptions = %#v, want only authored arrival", got)
	}
	for _, transition := range bundle.DerivedHandlerTransitions() {
		if transition.EventType == "accumulate.timeout" || transition.EventType == "platform.join_timeout" || transition.Join != nil {
			t.Fatalf("stream accumulator derived finite timeout transition: %#v", transition)
		}
	}
}

func TestWorkflowSemanticsDerivesStageTimersAndTimedTransitionEdges(t *testing.T) {
	bundle := &WorkflowContractBundle{
		RootSchema: &FlowSchemaDocument{
			Name: "validation",
			StageDeclarations: FlowStageDeclarations{
				Declared: true,
				Entries: []FlowStageDeclaration{
					{
						ID: "awaiting_review",
						Timers: []FlowStageTimerDeclaration{
							{
								ID:    "awaiting_review.review.sla_escalated",
								After: "48h",
								Emit:  "review.sla_escalated",
							},
							{
								ID:         "awaiting_review.expired",
								After:      "{{marginal_park_days}}d",
								AdvancesTo: "expired",
							},
						},
					},
					{ID: "expired", Terminal: true},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	timers := map[string]WorkflowTimerContract{}
	for _, timer := range bundle.WorkflowTimers() {
		timers[timer.ID] = timer
	}
	emitTimer, ok := timers["awaiting_review.review.sla_escalated"]
	if !ok {
		t.Fatalf("missing emit stage timer: %#v", timers)
	}
	if !emitTimer.StageOwned || emitTimer.Stage != "awaiting_review" || emitTimer.Event != "review.sla_escalated" || emitTimer.Delay != "48h" || emitTimer.StartOn != "state:awaiting_review" {
		t.Fatalf("emit timer = %#v, want stage-owned timer lowering", emitTimer)
	}
	advanceTimer, ok := timers["awaiting_review.expired"]
	if !ok {
		t.Fatalf("missing advance stage timer: %#v", timers)
	}
	if !advanceTimer.StageOwned || advanceTimer.Event != WorkflowStageTimerInternalEvent || advanceTimer.AdvancesTo != "expired" || advanceTimer.Delay != "{{marginal_park_days}}d" {
		t.Fatalf("advance timer = %#v, want internal event + advances_to lowering", advanceTimer)
	}

	topology, ok := bundle.WorkflowStageTopology(".")
	if !ok {
		t.Fatal("missing stage topology")
	}
	var foundTransition bool
	for _, transition := range topology.Edges {
		if transition.TimerID != "awaiting_review.expired" {
			continue
		}
		foundTransition = true
		if got, want := transition.From, "awaiting_review"; got != want {
			t.Fatalf("timer transition From = %#v, want %#v", got, want)
		}
		if transition.To != "expired" || transition.EventType != "timer:awaiting_review.expired" || transition.InternalOwner != "runtime" || topology.FlowID != "." || transition.Source != "timer" || transition.Node.Valid() {
			t.Fatalf("timer transition = %#v, want runtime timed edge to expired", transition)
		}
	}
	if !foundTransition {
		t.Fatalf("missing timed transition edge: %#v", topology.Edges)
	}
}

func TestWorkflowSemanticsScopesStageTimerIDsByFlow(t *testing.T) {
	review := FlowContractView{
		Paths: FlowContractPaths{FlowPath: "review"},
		Path:  "review",
		Schema: FlowSchemaDocument{
			StageDeclarations: FlowStageDeclarations{
				Declared: true,
				Entries: []FlowStageDeclaration{
					{
						ID: "pending",
						Timers: []FlowStageTimerDeclaration{{
							ID:         "pending.expired",
							After:      "48h",
							AdvancesTo: "expired",
						}},
					},
					{ID: "expired", Terminal: true},
				},
			},
		},
	}
	approval := FlowContractView{
		Paths: FlowContractPaths{FlowPath: "approval"},
		Path:  "approval",
		Schema: FlowSchemaDocument{
			StageDeclarations: FlowStageDeclarations{
				Declared: true,
				Entries: []FlowStageDeclaration{
					{
						ID: "pending",
						Timers: []FlowStageTimerDeclaration{{
							ID:         "pending.expired",
							After:      "72h",
							AdvancesTo: "expired",
						}},
					},
					{ID: "expired", Terminal: true},
				},
			},
		},
	}
	bundle := &WorkflowContractBundle{
		FlowTree: flowmodel.Tree[FlowContractView]{
			Root: &FlowContractView{
				Children: []FlowContractView{review, approval},
			},
			ByID: map[string]*FlowContractView{
				"review":   &review,
				"approval": &approval,
			},
		},
		FlowSchemas: map[string]FlowSchemaDocument{
			"review":   review.Schema,
			"approval": approval.Schema,
		},
	}

	populateWorkflowSemantics(bundle)

	timers := map[string]WorkflowTimerContract{}
	for _, timer := range bundle.WorkflowTimers() {
		timers[timer.ID] = timer
	}
	for _, id := range []string{"review.pending.expired", "approval.pending.expired"} {
		timer, ok := timers[id]
		if !ok {
			t.Fatalf("missing flow-scoped stage timer %q in %#v", id, timers)
		}
		if timer.FlowID == "" {
			t.Fatalf("timer %q missing FlowID: %#v", id, timer)
		}
	}
	if _, ok := timers["pending.expired"]; ok {
		t.Fatalf("unscoped stage timer survived in semantic timers: %#v", timers)
	}
}

func TestWorkflowSemanticsDerivesEffectiveSystemNodeFacts(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"worker": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"task.start": {
						DataAccumulation: WorkflowDataAccumulation{
							Writes: []WorkflowDataWrite{{Field: "status"}},
						},
						Emit: EmitSpec{Event: "task.done"},
					},
					"task.review": {
						Rules: []HandlerRuleEntry{{Emit: EmitSpec{Event: "task.approved"}}},
					},
					"task.timeout": {
						Emit: EmitSpec{Event: "task.expired"},
					},
					"task.rules": {
						Rules: []HandlerRuleEntry{
							{
								ID:        "priority",
								Condition: "payload.priority == 'urgent'",
								Emit:      EmitSpec{Event: "task.rules.then"},
							},
							{
								ID:        "fallback",
								Condition: "else",
								Emit:      EmitSpec{Event: "task.rules.else"},
							},
						},
						FanOut: &FanOutSpec{ItemsFrom: "payload.items", As: "task_item", Identity: "task_item", Emit: EmitSpec{Event: "task.child"}},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	worker := identitytest.RootNode(t, "worker")
	effective, ok := bundle.Semantics.EffectiveNodes[worker.Key()]
	if !ok {
		t.Fatal("missing effective node semantics")
	}
	if got, want := effective.ID, "worker"; got != want {
		t.Fatalf("effective ID = %q, want %q", got, want)
	}
	if got, want := effective.ExecutionType, SystemNodeExecutionType; got != want {
		t.Fatalf("effective execution type = %q, want %q", got, want)
	}
	if got, want := effective.RuntimeSubscriptions, []string{"task.review", "task.rules", "task.start", "task.timeout"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective subscriptions = %#v, want %#v", got, want)
	}
	if got, want := effective.Produces, []string{"task.approved", "task.child", "task.done", "task.expired", "task.rules.else", "task.rules.then"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective produces = %#v, want %#v", got, want)
	}
	handler, ok := bundle.Semantics.NodeHandlers[worker.Key()]["task.start"]
	if !ok {
		t.Fatal("missing task.start handler")
	}
	if got, want := handler.DataAccumulation.SourceEvent, "task.start"; got != want {
		t.Fatalf("effective source_event = %q, want %q", got, want)
	}
	var transition HandlerTransitionSemantic
	for _, candidate := range bundle.DerivedHandlerTransitions() {
		if candidate.Node.Equal(worker) && candidate.EventType == "task.start" {
			transition, ok = candidate, true
			break
		}
	}
	if !ok {
		t.Fatal("missing task.start transition")
	}
	if got, want := transition.DataAccumulation.SourceEvent, "task.start"; got != want {
		t.Fatalf("transition source_event = %q, want %q", got, want)
	}
}
