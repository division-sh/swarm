package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestA2JoinClosedGrammarM01M04(t *testing.T) {
	const explicit = "stage: waiting\nmembers: {from: state.members, by: payload.member}\noutput: payload.result\non_complete: {advances_to: done}\n"
	const count = "stage: waiting\nmembers: {count: 0, by: payload.member}\noutput: payload.result\non_complete: {advances_to: done}\nuntil: collection.closed\n"
	for name, source := range map[string]string{
		"explicit unbounded": explicit,
		"zero until":         count,
		"bounded maximum":    strings.Replace(count, "count: 0", "count: 1000", 1),
		"zero deadline":      strings.Replace(count, "until: collection.closed\n", "deadline: {after: 1h, from: stage_entry}\non_deadline: {advances_to: expired}\n", 1),
		"both closures":      count + "deadline: {after: 1h, from: stage_entry}\non_deadline: {advances_to: expired}\n",
	} {
		t.Run(name, func(t *testing.T) {
			var spec JoinSpec
			if err := decodeNodeTestYAML([]byte(source), &spec); err != nil {
				t.Fatal(err)
			}
			if spec.EffectiveID() != "waiting" || spec.Mode() != WorkflowJoinModeArrival {
				t.Fatalf("lost default/declaration mode: %#v", spec)
			}
		})
	}
	for name, source := range map[string]string{
		"missing membership":       strings.Replace(explicit, "members: {from: state.members, by: payload.member}\n", "", 1),
		"missing by":               strings.Replace(explicit, ", by: payload.member", "", 1),
		"missing stage":            strings.Replace(explicit, "stage: waiting\n", "", 1),
		"missing output":           strings.Replace(explicit, "output: payload.result\n", "", 1),
		"missing outcome":          strings.Replace(explicit, "on_complete: {advances_to: done}\n", "", 1),
		"mixed membership":         strings.Replace(count, "count: 0", "count: 0, from: state.members", 1),
		"negative count":           strings.Replace(count, "count: 0", "count: -1", 1),
		"count overflow":           strings.Replace(count, "count: 0", "count: 1001", 1),
		"float count":              strings.Replace(count, "count: 0", "count: 1.0", 1),
		"string count":             strings.Replace(count, "count: 0", `count: "1"`, 1),
		"expression count":         strings.Replace(count, "count: 0", "count: state.target", 1),
		"null count":               strings.Replace(count, "count: 0", "count: null", 1),
		"zero without closure":     strings.Replace(count, "until: collection.closed\n", "", 1),
		"null deadline":            explicit + "deadline: null\n",
		"deadline from arrival":    explicit + "deadline: {after: 1h, from: arrival}\non_deadline: {advances_to: expired}\n",
		"missing deadline outcome": explicit + "deadline: {after: 1h, from: stage_entry}\n",
		"outcome without deadline": explicit + "on_deadline: {advances_to: expired}\n",
		"null until":               explicit + "until: null\n",
		"empty until":              explicit + "until: ''\n",
		"unknown deadline field":   explicit + "deadline: {after: 1h, from: stage_entry, timestamp: now}\non_deadline: {advances_to: expired}\n",
	} {
		t.Run(name, func(t *testing.T) {
			var spec JoinSpec
			if err := decodeNodeTestYAML([]byte(source), &spec); err == nil {
				t.Fatalf("invalid join admitted: %#v", spec)
			}
		})
	}
	for _, retired := range []string{"window", "complete_when", "remaining", "timeout"} {
		t.Run("retired "+retired, func(t *testing.T) {
			for _, shape := range nodePresenceShapes("join.completed >= 1", "[legacy]", "{from: entity.batch, by: payload.batch}") {
				t.Run(shape.name, func(t *testing.T) {
					source := explicit
					if shape.name != "missing" {
						source += retired + ": " + shape.value + "\n"
					}
					var spec JoinSpec
					err := decodeNodeTestYAML([]byte(source), &spec)
					if shape.name == "missing" {
						if err != nil || spec.Members.From != "state.members" || spec.Members.By != "payload.member" || !spec.OnCompleteFound {
							t.Fatalf("canonical join changed with absent %s: %#v, %v", retired, spec, err)
						}
					} else if err == nil || !strings.Contains(err.Error(), `join field "`+retired+`" is not supported`) {
						t.Fatalf("retired field %s in %s state admitted or misdiagnosed: %v", retired, shape.name, err)
					}
				})
			}
		})
	}
	// Retire the former custom-completion positives; they do not acquire until
	// semantics or permission to ignore missing members under the new grammar.
	for _, source := range []string{"complete_when: join.completed >= 1\n", "remaining: ignore\n", "timeout: {after: 1h, advances_to: expired}\n", "window: {from: entity.batch, by: payload.batch}\n"} {
		field, _, _ := strings.Cut(source, ":")
		t.Run("legacy positive "+field, func(t *testing.T) {
			var spec JoinSpec
			if err := decodeNodeTestYAML([]byte(explicit+source), &spec); err == nil || !strings.Contains(err.Error(), `join field "`+field+`" is not supported`) {
				t.Fatalf("former standalone positive %s admitted or misdiagnosed: %v", field, err)
			}
		})
	}
}

func TestA2JoinTypedCompileM05M08M09(t *testing.T) {
	for name, memberType := range map[string]string{"list": "[MemberID]", "map": "map[MemberID]Result"} {
		t.Run(name, func(t *testing.T) {
			bundle, handler := a2JoinCompilerFixture(t, memberType)
			plan, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler)
			if err != nil {
				t.Fatal(err)
			}
			if plan.MembersCollectionProjection.ItemType().Kind != CatalogTypeText || plan.UntilEvent != bundle.ResolveExecutableNodeEventReference(plan.Node, "closed") {
				t.Fatalf("lost catalog membership/closure: %#v", plan)
			}
			result, err := plan.ResultType.Resolve()
			if err != nil || result.Kind != CatalogTypeObject || len(result.Fields) != 1 {
				t.Fatalf("named selected output erased: %#v, %v", result, err)
			}
		})
	}
	for name, mutate := range map[string]func(*WorkflowContractBundle, *SystemNodeEventHandler){
		"numeric list": func(b *WorkflowContractBundle, _ *SystemNodeEventHandler) { a2SetJoinMembersType(b, "[integer]") },
		"numeric map key": func(b *WorkflowContractBundle, _ *SystemNodeEventHandler) {
			a2SetJoinMembersType(b, "map[integer]text")
		},
		"record not map":   func(b *WorkflowContractBundle, _ *SystemNodeEventHandler) { a2SetJoinMembersType(b, "Result") },
		"undeclared state": func(_ *WorkflowContractBundle, h *SystemNodeEventHandler) { h.Join.Members.From = "state.absent" },
		"numeric by": func(b *WorkflowContractBundle, _ *SystemNodeEventHandler) {
			b.Events["arrived"].Payload.Properties["member"] = EventFieldSpec{Type: "integer"}
		},
		"missing by no pin fallback": func(_ *WorkflowContractBundle, h *SystemNodeEventHandler) { h.Join.Members.By = "" },
		"undeclared output":          func(_ *WorkflowContractBundle, h *SystemNodeEventHandler) { h.Join.Output = "payload.absent" },
		"dynamic output": func(b *WorkflowContractBundle, _ *SystemNodeEventHandler) {
			b.Events["arrived"].Payload.Properties["result"] = EventFieldSpec{Type: "jsonb"}
		},
		"unknown until":    func(_ *WorkflowContractBundle, h *SystemNodeEventHandler) { h.Join.Until = "absent" },
		"arrival as until": func(_ *WorkflowContractBundle, h *SystemNodeEventHandler) { h.Join.Until = "arrived" },
	} {
		t.Run(name, func(t *testing.T) {
			bundle, handler := a2JoinCompilerFixture(t, "[text]")
			mutate(bundle, &handler)
			if err := bundle.compileEventSchemaBindings(); err != nil {
				t.Fatal(err)
			}
			if plan, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler); err == nil {
				t.Fatalf("invalid typed join admitted: %#v", plan)
			}
		})
	}
	for _, outputType := range []string{"text", "integer", "Score", "Decision", "Result"} {
		t.Run("output "+outputType, func(t *testing.T) {
			bundle, handler := a2JoinCompilerFixture(t, "[text]")
			bundle.Events["arrived"].Payload.Properties["result"] = EventFieldSpec{Type: outputType}
			if err := bundle.compileEventSchemaBindings(); err != nil {
				t.Fatal(err)
			}
			handler.Join.Members.From = ""
			count := 0
			handler.Join.Members.Count = &count
			plan, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler)
			if err != nil || plan.MembersCollectionProjection.Kind != "" || plan.Spec.Members.Count == nil || *plan.Spec.Members.Count != 0 || plan.ResultType.Type != outputType {
				t.Fatalf("count/output type erased: %#v, %v", plan, err)
			}
		})
	}
}

func TestA2JoinPairedCompiledFanOutM03(t *testing.T) {
	bundle := fanOutPlanRegistryTestBundle(t, SystemNodeEventHandler{})
	bundle.Events["batch.ready"].Payload.Properties["items"] = EventFieldSpec{Type: "[text]"}
	if err := bundle.compileEventSchemaBindings(); err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "dispatcher")
	handler := SystemNodeEventHandler{
		FanOut: &FanOutSpec{ItemsFrom: "payload.items", As: "candidate", Emit: EmitSpec{Event: "item.requested"}},
		Join: &JoinSpec{ID: "settled", Members: JoinMembersSpec{FromFanOut: true},
			OnCompleteFound: true, OnComplete: HandlerRuleEntry{Emit: EmitSpec{Event: "batch.completed"}}},
	}
	qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, "batch.ready", handler)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := bundle.CompileWorkflowJoinPlan(node, "batch.ready", qualified)
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := plan.FanOut.FanOut.ElementRef.DeclarationIdentity()
	expected, _ := qualified.FanOut.DeclarationIdentity()
	if err != nil || !declaration.Equal(expected) || plan.Mode != WorkflowJoinModeFanOutDelivery || plan.MembersCollectionProjection.Kind != "" || plan.UntilEvent != "" {
		t.Fatalf("fan-out pairing lost exact site or acquired arrival semantics: %#v, %v", plan, err)
	}
	for name, mutate := range map[string]func(*SystemNodeEventHandler){
		"missing top-level site": func(h *SystemNodeEventHandler) { h.FanOut = nil },
		"nested site only": func(h *SystemNodeEventHandler) {
			h.Rules = []HandlerRuleEntry{{FanOut: h.FanOut}}
			h.FanOut = nil
		},
		"mixed count": func(h *SystemNodeEventHandler) { h.Join.Members.Count = new(int) },
		"arrival deadline": func(h *SystemNodeEventHandler) {
			h.Join.Deadline = &JoinDeadlineSpec{After: "1h", From: JoinDeadlineFromStageEntry}
		},
		"arrival until": func(h *SystemNodeEventHandler) { h.Join.Until = "closed" },
		"foreign compiled site": func(h *SystemNodeEventHandler) {
			foreign, err := QualifySystemNodeHandlerRuleRefsForEvent(identitytest.RootNode(t, "foreign"), "batch.ready", *h)
			if err != nil {
				t.Fatal(err)
			}
			h.FanOut = foreign.FanOut
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := qualified
			join := *qualified.Join
			candidate.Join = &join
			mutate(&candidate)
			if got, err := bundle.CompileWorkflowJoinPlan(node, "batch.ready", candidate); err == nil {
				t.Fatalf("invalid fan-out declaration admitted: %#v", got)
			}
		})
	}
}

func a2JoinCompilerFixture(t *testing.T, membersType string) (*WorkflowContractBundle, SystemNodeEventHandler) {
	t.Helper()
	spec := &JoinSpec{Stage: "waiting", Members: JoinMembersSpec{From: "state.members", By: "payload.member"},
		Output: "payload.result", OnCompleteFound: true, OnComplete: HandlerRuleEntry{AdvancesTo: "done"}, Until: "closed"}
	handler := SystemNodeEventHandler{Join: spec}
	bundle := &WorkflowContractBundle{
		RootSchema:   &FlowSchemaDocument{},
		RootEntities: EntityContractsDocument{"Collector": {Fields: map[string]EntityFieldDecl{"members": {Type: membersType}}}},
		RootTypes: TypeCatalogDocument{
			Scalars: map[string]ScalarTypeDecl{"MemberID": {Base: "text"}, "Score": {Base: "integer"}},
			Enums:   map[string]EnumTypeDecl{"Decision": {Values: []string{"yes", "no"}}},
			Types:   map[string]NamedTypeDecl{"Result": {Fields: map[string]TypeFieldSpec{"value": {Type: "text"}}}},
		},
		Events: map[string]EventCatalogEntry{
			"arrived": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"member": {Type: "MemberID"}, "result": {Type: "Result"}}}},
			"closed":  {},
		},
		Nodes: map[string]SystemNodeContract{"collector": {EventHandlers: map[string]SystemNodeEventHandler{"arrived": handler}}},
	}
	compileRootContractTestFixture(bundle)
	return bundle, handler
}

func a2SetJoinMembersType(bundle *WorkflowContractBundle, memberType string) {
	bundle.RootEntities["Collector"].Fields["members"] = EntityFieldDecl{Type: memberType}
}
