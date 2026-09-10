package bootverify

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledTransitionHandlerReferenceGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowContractBundle, *runtimecontracts.SystemNodeEventHandler)
		check  string
		want   string
	}{
		{name: "valid"},
		{name: "unknown_action", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.Action.ID = "missing_action"
		}, check: "transition_reference_validation", want: "references unknown action missing_action"},
		{name: "unknown_rule_action_without_advance", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.Action = runtimecontracts.ActionSpec{}
			h.AdvancesTo = ""
			h.Rules = []runtimecontracts.HandlerRuleEntry{{Condition: "true", Action: runtimecontracts.ActionSpec{ID: "missing_rule_action"}}}
		}, check: "transition_reference_validation", want: "references unknown action missing_rule_action"},
		{name: "missing_action_emit", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			b.Semantics.ActionByID["emit_opened"] = runtimecontracts.GuardActionEntry{ID: "emit_opened", Emits: "ticket.missing"}
		}, check: "transition_reference_validation", want: "action emit_opened emits missing event ticket.missing"},
		{name: "nonexecutable_action", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			b.Semantics.ActionByID["emit_opened"] = runtimecontracts.GuardActionEntry{ID: "emit_opened"}
		}, check: "handler_field_compliance", want: "action emit_opened is not executable"},
		{name: "unknown_guard", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.Guard.ID = "missing_guard"
		}, check: "transition_reference_validation", want: "references unknown guard missing_guard"},
		{name: "unknown_collection_guard_without_advance", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.AdvancesTo = ""
			h.Guard.Checks = []runtimecontracts.GuardCheck{{ID: "allow_ticket"}, {ID: "missing_guard"}}
		}, check: "transition_reference_validation", want: "references unknown guard missing_guard"},
		{name: "nonexecutable_collection_guard", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			b.Semantics.GuardByID["empty_guard"] = runtimecontracts.GuardActionEntry{ID: "empty_guard"}
			h.Guard.Checks = []runtimecontracts.GuardCheck{{ID: "allow_ticket"}, {ID: "empty_guard"}}
		}, check: "handler_field_compliance", want: "guard empty_guard has no executable runtime implementation"},
		{name: "inline_guard_label_is_not_registry_reference", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.Guard = &runtimecontracts.GuardSpec{ID: "inline_label", Check: "true"}
		}},
		{name: "checks_replace_top_level_guard", mutate: func(b *runtimecontracts.WorkflowContractBundle, h *runtimecontracts.SystemNodeEventHandler) {
			h.Guard = &runtimecontracts.GuardSpec{ID: "unused_unknown", Checks: []runtimecontracts.GuardCheck{{ID: "allow_ticket"}, {ID: "inline_label", Check: "true"}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyTransitionRuntimeOwnershipBundle()
			node := identitytest.RootNode(t, "dispatcher")
			handler := bundle.Semantics.NodeHandlers[node.Key()]["ticket.created"]
			if tc.mutate != nil {
				tc.mutate(bundle, &handler)
			}
			bundle.Semantics.NodeHandlers[node.Key()]["ticket.created"] = handler
			c := &checkerContext{source: semanticview.Wrap(bundle)}
			findings := append(c.transitionReferences(), c.handlerFieldCompliance()...)
			if tc.want == "" {
				if len(findings) != 0 {
					t.Fatalf("valid handler rejected: %#v", findings)
				}
				return
			}
			if !reportContains(findings, tc.check, tc.want) {
				t.Fatalf("missing %s %q: %#v", tc.check, tc.want, findings)
			}
		})
	}
}

func TestCompiledTransitionAllRuleContextActionReferences(t *testing.T) {
	for _, site := range []string{"rules", "on_complete", "join.on_complete", "join.timeout"} {
		for _, action := range []string{"unknown", "missing_emit", "valid"} {
			t.Run(site+"/"+action, func(t *testing.T) {
				bundle := bootverifyTransitionRuntimeOwnershipBundle()
				node := identitytest.RootNode(t, "dispatcher")
				handler := runtimecontracts.SystemNodeEventHandler{}
				rule := runtimecontracts.HandlerRuleEntry{Action: runtimecontracts.ActionSpec{ID: "emit_opened"}}
				switch action {
				case "unknown":
					rule.Action.ID = "missing_action"
				case "missing_emit":
					bundle.Semantics.ActionByID["emit_opened"] = runtimecontracts.GuardActionEntry{ID: "emit_opened", Emits: "ticket.missing"}
				}
				switch site {
				case "rules":
					handler.Rules = []runtimecontracts.HandlerRuleEntry{rule}
				case "on_complete":
					handler.OnComplete = []runtimecontracts.HandlerRuleEntry{rule}
				case "join.on_complete":
					handler.Join = &runtimecontracts.JoinSpec{OnCompleteFound: true, OnComplete: rule}
				case "join.timeout":
					handler.Join = &runtimecontracts.JoinSpec{TimeoutFound: true}
					handler.Join.Timeout.Outcome = rule
				}
				bundle.Semantics.NodeHandlers[node.Key()]["ticket.created"] = handler
				c := &checkerContext{source: semanticview.Wrap(bundle)}
				findings := c.transitionReferences()
				switch action {
				case "unknown":
					if !reportContains(findings, "transition_reference_validation", "references unknown action missing_action") {
						t.Fatalf("rule context escaped reference validation: %#v", findings)
					}
				case "missing_emit":
					if !reportContains(findings, "transition_reference_validation", "emits missing event ticket.missing") {
						t.Fatalf("rule context escaped emit validation: %#v", findings)
					}
				case "valid":
					if len(findings) != 0 {
						t.Fatalf("valid reference rejected: %#v", findings)
					}
				}
				// Reference validity never authorizes actions in unsupported contexts.
				if site != "rules" && !reportContains(c.handlerFieldCompliance(), "handler_field_compliance", "action is unsupported") {
					t.Fatal("unsupported action context was authorized")
				}
			})
		}
	}
}

func TestCompiledTransitionStrictCarrierReferences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowStageTopologyEdge)
		check  string
		want   string
	}{
		{"missing_trigger", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.EventType = "" }, "transition_reference_validation", "missing trigger"},
		{"unknown_platform_not_protocol", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.EventType = "platform.not_a_protocol" }, "transition_reference_validation", "missing from event catalog"},
		{"timer_prefix_not_authority", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.EventType = "timer:known" }, "transition_reference_validation", "missing from event catalog"},
		{"wrong_handler", func(e *runtimecontracts.WorkflowStageTopologyEdge) {
			e.HandlerEvent = "ticket.audit"
			e.EventType = "ticket.audit"
		}, "transition_ownership_validation", "originating handler ticket.audit is missing"},
		{"foreign_owner", func(e *runtimecontracts.WorkflowStageTopologyEdge) {
			e.Node = identitytest.FlowNode(t, "child", "dispatcher")
		}, "transition_ownership_validation", "invalid compiled carrier"},
		{"wrong_carrier", func(e *runtimecontracts.WorkflowStageTopologyEdge) {
			e.AdvanceCarrier = runtimecontracts.HandlerAdvanceCarrierOnComplete
		}, "transition_ownership_validation", "invalid compiled carrier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyTransitionRuntimeOwnershipBundle()
			// Even a real timer cannot authorize a handler event by its spelling.
			bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{{ID: "known", FlowID: ".", StageOwned: true, Stage: "created", AdvancesTo: "opened"}}
			topology := bundle.Semantics.StageTopologies["."]
			tc.mutate(&topology.Edges[0])
			bundle.Semantics.StageTopologies["."] = topology
			c := &checkerContext{source: semanticview.Wrap(bundle)}
			findings := append(c.transitionReferences(), c.transitionOwnership()...)
			if !reportContains(findings, tc.check, tc.want) {
				t.Fatalf("missing %s %q: %#v", tc.check, tc.want, findings)
			}
		})
	}
}

func TestCompiledTransitionOwnershipUsesCanonicalAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowStageTopologyEdge)
	}{
		{"valid", nil},
		{"unknown_source_kind", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Source = "invented" }},
		{"runtime_owner_on_handler", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.InternalOwner = "runtime" }},
		{"handler_protocol_disagreement", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.EventType = "platform.join_timeout" }},
		{"foreign_timer_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.TimerID = "foreign" }},
		{"foreign_gate_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.DecisionID = "foreign" }},
		{"foreign_verdict_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Verdict = "approved" }},
		{"foreign_loop_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.LoopID = "foreign" }},
		{"foreign_loop_operation", func(e *runtimecontracts.WorkflowStageTopologyEdge) {
			e.LoopOperation = runtimecontracts.LoopOperationRepeat
		}},
		{"foreign_timing_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Timed = true }},
		{"foreign_after_coordinate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.After = "1h" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyTransitionRuntimeOwnershipBundle()
			graph := bundle.Semantics.StageTopologies["."]
			if tc.mutate != nil {
				tc.mutate(&graph.Edges[0])
			}
			edge := graph.Edges[0]
			_, err := graph.AdmitTransition(edge.Site(), edge.From, edge.To)
			if (err != nil) != (tc.mutate != nil) {
				t.Fatalf("canonical admission: %v", err)
			}
			bundle.Semantics.StageTopologies["."] = graph
			findings := (&checkerContext{source: semanticview.Wrap(bundle)}).transitionOwnership()
			if tc.mutate == nil {
				if len(findings) != 0 {
					t.Fatalf("valid carrier rejected: %#v", findings)
				}
			} else if !reportContains(findings, "transition_ownership_validation", "invalid compiled carrier") {
				t.Fatalf("bootverify disagrees with canonical admission: %#v", findings)
			}
		})
	}
	t.Run("valid_shape_still_requires_declared_advance", func(t *testing.T) {
		bundle := bootverifyTransitionRuntimeOwnershipBundle()
		node := identitytest.RootNode(t, "dispatcher")
		handler := bundle.Semantics.NodeHandlers[node.Key()]["ticket.created"]
		handler.AdvancesTo = ""
		bundle.Semantics.NodeHandlers[node.Key()]["ticket.created"] = handler
		graph := bundle.Semantics.StageTopologies["."]
		edge := graph.Edges[0]
		if _, err := graph.AdmitTransition(edge.Site(), edge.From, edge.To); err != nil {
			t.Fatal(err)
		}
		findings := (&checkerContext{source: semanticview.Wrap(bundle)}).transitionOwnership()
		if !reportContains(findings, "transition_ownership_validation", "does not belong to its originating handler") {
			t.Fatalf("declaration ownership proof was lost: %#v", findings)
		}
	})
}

func TestCompiledTransitionRootGateUsesScopedStageMetadata(t *testing.T) {
	for _, foreignSource := range []bool{false, true} {
		bundle := loadLifecycleEmitterBundle(t, canonicalrouting.LifecycleGateLocal)
		if len(bundle.Semantics.Gates) != 1 {
			t.Fatalf("expected one gate: %#v", bundle.Semantics.Gates)
		}
		gate := &bundle.Semantics.Gates[0]
		// A sibling may terminate at the root's live gate stage and declare other stages.
		bundle.Semantics.TerminalStages = append(bundle.Semantics.TerminalStages, gate.Stage)
		bundle.Semantics.Stages = append(bundle.Semantics.Stages, runtimecontracts.WorkflowStageContract{ID: "child_only", Phase: "child"})
		bundle.Semantics.FlowStates["child"] = []string{gate.Stage, "child_only"}
		bundle.Semantics.FlowTerminal["child"] = []string{gate.Stage}
		if foreignSource {
			gate.Stage = "child_only"
		}
		findings := checkStageGateValidation(&checkerContext{source: semanticview.Wrap(bundle)})
		if foreignSource {
			if !reportContains(findings, "stage_gate_validation", "gate source stage child_only is not declared") {
				t.Fatalf("root gate borrowed sibling declaration: %#v", findings)
			}
		} else if len(findings) != 0 {
			t.Fatalf("sibling terminal blocked legal root gate: %#v", findings)
		}
	}
}

func TestCompiledTransitionConsumerAgreement(t *testing.T) {
	t.Run("ordinary_join_timer_matrix", testCompiledOrdinaryConsumerAgreement)
	for _, tc := range []struct {
		name    string
		variant canonicalrouting.LifecycleEmitterVariant
		flow    string
	}{
		{"gate", canonicalrouting.LifecycleGateLocal, "."},
		{"same_target_verdicts", canonicalrouting.LifecycleGateSharedEvent, "."},
		{"gate_no_emit", canonicalrouting.LifecycleGateNoEmit, "."},
		{"nested_gate", canonicalrouting.LifecycleGateNested, "outer/inner"},
		{"loop", canonicalrouting.LifecycleLoopConnected, "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadLifecycleEmitterBundle(t, tc.variant)
			source := semanticview.Wrap(bundle)
			topology, ok := semanticview.WorkflowStageTopology(source, tc.flow)
			if !ok || len(topology.Edges) == 0 {
				t.Fatal("strict source loading produced no carriers")
			}
			seen := map[string]int{}
			for _, edge := range topology.Edges {
				seen[edge.Source]++
				if edge.Source == "gate" || edge.Source == "timer" {
					if edge.Node.Valid() || edge.HandlerEvent != "" || edge.InternalOwner != "runtime" {
						t.Fatalf("runtime carrier fabricated node ownership: %#v", edge)
					}
				} else if edge.Node.FlowPath() != tc.flow || edge.HandlerEvent == "" {
					t.Fatalf("lost declaring handler owner: %#v", edge)
				}
			}
			if tc.name == "loop" {
				for _, kind := range []string{"loop.start", "loop.admit", "loop.repeat", "loop.close", "loop.escape"} {
					if seen[kind] == 0 {
						t.Fatalf("missing %s: %#v", kind, topology.Edges)
					}
				}
			} else if seen["gate"] == 0 || (tc.name == "same_target_verdicts" && seen["gate"] != 2) {
				t.Fatalf("gate outcomes lost or collapsed: %#v", topology.Edges)
			}
			report := Run(context.Background(), source, Options{})
			for _, finding := range report.Findings {
				switch finding.CheckID {
				case "transition_reference_validation", "transition_ownership_validation", "handler_field_compliance", "semantic_drift_unreachable_state", "join_validation", "timer_validation", "loop_validation", "stage_gate_validation":
					if strings.EqualFold(finding.Severity, "error") {
						t.Errorf("compiled consumer disagreement: %#v", finding)
					}
				}
			}
			projection, _ := semanticview.WorkflowStageTopology(source, tc.flow)
			projection.Edges[0].To = "foreign"
			after, _ := semanticview.WorkflowStageTopology(source, tc.flow)
			if !reflect.DeepEqual(topology, after) {
				t.Fatal("returned projection mutated canonical carrier")
			}
		})
	}
}

func TestCompiledTransitionLifecycleReferenceGuards(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant canonicalrouting.LifecycleEmitterVariant
		source  string
		mutate  func(*runtimecontracts.WorkflowStageTopologyEdge)
		check   string
		want    string
	}{
		{"unknown_verdict", canonicalrouting.LifecycleGateLocal, "gate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Verdict = "missing" }, "transition_reference_validation", "unknown or mismatched gate"},
		{"unknown_decision", canonicalrouting.LifecycleGateLocal, "gate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.DecisionID = "missing" }, "transition_reference_validation", "unknown or mismatched gate"},
		{"gate_wrong_target", canonicalrouting.LifecycleGateLocal, "gate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.To = "done" }, "transition_reference_validation", "unknown or mismatched gate"},
		{"gate_fake_node", canonicalrouting.LifecycleGateLocal, "gate", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Node = identitytest.RootNode(t, "requester") }, "transition_ownership_validation", "invalid compiled carrier"},
		{"unknown_loop", canonicalrouting.LifecycleLoopConnected, "loop.escape", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.LoopID = "missing" }, "transition_ownership_validation", "does not belong to its originating repeat operation"},
		{"loop_wrong_operation", canonicalrouting.LifecycleLoopConnected, "loop.escape", func(e *runtimecontracts.WorkflowStageTopologyEdge) {
			e.LoopOperation = runtimecontracts.LoopOperationClose
		}, "transition_ownership_validation", "invalid compiled carrier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadLifecycleEmitterBundle(t, tc.variant)
			topology := bundle.Semantics.StageTopologies["."]
			found := false
			for i := range topology.Edges {
				if topology.Edges[i].Source == tc.source {
					tc.mutate(&topology.Edges[i])
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("fixture has no %s carrier", tc.source)
			}
			bundle.Semantics.StageTopologies["."] = topology
			c := &checkerContext{source: semanticview.Wrap(bundle)}
			findings := append(c.transitionReferences(), c.transitionOwnership()...)
			if !reportContains(findings, tc.check, tc.want) {
				t.Fatalf("missing %s %q: %#v", tc.check, tc.want, findings)
			}
		})
	}
}

func TestCompiledTransitionStageTimerReferenceGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowStageTopologyEdge)
		check  string
		want   string
	}{
		{name: "valid"},
		{"unknown_timer", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.TimerID = "missing" }, "transition_reference_validation", "unknown or mismatched stage timer"},
		{"foreign_timer", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.TimerID = "child.advance" }, "transition_reference_validation", "unknown or mismatched stage timer"},
		{"wrong_source", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.From = "opened" }, "transition_reference_validation", "unknown or mismatched stage timer"},
		{"wrong_target", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.To = "created" }, "transition_reference_validation", "unknown or mismatched stage timer"},
		{"fake_node", func(e *runtimecontracts.WorkflowStageTopologyEdge) { e.Node = identitytest.RootNode(t, "dispatcher") }, "transition_ownership_validation", "invalid compiled carrier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyTransitionRuntimeOwnershipBundle()
			bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{
				{ID: "advance", FlowID: ".", StageOwned: true, Stage: "created", AdvancesTo: "opened"},
				{ID: "child.advance", FlowID: "child", StageOwned: true, Stage: "created", AdvancesTo: "opened"},
			}
			topology := runtimecontracts.BuildWorkflowStageTopology(".", "created", []string{"created", "opened"}, []string{"opened"}, nil, bundle.Semantics.Timers[:1], nil)
			if len(topology.Edges) != 1 {
				t.Fatalf("expected one compiled timer edge: %#v", topology)
			}
			if tc.mutate != nil {
				tc.mutate(&topology.Edges[0])
			}
			bundle.Semantics.StageTopologies["."] = topology
			c := &checkerContext{source: semanticview.Wrap(bundle)}
			findings := append(c.transitionReferences(), c.transitionOwnership()...)
			if tc.want == "" {
				if len(findings) != 0 {
					t.Fatalf("valid timer rejected: %#v", findings)
				}
			} else if !reportContains(findings, tc.check, tc.want) {
				t.Fatalf("missing %s %q: %#v", tc.check, tc.want, findings)
			}
		})
	}
}

func testCompiledOrdinaryConsumerAgreement(t *testing.T) {
	root := t.TempDir()
	for _, flow := range []string{".", "child", "sibling"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, flow, "schema.yaml"), `
stages:
  ready:
    initial: true
    timers:
      - {id: advance, after: 1h, advances_to: working}
      - {id: notify, after: 2h, emit: tick}
  working: {}
  awaiting: {}
  done: {terminal: true}
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, flow, "entities.yaml"), `
item:
  expected: {type: "[text]", initial: []}
  window: {type: text, initial: window}
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, flow, "events.yaml"), `
direct: {}
selected: {}
inherited: {}
completed: {}
tick: {}
arrived:
  member: text
  window: text
  result: text
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, flow, "nodes.yaml"), `
worker:
  execution_type: system_node
  event_handlers:
    direct: {advances_to: awaiting}
    selected:
      rules:
        first: {condition: "true", advances_to: done}
        second: {condition: else, advances_to: done}
    inherited:
      advances_to: done
      rules:
        first: {condition: "true"}
        second: {condition: else}
    completed:
      on_complete:
        - {condition: "true", advances_to: done}
    tick: {}
    arrived:
      join:
        stage: awaiting
        members: {from: entity.expected, by: payload.member}
        window: {from: entity.window, by: payload.window}
        output: payload.result
        on_complete: {advances_to: done}
        timeout: {after: 3h, advances_to: done}
`)
	}
	repo := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	source := semanticview.Wrap(bundle)
	for _, flow := range []string{"sibling", ".", "child"} {
		topology, ok := semanticview.WorkflowStageTopology(source, flow)
		if !ok {
			t.Fatalf("missing %s topology", flow)
		}
		counts := map[string]int{}
		ruleRefs := map[string]bool{}
		for _, edge := range topology.Edges {
			counts[edge.Source]++
			if edge.Source == "timer" {
				if edge.Node.Valid() || edge.HandlerEvent != "" || edge.From != "ready" || edge.To != "working" {
					t.Fatalf("stage timer acquired handler owner or emit-only transition: %#v", edge)
				}
				continue
			}
			if edge.Node.FlowPath() != flow {
				t.Fatalf("flow %s borrowed carrier: %#v", flow, edge)
			}
			if edge.AdvanceCarrier == runtimecontracts.HandlerAdvanceCarrierRules {
				if !edge.RuleRef.Valid() {
					t.Fatalf("rule lost canonical identity: %#v", edge)
				}
				ruleRefs[edge.RuleRef.Key()] = true
			}
			if edge.AdvanceCarrier == runtimecontracts.HandlerAdvanceCarrierJoinTimeout {
				if edge.HandlerEvent != "arrived" || edge.EventType != "platform.join_timeout" || edge.From != "awaiting" {
					t.Fatalf("join lost handler versus protocol event/source distinction: %#v", edge)
				}
			}
		}
		want := map[string]int{
			"handler.advances_to": 6, "handler.rules": 6, "handler.on_complete": 3,
			"handler.join.on_complete": 1, "handler.join.timeout": 1, "timer": 1,
		}
		if !reflect.DeepEqual(counts, want) || len(ruleRefs) != 2 {
			t.Fatalf("flow %s carrier inventory = %#v, rules=%v; want %#v and two distinct rule refs", flow, counts, ruleRefs, want)
		}
	}
	c := &checkerContext{source: source}
	findings := append(c.transitionReferences(), c.transitionOwnership()...)
	findings = append(findings, c.handlerFieldCompliance()...)
	findings = append(findings, c.stateReachability()...)
	findings = append(findings, c.timerValidation()...)
	findings = append(findings, checkJoinValidation(c)...)
	if len(findings) != 0 {
		t.Fatalf("strict-loaded carrier consumers disagree: %#v", findings)
	}
	// Reversing compiled presentation order cannot change references or ownership.
	for flow, topology := range bundle.Semantics.StageTopologies {
		for i, j := 0, len(topology.Edges)-1; i < j; i, j = i+1, j-1 {
			topology.Edges[i], topology.Edges[j] = topology.Edges[j], topology.Edges[i]
		}
		bundle.Semantics.StageTopologies[flow] = topology
	}
	reordered := &checkerContext{source: source}
	if got := append(reordered.transitionReferences(), reordered.transitionOwnership()...); len(got) != 0 {
		t.Fatalf("order changed canonical validation: %#v", got)
	}
}
