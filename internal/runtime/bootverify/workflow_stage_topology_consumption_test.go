package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunAcceptsTimerOnlyReachableTerminalStage(t *testing.T) {
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: timer-reachability
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	for _, file := range []string{"schema.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, file), "{}\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
stages:
  active:
    initial: true
    timers:
      - after: 720h
        advances_to: closed
  closed:
    terminal: true
`)
	for _, file := range []string{"entities.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", file), "{}\n")
	}
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "semantic_drift_unreachable_state", "closed") {
		t.Fatalf("timer-only terminal was classified unreachable: %#v", report.Errors())
	}
}

func TestTimerActivationUsesExactHandlerOriginForTwoJoinsOnOneNode(t *testing.T) {
	joinA := runtimecontracts.JoinSpec{
		ID: "join-a", Stage: "awaiting-a",
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "complete-a"},
		Timeout:    runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "timeout-a"}},
	}
	joinB := runtimecontracts.JoinSpec{
		ID: "join-b", Stage: "awaiting-b",
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "complete-b"},
		Timeout:    runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "timeout-b"}},
	}
	handlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"join.a.requested": {Join: &joinA},
		"join.b.requested": {Join: &joinB},
	}
	transitions := []runtimecontracts.HandlerTransitionSemantic{
		{NodeID: "join-node", EventType: "join.a.requested", Join: &joinA},
		{NodeID: "join-node", EventType: "join.b.requested", Join: &joinB},
	}
	stages := []string{"waiting", "awaiting-a", "complete-a", "timeout-a", "awaiting-b", "complete-b", "timeout-b"}
	topology := runtimecontracts.BuildWorkflowStageTopology("", "waiting", stages, []string{"complete-a", "timeout-a", "complete-b", "timeout-b"}, transitions, nil, nil)
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"join.a.requested": {},
			"join.b.requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"join-node": {EventHandlers: handlers}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "waiting",
			Stages:       stageContracts(stages),
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{"join-node": handlers},
			StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{
				"": topology,
			},
		},
	}
	trigger, err := timeridentity.ParseStartTrigger("event:join.a.requested")
	if err != nil {
		t.Fatal(err)
	}
	declared := stringSet(stages)
	got := timerActivationStates(semanticview.Wrap(bundle), runtimecontracts.WorkflowTimerContract{}, trigger, declared)
	for _, target := range []string{"complete-a", "timeout-a"} {
		if _, ok := got[target]; !ok {
			t.Fatalf("activation states = %#v, missing %s", got, target)
		}
	}
	for _, crossed := range []string{"complete-b", "timeout-b"} {
		if _, ok := got[crossed]; ok {
			t.Fatalf("activation states = %#v, cross-associated %s", got, crossed)
		}
	}
}

func TestTimerActivationUnionsMultipleMatchingHandlerTopologies(t *testing.T) {
	stages := []string{"waiting", "exact-target", "pattern-target"}
	handlers := map[string]runtimecontracts.SystemNodeContract{
		"exact-node": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"work.requested": {AdvancesTo: "exact-target"},
			},
		},
		"pattern-node": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"*.requested": {AdvancesTo: "pattern-target"},
			},
		},
	}
	topology := runtimecontracts.BuildWorkflowStageTopology(
		"", "waiting", stages, []string{"exact-target", "pattern-target"},
		[]runtimecontracts.HandlerTransitionSemantic{
			{NodeID: "exact-node", EventType: "work.requested", AdvancesTo: "exact-target"},
			{NodeID: "pattern-node", EventType: "*.requested", AdvancesTo: "pattern-target"},
		},
		nil,
		nil,
	)
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.requested": {}},
		Nodes:  handlers,
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"exact-node":   handlers["exact-node"].EventHandlers,
				"pattern-node": handlers["pattern-node"].EventHandlers,
			},
			StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"": topology},
		},
	}
	trigger, err := timeridentity.ParseStartTrigger("event:work.requested")
	if err != nil {
		t.Fatal(err)
	}
	got := timerActivationStates(semanticview.Wrap(bundle), runtimecontracts.WorkflowTimerContract{}, trigger, stringSet(stages))
	for _, target := range []string{"exact-target", "pattern-target"} {
		if _, ok := got[target]; !ok {
			t.Fatalf("activation states = %#v, missing %s from matching handler union", got, target)
		}
	}
	if len(got) != 2 {
		t.Fatalf("activation states = %#v, want exact union of two handler topologies", got)
	}
}

func TestTimerActivationConsumesEveryCanonicalHandlerCarrier(t *testing.T) {
	tests := []struct {
		name       string
		handler    runtimecontracts.SystemNodeEventHandler
		transition runtimecontracts.HandlerTransitionSemantic
		loops      []runtimecontracts.WorkflowLoopPlan
		want       []string
	}{
		{name: "source scopes without target", want: []string{"waiting", "working"}},
		{name: "direct", handler: runtimecontracts.SystemNodeEventHandler{AdvancesTo: "direct"}, transition: runtimecontracts.HandlerTransitionSemantic{AdvancesTo: "direct"}, want: []string{"direct"}},
		{name: "rules", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{AdvancesTo: "ruled"}}}, transition: runtimecontracts.HandlerTransitionSemantic{Rules: []runtimecontracts.HandlerRuleEntry{{AdvancesTo: "ruled"}}}, want: []string{"ruled"}},
		{name: "on complete", handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{AdvancesTo: "completed"}}}, transition: runtimecontracts.HandlerTransitionSemantic{OnComplete: []runtimecontracts.HandlerRuleEntry{{AdvancesTo: "completed"}}}, want: []string{"completed"}},
		{
			name: "loop target and escape",
			handler: runtimecontracts.SystemNodeEventHandler{
				AdvancesTo: "working",
				Loop:       &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "working"},
			},
			transition: runtimecontracts.HandlerTransitionSemantic{
				AdvancesTo: "working",
				Loop:       &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "working"},
			},
			loops: []runtimecontracts.WorkflowLoopPlan{{
				ID: "revision", Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escaped"},
				Operations: []runtimecontracts.WorkflowLoopOperationPlan{{NodeID: "node", HandlerEvent: "work", Kind: runtimecontracts.LoopOperationRepeat, From: "working"}},
			}},
			want: []string{"escaped", "working"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stages := []string{"waiting", "working", "done"}
			for _, target := range tc.want {
				if _, exists := stringSet(stages)[target]; !exists {
					stages = append(stages, target)
				}
			}
			transition := tc.transition
			transition.NodeID = "node"
			transition.EventType = "work"
			topology := runtimecontracts.BuildWorkflowStageTopology("", "waiting", stages, []string{"done"}, []runtimecontracts.HandlerTransitionSemantic{transition}, nil, tc.loops)
			handlers := map[string]runtimecontracts.SystemNodeEventHandler{"work": tc.handler}
			bundle := &runtimecontracts.WorkflowContractBundle{
				Events: map[string]runtimecontracts.EventCatalogEntry{"work": {}},
				Nodes:  map[string]runtimecontracts.SystemNodeContract{"node": {EventHandlers: handlers}},
				Semantics: runtimecontracts.WorkflowSemanticView{
					NodeHandlers:    map[string]map[string]runtimecontracts.SystemNodeEventHandler{"node": handlers},
					StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"": topology},
				},
			}
			trigger, err := timeridentity.ParseStartTrigger("event:work")
			if err != nil {
				t.Fatal(err)
			}
			got := timerActivationStates(semanticview.Wrap(bundle), runtimecontracts.WorkflowTimerContract{}, trigger, stringSet(stages))
			if len(got) != len(tc.want) {
				t.Fatalf("activation states = %#v, want %v", got, tc.want)
			}
			for _, want := range tc.want {
				if _, ok := got[want]; !ok {
					t.Fatalf("activation states = %#v, missing %s", got, want)
				}
			}
		})
	}
}

func TestLifecycleReachabilityConsumesLoopEscapeAndTimerCancelPreservesEveryOtherCarrier(t *testing.T) {
	join := &runtimecontracts.JoinSpec{
		ID: "approval", Stage: "joining",
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "joined"},
		Timeout: runtimecontracts.JoinTimeoutSpec{
			After:   "1h",
			Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "join-timed-out"},
		},
	}
	stages := []string{"waiting", "review", "joining", "joined", "join-timed-out", "expired", "escaped"}
	topology := runtimecontracts.BuildWorkflowStageTopology(
		"", "waiting", stages, []string{"joined", "join-timed-out", "expired", "escaped"},
		[]runtimecontracts.HandlerTransitionSemantic{
			{NodeID: "work-node", EventType: "work.started", AdvancesTo: "review"},
			{NodeID: "join-node", EventType: "approval.requested", AdvancesTo: "joining", Join: join},
		},
		[]runtimecontracts.WorkflowTimerContract{{ID: "review.expire", Stage: "review", StageOwned: true, Event: runtimecontracts.WorkflowStageTimerInternalEvent, AdvancesTo: "expired"}},
		[]runtimecontracts.WorkflowLoopPlan{{
			ID: "revision", Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escaped"},
			Operations: []runtimecontracts.WorkflowLoopOperationPlan{{NodeID: "review-node", HandlerEvent: "review.revision_requested", Kind: runtimecontracts.LoopOperationRepeat, From: "review"}},
		}},
	)
	handlers := map[string]runtimecontracts.SystemNodeContract{
		"work-node": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.started": {AdvancesTo: "review"}}},
		"join-node": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"approval.requested": {AdvancesTo: "joining", Join: join}}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.started": {}, "approval.requested": {}},
		Nodes:  handlers,
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"work-node": handlers["work-node"].EventHandlers,
				"join-node": handlers["join-node"].EventHandlers,
			},
			StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"": topology},
		},
	}
	source := semanticview.Wrap(bundle)
	reachable := authoredReachableStates(source, "", "waiting")
	if _, ok := reachable["escaped"]; !ok {
		t.Fatalf("reachable = %#v, want loop escape target", reachable)
	}
	edges := timerCancelStateGraphEdges(source, runtimecontracts.WorkflowTimerContract{Event: "work.started"})
	if _, ok := edges["waiting"]["review"]; ok {
		t.Fatalf("cancel graph retained firing handler edge: %#v", edges)
	}
	for _, edge := range [][2]string{
		{"review", "escaped"},
		{"review", "expired"},
		{"joining", "joined"},
		{"joining", "join-timed-out"},
	} {
		if _, ok := edges[edge[0]][edge[1]]; !ok {
			t.Fatalf("cancel graph dropped %s -> %s carrier: %#v", edge[0], edge[1], edges)
		}
	}
}

func stageContracts(ids []string) []runtimecontracts.WorkflowStageContract {
	out := make([]runtimecontracts.WorkflowStageContract, 0, len(ids))
	for _, id := range ids {
		out = append(out, runtimecontracts.WorkflowStageContract{ID: id})
	}
	return out
}
