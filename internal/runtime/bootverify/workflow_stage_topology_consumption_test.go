package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "{}\n")
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
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "semantic_drift_unreachable_state", "closed") {
		t.Fatalf("timer-only terminal was classified unreachable: %#v", report.Errors())
	}
}

func TestRunAcceptsNestedDeliveryJoinOnlyReachableTerminalStage(t *testing.T) {
	root := canonicalrouting.CopyNotifyAllChildren(t, canonicalrouting.NotifyAllChildrenOptions{FanOutDeliveryBarrier: true})
	nodesPath := filepath.Join(root, "flows", canonicalrouting.NotifyAllChildrenOwnerFlowID, "nodes.yaml")
	schemaPath := filepath.Join(root, "flows", canonicalrouting.NotifyAllChildrenOwnerFlowID, "schema.yaml")
	nodesRaw, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	nodes := strings.Replace(string(nodesRaw), `        on_complete:
          element_id: 4c6f93a5-21f9-40d0-8b2a-7b074a11e30d
          emit:`, `        on_complete:
          element_id: 4c6f93a5-21f9-40d0-8b2a-7b074a11e30d
          advances_to: done
          emit:`, 1)
	if nodes == string(nodesRaw) {
		t.Fatal("delivery join completion fixture replacement did not apply")
	}
	if err := os.WriteFile(nodesPath, []byte(nodes), 0o644); err != nil {
		t.Fatal(err)
	}
	schemaRaw, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	schema := strings.Replace(string(schemaRaw), "states: [active]\n", "states: [active, done]\nterminal_states: [done]\n", 1)
	if schema == string(schemaRaw) {
		t.Fatal("delivery join lifecycle fixture replacement did not apply")
	}
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	topology := bundle.Semantics.StageTopologies[canonicalrouting.NotifyAllChildrenOwnerFlowID]
	var found bool
	for _, edge := range topology.Edges {
		if edge.Source == string(runtimecontracts.HandlerAdvanceCarrierJoinOnComplete) && edge.From == "active" && edge.To == "done" {
			found = true
		}
	}
	if !found {
		t.Fatalf("nested delivery join topology = %#v, want active -> done completion edge", topology.Edges)
	}
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "semantic_drift_unreachable_state", "done") {
		t.Fatalf("delivery-join-only terminal was classified unreachable: %#v", report.Errors())
	}
}

func TestTimerActivationUsesExactHandlerOriginForTwoJoinsOnOneNode(t *testing.T) {
	joinNode := identitytest.RootNode(t, "join-node")
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
		{Node: joinNode, EventType: "join.a.requested", Join: &joinA},
		{Node: joinNode, EventType: "join.b.requested", Join: &joinB},
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
	exactNode := identitytest.RootNode(t, "exact-node")
	patternNode := identitytest.RootNode(t, "pattern-node")
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
			{Node: exactNode, EventType: "work.requested", AdvancesTo: "exact-target"},
			{Node: patternNode, EventType: "*.requested", AdvancesTo: "pattern-target"},
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
	node := identitytest.RootNode(t, "node")
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
				Operations: []runtimecontracts.WorkflowLoopOperationPlan{{Node: node, HandlerEvent: "work", Kind: runtimecontracts.LoopOperationRepeat, From: "working"}},
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
			transition.Node = node
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
	workNode := identitytest.RootNode(t, "work-node")
	joinNode := identitytest.RootNode(t, "join-node")
	reviewNode := identitytest.RootNode(t, "review-node")
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
			{Node: workNode, EventType: "work.started", AdvancesTo: "review"},
			{Node: joinNode, EventType: "approval.requested", AdvancesTo: "joining", Join: join},
		},
		[]runtimecontracts.WorkflowTimerContract{{ID: "review.expire", Stage: "review", StageOwned: true, Event: runtimecontracts.WorkflowStageTimerInternalEvent, AdvancesTo: "expired"}},
		[]runtimecontracts.WorkflowLoopPlan{{
			ID: "revision", Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escaped"},
			Operations: []runtimecontracts.WorkflowLoopOperationPlan{{Node: reviewNode, HandlerEvent: "review.revision_requested", Kind: runtimecontracts.LoopOperationRepeat, From: "review"}},
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
