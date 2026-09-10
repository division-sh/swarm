package pipeline

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func terminalOwnershipSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":       "name: terminal-ownership\ninitial_state: ready\nstates: [ready, done, reopened]\n",
		"nodes.yaml":        "reopener:\n  id: reopener\n  execution_type: system_node\n  subscribes_to: [task.reopen_requested]\n  event_handlers:\n    task.reopen_requested:\n      advances_to: reopened\n",
		"events.yaml":       "task.reopen_requested: {}\n",
		"child/schema.yaml": "name: child\nmode: template\ninitial_state: ready\nstates: [ready, done, reopened, child_only]\nterminal_states: [done]\n",
		"child/nodes.yaml":  "reopener:\n  id: reopener\n  execution_type: system_node\n  subscribes_to: [task.reopen_requested]\n  event_handlers:\n    task.reopen_requested:\n      advances_to: reopened\n",
		"child/events.yaml": "task.reopen_requested: {}\n",
	})
}

func TestWorkflowGraphBlocksTerminalExit(t *testing.T) {
	source := terminalOwnershipSource(t)
	graph, ok := semanticview.WorkflowStageTopology(source, "child")
	if !ok {
		t.Fatal("missing child graph")
	}
	site := runtimecontracts.WorkflowTransitionSite{
		Node:         pipelineSourceNode(t, source, "child", "reopener"),
		HandlerEvent: "task.reopen_requested", AdvanceCarrier: runtimecontracts.HandlerAdvanceCarrierHandler,
	}
	if _, err := graph.AdmitTransition(site, "ready", "reopened"); err != nil {
		t.Fatalf("declared nonterminal transition: %v", err)
	}
	if _, err := graph.AdmitTransition(site, "done", "reopened"); err == nil {
		t.Fatal("terminal exit admitted")
	}
}

func TestWorkflowGraphUsesFlowScopedTerminalOwnership(t *testing.T) {
	source := terminalOwnershipSource(t)
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("missing root graph")
	}
	site := runtimecontracts.WorkflowTransitionSite{
		Node:         pipelineSourceNode(t, source, ".", "reopener"),
		HandlerEvent: "task.reopen_requested", AdvanceCarrier: runtimecontracts.HandlerAdvanceCarrierHandler,
	}
	transition, err := graph.AdmitTransition(site, "done", "reopened")
	if err != nil {
		t.Fatalf("child terminal ownership must not mark root done terminal: %v", err)
	}
	if transition.FlowID() != "." || transition.Edge().Site() != site {
		t.Fatalf("wrong transition provenance: %#v", transition)
	}
	site.Node = pipelineSourceNode(t, source, "child", "reopener")
	if _, err := graph.AdmitTransition(site, "done", "reopened"); err == nil {
		t.Fatal("root graph admitted a foreign-flow handler with the same stages")
	}
	if _, ok := semanticview.WorkflowStageTopology(source, "missing"); ok {
		t.Fatal("undeclared flow resolved to an ambient graph")
	}
}

func TestWorkflowGraphRejectsForeignStageMembership(t *testing.T) {
	source := terminalOwnershipSource(t)
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("missing root graph")
	}
	for _, stage := range graph.Stages {
		if stage == "child_only" {
			t.Fatal("root graph contains a child-only stage")
		}
	}
	site := runtimecontracts.WorkflowTransitionSite{
		Node:         pipelineSourceNode(t, source, ".", "reopener"),
		HandlerEvent: "task.reopen_requested", AdvanceCarrier: runtimecontracts.HandlerAdvanceCarrierHandler,
	}
	if _, err := graph.AdmitTransition(site, "child_only", "reopened"); err == nil {
		t.Fatal("root handler admitted a foreign source stage")
	}
}
