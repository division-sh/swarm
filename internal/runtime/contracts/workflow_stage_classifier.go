package contracts

import "fmt"

// WorkflowStageClassifier indexes exact selected-source flow identities onto
// their compiled graphs. It does not derive stage meaning from flow names.
type WorkflowStageClassifier struct {
	root  WorkflowStageTopology
	flows map[string]WorkflowStageTopology
}

func NewWorkflowStageClassifier(root WorkflowStageTopology, flows map[string]WorkflowStageTopology) (WorkflowStageClassifier, error) {
	if root.FlowID != "." || !root.ValidStageCatalog() {
		return WorkflowStageClassifier{}, fmt.Errorf("selected source has no compiled root stage topology")
	}
	out := WorkflowStageClassifier{root: root, flows: make(map[string]WorkflowStageTopology, len(flows))}
	for key, graph := range flows {
		if key == "" || !graph.ValidStageCatalog() {
			return WorkflowStageClassifier{}, fmt.Errorf("flow key %q has no compiled stage topology", key)
		}
		out.flows[key] = graph
	}
	return out, nil
}

func (c WorkflowStageClassifier) Valid() bool {
	return c.root.FlowID == "." && c.root.ValidStageCatalog()
}

func (c WorkflowStageClassifier) Terminal(flowTemplate, flowInstance, state string) (bool, bool) {
	if !c.Valid() {
		return false, false
	}
	graph := c.root
	if flowTemplate != "" {
		var ok bool
		graph, ok = c.flows[flowTemplate]
		if !ok {
			return false, false
		}
		if instanceGraph, known := c.flows[flowInstance]; flowInstance != "" && known && !graph.SameStageCatalog(instanceGraph) {
			return false, false
		}
	} else if flowInstance != "" {
		var ok bool
		graph, ok = c.flows[flowInstance]
		if !ok {
			return false, false
		}
	}
	ref, err := graph.ResolveStage(state)
	return err == nil && ref.IsTerminal(), err == nil
}
