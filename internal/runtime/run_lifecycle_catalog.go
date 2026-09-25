package runtime

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func runLifecycleRequiresGenericSchedules(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	for _, join := range source.WorkflowJoins() {
		if join.Mode == runtimecontracts.WorkflowJoinModeFanOutDelivery {
			return true
		}
	}
	return false
}

func runLifecycleTerminalCatalog(source semanticview.Source) (runtimerunlifecycle.TerminalCatalog, error) {
	classifier, err := selectedWorkflowStageClassifier(source)
	if err != nil {
		return runtimerunlifecycle.TerminalCatalog{}, err
	}
	return runtimerunlifecycle.NewCompiledTerminalCatalog(classifier)
}

func selectedWorkflowStageClassifier(source semanticview.Source) (runtimecontracts.WorkflowStageClassifier, error) {
	if source == nil {
		return runtimecontracts.WorkflowStageClassifier{}, fmt.Errorf("stage classifier requires selected semantic source")
	}
	workflow, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok || workflow.FlowID != "." {
		return runtimecontracts.WorkflowStageClassifier{}, fmt.Errorf("stage classifier requires selected compiled root topology")
	}
	flows := make(map[string]runtimecontracts.WorkflowStageTopology)
	add := func(key string, graph runtimecontracts.WorkflowStageTopology) error {
		if key == "" {
			return nil
		}
		if previous, exists := flows[key]; exists && !previous.SameStageCatalog(graph) {
			return fmt.Errorf("stage classifier flow key %q resolves to multiple compiled stage catalogs", key)
		}
		flows[key] = graph
		return nil
	}
	if workflowName := strings.TrimSpace(source.WorkflowName()); workflowName != "" {
		if err := add(workflowName, workflow); err != nil {
			return runtimecontracts.WorkflowStageClassifier{}, err
		}
	}
	for flowID := range source.FlowSchemaEntries() {
		if flowID == "" || flowID == "." {
			continue
		}
		graph, ok := semanticview.WorkflowStageTopology(source, flowID)
		if !ok || graph.FlowID != flowID {
			return runtimecontracts.WorkflowStageClassifier{}, fmt.Errorf("stage classifier flow %q has no selected compiled topology", flowID)
		}
		for _, key := range []string{flowID, source.FlowPath(flowID)} {
			if err := add(key, graph); err != nil {
				return runtimecontracts.WorkflowStageClassifier{}, err
			}
		}
	}
	for _, scope := range source.FlowScopes() {
		graph, ok := semanticview.WorkflowStageTopology(source, scope.ID)
		if !ok || graph.FlowID != scope.ID {
			return runtimecontracts.WorkflowStageClassifier{}, fmt.Errorf("stage classifier scope %q has no selected compiled topology", scope.ID)
		}
		for _, key := range []string{scope.ID, scope.Path} {
			if err := add(key, graph); err != nil {
				return runtimecontracts.WorkflowStageClassifier{}, err
			}
		}
	}
	return runtimecontracts.NewWorkflowStageClassifier(workflow, flows)
}
