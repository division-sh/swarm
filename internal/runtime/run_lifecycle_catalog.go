package runtime

import (
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

func runLifecycleTerminalCatalog(source semanticview.Source) runtimerunlifecycle.TerminalCatalog {
	if source == nil {
		return runtimerunlifecycle.TerminalCatalog{}
	}
	workflow := source.FlowTerminalStages("")
	flows := make(map[string][]string)
	add := func(key string, states []string) {
		key = strings.Trim(strings.TrimSpace(key), "/")
		if key != "" && len(states) > 0 {
			flows[key] = states
		}
	}
	if workflowName := strings.Trim(strings.TrimSpace(source.WorkflowName()), "/"); workflowName != "" {
		add(workflowName, workflow)
	}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		states := source.FlowTerminalStages(flowID)
		add(flowID, states)
		add(source.FlowPath(flowID), states)
	}
	for _, scope := range source.FlowScopes() {
		states := source.FlowTerminalStages(scope.ID)
		add(scope.ID, states)
		add(scope.Path, states)
		add(scope.OwningFlowID, states)
	}
	return runtimerunlifecycle.NewTerminalCatalog(workflow, flows)
}
