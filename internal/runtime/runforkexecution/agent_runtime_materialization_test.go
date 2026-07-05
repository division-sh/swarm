package runforkexecution

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectedContractStaticAgentRecordsIncludeInferredFlowRequiredAgents(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "analysis",
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analysis",
			Mode: runtimecontracts.FlowModeStatic,
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:          "generic",
				Role:          "analyzer",
				Subscriptions: []string{"analysis.requested"},
				EmitEvents:    []string{"analysis.done"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analysis": flow.Schema,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{flow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analysis": &flow,
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}

	records, err := selectedContractStaticAgentRecords(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("selectedContractStaticAgentRecords: %v", err)
	}
	count := 0
	for _, record := range records {
		if strings.TrimSpace(record.Config.ID) == "analyzer" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("records = %#v, want analyzer from static-agent and inferred flow-required-agent materialization paths", records)
	}
}
