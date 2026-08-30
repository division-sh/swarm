package bootverify

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func standingActivatedFlow(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return false
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	schema, ok := source.FlowSchemaByID(flowID)
	if ok && strings.EqualFold(strings.TrimSpace(schema.Activation), runtimecontracts.FlowActivationStanding) {
		_, err := bundle.ResolveFlowSingleton(flowID)
		return err == nil
	}
	return false
}
