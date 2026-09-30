package bootverify

import (
	"fmt"
	"strings"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkPrimaryEntityValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return nil
	}
	findings := []Finding{}
	demands := primaryEntityOperationDemands(c.source)
	rootEntities := bundle.RootEntityContracts()
	if len(rootEntities) > 1 {
		if _, err := bundle.ResolveRootPrimaryEntity(); err != nil {
			findings = append(findings, Finding{
				CheckID:  "primary_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow <root> primary entity invalid: %v", err),
				Location: "<root>",
			})
		}
	}
	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		entities, _ := bundle.FlowEntityContractsByID(flowID)
		hasEntityContracts := len(entities) > 0
		if !hasEntityContracts && !demands[flowID] {
			continue
		}
		if _, err := bundle.ResolveFlowPrimaryEntity(flowID); err != nil {
			findings = append(findings, Finding{
				CheckID:  "primary_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s primary entity invalid: %v", flowID, err),
				Location: flowID,
			})
		}
	}
	return findings
}

func primaryEntityOperationDemands(source semanticview.Source) map[string]bool {
	demands := make(map[string]bool)
	for _, target := range wave1AllEntityWriteTargets(source) {
		if target.Entity && !wave1SpecialClearTarget(target.Field) {
			demands[target.flowID()] = true
		}
	}
	for _, record := range wave1ScopedNodeRecords(source) {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for event, handler := range record.Entry.EventHandlers {
			if bootverifyHandlerMaterializesEntity(source, node, event, node.FlowPath(), handler) {
				demands[node.FlowPath()] = true
			}
			for _, expression := range handlerExecutableReaderExpressionsForSource(source, node, event, handler) {
				if len(runtimepipeline.WorkflowEntityReferences(expression.Expression)) > 0 {
					demands[node.FlowPath()] = true
				}
			}
		}
	}
	return demands
}
