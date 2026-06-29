package bootverify

import (
	"fmt"
	"strings"

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
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		entities, _ := bundle.FlowEntityContractsByID(flowID)
		hasEntityDeclaration := strings.TrimSpace(schema.Entity) != ""
		hasEntityContracts := len(entities) > 0
		statefulNormal := strings.TrimSpace(schema.InitialState) != "" && strings.TrimSpace(schema.Mode) == ""
		if !hasEntityDeclaration && !hasEntityContracts && !statefulNormal {
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
