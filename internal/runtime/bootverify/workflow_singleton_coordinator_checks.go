package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkSingletonCoordinatorValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return nil
	}
	findings := []Finding{}
	if bundle.RootSchema != nil && strings.TrimSpace(bundle.RootSchema.Mode) == runtimecontracts.FlowModeSingleton {
		findings = append(findings, Finding{
			CheckID:  "singleton_coordinator_validation",
			Severity: "error",
			Message:  "flow <root> singleton coordinator invalid: root schema must not declare mode: singleton; singleton coordinators are child flow contracts",
			Location: "<root>",
		})
	}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" || strings.TrimSpace(schema.Mode) != runtimecontracts.FlowModeSingleton {
			continue
		}
		if _, err := bundle.ResolveFlowSingleton(flowID); err != nil {
			findings = append(findings, Finding{
				CheckID:  "singleton_coordinator_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s singleton cardinality invalid: %v", flowID, err),
				Location: flowID,
			})
		}
	}
	for _, demand := range BuildSingletonCoordinatorDemandProjection(c.source) {
		if demand.Kind == "fan_in_input" || strings.HasPrefix(demand.Kind, "contained_operation.") {
			continue
		}
		if _, err := bundle.ResolveFlowSingletonCoordinator(demand.FlowID); err != nil {
			findings = append(findings, Finding{
				CheckID:  "singleton_coordinator_validation",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("%s requires singleton coordinator state: %v", demand.Detail(), err),
				Location: firstNonEmptyString(demand.Location, demand.FlowID),
			})
		}
	}
	return findings
}
