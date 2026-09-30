package bootverify

import (
	"fmt"
	"strings"

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
