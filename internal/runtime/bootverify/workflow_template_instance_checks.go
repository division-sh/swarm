package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkTemplateInstanceValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return nil
	}
	findings := []Finding{}
	if bundle.RootSchema != nil && !bundle.RootSchema.Instance.Empty() {
		findings = append(findings, Finding{
			CheckID:  "template_instance_validation",
			Severity: "error",
			Message:  "flow <root> template instance invalid: root schema must not declare instance; template instance keys belong to child flow contracts",
			Location: "<root>",
		})
	}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		isTemplate := strings.TrimSpace(schema.Mode) == "template"
		hasInstance := !schema.Instance.Empty()
		if !isTemplate && !hasInstance {
			continue
		}
		resolved, err := bundle.ResolveFlowTemplateInstance(flowID)
		if err != nil {
			findings = append(findings, Finding{
				CheckID:  "template_instance_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s template instance invalid: %v", flowID, err),
				Location: flowID,
			})
			continue
		}
		entityContract, ok := entityruntime.ResolveForFlow(c.source, flowID)
		if !ok {
			findings = append(findings, Finding{
				CheckID:  "template_instance_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s template instance invalid: primary entity %s is unavailable for type checking", flowID, resolved.PrimaryEntity.EntityType),
				Location: flowID,
			})
			continue
		}
		for _, field := range resolved.By {
			if _, err := entityruntime.ResolveLeafField(entityContract, field); err != nil {
				findings = append(findings, Finding{
					CheckID:  "template_instance_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s template instance invalid: instance.by field %q must resolve to a scalar or enum primary-entity field: %v", flowID, field, err),
					Location: flowID,
				})
			}
		}
	}
	return findings
}
