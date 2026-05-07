package bootverify

import (
	"fmt"
	"strings"

	"swarm/internal/runtime/accprojection"
)

func checkAccumulatorEntityProjection(c *checkerContext) []Finding {
	return c.accumulatorEntityProjection()
}

func (c *checkerContext) accumulatorEntityProjection() []Finding {
	if c.accumulatorProjectionLoaded {
		return c.accumulatorProjectionFindings
	}
	c.accumulatorProjectionLoaded = true

	resolved := accprojection.Resolve(c.source)
	for _, issue := range resolved.Issues {
		c.accumulatorProjectionFindings = append(c.accumulatorProjectionFindings, Finding{
			CheckID:  "accumulator_entity_projection",
			Severity: SeverityHardInvalidity,
			Message:  issue.Message,
			Location: issue.Location,
		})
	}
	for _, binding := range resolved.Bindings {
		c.accumulatorProjectionFindings = append(c.accumulatorProjectionFindings, accumulatorProjectionWriterConflictFindings(c, binding)...)
	}
	return c.accumulatorProjectionFindings
}

func accumulatorProjectionWriterConflictFindings(c *checkerContext, binding accprojection.Binding) []Finding {
	out := make([]Finding, 0)
	for _, target := range wave1AllEntityWriteTargets(c.source) {
		if !target.Entity || wave1EntityEnvelopeField(target.Field) || wave1SpecialClearTarget(target.Field) {
			continue
		}
		_, ownerFlowID, rootField, err := wave1ResolveWriteTargetPath(c.source, target)
		if err != nil {
			continue
		}
		if strings.TrimSpace(ownerFlowID) != strings.TrimSpace(binding.FlowID) ||
			strings.TrimSpace(rootField) != strings.TrimSpace(binding.TargetField) {
			continue
		}
		out = append(out, Finding{
			CheckID:  "accumulator_entity_projection",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("field %q declares materialize_from but also has authored writer at %s on node %s handler %s; remove the authored writer", binding.TargetField, target.Kind, target.NodeID, target.EventType),
			Location: target.NodeID,
		})
	}
	for flowID, fields := range wave1AgentExplicitEntityWriteCoverageByFlow(c.source) {
		if strings.TrimSpace(flowID) != strings.TrimSpace(binding.FlowID) {
			continue
		}
		if _, ok := fields[strings.TrimSpace(binding.TargetField)]; !ok {
			continue
		}
		out = append(out, Finding{
			CheckID:  "accumulator_entity_projection",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("field %q declares materialize_from but also appears in an agent entity_writes declaration; generated/agent write ownership is forbidden", binding.TargetField),
			Location: defaultFlowLabel(binding.FlowID),
		})
	}
	return out
}
