package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/accprojection"
	"github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkAccumulatorEntityProjection(c *checkerContext) []Finding {
	return c.accumulatorEntityProjection()
}

func (c *checkerContext) accumulatorEntityProjection() []Finding {
	if c.accumulatorProjectionLoaded {
		return c.accumulatorProjectionFindings
	}
	c.accumulatorProjectionLoaded = true
	c.accumulatorProjectionFindings = accumulatorTargetBindingFindings(c.source)

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

func accumulatorTargetBindingFindings(source semanticview.Source) []Finding {
	var findings []Finding
	seen := map[string]struct{}{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Consumers() {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || !endpoint.Node.Valid() {
			continue
		}
		key := endpoint.Node.Key() + "\x00" + endpoint.HandlerEvent
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		handler, ok := source.ExecutableNodeEventHandler(endpoint.Node, endpoint.HandlerEvent)
		if !ok || handler.Accumulate == nil {
			continue
		}
		if err := accumulator.ValidateSpecForHandler(source, endpoint.Node, endpoint.HandlerEvent, handler.Accumulate); err != nil {
			location := accumulatorHandlerLocation(endpoint.FlowID, endpoint.Node.Key(), endpoint.HandlerEvent)
			if endpoint.SourceFile != "" {
				location = fmt.Sprintf("%s:%d %s", endpoint.SourceFile, endpoint.SourceLine, location)
			}
			findings = append(findings, Finding{
				CheckID: "accumulator_entity_projection", Severity: SeverityHardInvalidity,
				Message: err.Error(), Location: location,
			})
		}
	}
	return findings
}

func accumulatorProjectionWriterConflictFindings(c *checkerContext, binding accprojection.Binding) []Finding {
	out := make([]Finding, 0)
	for _, target := range wave1AllEntityWriteTargets(c.source) {
		if !target.Entity || wave1SpecialClearTarget(target.Field) {
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
			Message:  fmt.Sprintf("field %q declares materialize_from but also has authored writer at %s on node %s handler %s; remove the authored writer", binding.TargetField, target.Kind, target.nodeID(), target.EventType),
			Location: target.nodeID(),
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
