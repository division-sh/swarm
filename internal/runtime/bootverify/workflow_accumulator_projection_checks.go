package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/accumulator"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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

	owners := map[string]string{}
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for _, event := range sortedSetKeysLocal(c.source.ExecutableNodeEventHandlers(node)) {
			handler, _ := c.source.ExecutableNodeEventHandler(node, event)
			if handler.Accumulate == nil {
				continue
			}
			field := strings.TrimSpace(handler.Accumulate.Target)
			key := node.FlowPath() + "\x00" + field
			owner := node.Key() + ":" + event
			if previous, exists := owners[key]; exists {
				c.accumulatorProjectionFindings = append(c.accumulatorProjectionFindings, Finding{
					CheckID: "accumulator_entity_projection", Severity: SeverityHardInvalidity,
					Message: fmt.Sprintf("accumulate target %q has multiple writers: %s and %s", field, previous, owner), Location: owner,
				})
			}
			owners[key] = owner
			c.accumulatorProjectionFindings = append(c.accumulatorProjectionFindings, accumulatorTargetWriterConflictFindings(c, node, field)...)
		}
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

func accumulatorTargetWriterConflictFindings(c *checkerContext, node runtimeidentity.ExecutableNode, field string) []Finding {
	out := make([]Finding, 0)
	for _, target := range wave1AllEntityWriteTargets(c.source) {
		if !target.Entity || wave1SpecialClearTarget(target.Field) {
			continue
		}
		_, ownerFlowID, rootField, err := wave1ResolveWriteTargetPath(c.source, target)
		if err != nil {
			continue
		}
		if strings.TrimSpace(ownerFlowID) != node.FlowPath() || strings.TrimSpace(rootField) != field {
			continue
		}
		out = append(out, Finding{
			CheckID:  "accumulator_entity_projection",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("accumulate target %q also has an ordinary writer at %s on node %s handler %s; one accumulator owns the field", field, target.Kind, target.nodeID(), target.EventType),
			Location: target.nodeID(),
		})
	}
	for flowID, fields := range wave1AgentExplicitEntityWriteCoverageByFlow(c.source) {
		if strings.TrimSpace(flowID) != node.FlowPath() {
			continue
		}
		if _, ok := fields[field]; !ok {
			continue
		}
		out = append(out, Finding{
			CheckID:  "accumulator_entity_projection",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("accumulate target %q also appears in an agent entity_writes declaration; ordinary writes to an accumulate target are forbidden", field),
			Location: defaultFlowLabel(node.FlowPath()),
		})
	}
	return out
}
