package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	checkIDAccumulateAllBoundedEscape        = "accumulate_all_bounded_escape"
	checkIDAccumulatorTimeoutRequiresTimeout = "accumulator_timeout_requires_timeout_ms"
	checkIDAccumulatorInputProducer          = "accumulator_input_producer_path"
)

func checkAccumulateAllBoundedEscape(c *checkerContext) []Finding {
	return c.accumulatorSafetyByCheck(checkIDAccumulateAllBoundedEscape)
}

func checkAccumulatorInputProducerPath(c *checkerContext) []Finding {
	return c.accumulatorSafetyByCheck(checkIDAccumulatorInputProducer)
}

func checkAccumulatorTimeoutRequiresTimeout(c *checkerContext) []Finding {
	return c.accumulatorSafetyByCheck(checkIDAccumulatorTimeoutRequiresTimeout)
}

func (c *checkerContext) accumulatorSafetyByCheck(checkID string) []Finding {
	findings := c.accumulatorSafety()
	out := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if finding.CheckID == checkID {
			out = append(out, finding)
		}
	}
	return out
}

func (c *checkerContext) accumulatorSafety() []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	if c.accumulatorSafetyLoaded {
		return c.accumulatorSafetyFindings
	}
	c.accumulatorSafetyLoaded = true

	for nodeID := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range c.source.NodeHandlerSubscriptions(nodeID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			handler, ok := c.source.NodeEventHandler(nodeID, eventType)
			if !ok || handler.Accumulate == nil {
				continue
			}
			location := accumulatorHandlerLocation(flowID, nodeID, eventType)
			spec := handler.Accumulate
			if accumulatorCompletionAllFamily(spec) && !accumulatorHasBoundedEscape(spec) {
				c.accumulatorSafetyFindings = append(c.accumulatorSafetyFindings, Finding{
					CheckID:  checkIDAccumulateAllBoundedEscape,
					Severity: "warning",
					Location: location,
					Message: fmt.Sprintf(
						"flow %s node %s handler %s uses accumulate completion %q without a bounded timeout escape; if an expected arrival never appears this handler can wait indefinitely. Add a schedulable timeout escape such as completion: timeout with timeout_ms, or an on_timeout branch with timeout_ms.",
						accumulatorFlowLabel(flowID),
						nodeID,
						eventType,
						accumulatorCompletionLabel(spec),
					),
				})
			}
			if accumulatorTimeoutCompletionWithoutSchedulableTimeout(spec) {
				c.accumulatorSafetyFindings = append(c.accumulatorSafetyFindings, Finding{
					CheckID:  checkIDAccumulatorTimeoutRequiresTimeout,
					Severity: SeverityHardInvalidity,
					Location: location,
					Message: fmt.Sprintf(
						"flow %s node %s handler %s uses accumulate completion %q without positive timeout_ms; runtime cannot schedule the accumulate.timeout event, so the handler can wait indefinitely. Add timeout_ms > 0 or choose a completion mode with a schedulable bounded escape.",
						accumulatorFlowLabel(flowID),
						nodeID,
						eventType,
						accumulatorCompletionLabel(spec),
					),
				})
			}
			producerPaths := c.accumulatorProducerPaths(flowID, eventType)
			if producerPaths.hasAny() {
				continue
			}
			c.accumulatorSafetyFindings = append(c.accumulatorSafetyFindings, Finding{
				CheckID:  checkIDAccumulatorInputProducer,
				Severity: SeverityHardInvalidity,
				Location: location,
				Message:  producerPaths.message(flowID, nodeID, eventType),
			})
		}
	}
	return c.accumulatorSafetyFindings
}

func accumulatorCompletionAllFamily(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil {
		return false
	}
	return spec.Completion.Mode == runtimecontracts.AccumulateModeDefault ||
		spec.Completion.Mode == runtimecontracts.AccumulateModeAll
}

func accumulatorHasBoundedEscape(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil || spec.TimeoutMS <= 0 {
		return false
	}
	if spec.Completion.Mode == runtimecontracts.AccumulateModeTimeout {
		return true
	}
	return spec.OnTimeout != nil
}

func accumulatorTimeoutCompletionWithoutSchedulableTimeout(spec *runtimecontracts.AccumulateSpec) bool {
	return spec != nil &&
		spec.Completion.Mode == runtimecontracts.AccumulateModeTimeout &&
		spec.TimeoutMS <= 0
}

func accumulatorCompletionLabel(spec *runtimecontracts.AccumulateSpec) string {
	if spec == nil {
		return ""
	}
	if text := strings.TrimSpace(spec.Completion.String()); text != "" {
		return text
	}
	return "default/all"
}

func accumulatorHandlerLocation(flowID, nodeID, eventType string) string {
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	flowID = accumulatorFlowLabel(flowID)
	return strings.Trim(flowID+"/"+nodeID+":"+eventType, "/:")
}

func accumulatorFlowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "root"
	}
	return flowID
}

type accumulatorProducerPaths struct {
	inputProof inputPinProducerSourceProof
}

func (p accumulatorProducerPaths) hasAny() bool {
	return p.inputProof.hasAny()
}

func (p accumulatorProducerPaths) message(flowID, nodeID, eventType string) string {
	return fmt.Sprintf(
		"Flow %s node %s handler %s accumulates event %s but no accepted producer/source path was found in the authored bundle.\n\nChecked producer source classes:\n- Boundary/external source: %s\n- Parent connect: %s\n- Explicit harness injection: %s\n- Platform source: %s\n- Internal topology producer: %s\n\nFix one of:\n- Add an accepted producer path for %s\n- Register an explicit harness injection for validation-only fixtures\n- Remove the accumulator if the event is not produced",
		accumulatorFlowLabel(flowID),
		strings.TrimSpace(nodeID),
		strings.TrimSpace(eventType),
		strings.TrimSpace(eventType),
		p.inputProof.detailsForKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress),
		p.inputProof.detailsForKind(runtimecontracts.FlowInputProducerBoundaryParentConnect),
		p.inputProof.detailsForKind(runtimecontracts.FlowInputProducerBoundaryHarnessInjection),
		p.inputProof.detailsForKind(runtimecontracts.FlowInputProducerPlatformSource),
		p.inputProof.detailsForKind(runtimecontracts.FlowInputProducerInternalTopology),
		strings.TrimSpace(eventType),
	)
}

func (c *checkerContext) accumulatorProducerPaths(flowID, eventType string) accumulatorProducerPaths {
	return accumulatorProducerPaths{
		inputProof: inputPinProducerSourceProof{resolution: semanticview.ResolveFlowInputProducerWithOptions(c.source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{
			HarnessInjections:  c.opts.HarnessInjections,
			AllowNonInputEvent: true,
		})},
	}
}

func (c *checkerContext) accumulatorSameFlowNodeEmitPath(flowID, eventType string) string {
	matches := map[string]struct{}{}
	for nodeID := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		if strings.TrimSpace(nodeSource.FlowID) != strings.TrimSpace(flowID) {
			continue
		}
		for handlerEvent, handler := range c.source.NodeEventHandlers(nodeID) {
			for _, emitted := range handlerEmits(handler) {
				if accumulatorEventMatches(c.source, flowID, eventType, emitted) {
					matches[fmt.Sprintf("%s handler %s", nodeID, strings.TrimSpace(handlerEvent))] = struct{}{}
				}
			}
		}
	}
	if len(matches) == 0 {
		return "not found"
	}
	labels := sortedSetKeysLocal(matches)
	if len(labels) == 1 {
		return fmt.Sprintf("found on %s", labels[0])
	}
	return fmt.Sprintf("found on %s", strings.Join(labels, ", "))
}

func (c *checkerContext) accumulatorSameFlowAgentEmitPath(flowID, eventType string) string {
	matches := map[string]struct{}{}
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		agentSource, _ := c.source.AgentContractSource(agentID)
		if strings.TrimSpace(agentSource.FlowID) != strings.TrimSpace(flowID) {
			continue
		}
		for _, emitted := range agent.EmitEvents {
			if accumulatorEventMatches(c.source, flowID, eventType, emitted) {
				matches[agentID] = struct{}{}
			}
		}
	}
	if len(matches) == 0 {
		return "not found"
	}
	labels := sortedSetKeysLocal(matches)
	if len(labels) == 1 {
		return fmt.Sprintf("found on agent %s", labels[0])
	}
	return fmt.Sprintf("found on agents %s", strings.Join(labels, ", "))
}

func accumulatorEventMatches(source interface {
	ResolveFlowEventReference(flowID, eventType string) string
	FlowEventMatches(flowID, subscription, eventType string) bool
}, flowID, eventType, candidate string) bool {
	eventType = eventidentity.Normalize(eventType)
	candidate = eventidentity.Normalize(candidate)
	if eventType == "" || candidate == "" {
		return false
	}
	if eventType == candidate {
		return true
	}
	if source == nil {
		return false
	}
	canonicalEvent := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType))
	canonicalCandidate := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, candidate))
	if canonicalEvent != "" && canonicalCandidate == canonicalEvent {
		return true
	}
	if source.FlowEventMatches(flowID, eventType, candidate) {
		return true
	}
	if canonicalCandidate != "" && canonicalCandidate != candidate {
		return source.FlowEventMatches(flowID, eventType, canonicalCandidate)
	}
	return false
}
