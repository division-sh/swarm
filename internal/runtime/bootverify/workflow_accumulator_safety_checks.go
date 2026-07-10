package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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

	seenHandlers := map[string]struct{}{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(c.source).Consumers() {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || strings.TrimSpace(endpoint.NodeID) == "" || strings.TrimSpace(endpoint.HandlerEvent) == "" {
			continue
		}
		nodeID := strings.TrimSpace(endpoint.NodeID)
		flowID := strings.TrimSpace(endpoint.FlowID)
		eventType := strings.TrimSpace(endpoint.HandlerEvent)
		key := flowID + "\x00" + nodeID + "\x00" + eventType
		if _, exists := seenHandlers[key]; exists {
			continue
		}
		seenHandlers[key] = struct{}{}
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
					accumulatorFlowLabel(flowID), nodeID, eventType, accumulatorCompletionLabel(spec),
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
					accumulatorFlowLabel(flowID), nodeID, eventType, accumulatorCompletionLabel(spec),
				),
			})
		}
		producerPaths := c.accumulatorProducerPaths(flowID, eventType)
		if !producerPaths.hasAny() {
			c.accumulatorSafetyFindings = append(c.accumulatorSafetyFindings, Finding{
				CheckID: checkIDAccumulatorInputProducer, Severity: SeverityHardInvalidity,
				Location: location, Message: producerPaths.message(flowID, nodeID, eventType),
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
