package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	checkIDAccumulatorHandlerIsolation = "accumulator_handler_isolation"
	checkIDAccumulatorInputProducer    = "accumulator_input_producer_path"
)

func checkAccumulatorHandlerIsolation(c *checkerContext) []Finding {
	return c.accumulatorSafetyByCheck(checkIDAccumulatorHandlerIsolation)
}

func checkAccumulatorInputProducerPath(c *checkerContext) []Finding {
	return c.accumulatorSafetyByCheck(checkIDAccumulatorInputProducer)
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
		if err := runtimecontracts.ValidateAccumulateHandlerIsolation(handler); err != nil {
			c.accumulatorSafetyFindings = append(c.accumulatorSafetyFindings, Finding{
				CheckID:  checkIDAccumulatorHandlerIsolation,
				Severity: SeverityHardInvalidity,
				Location: location,
				Message:  err.Error(),
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
		"Flow %s node %s handler %s accumulates event %s but no accepted producer/source path was found in the authored bundle.\n\nChecked producer source classes:\n- Boundary/external source: %s\n- Parent connect: %s\n- Validation-only harness input: %s\n- Platform source: %s\n- Internal topology producer: %s\n\nFix one of:\n- Add an accepted production producer path for %s\n- Remove the accumulator if the event is not produced\n- For a validation fixture only, set source: harness on the input pin; this will remain non-production-valid",
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
			AllowNonInputEvent: true,
		})},
	}
}
