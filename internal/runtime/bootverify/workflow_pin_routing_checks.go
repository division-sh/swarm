package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkPinTargetResolution(c *checkerContext) []Finding {
	findings := []Finding{}
	flowIDs := make([]string, 0, len(c.source.FlowSchemaEntries()))
	for flowID := range c.source.FlowSchemaEntries() {
		if strings.TrimSpace(flowID) != "" {
			flowIDs = append(flowIDs, flowID)
		}
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		for _, pin := range c.source.FlowOutputEventPins(flowID) {
			consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, flowID, pin.EventType())
			if consumer.InvalidSink() {
				location := strings.TrimSpace(flowID)
				if location == "" {
					location = "root"
				}
				findings = append(findings, Finding{
					CheckID:  "pin_target_resolution",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("output pin %s in %s declares an invalid sink; the only supported value is sink: harness", pin.EventType(), location),
					Location: location,
				})
				continue
			}
			if !consumer.Has(runtimepinrouting.OutputConsumerHarness) || !consumer.HasRuntimeConsumer() {
				continue
			}
			location := strings.TrimSpace(flowID)
			if location == "" {
				location = "root"
			}
			findings = append(findings, Finding{
				CheckID:  "pin_target_resolution",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("output pin %s in %s declares validation-only sink: harness and a canonical runtime consumer; remove sink: harness or remove the runtime consumer", pin.EventType(), location),
				Location: location,
			})
		}
	}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(c.source).Producers() {
		if endpoint.Kind == semanticview.EventEndpointExternal || endpoint.Kind == semanticview.EventEndpointPlatform {
			continue
		}
		eventType := endpoint.Event.Authored
		if !runtimepinrouting.PinDeclaredOutput(c.source, endpoint.FlowID, eventType) {
			continue
		}
		if runtimepinrouting.OutputHarnessSink(c.source, endpoint.FlowID, eventType) {
			continue
		}
		consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, endpoint.FlowID, eventType)
		if !consumer.HasRuntimeConsumer() {
			findings = append(findings, Finding{
				CheckID: "pin_target_resolution", Severity: "error", Location: endpoint.FlowID,
				Message: fmt.Sprintf("%s emits pin-declared output %s without valid target mechanism: %s", endpoint.ProducerDescription(), eventType, runtimepinrouting.FailureTargetRequiredMissing.Code()),
			})
		}
	}
	return findings
}
