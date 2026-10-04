package bootverify

import (
	"fmt"

	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkPinTargetResolution(c *checkerContext) []Finding {
	findings := []Finding{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(c.source).Producers() {
		if endpoint.Kind == semanticview.EventEndpointExternal || endpoint.Kind == semanticview.EventEndpointPlatform {
			continue
		}
		eventType := endpoint.Event.Authored
		if !runtimepinrouting.PinDeclaredOutput(c.source, endpoint.FlowID, eventType) {
			continue
		}
		consumer := runtimepinrouting.ClassifyOutputConsumer(c.source, endpoint.FlowID, eventType)
		if !consumer.HasRuntimeConsumer() && !consumer.Has(runtimepinrouting.OutputConsumerRootExport) {
			findings = append(findings, Finding{
				CheckID: "pin_target_resolution", Severity: "error", Location: endpoint.FlowID,
				Message: fmt.Sprintf("%s emits pin-declared output %s without valid target mechanism: %s", endpoint.ProducerDescription(), eventType, runtimepinrouting.FailureTargetRequiredMissing.Code()),
			})
		}
	}
	return findings
}
