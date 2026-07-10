package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func ResolveFlowInputProducer(source Source, flowID, eventType string) runtimecontracts.FlowInputProducerResolution {
	return ResolveFlowInputProducerWithOptions(source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{})
}

func ResolveFlowInputProducerWithOptions(source Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) runtimecontracts.FlowInputProducerResolution {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	out := runtimecontracts.FlowInputProducerResolution{FlowID: flowID, EventType: eventType}
	appendEvidence := func(evidence runtimecontracts.FlowInputProducerEvidence) {
		evidence.Kind = strings.TrimSpace(evidence.Kind)
		evidence.FlowID = strings.TrimSpace(evidence.FlowID)
		evidence.EventType = eventidentity.Normalize(evidence.EventType)
		evidence.Pin = strings.TrimSpace(evidence.Pin)
		evidence.Pattern = eventidentity.Normalize(evidence.Pattern)
		evidence.Detail = strings.TrimSpace(evidence.Detail)
		if evidence.Kind == "" {
			return
		}
		for _, existing := range out.Evidence {
			if existing.Kind == evidence.Kind &&
				existing.FlowID == evidence.FlowID &&
				existing.EventType == evidence.EventType &&
				existing.Pin == evidence.Pin &&
				existing.Pattern == evidence.Pattern &&
				existing.Detail == evidence.Detail {
				return
			}
		}
		out.Evidence = append(out.Evidence, evidence)
	}

	if source == nil || eventType == "" {
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:   runtimecontracts.FlowInputProducerInvalidContext,
			Detail: "semantic source and input event are required",
		})
		return out
	}
	isInputEvent := source.FlowHasInputEvent(flowID, eventType)
	if !isInputEvent && !opts.AllowNonInputEvent {
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerInvalidContext,
			EventType: eventType,
			Detail:    "event is not declared as a flow input",
		})
		return out
	}

	if isInputEvent && !opts.AllowNonInputEvent {
		appendBoundaryIngressEvidence(source, flowID, eventType, opts, appendEvidence)
		appendParentConnectEvidence(source, flowID, eventType, appendEvidence)
	} else {
		if isInputEvent {
			appendParentConnectEvidence(source, flowID, eventType, appendEvidence)
		}
		appendExternalMetadataEvidence(source, flowID, eventType, appendEvidence)
	}
	appendHarnessInjectionEvidence(flowID, eventType, opts, appendEvidence)
	appendPlatformSourceEvidence(source, flowID, eventType, appendEvidence)
	appendInternalTopologyEvidence(source, flowID, eventType, appendEvidence)

	sort.SliceStable(out.Evidence, func(i, j int) bool {
		left := flowInputProducerEvidenceSortKey(out.Evidence[i])
		right := flowInputProducerEvidenceSortKey(out.Evidence[j])
		return left < right
	})
	return out
}

func appendBoundaryIngressEvidence(source Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	if flowID == "" && !opts.AllowNonInputEvent {
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerBoundaryExternalIngress,
			EventType: eventType,
			Detail:    "root input pin is externally ingressible",
		})
		return
	}
	for _, pin := range flowInputPinsForEvent(source, flowID, eventType) {
		if strings.TrimSpace(pin.Source) != "external" {
			continue
		}
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress,
			EventType: eventType,
			Pin:       pin.PinName(),
			Detail:    "input pin declares source: external",
		})
	}
}

func appendParentConnectEvidence(source Source, flowID, eventType string, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	for _, alias := range ImportBoundaryInputAliases(source, flowID, eventType) {
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerBoundaryParentConnect,
			FlowID:    strings.TrimSpace(alias.FlowID),
			EventType: alias.ParentEvent,
			Pin:       alias.Pin,
			Pattern:   alias.EventPattern,
			Detail:    "import-boundary input bind",
		})
	}
	for _, pin := range flowInputPinsForEvent(source, flowID, eventType) {
		connects := source.CompositionConnectsTo(flowID, pin.PinName())
		if len(connects) == 0 {
			continue
		}
		for _, connect := range connects {
			ref, _ := connect.FromRef()
			appendEvidence(runtimecontracts.FlowInputProducerEvidence{
				Kind:      runtimecontracts.FlowInputProducerBoundaryParentConnect,
				FlowID:    ref.FlowID,
				EventType: eventType,
				Pin:       pin.PinName(),
				Detail:    fmt.Sprintf("parent connect from %s", strings.TrimSpace(connect.From)),
			})
		}
	}
}

func appendHarnessInjectionEvidence(flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	for _, injection := range opts.HarnessInjections {
		if strings.TrimSpace(injection.FlowID) != flowID || eventidentity.Normalize(injection.EventType) != eventType {
			continue
		}
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerBoundaryHarnessInjection,
			FlowID:    flowID,
			EventType: eventType,
			Detail:    "explicit validation harness injection",
		})
	}
}

func appendPlatformSourceEvidence(source Source, flowID, eventType string, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	if source == nil {
		return
	}
	candidates := uniqueFlowInputProducerEvents(eventType, source.ResolveFlowEventReference(flowID, eventType))
	for _, candidate := range candidates {
		if runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), candidate) {
			appendEvidence(runtimecontracts.FlowInputProducerEvidence{
				Kind:      runtimecontracts.FlowInputProducerPlatformSource,
				EventType: candidate,
				Detail:    "platform event catalog",
			})
			return
		}
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	if ok && eventMetadataPlatformSource(entry.SwarmSource()) {
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerPlatformSource,
			EventType: eventType,
			Detail:    "event metadata swarm.source: platform",
		})
	}
}

func appendExternalMetadataEvidence(source Source, flowID, eventType string, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
	if !ok || !eventMetadataExternalSource(entry.SwarmSource()) {
		return
	}
	appendEvidence(runtimecontracts.FlowInputProducerEvidence{
		Kind:      runtimecontracts.FlowInputProducerBoundaryExternalIngress,
		EventType: eventType,
		Detail:    "event metadata swarm.source: external",
	})
}

func appendInternalTopologyEvidence(source Source, flowID, eventType string, appendEvidence func(runtimecontracts.FlowInputProducerEvidence)) {
	census := BuildAuthoredEventEndpointCensus(source)
	for _, endpoint := range census.MatchingProducersAcrossFlows(flowID, eventType) {
		if endpoint.Kind == EventEndpointExternal || endpoint.Kind == EventEndpointPlatform {
			continue
		}
		detail := endpointProducerEvidenceDetail(endpoint)
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerInternalTopology,
			FlowID:    endpoint.FlowID,
			EventType: eventType,
			Detail:    detail,
		})
	}
	for _, endpoint := range census.MatchingOutputPinsAcrossFlows(flowID, eventType) {
		if strings.TrimSpace(endpoint.FlowID) == strings.TrimSpace(flowID) {
			continue
		}
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerInternalTopology,
			FlowID:    endpoint.FlowID,
			EventType: eventType,
			Pin:       endpoint.PinName,
			Detail:    fmt.Sprintf("sibling flow %s output pin %s", endpoint.FlowID, endpoint.PinName),
		})
	}
}

func flowInputPinsForEvent(source Source, flowID, eventType string) []runtimecontracts.FlowInputEventPin {
	if source == nil || eventidentity.Normalize(eventType) == "" {
		return nil
	}
	association := BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, eventType)
	endpoint, ok := association.Endpoint()
	if !ok {
		return nil
	}
	pin, ok := source.FlowInputEventPin(flowID, endpoint.PinName)
	if !ok {
		return nil
	}
	return []runtimecontracts.FlowInputEventPin{pin}
}

func endpointProducerEvidenceDetail(endpoint AuthoredEventEndpoint) string {
	switch endpoint.Kind {
	case EventEndpointNodeHandler:
		return fmt.Sprintf("node %s handler %s emits", endpoint.NodeID, endpoint.HandlerEvent)
	case EventEndpointNodeGenerated:
		return fmt.Sprintf("node %s generated producer", endpoint.NodeID)
	case EventEndpointAgent:
		return fmt.Sprintf("agent %s emit_events", endpoint.AgentID)
	case EventEndpointRequiredAgentRole:
		return fmt.Sprintf("required agent role %s emits", endpoint.Role)
	case EventEndpointTimer:
		if endpoint.FlowID == "" {
			return strings.TrimSpace("root timer " + endpoint.TimerID)
		}
		return strings.TrimSpace(fmt.Sprintf("timer in flow %s %s", endpoint.FlowID, endpoint.TimerID))
	case EventEndpointAutoEmit:
		if endpoint.FlowID == "" {
			return "root auto_emit_on_create"
		}
		return fmt.Sprintf("flow %s auto_emit_on_create", endpoint.FlowID)
	case EventEndpointPlatform:
		return "platform event catalog"
	case EventEndpointExternal:
		return "external event metadata"
	default:
		return strings.TrimSpace(endpoint.SourceLocation)
	}
}

func uniqueFlowInputProducerEvents(values ...string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = eventidentity.Normalize(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func eventMetadataPlatformSource(source string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "platform")
}

func eventMetadataExternalSource(source string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "external")
}

func flowInputProducerEvidenceSortKey(evidence runtimecontracts.FlowInputProducerEvidence) string {
	return strings.Join([]string{
		strings.TrimSpace(evidence.Kind),
		strings.TrimSpace(evidence.FlowID),
		strings.TrimSpace(evidence.EventType),
		strings.TrimSpace(evidence.Pin),
		strings.TrimSpace(evidence.Pattern),
		strings.TrimSpace(evidence.Detail),
	}, "\x00")
}
