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
	localEvent := eventidentity.Normalize(eventType)
	canonicalEvent := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType))
	if canonicalEvent == "" {
		canonicalEvent = eventType
	}
	if schema, ok := source.FlowSchemaByID(flowID); ok {
		if flowInputProducerEventMatches(source, flowID, localEvent, canonicalEvent, schema.AutoEmitOnCreate.Event) {
			detail := "root auto_emit_on_create"
			if flowID != "" {
				detail = fmt.Sprintf("flow %s auto_emit_on_create", flowID)
			}
			appendEvidence(runtimecontracts.FlowInputProducerEvidence{
				Kind:      runtimecontracts.FlowInputProducerInternalTopology,
				EventType: eventType,
				Detail:    detail,
			})
		}
	}
	for _, scope := range source.FlowScopes() {
		producerFlowID := strings.TrimSpace(scope.ID)
		if producerFlowID == "" || producerFlowID == flowID {
			continue
		}
		for _, output := range scope.OutputEvents {
			if !flowInputProducerEventMatches(source, producerFlowID, localEvent, canonicalEvent, output) {
				continue
			}
			appendEvidence(runtimecontracts.FlowInputProducerEvidence{
				Kind:      runtimecontracts.FlowInputProducerInternalTopology,
				EventType: eventType,
				Detail:    fmt.Sprintf("sibling flow %s output pin", producerFlowID),
			})
			break
		}
	}
	for nodeID, node := range source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		nodeSource, _ := source.NodeContractSource(nodeID)
		nodeFlowID := strings.TrimSpace(nodeSource.FlowID)
		for handlerEvent, handler := range node.EventHandlers {
			for _, emitted := range runtimecontracts.HandlerEmitEvents(handler) {
				if !flowInputProducerEventMatches(source, nodeFlowID, localEvent, canonicalEvent, emitted) {
					continue
				}
				appendEvidence(runtimecontracts.FlowInputProducerEvidence{
					Kind:      runtimecontracts.FlowInputProducerInternalTopology,
					EventType: eventType,
					Detail:    fmt.Sprintf("node %s handler %s emits", nodeID, strings.TrimSpace(handlerEvent)),
				})
			}
		}
	}
	for agentID, agent := range source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		agentSource, _ := source.AgentContractSource(agentID)
		agentFlowID := strings.TrimSpace(agentSource.FlowID)
		for _, emitted := range agent.EmitEvents {
			if !flowInputProducerEventMatches(source, agentFlowID, localEvent, canonicalEvent, emitted) {
				continue
			}
			appendEvidence(runtimecontracts.FlowInputProducerEvidence{
				Kind:      runtimecontracts.FlowInputProducerInternalTopology,
				EventType: eventType,
				Detail:    fmt.Sprintf("agent %s emit_events", agentID),
			})
		}
	}
	for _, timer := range source.WorkflowTimers() {
		timerFlowID := strings.TrimSpace(timer.FlowID)
		if !flowInputProducerEventMatches(source, timerFlowID, localEvent, canonicalEvent, timer.Event) {
			continue
		}
		detail := fmt.Sprintf("timer in flow %s", timerFlowID)
		if timerFlowID == "" {
			detail = "root timer"
		}
		if timerID := strings.TrimSpace(timer.ID); timerID != "" {
			detail += " " + timerID
		}
		appendEvidence(runtimecontracts.FlowInputProducerEvidence{
			Kind:      runtimecontracts.FlowInputProducerInternalTopology,
			EventType: eventType,
			Detail:    detail,
		})
	}
}

func flowInputPinsForEvent(source Source, flowID, eventType string) []runtimecontracts.FlowInputEventPin {
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return nil
	}
	out := make([]runtimecontracts.FlowInputEventPin, 0)
	for _, pin := range source.FlowInputEventPins(flowID) {
		if eventidentity.Normalize(pin.EventType()) != eventType && eventidentity.Normalize(pin.PinName()) != eventType {
			continue
		}
		out = append(out, pin)
	}
	return out
}

func flowInputProducerEventMatches(source Source, flowID, localEvent, canonicalEvent, candidate string) bool {
	candidate = eventidentity.Normalize(candidate)
	localEvent = eventidentity.Normalize(localEvent)
	canonicalEvent = eventidentity.Normalize(canonicalEvent)
	if candidate == "" || canonicalEvent == "" {
		return false
	}
	if candidate == localEvent || eventidentity.MatchPattern(localEvent, candidate) || eventidentity.MatchPattern(candidate, localEvent) {
		return true
	}
	if candidate == canonicalEvent {
		return true
	}
	if source == nil {
		return false
	}
	resolved := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, candidate))
	return resolved == canonicalEvent ||
		eventidentity.MatchPattern(canonicalEvent, resolved) ||
		eventidentity.MatchPattern(resolved, canonicalEvent)
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
