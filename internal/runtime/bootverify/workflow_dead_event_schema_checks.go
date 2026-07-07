package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkSemanticDriftDeadEventSchema(c *checkerContext) []Finding {
	return c.deadEventSchema()
}

type deadEventSchemaUsage struct {
	handlerEmits       int
	handlerSubscribes  int
	agentEmitEvents    int
	agentSubscriptions int
	timerReferences    int
	fanOutEmit         int
	autoEmitOnCreate   bool
	externalSource     bool
	externalConsumer   bool
	connectOutputs     int
	connectInputs      int
}

func (u deadEventSchemaUsage) hasAny() bool {
	return u.handlerEmits > 0 ||
		u.handlerSubscribes > 0 ||
		u.agentEmitEvents > 0 ||
		u.agentSubscriptions > 0 ||
		u.timerReferences > 0 ||
		u.fanOutEmit > 0 ||
		u.autoEmitOnCreate ||
		u.externalSource ||
		u.externalConsumer ||
		u.connectOutputs > 0 ||
		u.connectInputs > 0
}

func (c *checkerContext) deadEventSchema() []Finding {
	if c.deadEventSchemaLoaded {
		return c.deadEventSchemaFindings
	}
	c.deadEventSchemaLoaded = true

	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return nil
	}

	for _, decl := range deadEventDeclarations(c.source) {
		usage := c.deadEventSchemaUsageFor(decl)
		if usage.hasAny() {
			continue
		}
		fileLabel := deadEventSchemaFileLabel(strings.TrimSpace(decl.File), strings.TrimSpace(decl.FlowID))
		c.deadEventSchemaFindings = append(c.deadEventSchemaFindings, Finding{
			CheckID:  "semantic_drift_dead_event_schema",
			Severity: "warning",
			Message: fmt.Sprintf(
				"Event %s declared in %s has no active role in the authored bundle.\n\nChecked usage sites:\n- Handler emits: %d\n- Handler subscribes: %d\n- Agent emit_events: %d\n- Agent subscriptions: %d\n- Timer fire/start/cancel references: %d\n- Resolver-backed input source or non-input source metadata: %s\n- External consumer metadata (swarm.consumer): %s\n- Fan-out emit: %d\n- Auto-emit-on-create: %s\n- Parent connect outputs: %d\n- Parent connect inputs: %d\n\nIf this event is no longer used, remove it from %s.\nIf it is used by an external system, add input-pin source: external for true ingress, a parent connect, a harness injection, or non-input swarm.source/swarm.consumer metadata as appropriate.",
				decl.Canonical,
				fileLabel,
				usage.handlerEmits,
				usage.handlerSubscribes,
				usage.agentEmitEvents,
				usage.agentSubscriptions,
				usage.timerReferences,
				yesNoLocal(usage.externalSource),
				yesNoLocal(usage.externalConsumer),
				usage.fanOutEmit,
				yesNoLocal(usage.autoEmitOnCreate),
				usage.connectOutputs,
				usage.connectInputs,
				fileLabel,
			),
			Location: decl.Canonical,
		})
	}

	return c.deadEventSchemaFindings
}

func deadEventDeclarations(source semanticview.Source) []deadEventDeclaration {
	if source == nil {
		return nil
	}
	out := make([]deadEventDeclaration, 0)
	for _, scope := range semanticview.ProjectScopes(source) {
		if strings.TrimSpace(scope.OwningFlowID) != "" {
			continue
		}
		for eventName, entry := range scope.Events {
			canonical := eventidentity.Normalize(eventName)
			if canonical == "" {
				continue
			}
			out = append(out, deadEventDeclaration{
				Canonical: canonical,
				File:      deadEventProjectFileLabel(scope.Key),
				Entry:     entry,
			})
		}
	}
	for _, scope := range semanticview.FlowScopes(source) {
		flowID := strings.TrimSpace(scope.ID)
		for eventName, entry := range scope.Events {
			canonical := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventName))
			if canonical == "" {
				continue
			}
			out = append(out, deadEventDeclaration{
				Canonical: canonical,
				FlowID:    flowID,
				File:      deadEventSchemaFileLabel("", flowID),
				Entry:     entry,
			})
		}
	}
	return out
}

func deadEventProjectFileLabel(packageKey string) string {
	packageKey = strings.Trim(strings.TrimSpace(packageKey), "/")
	if packageKey == "" || packageKey == "." {
		return "events.yaml"
	}
	return fmt.Sprintf("%s/events.yaml", packageKey)
}

type deadEventDeclaration struct {
	Canonical string
	FlowID    string
	File      string
	Entry     runtimecontracts.EventCatalogEntry
}

func (c *checkerContext) deadEventSchemaUsageFor(decl deadEventDeclaration) deadEventSchemaUsage {
	usage := deadEventSchemaUsage{
		externalSource:   c.deadEventSourceRole(decl),
		externalConsumer: deadEventExternalConsumer(decl.Entry),
	}

	for _, required := range c.source.RequiredAgents() {
		for _, eventType := range required.Emits {
			if deadEventRoleMatches(c.source, decl, "", eventType) {
				usage.agentEmitEvents++
			}
		}
		for _, eventType := range required.SubscribesTo {
			if deadEventRoleMatches(c.source, decl, "", eventType) {
				usage.agentSubscriptions++
			}
		}
	}
	for _, scope := range c.source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if deadEventSameScope(decl.FlowID, flowID) && deadEventRoleMatches(c.source, decl, flowID, scope.AutoEmitEvent) {
			usage.autoEmitOnCreate = true
		}
		for _, required := range c.source.FlowRequiredAgents(flowID) {
			for _, eventType := range required.Emits {
				if deadEventRoleMatches(c.source, decl, flowID, eventType) {
					usage.agentEmitEvents++
				}
			}
			for _, eventType := range required.SubscribesTo {
				if deadEventRoleMatches(c.source, decl, flowID, eventType) {
					usage.agentSubscriptions++
				}
			}
		}
	}
	for _, scope := range semanticview.ProjectScopes(c.source) {
		flowID := strings.TrimSpace(scope.OwningFlowID)
		for _, agent := range scope.Agents {
			for _, eventType := range agent.EmitEvents {
				if deadEventRoleMatches(c.source, decl, flowID, eventType) {
					usage.agentEmitEvents++
				}
			}
			for _, eventType := range agent.Subscriptions {
				if deadEventRoleMatches(c.source, decl, flowID, eventType) {
					usage.agentSubscriptions++
				}
			}
		}
		for _, node := range scope.Nodes {
			for _, eventType := range runtimecontracts.EffectiveSystemNodeSubscriptions(node) {
				if deadEventRoleMatches(c.source, decl, flowID, eventType) {
					usage.handlerSubscribes++
				}
			}
			for handlerEvent, handler := range node.EventHandlers {
				if deadEventRoleMatches(c.source, decl, flowID, handlerEvent) {
					usage.handlerSubscribes++
				}
				for _, emitted := range deadEventHandlerEmits(handler) {
					if deadEventRoleMatches(c.source, decl, flowID, emitted) {
						usage.handlerEmits++
					}
				}
				for _, emitted := range deadEventHandlerFanOutEmits(handler) {
					if deadEventSameScope(decl.FlowID, flowID) && deadEventRoleMatches(c.source, decl, flowID, emitted) {
						usage.fanOutEmit++
					}
				}
			}
		}
	}
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil && bundle.RootSchema != nil {
		if deadEventSameScope(decl.FlowID, "") && deadEventRoleMatches(c.source, decl, "", bundle.RootSchema.AutoEmitOnCreate.Event) {
			usage.autoEmitOnCreate = true
		}
	}

	for agentID, agent := range c.source.AgentEntries() {
		agentSource, _ := c.source.AgentContractSource(agentID)
		flowID := strings.TrimSpace(agentSource.FlowID)
		for _, eventType := range agent.EmitEvents {
			if deadEventRoleMatches(c.source, decl, flowID, eventType) {
				usage.agentEmitEvents++
			}
		}
		for _, eventType := range agent.Subscriptions {
			if deadEventRoleMatches(c.source, decl, flowID, eventType) {
				usage.agentSubscriptions++
			}
		}
	}

	for nodeID, node := range c.source.NodeEntries() {
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range semanticview.NodeEffectiveSubscriptions(c.source, nodeID) {
			if deadEventRoleMatches(c.source, decl, flowID, eventType) {
				usage.handlerSubscribes++
			}
		}
		for handlerEvent, handler := range node.EventHandlers {
			if deadEventRoleMatches(c.source, decl, flowID, handlerEvent) {
				usage.handlerSubscribes++
			}
			for _, emitted := range deadEventHandlerEmits(handler) {
				if deadEventRoleMatches(c.source, decl, flowID, emitted) {
					usage.handlerEmits++
				}
			}
			for _, emitted := range deadEventHandlerFanOutEmits(handler) {
				if deadEventSameScope(decl.FlowID, flowID) && deadEventRoleMatches(c.source, decl, flowID, emitted) {
					usage.fanOutEmit++
				}
			}
		}
	}

	for _, timer := range c.source.WorkflowTimers() {
		timerFlowID := strings.TrimSpace(timer.FlowID)
		if !deadEventSameScope(decl.FlowID, timerFlowID) {
			continue
		}
		for _, eventType := range deadEventTimerReferences(timer) {
			if deadEventRoleMatches(c.source, decl, timerFlowID, eventType) {
				usage.timerReferences++
			}
		}
	}
	for _, connect := range c.source.CompositionConnects() {
		from, fromErr := connect.FromRef()
		if fromErr == nil {
			if outputPin, ok := c.source.FlowOutputEventPin(from.FlowID, from.Pin); ok && deadEventRoleMatches(c.source, decl, from.FlowID, outputPin.EventType()) {
				usage.connectOutputs++
			}
		}
		to, toErr := connect.ToRef()
		if toErr == nil {
			if inputPin, ok := c.source.FlowInputEventPin(to.FlowID, to.Pin); ok && deadEventRoleMatches(c.source, decl, to.FlowID, inputPin.EventType()) {
				usage.connectInputs++
			}
		}
	}

	return usage
}

func deadEventHandlerEmits(handler runtimecontracts.SystemNodeEventHandler) []string {
	return runtimecontracts.HandlerEmitEvents(handler)
}

func deadEventHandlerFanOutEmits(handler runtimecontracts.SystemNodeEventHandler) []string {
	out := make([]string, 0, 1+len(handler.Rules)+len(handler.OnComplete)+2)
	if handler.FanOut != nil {
		if emitted := handler.FanOut.Emit.EventType(); emitted != "" {
			out = append(out, emitted)
		}
	}
	for _, rule := range handler.Rules {
		if rule.FanOut != nil {
			if emitted := rule.FanOut.Emit.EventType(); emitted != "" {
				out = append(out, emitted)
			}
		}
	}
	for _, rule := range handler.OnComplete {
		if rule.FanOut != nil {
			if emitted := rule.FanOut.Emit.EventType(); emitted != "" {
				out = append(out, emitted)
			}
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if rule.FanOut != nil {
				if emitted := rule.FanOut.Emit.EventType(); emitted != "" {
					out = append(out, emitted)
				}
			}
		}
		if handler.Accumulate.OnTimeout != nil && handler.Accumulate.OnTimeout.FanOut != nil {
			if emitted := handler.Accumulate.OnTimeout.FanOut.Emit.EventType(); emitted != "" {
				out = append(out, emitted)
			}
		}
	}
	return out
}

func deadEventTimerReferences(timer runtimecontracts.WorkflowTimerContract) []string {
	out := make([]string, 0, 3)
	if eventType := strings.TrimSpace(timer.Event); eventType != "" {
		out = append(out, eventType)
	}
	for _, gate := range []string{timer.StartOn, timer.CancelOn} {
		gate = strings.TrimSpace(gate)
		if !strings.HasPrefix(gate, "event:") {
			continue
		}
		if eventType := strings.TrimSpace(strings.TrimPrefix(gate, "event:")); eventType != "" {
			out = append(out, eventType)
		}
	}
	return out
}

func deadEventRoleMatches(source semanticview.Source, decl deadEventDeclaration, referenceFlowID, reference string) bool {
	reference = eventidentity.Normalize(reference)
	if reference == "" {
		return false
	}
	referenceFlowID = strings.TrimSpace(referenceFlowID)
	proof := semanticview.ResolveFlowEventProof(source, referenceFlowID, reference)
	if eventidentity.Normalize(proof.Canonical) != eventidentity.Normalize(decl.Canonical) {
		return false
	}

	if strings.TrimSpace(decl.FlowID) == "" {
		return referenceFlowID == ""
	}
	if strings.TrimSpace(decl.FlowID) == referenceFlowID {
		return true
	}
	return strings.Contains(reference, "/")
}

func deadEventSameScope(a, b string) bool {
	return strings.TrimSpace(a) == strings.TrimSpace(b)
}

func (c *checkerContext) deadEventSourceRole(decl deadEventDeclaration) bool {
	if resolution, ok := c.resolveDeclaredInputProducerSource(decl.FlowID, decl.Canonical); ok {
		return resolution.HasEvidence()
	}
	return nonInputEventMetadataProducerSource(decl.Entry)
}

func deadEventExternalConsumer(entry runtimecontracts.EventCatalogEntry) bool {
	return len(entry.SwarmConsumer()) > 0
}

func deadEventSchemaFileLabel(path, flowID string) string {
	path = strings.TrimSpace(path)
	if path != "" {
		return path
	}
	if strings.TrimSpace(flowID) == "" {
		return "events.yaml"
	}
	return fmt.Sprintf("flows/%s/events.yaml", strings.TrimSpace(flowID))
}

func yesNoLocal(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
