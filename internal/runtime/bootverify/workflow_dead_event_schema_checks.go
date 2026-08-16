package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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
				"Event %s declared in %s has no active role in the authored bundle.\n\nChecked usage sites:\n- Handler emits: %d\n- Handler subscribes: %d\n- Agent emit_events: %d\n- Agent subscriptions: %d\n- Timer fire/start/cancel references: %d\n- Resolver-backed input source or non-input source metadata: %s\n- External consumer metadata (swarm.consumer): %s\n- Fan-out emit: %d\n- Auto-emit-on-create: %s\n- Parent connect outputs: %d\n- Parent connect inputs: %d\n\nIf this event is no longer used, remove it from %s.\nIf it is produced outside this flow, add input-pin source: external for true ingress, a parent connect, or non-input swarm.source/swarm.consumer metadata as appropriate. For a validation fixture only, set source: harness on the input pin; it will remain non-production-valid.",
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
			if strings.TrimSpace(entry.Source) == "provider_trigger_pack_raw" {
				continue
			}
			canonical := eventidentity.Normalize(eventName)
			if canonical == "" {
				continue
			}
			out = append(out, deadEventDeclaration{
				Canonical:  canonical,
				PackageKey: scope.Key,
				FlowID:     scope.OwningFlowID,
				File:       deadEventProjectFileLabel(scope.Key),
				Entry:      entry,
			})
		}
	}
	for _, scope := range semanticview.FlowScopes(source) {
		flowID := strings.TrimSpace(scope.ID)
		for eventName, entry := range scope.Events {
			scopeNode, _ := runtimeidentity.AdmitExecutableNodeDeclaration(scope.PackageKey, flowID, "__event_schema__")
			canonical := eventidentity.Normalize(source.ResolveExecutableNodeEventReference(scopeNode, eventName))
			if canonical == "" {
				continue
			}
			out = append(out, deadEventDeclaration{
				Canonical:  canonical,
				PackageKey: scope.PackageKey,
				FlowID:     flowID,
				File:       deadEventSchemaFileLabel("", flowID),
				Entry:      entry,
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
	Canonical  string
	PackageKey string
	FlowID     string
	File       string
	Entry      runtimecontracts.EventCatalogEntry
}

func (c *checkerContext) deadEventSchemaUsageFor(decl deadEventDeclaration) deadEventSchemaUsage {
	usage := deadEventSchemaUsage{
		externalSource:   c.deadEventSourceRole(decl),
		externalConsumer: deadEventExternalConsumer(decl.Entry),
	}

	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, endpoint := range census.Producers() {
		if !endpointMatchesDeadEventDeclaration(endpoint, decl) {
			continue
		}
		switch endpoint.Kind {
		case semanticview.EventEndpointNodeHandler, semanticview.EventEndpointNodeGenerated:
			usage.handlerEmits++
			if strings.Contains(endpoint.Site, "fan_out") {
				usage.fanOutEmit++
			}
		case semanticview.EventEndpointAgent, semanticview.EventEndpointRequiredAgentRole:
			usage.agentEmitEvents++
		case semanticview.EventEndpointTimer:
			usage.timerReferences++
		case semanticview.EventEndpointAutoEmit:
			usage.autoEmitOnCreate = true
		}
	}
	for _, match := range deadEventTypedConsumerMatches(c.source, census, decl) {
		endpoint := match.Consumer
		switch endpoint.Kind {
		case semanticview.EventEndpointNodeHandler, semanticview.EventEndpointNodeGenerated:
			usage.handlerSubscribes++
		case semanticview.EventEndpointAgent, semanticview.EventEndpointRequiredAgentRole:
			usage.agentSubscriptions++
		case semanticview.EventEndpointTimer:
			usage.timerReferences++
		}
	}
	for _, edge := range runtimepinrouting.CompileConnectGraph(c.source).Edges() {
		if edge.Producer().Matches(decl.FlowID, events.EventType(decl.Canonical)) {
			usage.connectOutputs++
		}
		if edge.Consumer().Matches(decl.FlowID, events.EventType(decl.Canonical)) {
			usage.connectInputs++
		}
	}

	return usage
}

func deadEventTypedConsumerMatches(source semanticview.Source, census semanticview.AuthoredEventEndpointCensus, decl deadEventDeclaration) []semanticview.TypedPubSubConsumerMatch {
	producer := semanticview.AuthoredEventEndpoint{
		ID:         "dead-event-schema:" + strings.TrimSpace(decl.PackageKey) + ":" + strings.TrimSpace(decl.FlowID) + ":" + eventidentity.Normalize(decl.Canonical),
		Direction:  semanticview.EventEndpointProducer,
		FlowID:     strings.TrimSpace(decl.FlowID),
		PackageKey: strings.TrimSpace(decl.PackageKey),
	}
	scopeNode, _ := runtimeidentity.AdmitExecutableNodeDeclaration(decl.PackageKey, decl.FlowID, "__event_schema__")
	producer.Event = semanticview.ResolveExecutableNodeEventProof(source, scopeNode, decl.Canonical)
	matches, _ := census.ResolveTypedPubSubConsumerMatches(producer)
	return matches
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
	return entry.AcceptedConsumerBoundary() == runtimecontracts.EventConsumerBoundaryExternal
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
