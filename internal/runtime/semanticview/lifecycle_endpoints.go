package semanticview

import "strings"

func (b *endpointCensusBuilder) addLifecycleEndpoints() {
	for _, gate := range b.source.WorkflowGates() {
		for _, verdict := range sortedMapKeys(gate.Outcomes) {
			outcome := gate.Outcomes[verdict]
			if outcome.Emit.EventType() == "" {
				continue
			}
			endpoint := b.endpoint(EventEndpointProducer, EventEndpointGateOutcome, gate.FlowID, outcome.Emit.EventType())
			endpoint.StageID = gate.Stage
			endpoint.DecisionID = gate.Decision
			endpoint.Verdict = verdict
			b.lifecycleSource(&endpoint, "stages", gate.Stage, "gate", "outcomes", verdict, "emit")
			b.add(endpoint)
		}
	}
	for _, loop := range WorkflowLoops(b.source) {
		if loop.Escape.Emit.EventType() == "" {
			continue
		}
		endpoint := b.endpoint(EventEndpointProducer, EventEndpointLoopEscape, loop.FlowID, loop.Escape.Emit.EventType())
		endpoint.LoopID = loop.ID
		b.lifecycleSource(&endpoint, "loops", loop.ID, "escape", "emit")
		b.add(endpoint)
	}
}

// Coordinates are read from the admitted artifact, never from ambient files.
// Only the compiled plan above determines whether an emission exists.
func (b *endpointCensusBuilder) lifecycleSource(endpoint *AuthoredEventEndpoint, path ...string) {
	endpoint.Site = strings.Join(path, ".")
	endpoint.SourceLocation = endpoint.Site
	bundle, ok := Bundle(b.source)
	if !ok || bundle == nil {
		return
	}
	flow := endpoint.FlowID
	if flow == "" {
		flow = "."
	}
	endpoint.SourceFile = bundle.FlowSources[flow].Schema
	node := yamlDocumentMapping(b.yamlFile(endpoint.SourceFile))
	for _, key := range path {
		node = yamlMappingValue(node, key)
	}
	if node.Valid() {
		endpoint.SourceLine = node.Line()
	}
}
