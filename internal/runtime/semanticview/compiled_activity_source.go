package semanticview

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

// CompileActivityToolBindings freezes connector-composed schemas through the
// declaration compiler before any executable consumer sees the source.
func CompileActivityToolBindings(source Source) (Source, error) {
	bundle, ok := Bundle(source)
	if !ok {
		return nil, fmt.Errorf("activity tool compilation requires a bundle-backed source")
	}
	tools := make(map[string]map[string]contracts.ToolSchemaEntry)
	for _, scope := range source.FlowScopes() {
		tools[scope.ID] = scope.Tools
	}
	compiled, err := contracts.CompileActivityToolBindings(bundle, tools)
	if err != nil {
		return nil, err
	}
	out := compiledActivitySource{Source: source, compiled: bundleSource{bundle: compiled}, inputPins: map[string][]contracts.CompiledFlowInputPin{}}
	for _, scope := range source.FlowScopes() {
		pins := out.compiled.FlowInputEventPins(scope.ID)
		for index, pin := range pins {
			if _, exists := pin.ProducerEventSchema(); exists {
				continue
			}
			// A previously admitted provider import remains owned by its typed
			// schema. Rebind it through the same pin compiler, never its JSON map.
			previous, ok := source.FlowInputEventPin(scope.ID, pin.EventType())
			if !ok {
				continue
			}
			imported, exists := previous.ProducerEventSchema()
			if !exists || imported.Classification() != contracts.CompiledEventSchemaImported {
				continue
			}
			pins[index], err = pin.BindImportedEventSchema(imported)
			if err != nil {
				return nil, err
			}
		}
		out.inputPins[scope.ID] = pins
	}
	return out, nil
}

type compiledActivitySource struct {
	Source
	compiled  bundleSource
	inputPins map[string][]contracts.CompiledFlowInputPin
}

func (s compiledActivitySource) semanticSourceCore() sourceCore {
	return s.compiled.semanticSourceCore()
}
func (s compiledActivitySource) ResolvedEventCatalog() map[string]contracts.EventCatalogEntry {
	out := s.Source.ResolvedEventCatalog()
	for name, entry := range s.compiled.ResolvedEventCatalog() {
		out[name] = entry
	}
	return out
}
func (s compiledActivitySource) EventEntries() map[string]contracts.EventCatalogEntry {
	out := s.Source.EventEntries()
	for name, entry := range s.compiled.EventEntries() {
		out[name] = entry
	}
	return out
}
func (s compiledActivitySource) EventEntry(name string) (contracts.EventCatalogEntry, bool) {
	if entry, ok := s.compiled.EventEntry(name); ok {
		return entry, true
	}
	return s.Source.EventEntry(name)
}
func (s compiledActivitySource) ResolveFlowEventCatalogEntry(flowID, event string) (contracts.EventCatalogEntry, string, bool) {
	if entry, resolved, ok := s.compiled.ResolveFlowEventCatalogEntry(flowID, event); ok {
		return entry, resolved, true
	}
	return s.Source.ResolveFlowEventCatalogEntry(flowID, event)
}
func (s compiledActivitySource) ResolveExecutableNodeEventCatalogEntry(node identity.ExecutableNode, event string) (contracts.EventCatalogEntry, string, bool) {
	return s.ResolveFlowEventCatalogEntry(node.FlowPath(), s.ResolveExecutableNodeEventReference(node, event))
}
func (s compiledActivitySource) ResolveEffectiveCompiledFlowEventSchema(flowID, event string) (contracts.CompiledEventSchema, bool, error) {
	if pin, ok := s.FlowInputEventPin(flowID, event); ok {
		if schema, exists := pin.ReceiverEventSchema(); exists {
			return schema, true, nil
		}
	}
	if schema, found, err := s.compiled.ResolveEffectiveCompiledFlowEventSchema(flowID, event); err != nil || found {
		return schema, found, err
	}
	return s.Source.ResolveEffectiveCompiledFlowEventSchema(flowID, event)
}
func (s compiledActivitySource) ResolveFlowEventStructuralType(flowID, event string) (contracts.ResolvedCatalogType, bool) {
	schema, found, err := s.ResolveEffectiveCompiledFlowEventSchema(flowID, event)
	if err != nil || !found {
		return contracts.ResolvedCatalogType{}, false
	}
	return schema.StructuralType()
}
func (s compiledActivitySource) FlowInputEventPins(flowID string) []contracts.CompiledFlowInputPin {
	if strings.TrimSpace(flowID) == "" {
		flowID = "."
	}
	return append([]contracts.CompiledFlowInputPin(nil), s.inputPins[flowID]...)
}
func (s compiledActivitySource) FlowInputEventPin(flowID, event string) (contracts.CompiledFlowInputPin, bool) {
	for _, pin := range s.FlowInputEventPins(flowID) {
		if pin.EventType() == event {
			return pin, true
		}
	}
	return contracts.CompiledFlowInputPin{}, false
}
func (s compiledActivitySource) FlowOutputEventPins(flowID string) []contracts.CompiledFlowOutputPin {
	return s.compiled.FlowOutputEventPins(flowID)
}
func (s compiledActivitySource) FlowOutputEventPin(flowID, event string) (contracts.CompiledFlowOutputPin, bool) {
	return s.compiled.FlowOutputEventPin(flowID, event)
}
