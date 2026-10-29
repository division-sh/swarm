package runtime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func SourceWithProviderTriggerEvents(source semanticview.Source, _ *providertriggers.CatalogSnapshot) (semanticview.Source, error) {
	bundle, present := semanticview.Bundle(source)
	if !present || bundle == nil {
		return nil, fmt.Errorf("provider trigger events require their compiled rooted source")
	}
	providers, err := packadmission.ProviderSourceForBundle(bundle)
	if err != nil {
		return nil, err
	}
	return sourceWithDirectoryTriggerSchemas(source, providers)
}

func bindProviderTriggerOutputPins(source semanticview.Source, compiledByFlow map[string]map[string]runtimecontracts.CompiledEventSchema) (map[string][]runtimecontracts.CompiledFlowOutputPin, error) {
	out := map[string][]runtimecontracts.CompiledFlowOutputPin{}
	for _, scope := range source.FlowScopes() {
		pins := source.FlowOutputEventPins(scope.ID)
		bound := append([]runtimecontracts.CompiledFlowOutputPin(nil), pins...)
		schemas := compiledByFlow[normalizedProviderTriggerProjectKey(scope.Path)]
		for index, pin := range bound {
			if _, exists := pin.EventSchema(); exists {
				continue
			}
			schema, imported := schemas[pin.EventType()]
			if !imported {
				continue
			}
			var err error
			bound[index], err = pin.BindImportedEventSchema(schema)
			if err != nil {
				return nil, fmt.Errorf("bind provider-trigger output pin %s.%s: %w", scope.ID, pin.EventType(), err)
			}
		}
		out[normalizedProviderTriggerProjectKey(scope.ID)] = bound
	}
	return out, nil
}

func compileProviderTriggerEventSchemas(byProject map[string]map[string]runtimecontracts.EventCatalogEntry, owners map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner) (map[string]map[string]runtimecontracts.CompiledEventSchema, error) {
	out := make(map[string]map[string]runtimecontracts.CompiledEventSchema, len(byProject))
	for flowPath, entries := range byProject {
		ownerKey := normalizedProviderTriggerProjectKey(flowPath)
		out[ownerKey] = make(map[string]runtimecontracts.CompiledEventSchema, len(entries))
		for eventName, entry := range entries {
			owner := owners[providerTriggerSchemaCoordinate{flowPath, eventName}]
			compiled, err := runtimecontracts.CompileImportedEventSchema(ownerKey, eventName, entry, runtimecontracts.CompiledEventSchemaSource{
				Layer: "provider_trigger_pack", File: owner.diagnostic(),
			})
			if err != nil {
				return nil, err
			}
			out[ownerKey][eventName] = compiled
		}
	}
	return out, nil
}

func bindProviderTriggerInputPins(source semanticview.Source, compiledByProject map[string]map[string]runtimecontracts.CompiledEventSchema, out map[string][]runtimecontracts.CompiledFlowInputPin) (map[string][]runtimecontracts.CompiledFlowInputPin, error) {
	bind := func(flowID, ownerFlowPath string) error {
		pins := out[normalizedProviderTriggerProjectKey(flowID)]
		if len(pins) == 0 {
			return nil
		}
		compiledSchemas := compiledByProject[normalizedProviderTriggerProjectKey(ownerFlowPath)]
		bound := append([]runtimecontracts.CompiledFlowInputPin(nil), pins...)
		for index, pin := range bound {
			if _, exists := pin.ProducerEventSchema(); exists {
				continue
			}
			schema, imported := compiledSchemas[pin.EventType()]
			if !imported {
				continue
			}
			var err error
			bound[index], err = pin.BindImportedEventSchema(schema)
			if err != nil {
				return fmt.Errorf("bind provider-trigger input pin %s.%s: %w", flowID, pin.EventType(), err)
			}
		}
		out[normalizedProviderTriggerProjectKey(flowID)] = bound
		return nil
	}
	if err := bind(".", "."); err != nil {
		return nil, err
	}
	for _, scope := range source.FlowScopes() {
		if scope.ID == "." {
			continue
		}
		if err := bind(scope.ID, scope.Path); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func normalizedProviderTriggerProjectKey(projectKey string) string {
	projectKey = strings.Trim(strings.TrimSpace(projectKey), "/")
	if projectKey == "" {
		return "."
	}
	return projectKey
}

type providerTriggerSchemaOwner struct {
	provider   string
	event      string
	kind       providertriggers.OutputKind
	identity   providertriggers.PackIdentity
	generation triggergeneration.Generation
	source     string
}

type providerTriggerSchemaCoordinate struct{ flow, event string }

func newProviderTriggerSchemaOwner(provider, event string, kind providertriggers.OutputKind, identity providertriggers.PackIdentity, generation triggergeneration.Generation, source string) providerTriggerSchemaOwner {
	return providerTriggerSchemaOwner{
		provider: provider, event: strings.TrimSpace(event), kind: kind,
		identity: identity, generation: generation, source: source,
	}
}

func (o providerTriggerSchemaOwner) equal(other providerTriggerSchemaOwner) bool {
	return o.provider == other.provider && o.event == other.event && o.kind == other.kind &&
		o.identity.ID == other.identity.ID && o.identity.Version == other.identity.Version &&
		o.identity.ManifestHash == other.identity.ManifestHash && o.identity.Provenance == other.identity.Provenance &&
		o.generation.Equal(other.generation) && o.source == other.source
}

func (o providerTriggerSchemaOwner) diagnostic() string {
	return fmt.Sprintf(
		"trigger %s version=%s body_hash=%s source=%s catalog_generation=%s",
		o.identity.ID, o.identity.Version, o.identity.ManifestHash, o.source, o.generation.Diagnostic(),
	)
}

func addProviderTriggerSchema(
	source semanticview.Source,
	projectKey string,
	eventName string,
	eventEntry runtimecontracts.EventCatalogEntry,
	owner providerTriggerSchemaOwner,
	owners map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner,
	byProject map[string]map[string]runtimecontracts.EventCatalogEntry,
) error {
	eventName = strings.TrimSpace(eventName)
	coordinate := providerTriggerSchemaCoordinate{projectKey, eventName}
	if existingOwner, duplicate := owners[coordinate]; duplicate {
		if !existingOwner.equal(owner) {
			return fmt.Errorf("provider trigger event %q collision between %s and %s; remove one provider-trigger declaration", eventName, existingOwner.diagnostic(), owner.diagnostic())
		}
	} else {
		if _, collision := source.AuthoredEventEntries()[eventName]; collision {
			existingOwner := "existing event catalog"
			return fmt.Errorf("provider trigger event %q collision between %s and %s; remove the local redeclaration and inspect the pack with `swarm describe pack %s`", eventName, existingOwner, owner.diagnostic(), owner.identity.ID)
		}
		owners[coordinate] = owner
	}
	projectKey = strings.TrimSpace(projectKey)
	if byProject[projectKey] == nil {
		byProject[projectKey] = map[string]runtimecontracts.EventCatalogEntry{}
	}
	byProject[projectKey][eventName] = cloneProviderTriggerEventCatalogEntry(eventEntry)
	return nil
}

func providerTriggerGlobalReadback(byFlow map[string]map[string]runtimecontracts.EventCatalogEntry, owners map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner) (map[string]runtimecontracts.EventCatalogEntry, map[string]runtimecontracts.CompiledEventSchema, error) {
	entries := map[string]runtimecontracts.EventCatalogEntry{}
	aliases := map[string]providerTriggerSchemaOwner{}
	ambiguous := map[string]bool{}
	for coordinate, owner := range owners {
		if previous, found := aliases[coordinate.event]; found && !previous.equal(owner) {
			ambiguous[coordinate.event] = true
		}
		aliases[coordinate.event] = owner
		entries[coordinate.event] = cloneProviderTriggerEventCatalogEntry(byFlow[coordinate.flow][coordinate.event])
	}
	compiled := map[string]runtimecontracts.CompiledEventSchema{}
	for event, entry := range entries {
		if ambiguous[event] {
			delete(entries, event)
			continue
		}
		schema, err := runtimecontracts.CompileImportedEventSchema(".", event, entry, runtimecontracts.CompiledEventSchemaSource{
			Layer: "provider_trigger_pack", File: aliases[event].diagnostic(),
		})
		if err != nil {
			return nil, nil, err
		}
		compiled[event] = schema
	}
	return entries, compiled, nil
}

func providerTriggerCatalogEvent(entry providertriggers.CatalogEntry, eventName string) (runtimecontracts.EventCatalogEntry, bool, bool) {
	eventName = strings.TrimSpace(eventName)
	entries := entry.Manifest.EventCatalogEntries()
	eventEntry, exists := entries[eventName]
	if !exists {
		return runtimecontracts.EventCatalogEntry{}, false, false
	}
	for _, output := range entry.Manifest.OutputManifest() {
		if output.Kind == providertriggers.OutputKindNormalized && output.Event == eventName {
			return eventEntry, true, true
		}
	}
	return eventEntry, true, false
}

type providerTriggerEventSource struct {
	semanticview.Source
	generation         triggergeneration.Generation
	imported           map[string]runtimecontracts.EventCatalogEntry
	owners             map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner
	byFlow             map[string]map[string]runtimecontracts.EventCatalogEntry
	outputs            map[string][]runtimeprovideroutput.Authorization
	scopeGenerations   map[string]triggergeneration.Generation
	ingressEvents      map[string][]string
	compiledInputPins  map[string][]runtimecontracts.CompiledFlowInputPin
	compiledOutputPins map[string][]runtimecontracts.CompiledFlowOutputPin
	compiledByFlow     map[string]map[string]runtimecontracts.CompiledEventSchema
	compiledCatalog    map[string]runtimecontracts.CompiledEventSchema
	connectionInputs   runtimecontracts.CompiledConnectionInputs
}

func (s providerTriggerEventSource) ConnectionInputs() runtimecontracts.CompiledConnectionInputs {
	return s.connectionInputs
}

func (s providerTriggerEventSource) ResolveExecutableNodeEventCatalogEntry(node runtimeidentity.ExecutableNode, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	if !node.Valid() {
		return runtimecontracts.EventCatalogEntry{}, "", false
	}
	eventType = s.ResolveExecutableNodeEventReference(node, eventType)
	if local, owned := s.connectionInputs.ReceiverEvent(node.FlowPath(), eventType); owned {
		return s.ResolveFlowEventCatalogEntry(node.FlowPath(), local)
	}
	return s.Source.ResolveExecutableNodeEventCatalogEntry(node, eventType)
}

func (s providerTriggerEventSource) ResolveEffectiveCompiledFlowEventSchema(flowID, eventType string) (runtimecontracts.CompiledEventSchema, bool, error) {
	if !s.hasExactFlow(flowID) {
		return runtimecontracts.CompiledEventSchema{}, false, nil
	}
	if schema, owned, err := s.connectionInputs.ReceiverCommonEventSchema(flowID, eventType); err != nil || owned {
		return schema, owned, err
	}
	if pin, ok := s.FlowInputEventPin(flowID, eventType); ok {
		if schema, owned := pin.ReceiverEventSchema(); owned {
			return schema, true, nil
		}
	}
	if schema, ok := s.compiledByFlow[flowID][eventType]; ok {
		return schema, true, nil
	}
	return s.Source.ResolveEffectiveCompiledFlowEventSchema(flowID, eventType)
}

func (s providerTriggerEventSource) FlowInputEventPins(flowID string) []runtimecontracts.CompiledFlowInputPin {
	if pins, ok := s.compiledInputPins[normalizedProviderTriggerProjectKey(flowID)]; ok {
		return append([]runtimecontracts.CompiledFlowInputPin(nil), pins...)
	}
	return s.Source.FlowInputEventPins(flowID)
}

func (s providerTriggerEventSource) FlowOutputEventPins(flowID string) []runtimecontracts.CompiledFlowOutputPin {
	if pins, ok := s.compiledOutputPins[normalizedProviderTriggerProjectKey(flowID)]; ok {
		return append([]runtimecontracts.CompiledFlowOutputPin(nil), pins...)
	}
	return s.Source.FlowOutputEventPins(flowID)
}

func (s providerTriggerEventSource) FlowOutputEventPin(flowID, eventType string) (runtimecontracts.CompiledFlowOutputPin, bool) {
	for _, pin := range s.FlowOutputEventPins(flowID) {
		if pin.EventType() == eventType {
			return pin, true
		}
	}
	return runtimecontracts.CompiledFlowOutputPin{}, false
}

func (s providerTriggerEventSource) FlowInputEventPin(flowID, eventType string) (runtimecontracts.CompiledFlowInputPin, bool) {
	eventType = strings.TrimSpace(eventType)
	for _, pin := range s.FlowInputEventPins(flowID) {
		if pin.EventType() == eventType {
			return pin, true
		}
	}
	return runtimecontracts.CompiledFlowInputPin{}, false
}

func (s providerTriggerEventSource) SemanticCapabilities() semanticview.Capabilities {
	capabilities := s.Source.SemanticCapabilities().WithProviderTriggerEvents(s.Source, s.generation, s.outputs, s.scopeGenerations).WithProviderIngressEvents(s.ingressEvents)
	return capabilities.WithProviderTriggerEventProvenance(s.provenanceReadback()).WithProviderTriggerSchemaReadback(s.compiledCatalog)
}

func (s providerTriggerEventSource) provenanceReadback() []semanticview.ProviderTriggerEventProvenance {
	coordinates := make([]providerTriggerSchemaCoordinate, 0, len(s.owners))
	for coordinate := range s.owners {
		coordinates = append(coordinates, coordinate)
	}
	sort.Slice(coordinates, func(i, j int) bool {
		if coordinates[i].flow != coordinates[j].flow {
			return coordinates[i].flow < coordinates[j].flow
		}
		return coordinates[i].event < coordinates[j].event
	})
	out := make([]semanticview.ProviderTriggerEventProvenance, 0, len(coordinates))
	for _, coordinate := range coordinates {
		owner := s.owners[coordinate]
		out = append(out, semanticview.ProviderTriggerEventProvenance{
			Provider: owner.provider, Event: owner.event, Kind: string(owner.kind),
			PackID: owner.identity.ID, PackVersion: owner.identity.Version,
			ManifestHash: owner.identity.ManifestHash, SourceProvenance: owner.source,
			Generation: owner.generation, FlowScopes: []string{coordinate.flow},
		})
	}
	return out
}

func (s providerTriggerEventSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	out := cloneEventCatalog(s.Source.ResolvedEventCatalog())
	for name, entry := range s.imported {
		out[name] = cloneProviderTriggerEventCatalogEntry(entry)
	}
	return out
}

func (s providerTriggerEventSource) ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	if !s.hasExactFlow(flowID) {
		return runtimecontracts.EventCatalogEntry{}, "", false
	}
	if schema, owned, err := s.connectionInputs.ReceiverCommonEventSchema(flowID, eventType); err != nil {
		return runtimecontracts.EventCatalogEntry{}, "", false
	} else if owned && schema.Classification() == runtimecontracts.CompiledEventSchemaImported {
		entry, present := s.byFlow[schema.FlowPath()][schema.EventName()]
		return cloneProviderTriggerEventCatalogEntry(entry), schema.EventName(), present
	}
	if entry, resolved, ok := s.Source.ResolveFlowEventCatalogEntry(flowID, eventType); ok {
		return entry, resolved, true
	}
	eventType = strings.TrimSpace(eventType)
	entry, ok := s.byFlow[flowID][eventType]
	return cloneProviderTriggerEventCatalogEntry(entry), eventType, ok
}

func (s providerTriggerEventSource) EventEntries() map[string]runtimecontracts.EventCatalogEntry {
	out := cloneEventCatalog(s.Source.EventEntries())
	for name, entry := range s.imported {
		out[name] = cloneProviderTriggerEventCatalogEntry(entry)
	}
	return out
}

func (s providerTriggerEventSource) EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	if entry, ok := s.Source.EventEntry(eventType); ok {
		return entry, true
	}
	entry, ok := s.imported[strings.TrimSpace(eventType)]
	return cloneProviderTriggerEventCatalogEntry(entry), ok
}

func (s providerTriggerEventSource) FlowScopes() []semanticview.FlowScope {
	scopes := s.Source.FlowScopes()
	out := make([]semanticview.FlowScope, 0, len(scopes))
	for _, scope := range scopes {
		scope.Events = cloneEventCatalog(scope.Events)
		for name, entry := range s.byFlow[scope.ID] {
			scope.Events[name] = cloneProviderTriggerEventCatalogEntry(entry)
		}
		out = append(out, scope)
	}
	return out
}

func (s providerTriggerEventSource) hasExactFlow(flowID string) bool {
	scope, found := s.Source.FlowScopeByID(flowID)
	return found && scope.ID == flowID
}

func (s providerTriggerEventSource) FlowScopeByID(flowID string) (semanticview.FlowScope, bool) {
	for _, scope := range s.FlowScopes() {
		if scope.ID == flowID {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}

func cloneEventCatalog(in map[string]runtimecontracts.EventCatalogEntry) map[string]runtimecontracts.EventCatalogEntry {
	out := make(map[string]runtimecontracts.EventCatalogEntry, len(in))
	for name, entry := range in {
		out[name] = cloneProviderTriggerEventCatalogEntry(entry)
	}
	return out
}

func cloneProviderTriggerEventCatalogEntry(in runtimecontracts.EventCatalogEntry) runtimecontracts.EventCatalogEntry {
	out := in
	out.Payload.Required = append([]string(nil), in.Payload.Required...)
	out.Payload.Properties = make(map[string]runtimecontracts.EventFieldSpec, len(in.Payload.Properties))
	for name, field := range in.Payload.Properties {
		field.Citation.AllowedClasses = append([]string(nil), field.Citation.AllowedClasses...)
		field.Refinements = field.Refinements.Clone()
		if field.ExactSchema != nil {
			schema := *field.ExactSchema
			field.ExactSchema = &schema
		}
		out.Payload.Properties[name] = field
	}
	return out
}
