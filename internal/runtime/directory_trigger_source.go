package runtime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func sourceWithDirectoryTriggerSchemas(source semanticview.Source, providers packadmission.ProviderSource) (semanticview.Source, error) {
	if _, base, applied := source.SemanticCapabilities().ProviderTriggerEvents(); applied {
		source = base
	}
	owners := map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner{}
	byFlow := map[string]map[string]runtimecontracts.EventCatalogEntry{}
	var evidence []map[string]any
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return nil, err
		}
		for _, definition := range scope.Providers() {
			catalog, found := definition.TriggerCatalog()
			if !found {
				continue
			}
			for _, entry := range catalog.Entries() {
				if err := addDirectoryTriggerSchemas(source, flow, entry, owners, byFlow); err != nil {
					return nil, err
				}
				evidence = append(evidence, map[string]any{
					"flow": flow, "provider": entry.Manifest.Provider(), "source": entry.SourcePath,
					"version": entry.Identity.Version, "body_hash": entry.Identity.ManifestHash,
					"catalog_generation": catalog.Generation().Diagnostic(),
				})
			}
		}
	}
	if len(owners) == 0 {
		return source, nil
	}
	encoded, err := canonicaljson.Bytes(evidence)
	if err != nil {
		return nil, fmt.Errorf("encode directory trigger schema selection: %w", err)
	}
	generation := triggergeneration.FromCanonicalBytes(encoded)
	for coordinate, owner := range owners {
		owner.generation = generation
		owners[coordinate] = owner
	}
	compiled, err := compileProviderTriggerEventSchemas(byFlow, owners)
	if err != nil {
		return nil, err
	}
	inputs, err := bindProviderTriggerInputPins(source, compiled)
	if err != nil {
		return nil, err
	}
	outputs, err := bindProviderTriggerOutputPins(source, compiled)
	if err != nil {
		return nil, err
	}
	aliases, readback, err := providerTriggerGlobalReadback(byFlow, owners)
	if err != nil {
		return nil, err
	}
	out := providerTriggerEventSource{
		Source: source, generation: generation, owners: owners, byFlow: byFlow,
		imported: aliases, compiledCatalog: readback, compiledByFlow: compiled,
		compiledInputPins: inputs, compiledOutputPins: outputs,
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		return nil, fmt.Errorf("directory trigger schemas require their compiled source")
	}
	out.connectionInputs = runtimecontracts.CompileConnectionInputs(bundle, out)
	if err := out.connectionInputs.ValidateBindings(); err != nil {
		return nil, err
	}
	return out, nil
}

func addDirectoryTriggerSchemas(source semanticview.Source, flow string, entry providertriggers.CatalogEntry, owners map[providerTriggerSchemaCoordinate]providerTriggerSchemaOwner, byFlow map[string]map[string]runtimecontracts.EventCatalogEntry) error {
	entries := entry.Manifest.EventCatalogEntries()
	for _, output := range entry.Manifest.OutputManifest() {
		if output.Kind != providertriggers.OutputKindRaw || output.EventName.Template == "" {
			continue
		}
		for _, pin := range source.FlowInputEventPins(flow) {
			if output.EventName.Accepts(pin.EventType()) {
				entries[pin.EventType()] = providertriggers.RawEventCatalogEntry()
			}
		}
	}
	for event, schema := range entries {
		kind := providertriggers.OutputKindRaw
		if _, _, normalized := providerTriggerCatalogEvent(entry, event); normalized {
			kind = providertriggers.OutputKindNormalized
		}
		owner := newProviderTriggerSchemaOwner(entry.Manifest.Provider(), event, kind, entry.Identity, triggergeneration.Generation{}, entry.SourcePath)
		if err := addProviderTriggerSchema(source, flow, event, schema, owner, owners, byFlow); err != nil {
			return err
		}
	}
	return nil
}
