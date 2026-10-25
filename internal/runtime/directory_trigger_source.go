package runtime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func sourceWithDirectoryTriggerSchemas(source semanticview.Source, providers packadmission.ProviderSource) (semanticview.Source, error) {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return nil, fmt.Errorf("directory trigger schemas require their compiled source")
	}
	schemaSource := semanticview.Wrap(bundle)
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
				if err := addDirectoryTriggerSchemas(schemaSource, flow, entry, owners, byFlow); err != nil {
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
	outputs, err := bindProviderTriggerOutputPins(schemaSource, compiled)
	if err != nil {
		return nil, err
	}
	inputs, err := bundle.BindConnectedProviderInputPins(outputs)
	if err != nil {
		return nil, err
	}
	inputs, err = bindProviderTriggerInputPins(schemaSource, compiled, inputs)
	if err != nil {
		return nil, err
	}
	aliases, readback, err := providerTriggerGlobalReadback(byFlow, owners)
	if err != nil {
		return nil, err
	}
	authorizations := map[string][]runtimeprovideroutput.Authorization{}
	scopeGenerations := map[string]triggergeneration.Generation{}
	ingressEvents := map[string][]string{}
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return nil, err
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			return nil, err
		}
		var inputEvents []string
		for _, pin := range schemaSource.FlowInputEventPins(flow) {
			inputEvents = append(inputEvents, pin.EventType())
		}
		for _, definition := range scope.ConsumedTriggerProviders(inputEvents) {
			entry, found := catalog.EntryByProvider(definition.Provider().Key())
			if !found {
				return nil, fmt.Errorf("flow %q consumed provider %q is absent from its exact captured catalog", flow, definition.Provider().Key())
			}
			scopeGenerations[flow] = catalog.Generation()
			for _, pin := range schemaSource.FlowInputEventPins(flow) {
				for _, output := range entry.Manifest.OutputManifest() {
					if output.Kind == providertriggers.OutputKindRaw {
						if output.EventName.Accepts(pin.EventType()) {
							ingressEvents[flow] = append(ingressEvents[flow], pin.EventType())
						}
						continue
					}
					if output.Event != pin.EventType() {
						continue
					}
					authorization, err := runtimeprovideroutput.NewAuthorization(entry.Manifest.Provider(), output.Event, entry.Identity.ID, entry.Identity.Version, entry.Identity.ManifestHash, catalog.Generation())
					if err != nil {
						return nil, err
					}
					authorizations[flow] = append(authorizations[flow], authorization)
					ingressEvents[flow] = append(ingressEvents[flow], output.Event)
				}
			}
		}
	}
	out := providerTriggerEventSource{
		Source: source, generation: generation, owners: owners, byFlow: byFlow,
		imported: aliases, compiledCatalog: readback, compiledByFlow: compiled,
		compiledInputPins: inputs, compiledOutputPins: outputs,
		outputs: authorizations, ingressEvents: ingressEvents, scopeGenerations: scopeGenerations,
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
