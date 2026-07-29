package runtime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func SourceWithProviderTriggerEvents(source semanticview.Source, catalog *providertriggers.CatalogSnapshot) (semanticview.Source, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required")
	}
	if generation, base, applied := source.SemanticCapabilities().ProviderTriggerEvents(); applied {
		if catalog != nil && generation.Equal(catalog.Generation()) {
			return source, nil
		}
		source = base
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("provider trigger event import requires a bundle-backed semantic source")
	}
	if catalog == nil {
		return source, nil
	}

	imported := map[string]runtimecontracts.EventCatalogEntry{}
	owners := map[string]string{}
	byProject := map[string]map[string]runtimecontracts.EventCatalogEntry{}
	targetFree := map[string]runtimeprovideroutput.Authorization{}
	for _, pkg := range bundle.PackageTree {
		for _, ref := range pkg.Manifest.Flows {
			if ref.Ingress == nil {
				continue
			}
			alias := strings.TrimSpace(ref.Ingress.Alias)
			if alias == "" {
				alias = strings.TrimSpace(ref.ID)
			}
			for _, binding := range ref.Ingress.Providers {
				plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
					Alias: alias, Provider: binding.Provider, SigningSecret: binding.SigningSecret,
					Declaration: providerAdmissionDeclaration(binding.Admission),
				})
				if err != nil {
					return nil, fmt.Errorf("%s: %w", standingDeclarationLocation(pkg, ref.ID), err)
				}
				identity, packBacked := plan.PackIdentity()
				if !packBacked {
					continue
				}
				entry, exists := catalog.EntryByID(identity.ID)
				if !exists {
					return nil, fmt.Errorf("effective ingress pack %q disappeared from verified catalog generation %s", identity.ID, catalog.Generation().Diagnostic())
				}
				entries := entry.Manifest.EventCatalogEntries()
				for _, output := range plan.Outputs() {
					if output.Kind != providertriggers.OutputKindRaw || strings.TrimSpace(output.EventName.Template) == "" {
						continue
					}
					for _, pin := range source.FlowInputEventPins(strings.TrimSpace(ref.ID)) {
						name := strings.TrimSpace(pin.EventType())
						if output.EventName.Accepts(name) {
							entries[name] = providertriggers.RawEventCatalogEntry()
						}
					}
				}
				for eventName, eventEntry := range entries {
					eventName = strings.TrimSpace(eventName)
					owner := fmt.Sprintf("trigger pack %s version=%s manifest_hash=%s", identity.ID, identity.Version, identity.ManifestHash)
					if existingOwner, duplicate := owners[eventName]; duplicate {
						if existingOwner != owner {
							return nil, fmt.Errorf("provider trigger event %q collision between %s and %s; remove one ingress binding", eventName, existingOwner, owner)
						}
						continue
					}
					if existing, collision := source.EventEntry(eventName); collision {
						existingOwner := strings.TrimSpace(existing.Source)
						if existingOwner == "" {
							existingOwner = "authored event catalog"
						}
						return nil, fmt.Errorf("provider trigger event %q collision between %s and %s; remove the local redeclaration and inspect the pack with `swarm describe pack %s`", eventName, existingOwner, owner, identity.ID)
					}
					imported[eventName] = eventEntry
					owners[eventName] = owner
					if strings.TrimSpace(eventEntry.Source) == "provider_trigger_pack_normalized" {
						authorization, err := runtimeprovideroutput.NewAuthorization(
							providertriggers.NormalizeProviderName(binding.Provider),
							eventName,
							identity.ID,
							identity.Version,
							identity.ManifestHash,
							catalog.Generation(),
						)
						if err != nil {
							return nil, fmt.Errorf("admit provider trigger output %q: %w", eventName, err)
						}
						targetFree[eventName] = authorization
					}
					if byProject[strings.TrimSpace(pkg.Key)] == nil {
						byProject[strings.TrimSpace(pkg.Key)] = map[string]runtimecontracts.EventCatalogEntry{}
					}
					byProject[strings.TrimSpace(pkg.Key)][eventName] = eventEntry
				}
			}
		}
	}
	if len(imported) == 0 {
		return source, nil
	}
	return providerTriggerEventSource{Source: source, generation: catalog.Generation(), imported: imported, owners: owners, byProject: byProject, targetFree: targetFree}, nil
}

type providerTriggerEventSource struct {
	semanticview.Source
	generation triggergeneration.Generation
	imported   map[string]runtimecontracts.EventCatalogEntry
	owners     map[string]string
	byProject  map[string]map[string]runtimecontracts.EventCatalogEntry
	targetFree map[string]runtimeprovideroutput.Authorization
}

func (s providerTriggerEventSource) SemanticCapabilities() semanticview.Capabilities {
	names := make([]string, 0, len(s.targetFree))
	for name := range s.targetFree {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]runtimeprovideroutput.Authorization, 0, len(names))
	for _, name := range names {
		out = append(out, s.targetFree[name])
	}
	return s.Source.SemanticCapabilities().WithProviderTriggerEvents(s.Source, s.generation, out)
}

func (s providerTriggerEventSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	out := cloneEventCatalog(s.Source.ResolvedEventCatalog())
	for name, entry := range s.imported {
		out[name] = cloneProviderTriggerEventCatalogEntry(entry)
	}
	return out
}

func (s providerTriggerEventSource) ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	if entry, resolved, ok := s.Source.ResolveFlowEventCatalogEntry(flowID, eventType); ok {
		return entry, resolved, true
	}
	eventType = strings.TrimSpace(eventType)
	entry, ok := s.imported[eventType]
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

func (s providerTriggerEventSource) ProjectScopes() []semanticview.ProjectScope {
	scopes := s.Source.ProjectScopes()
	out := make([]semanticview.ProjectScope, 0, len(scopes))
	for _, scope := range scopes {
		scope.Events = cloneEventCatalog(scope.Events)
		for name, entry := range s.byProject[strings.TrimSpace(scope.Key)] {
			scope.Events[name] = cloneProviderTriggerEventCatalogEntry(entry)
		}
		out = append(out, scope)
	}
	return out
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
	out.Swarm.Producer = append([]string(nil), in.Swarm.Producer...)
	out.Swarm.Consumer = append([]string(nil), in.Swarm.Consumer...)
	out.Producer = append([]string(nil), in.Producer...)
	out.AlternateEmitters = append([]string(nil), in.AlternateEmitters...)
	out.Consumer = append([]string(nil), in.Consumer...)
	out.ConsumerType = append([]string(nil), in.ConsumerType...)
	out.Required = append([]string(nil), in.Required...)
	out.Payload.Required = append([]string(nil), in.Payload.Required...)
	out.Payload.Properties = make(map[string]runtimecontracts.EventFieldSpec, len(in.Payload.Properties))
	for name, field := range in.Payload.Properties {
		field.Citation.AllowedClasses = append([]string(nil), field.Citation.AllowedClasses...)
		field.Refinements.Length.Min = cloneProviderTriggerInt(field.Refinements.Length.Min)
		field.Refinements.Length.Max = cloneProviderTriggerInt(field.Refinements.Length.Max)
		field.Refinements.Range.Min = cloneProviderTriggerFloat(field.Refinements.Range.Min)
		field.Refinements.Range.Max = cloneProviderTriggerFloat(field.Refinements.Range.Max)
		if field.ExactSchema != nil {
			schema := *field.ExactSchema
			field.ExactSchema = &schema
		}
		out.Payload.Properties[name] = field
	}
	return out
}

func cloneProviderTriggerInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneProviderTriggerFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
