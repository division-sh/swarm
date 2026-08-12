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
	imported := map[string]runtimecontracts.EventCatalogEntry{}
	owners := map[string]providerTriggerSchemaOwner{}
	byProject := map[string]map[string]runtimecontracts.EventCatalogEntry{}
	targetFree := map[string]runtimeprovideroutput.Authorization{}
	for _, pkg := range bundle.PackageTree {
		seen := map[string]struct{}{}
		for index, declaration := range pkg.Manifest.ProviderTriggerEvents.Imports {
			provider := providertriggers.NormalizeProviderName(declaration.Provider)
			eventName := strings.TrimSpace(declaration.Event)
			declarationKey := provider + "\x00" + eventName
			if _, duplicate := seen[declarationKey]; duplicate {
				return nil, fmt.Errorf("%s provider_trigger_events.imports[%d] duplicates provider %q event %q; remove the duplicate declaration", packageDeclarationLocation(pkg), index, provider, eventName)
			}
			seen[declarationKey] = struct{}{}
			if catalog == nil {
				return nil, fmt.Errorf("%s provider_trigger_events.imports[%d] requires a verified provider-trigger catalog", packageDeclarationLocation(pkg), index)
			}
			entry, exists := catalog.EntryByProvider(provider)
			if !exists {
				return nil, fmt.Errorf("%s provider_trigger_events.imports[%d] references unknown provider %q; available providers: %s", packageDeclarationLocation(pkg), index, provider, strings.Join(providerTriggerCatalogProviders(catalog), ", "))
			}
			eventEntry, exists, normalized := providerTriggerCatalogEvent(entry, eventName)
			if !exists {
				return nil, fmt.Errorf("%s provider_trigger_events.imports[%d] references unknown event %q for provider %q; normalized events: %s", packageDeclarationLocation(pkg), index, eventName, provider, strings.Join(providerTriggerNormalizedEvents(entry), ", "))
			}
			if !normalized {
				return nil, fmt.Errorf("%s provider_trigger_events.imports[%d] event %q is a raw provider event; schema-only imports accept normalized events only", packageDeclarationLocation(pkg), index, eventName)
			}
			owner := newProviderTriggerSchemaOwner(provider, eventName, providertriggers.OutputKindNormalized, entry.Identity, catalog.Generation())
			if err := addProviderTriggerSchema(source, strings.TrimSpace(pkg.Key), eventName, eventEntry, owner, imported, owners, byProject); err != nil {
				return nil, err
			}
		}
	}
	if catalog == nil {
		return source, nil
	}

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
					kind := providertriggers.OutputKindRaw
					if strings.TrimSpace(eventEntry.Source) == "provider_trigger_pack_normalized" {
						kind = providertriggers.OutputKindNormalized
					}
					owner := newProviderTriggerSchemaOwner(binding.Provider, eventName, kind, identity, catalog.Generation())
					if err := addProviderTriggerSchema(source, strings.TrimSpace(pkg.Key), eventName, eventEntry, owner, imported, owners, byProject); err != nil {
						return nil, err
					}
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
				}
			}
		}
	}
	if len(imported) == 0 {
		return source, nil
	}
	return providerTriggerEventSource{Source: source, generation: catalog.Generation(), imported: imported, owners: owners, byProject: byProject, targetFree: targetFree}, nil
}

type providerTriggerSchemaOwner struct {
	provider   string
	event      string
	kind       providertriggers.OutputKind
	identity   providertriggers.PackIdentity
	generation triggergeneration.Generation
}

func newProviderTriggerSchemaOwner(provider, event string, kind providertriggers.OutputKind, identity providertriggers.PackIdentity, generation triggergeneration.Generation) providerTriggerSchemaOwner {
	return providerTriggerSchemaOwner{
		provider: providertriggers.NormalizeProviderName(provider), event: strings.TrimSpace(event), kind: kind,
		identity: identity, generation: generation,
	}
}

func (o providerTriggerSchemaOwner) equal(other providerTriggerSchemaOwner) bool {
	return o.provider == other.provider && o.event == other.event && o.kind == other.kind &&
		o.identity.ID == other.identity.ID && o.identity.Version == other.identity.Version &&
		o.identity.ManifestHash == other.identity.ManifestHash && o.identity.Provenance == other.identity.Provenance &&
		o.generation.Equal(other.generation)
}

func (o providerTriggerSchemaOwner) diagnostic() string {
	return fmt.Sprintf(
		"trigger pack %s version=%s manifest_hash=%s provenance=%s catalog_generation=%s",
		o.identity.ID, o.identity.Version, o.identity.ManifestHash, o.identity.Provenance, o.generation.Diagnostic(),
	)
}

func addProviderTriggerSchema(
	source semanticview.Source,
	projectKey string,
	eventName string,
	eventEntry runtimecontracts.EventCatalogEntry,
	owner providerTriggerSchemaOwner,
	imported map[string]runtimecontracts.EventCatalogEntry,
	owners map[string]providerTriggerSchemaOwner,
	byProject map[string]map[string]runtimecontracts.EventCatalogEntry,
) error {
	eventName = strings.TrimSpace(eventName)
	if existingOwner, duplicate := owners[eventName]; duplicate {
		if !existingOwner.equal(owner) {
			return fmt.Errorf("provider trigger event %q collision between %s and %s; remove one provider-trigger declaration", eventName, existingOwner.diagnostic(), owner.diagnostic())
		}
	} else {
		if existing, collision := source.EventEntry(eventName); collision {
			existingOwner := strings.TrimSpace(existing.Source)
			if existingOwner == "" {
				existingOwner = "authored event catalog"
			}
			return fmt.Errorf("provider trigger event %q collision between %s and %s; remove the local redeclaration and inspect the pack with `swarm describe pack %s`", eventName, existingOwner, owner.diagnostic(), owner.identity.ID)
		}
		imported[eventName] = cloneProviderTriggerEventCatalogEntry(eventEntry)
		owners[eventName] = owner
	}
	projectKey = strings.TrimSpace(projectKey)
	if byProject[projectKey] == nil {
		byProject[projectKey] = map[string]runtimecontracts.EventCatalogEntry{}
	}
	byProject[projectKey][eventName] = cloneProviderTriggerEventCatalogEntry(eventEntry)
	return nil
}

func packageDeclarationLocation(pkg runtimecontracts.LoadedProjectPackage) string {
	if path := strings.TrimSpace(pkg.Paths.PackageFile); path != "" {
		return path
	}
	if key := strings.TrimSpace(pkg.Key); key != "" {
		return "package " + key
	}
	return "package.yaml"
}

func providerTriggerCatalogProviders(catalog *providertriggers.CatalogSnapshot) []string {
	if catalog == nil {
		return nil
	}
	providers := make([]string, 0, len(catalog.Entries()))
	for _, entry := range catalog.Entries() {
		providers = append(providers, providertriggers.NormalizeProviderName(entry.Manifest.Provider))
	}
	sort.Strings(providers)
	return providers
}

func providerTriggerNormalizedEvents(entry providertriggers.CatalogEntry) []string {
	events := make([]string, 0, len(entry.Manifest.NormalizedEvents))
	for _, declaration := range entry.Manifest.NormalizedEvents {
		if name := strings.TrimSpace(declaration.Event); name != "" {
			events = append(events, name)
		}
	}
	sort.Strings(events)
	return events
}

func providerTriggerCatalogEvent(entry providertriggers.CatalogEntry, eventName string) (runtimecontracts.EventCatalogEntry, bool, bool) {
	eventName = strings.TrimSpace(eventName)
	entries := entry.Manifest.EventCatalogEntries()
	eventEntry, exists := entries[eventName]
	if !exists {
		return runtimecontracts.EventCatalogEntry{}, false, false
	}
	for _, normalized := range entry.Manifest.NormalizedEvents {
		if strings.TrimSpace(normalized.Event) == eventName {
			return eventEntry, true, true
		}
	}
	return eventEntry, true, false
}

type providerTriggerEventSource struct {
	semanticview.Source
	generation triggergeneration.Generation
	imported   map[string]runtimecontracts.EventCatalogEntry
	owners     map[string]providerTriggerSchemaOwner
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
	capabilities := s.Source.SemanticCapabilities().WithProviderTriggerEvents(s.Source, s.generation, out)
	return capabilities.WithProviderTriggerEventProvenance(s.provenanceReadback())
}

func (s providerTriggerEventSource) provenanceReadback() []semanticview.ProviderTriggerEventProvenance {
	names := make([]string, 0, len(s.owners))
	for name := range s.owners {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]semanticview.ProviderTriggerEventProvenance, 0, len(names))
	for _, name := range names {
		owner := s.owners[name]
		scopes := make([]string, 0)
		for projectKey, events := range s.byProject {
			if _, ok := events[name]; ok {
				scopes = append(scopes, projectKey)
			}
		}
		sort.Strings(scopes)
		out = append(out, semanticview.ProviderTriggerEventProvenance{
			Provider: owner.provider, Event: owner.event, Kind: string(owner.kind),
			PackID: owner.identity.ID, PackVersion: owner.identity.Version,
			ManifestHash: owner.identity.ManifestHash, SourceProvenance: owner.identity.Provenance,
			Generation: owner.generation, ProjectScopes: scopes,
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
