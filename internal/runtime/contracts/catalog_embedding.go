package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// References are retained only until all declaring catalogs are available.
type catalogEmbedding struct {
	Type   string
	Index  int
	Source yamlsource.Location
}

type expandedCatalogField struct {
	Name string
	TypeFieldSpec
	Provenance EffectiveValueProvenance
}

func admitCatalogEmbeddings(value yamlsource.Value) ([]catalogEmbedding, error) {
	if value.Presence() != yamlsource.PresenceSequence {
		return nil, fmt.Errorf("_embed at %s is a nonempty list of named types - write _embed: [Lead]", value.IntroductionLocation())
	}
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	seen := map[string]yamlsource.Location{}
	refs := make([]catalogEmbedding, 0, len(items))
	for i, item := range items {
		name, err := requiredLiteralString(item, "_embed named record")
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(name) != name {
			return nil, fmt.Errorf("_embed type %q at %s must not have surrounding whitespace", name, item.IntroductionLocation())
		}
		location := item.IntroductionLocation()
		if previous, ok := seen[name]; ok {
			return nil, fmt.Errorf("_embed repeats named record %s at %s and %s", name, previous, location)
		}
		seen[name] = location
		refs = append(refs, catalogEmbedding{Type: name, Index: i, Source: location})
	}
	return refs, nil
}

// Resolve with the shared reference owner, but copy authored field facts too:
// structural lowering alone loses descriptions and defining source locations.
func expandCatalogEmbeddings(refs []catalogEmbedding, catalog TypeCatalogDocument, own map[string]yamlsource.Location, owner string) ([]expandedCatalogField, error) {
	seen := make(map[string]string, len(own))
	for name, location := range own {
		seen[name] = fmt.Sprintf("an own field of %s at %s", owner, location)
	}
	var result []expandedCatalogField
	for _, ref := range refs {
		decl, ok := catalog.Types[ref.Type]
		if !ok || len(decl.Fields) == 0 || strings.Contains(ref.Type, "?") {
			return nil, fmt.Errorf("%s at %s is not a nonempty record; _embed takes named records", ref.Type, ref.Source)
		}
		resolved, err := (CatalogTypeReference{Type: ref.Type, Catalog: catalog}).Resolve()
		if err != nil {
			return nil, fmt.Errorf("_embed %s at %s: %w", ref.Type, ref.Source, err)
		}
		if resolved.Kind != CatalogTypeObject || resolved.Name != ref.Type {
			return nil, fmt.Errorf("%s at %s is not a named record reference; _embed takes named records", ref.Type, ref.Source)
		}
		for _, name := range sortedContractKeys(decl.Fields) {
			field := decl.Fields[name]
			definition := fmt.Sprintf("%s at %s", ref.Type, field.source)
			if previous, exists := seen[name]; exists {
				return nil, fmt.Errorf("field %s is declared by %s and again by %s; remove the duplicate, embedding never overrides", name, definition, previous)
			}
			if strings.HasPrefix(name, "_") {
				return nil, fmt.Errorf("embedded field %s at %s is metadata, not a business field", name, field.source)
			}
			seen[name] = definition
			field.Refinements = field.Refinements.Clone()
			result = append(result, expandedCatalogField{Name: name, TypeFieldSpec: field, Provenance: EffectiveValueProvenance{
				Origin: EffectiveValueOriginDerived, RuleID: "catalog.named_record_embedding/v1",
				InputPaths: []string{fmt.Sprintf("_embed[%d]", ref.Index)},
				SourceFile: field.source.File, SourceLine: field.source.Line, SourceColumn: field.source.Column,
			}})
		}
	}
	return result, nil
}

func expandScopedCatalogDeclarations(bundle *WorkflowContractBundle, views map[string]FlowContractView) error {
	for _, source := range sortedFlowSources(bundle.FlowSources) {
		catalog := bundle.ResolvedTypeCatalogForFlow(source.FlowPath)
		if err := validateFlowCatalogReferences(bundle, source); err != nil {
			return err
		}
		view := views[source.FlowPath]
		for _, name := range sortedContractKeys(view.Events) {
			entry, err := expandEventCatalogEntry(name, view.Events[name], catalog)
			if err != nil {
				return wrapLoaderDiagnosticFile(err, source.Events)
			}
			view.Events[name] = entry
		}
		views[source.FlowPath] = view
		entities := bundle.RootEntities
		if source.FlowPath != "." {
			entities = bundle.flowEntities[source.FlowPath]
		}
		for _, name := range sortedEntityContractKeys(entities) {
			entry, err := expandEntityContract(name, entities[name], catalog)
			if err != nil {
				return wrapLoaderDiagnosticFile(err, source.Entities)
			}
			entities[name] = entry
		}
	}
	return nil
}

func expandEventCatalogEntry(name string, entry EventCatalogEntry, catalog TypeCatalogDocument) (EventCatalogEntry, error) {
	entry = cloneEventCatalogEntry(entry)
	fields, err := expandCatalogEmbeddings(entry.embeds, catalog, entry.fieldSources, name)
	if err != nil {
		return EventCatalogEntry{}, err
	}
	for _, field := range fields {
		entry.Payload.Properties[field.Name] = EventFieldSpec{Type: field.Type, Description: field.Description, Refinements: field.Refinements.Clone()}
		if !field.IsOptional {
			entry.Payload.Required = append(entry.Payload.Required, field.Name)
		}
		entry.admissionProvenance["fields."+field.Name+".type"] = cloneEffectiveValueProvenance(field.Provenance)
		entry.admissionProvenance["fields."+field.Name+".is_optional"] = cloneEffectiveValueProvenance(field.Provenance)
		addExpandedFieldMetadataProvenance(entry.admissionProvenance, field)
	}
	entry.embeds = nil
	bound := cloneTypeCatalogDocument(catalog)
	entry.types = &bound
	sort.Strings(entry.Payload.Required)
	if len(fields) > 0 {
		entry.admissionProvenance["payload.required"] = EffectiveValueProvenance{
			Origin: EffectiveValueOriginDerived, RuleID: eventRequiredByDefaultRule,
			InputPaths: eventFieldTypePaths(sortedContractKeys(entry.Payload.Properties)),
		}
	}
	if err := validateEventCatalogBusinessKey(entry); err != nil {
		return EventCatalogEntry{}, err
	}
	return entry, nil
}

func expandEntityContract(name string, entry EntityContract, catalog TypeCatalogDocument) (EntityContract, error) {
	entry = cloneEntityContract(entry)
	if entry.Fields == nil {
		entry.Fields = map[string]EntityFieldDecl{}
	}
	own := make(map[string]yamlsource.Location, len(entry.Fields))
	for name, field := range entry.Fields {
		own[name] = field.source
	}
	fields, err := expandCatalogEmbeddings(entry.embeds, catalog, own, name)
	if err != nil {
		return EntityContract{}, err
	}
	if entry.admissionProvenance == nil {
		entry.admissionProvenance = map[string]EffectiveValueProvenance{}
	}
	for _, field := range fields {
		entry.Fields[field.Name] = EntityFieldDecl{Type: field.Type, IsOptional: field.IsOptional, Description: field.Description, Refinements: field.Refinements.Clone(), source: field.source}
		entry.admissionProvenance["fields."+field.Name+".type"] = cloneEffectiveValueProvenance(field.Provenance)
		entry.admissionProvenance["fields."+field.Name+".is_optional"] = cloneEffectiveValueProvenance(field.Provenance)
		addExpandedFieldMetadataProvenance(entry.admissionProvenance, field)
	}
	entry.embeds = nil
	bound := cloneTypeCatalogDocument(catalog)
	entry.types = &bound
	return entry, nil
}

func addExpandedFieldMetadataProvenance(target map[string]EffectiveValueProvenance, field expandedCatalogField) {
	for slot, present := range map[string]bool{
		"description":            field.Description != "",
		"refinements.pattern":    field.Refinements.Pattern != "",
		"refinements.equal_to":   field.Refinements.EqualTo != "",
		"refinements.length.min": field.Refinements.Length.Min != nil,
		"refinements.length.max": field.Refinements.Length.Max != nil,
		"refinements.range.min":  field.Refinements.Range.Min != nil,
		"refinements.range.max":  field.Refinements.Range.Max != nil,
	} {
		if present {
			target["fields."+field.Name+"."+slot] = cloneEffectiveValueProvenance(field.Provenance)
		}
	}
}

func eventDeclarationCatalog(entry EventCatalogEntry, fallback TypeCatalogDocument) TypeCatalogDocument {
	if entry.types != nil {
		return cloneTypeCatalogDocument(*entry.types)
	}
	return fallback
}

func cloneCatalogEmbeddingTypes(types *TypeCatalogDocument) *TypeCatalogDocument {
	if types == nil {
		return nil
	}
	copy := cloneTypeCatalogDocument(*types)
	return &copy
}

// FieldProvenance returns detached declaration evidence, never a runtime grant.
func (entry EventCatalogEntry) FieldProvenance(name string) (EffectiveValueProvenance, bool) {
	value, ok := entry.admissionProvenance["fields."+name+".type"]
	return cloneEffectiveValueProvenance(value), ok
}

func (entry EntityContract) FieldProvenance(name string) (EffectiveValueProvenance, bool) {
	value, ok := entry.admissionProvenance["fields."+name+".type"]
	if !ok {
		if field, exists := entry.Fields[name]; exists && field.source.Line > 0 {
			return EffectiveValueProvenance{Origin: EffectiveValueOriginAuthored, SourceFile: field.source.File, SourceLine: field.source.Line, SourceColumn: field.source.Column}, true
		}
	}
	return cloneEffectiveValueProvenance(value), ok
}
