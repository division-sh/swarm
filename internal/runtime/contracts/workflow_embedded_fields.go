package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// Value-only source facts copy with their field at existing clone boundaries;
// they never participate in acceptance schemas or retained YAML ownership.
type fieldOrigin struct {
	definition yamlsource.Location
	use        yamlsource.Location
	record     string
	index      int
}

func (o fieldOrigin) embeddedSource() (int, yamlsource.Location, bool) {
	return o.index, o.definition, o.record != "" && o.definition.File != "" && o.definition.Line > 0
}

func (f EventFieldSpec) EmbeddedSource() (int, yamlsource.Location, bool) {
	return f.origin.embeddedSource()
}

func (f EntityFieldDecl) EmbeddedSource() (int, yamlsource.Location, bool) {
	return f.origin.embeddedSource()
}

func (o fieldOrigin) describe(field string) string {
	return fmt.Sprintf("%s.%s defined at %s, _embed[%d] used at %s", o.record, field, definitionLocation(o.definition), o.index, o.use)
}

func definitionLocation(location yamlsource.Location) string {
	if location.Line <= 0 {
		return "unavailable (non-authored declaration)"
	}
	return location.String()
}

// Embedded records contribute only field types and optionality, never overrides.
func embeddedRecordFields(fields []yamlsource.MappingField, types TypeCatalogDocument) (map[string]TypeFieldSpec, error) {
	out := map[string]TypeFieldSpec{}
	locations := map[string]string{}
	for _, field := range fields {
		if field.Name != "_embed" {
			locations[field.Name] = field.IntroductionLocation().String()
		}
	}
	for _, field := range fields {
		if field.Name != "_embed" {
			continue
		}
		entries, err := field.Value.Sequence()
		if err != nil || len(entries) == 0 {
			return nil, fmt.Errorf("_embed at %s requires a nonempty list of named records", field.Value.IntroductionLocation())
		}
		seen := map[string]string{}
		for index, entry := range entries {
			name, err := requiredLiteralString(entry, "embedded record")
			if err != nil {
				return nil, err
			}
			at := fmt.Sprintf("_embed[%d] at %s", index, entry.IntroductionLocation())
			if previous, exists := seen[name]; exists {
				return nil, fmt.Errorf("_embed repeats record %q at %s and %s", name, previous, at)
			}
			seen[name] = at
			record, err := resolveEmbeddedRecord(types, name, at)
			if err != nil {
				return nil, err
			}
			for _, key := range sortedContractKeys(record.Fields) {
				value := record.Fields[key]
				value.origin.record, value.origin.index, value.origin.use = name, index, entry.IntroductionLocation()
				origin := value.origin.describe(key)
				if previous, exists := locations[key]; exists {
					return nil, fmt.Errorf("embedded field %q collides at %s and %s; embedded fields cannot be overridden", key, previous, origin)
				}
				locations[key] = origin
				out[key] = value
			}
		}
	}
	return out, nil
}

func resolveEmbeddedRecord(types TypeCatalogDocument, name, at string) (NamedTypeDecl, error) {
	record, exists := types.Types[name]
	if !exists {
		return NamedTypeDecl{}, fmt.Errorf("_embed record %q at %s is not a named record in this flow's catalog", name, at)
	}
	if len(record.Fields) == 0 {
		return NamedTypeDecl{}, fmt.Errorf("_embed record %q defined at %s, used at %s, has no fields", name, definitionLocation(record.definition), at)
	}
	if _, err := (CatalogTypeReference{Type: name, Catalog: types}).Resolve(); err != nil {
		return NamedTypeDecl{}, fmt.Errorf("_embed record %q defined at %s, used at %s: %w", name, definitionLocation(record.definition), at, err)
	}
	return record, nil
}
