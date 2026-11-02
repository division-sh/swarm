package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// Embedded records contribute only field types and optionality. Locations name
// the actual authored use, so a collision cannot silently choose a winner.
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
		for _, entry := range entries {
			name, err := requiredLiteralString(entry, "embedded record")
			if err != nil {
				return nil, err
			}
			at := entry.IntroductionLocation().String()
			if previous, exists := seen[name]; exists {
				return nil, fmt.Errorf("_embed repeats record %q at %s and %s", name, previous, at)
			}
			seen[name] = at
			record, exists := types.Types[name]
			if !exists {
				return nil, fmt.Errorf("_embed record %q at %s is not a named record in this flow's catalog", name, at)
			}
			for _, key := range sortedContractKeys(record.Fields) {
				origin := fmt.Sprintf("%s (_embed %s.%s)", at, name, key)
				if previous, exists := locations[key]; exists {
					return nil, fmt.Errorf("embedded field %q collides at %s and %s; embedded fields cannot be overridden", key, previous, origin)
				}
				locations[key] = origin
				value := record.Fields[key]
				out[key] = TypeFieldSpec{Type: value.Type, IsOptional: value.IsOptional}
			}
		}
	}
	return out, nil
}
