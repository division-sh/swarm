package contracts

import (
	"fmt"
	"strings"
)

// ResolveMapKey retains the validation domain carried by a string-like key.
func (r CatalogTypeReference) ResolveMapKey() (ResolvedCatalogType, error) {
	resolved, err := r.Resolve()
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	if err := validateCatalogMapKey(resolved, r.Type, r.Catalog); err != nil {
		return ResolvedCatalogType{}, err
	}
	return resolved, nil
}

func validateCatalogMapKey(key ResolvedCatalogType, ref string, catalog TypeCatalogDocument) error {
	_, enum := catalog.Enums[key.Name]
	if key.Kind == CatalogTypeText && (key.PlainText() || key.Format == "uuid" || enum) {
		return nil
	}
	return catalogReferenceError(fmt.Sprintf("map key type %q is unsupported; map keys must be text, enum or uuid", strings.TrimSpace(ref)))
}

// PlainText excludes the UUID, timestamp and enum domains despite their string
// representation. Alias spelling does not change the resolved domain.
func (t ResolvedCatalogType) PlainText() bool {
	return t.Kind == CatalogTypeText && t.Name == "" && t.Format == ""
}

// ComparableDomain preserves declared domains, unlike structural assignment.
func (r CatalogTypeReference) ComparableDomain() string {
	resolved, err := r.Resolve()
	if err != nil {
		return ""
	}
	return catalogComparableDomain(resolved)
}

func catalogComparableDomain(resolved ResolvedCatalogType) string {
	switch resolved.Kind {
	case CatalogTypeList:
		return "list:" + catalogComparableDomain(*resolved.Element)
	case CatalogTypeMap:
		return "map:" + catalogComparableDomain(*resolved.Key) + ":" + catalogComparableDomain(*resolved.Value)
	case CatalogTypeObject:
		return "type:" + resolved.Name
	case CatalogTypeText:
		if resolved.Name != "" {
			return "enum:" + resolved.Name
		}
		switch resolved.Format {
		case "uuid":
			return "uuid"
		case "date-time":
			return "timestamp"
		default:
			return "string"
		}
	case CatalogTypeInteger:
		return "integer"
	case CatalogTypeNumber:
		return "number"
	case CatalogTypeBoolean:
		return "boolean"
	case CatalogTypeDynamic:
		return resolved.primitive
	}
	return ""
}
