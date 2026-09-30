package contracts

import (
	"fmt"
	"reflect"
	"sort"
)

type CollectionProjectionKind string

const (
	CollectionListItems   CollectionProjectionKind = "list_items"
	CollectionMapTextKeys CollectionProjectionKind = "map_text_keys"
)

// CollectionProjection is admitted from a catalog, never from a runtime JSON
// delimiter. Map values remain source data; only the exact text keys are items.
type CollectionProjection struct {
	Kind CollectionProjectionKind `json:"kind"`
	Type ResolvedCatalogType      `json:"type"`
}

func AdmitCollectionProjection(ref CatalogTypeReference) (CollectionProjection, error) {
	resolved, err := ref.Resolve()
	if err != nil {
		return CollectionProjection{}, err
	}
	projection := CollectionProjection{Type: resolved.Clone()}
	switch resolved.Kind {
	case CatalogTypeList:
		projection.Kind = CollectionListItems
	case CatalogTypeMap:
		projection.Kind = CollectionMapTextKeys
	default:
		return CollectionProjection{}, fmt.Errorf("collection must declare list items or map[text]T keys, got %s", resolved.Kind)
	}
	if err := projection.Validate(); err != nil {
		return CollectionProjection{}, err
	}
	return projection, nil
}

func (p CollectionProjection) Validate() error {
	switch p.Kind {
	case CollectionListItems:
		if p.Type.Kind == CatalogTypeList && p.Type.Element != nil {
			return nil
		}
	case CollectionMapTextKeys:
		if p.Type.Kind == CatalogTypeMap && p.Type.Key != nil && p.Type.Key.Kind == CatalogTypeText && p.Type.Value != nil {
			return nil
		}
	}
	return fmt.Errorf("invalid admitted collection projection %q for type %s (maps require declared text keys)", p.Kind, p.Type.Kind)
}

func (p CollectionProjection) Clone() CollectionProjection {
	p.Type = p.Type.Clone()
	return p
}

func (p CollectionProjection) ItemType() ResolvedCatalogType {
	if p.Kind == CollectionListItems && p.Type.Element != nil {
		return p.Type.Element.Clone()
	}
	if p.Kind == CollectionMapTextKeys && p.Type.Key != nil {
		return p.Type.Key.Clone()
	}
	return ResolvedCatalogType{}
}

// Project preserves list multiplicity and orders map keys lexically without
// trimming, coercion, or treating a record as an admitted map.
func (p CollectionProjection) Project(value any) ([]any, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("collection source is null or absent")
	}
	raw := reflect.ValueOf(value)
	switch p.Kind {
	case CollectionListItems:
		if (raw.Kind() != reflect.Slice && raw.Kind() != reflect.Array) || (raw.Kind() == reflect.Slice && raw.IsNil()) {
			return nil, fmt.Errorf("list-items source is %T, want non-null list", value)
		}
		items := make([]any, raw.Len())
		for i := range items {
			items[i] = raw.Index(i).Interface()
		}
		return items, nil
	case CollectionMapTextKeys:
		if raw.Kind() != reflect.Map || raw.Type().Key().Kind() != reflect.String || raw.IsNil() {
			return nil, fmt.Errorf("map-text-keys source is %T, want non-null text-keyed map", value)
		}
		keys := make([]string, 0, raw.Len())
		for _, key := range raw.MapKeys() {
			keys = append(keys, key.String())
		}
		sort.Strings(keys)
		items := make([]any, len(keys))
		for i, key := range keys {
			items[i] = key
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported collection projection %q", p.Kind)
	}
}
