package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

type FanOutEffectiveSemantics struct {
	ItemsFrom        string
	ItemsPath        paths.Path
	CollectionType   CatalogTypeReference
	ItemType         ResolvedCatalogType
	ItemAlias        string
	Identity         string
	IdentityDerived  bool
	MaxItems         int
	AuthoredMaxItems int
	MaxItemsSet      bool
}

type WorkflowFanOutSite struct {
	Source string
	Spec   *FanOutSpec
}

func HandlerFanOutSites(handler SystemNodeEventHandler) []WorkflowFanOutSite {
	out := make([]WorkflowFanOutSite, 0, 5)
	add := func(source string, spec *FanOutSpec) {
		if spec != nil {
			out = append(out, WorkflowFanOutSite{Source: strings.TrimSpace(source), Spec: spec})
		}
	}
	add("handler.fan_out", handler.FanOut)
	for idx := range handler.Rules {
		add(indexedFanOutSiteSource("handler.rules", idx, handler.Rules[idx].ID), handler.Rules[idx].FanOut)
	}
	for idx := range handler.OnComplete {
		add(indexedFanOutSiteSource("handler.on_complete", idx, handler.OnComplete[idx].ID), handler.OnComplete[idx].FanOut)
	}
	return out
}

func indexedFanOutSiteSource(scope string, index int, id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return fmt.Sprintf("%s[%s].fan_out", scope, id)
	}
	return fmt.Sprintf("%s[%d].fan_out", scope, index)
}

func (b *WorkflowContractBundle) ResolveFanOutEffectiveSemantics(flowID, eventType string, spec FanOutSpec) (FanOutEffectiveSemantics, error) {
	itemsPath, err := ValidateFanOutItemsSource(spec)
	if err != nil {
		return FanOutEffectiveSemantics{}, err
	}
	if err := ValidateFanOutAlias(spec.As); err != nil {
		return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.%w", err)
	}
	if err := ValidateFanOutMaxItems(spec); err != nil {
		return FanOutEffectiveSemantics{}, err
	}

	collectionType, err := b.resolveFanOutCollectionType(flowID, eventType, itemsPath)
	if err != nil {
		return FanOutEffectiveSemantics{}, err
	}
	itemType, err := resolveFanOutCollectionItemType(collectionType)
	if err != nil {
		return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.items_from %q %w", strings.TrimSpace(spec.ItemsFrom), err)
	}

	identity := strings.TrimSpace(spec.Identity)
	derived := false
	if identity == "" {
		if !fanOutScalarItemKind(itemType.Kind) {
			kind := strings.TrimSpace(string(itemType.Kind))
			if kind == "" {
				kind = string(CatalogTypeDynamic)
			}
			return FanOutEffectiveSemantics{}, fmt.Errorf("fan_out.identity is required because items_from %q has non-scalar or unresolved item type %s", strings.TrimSpace(spec.ItemsFrom), kind)
		}
		identity = strings.TrimSpace(spec.As)
		derived = true
	}

	return FanOutEffectiveSemantics{
		ItemsFrom:        strings.TrimSpace(spec.ItemsFrom),
		ItemsPath:        itemsPath,
		CollectionType:   collectionType,
		ItemType:         itemType,
		ItemAlias:        strings.TrimSpace(spec.As),
		Identity:         identity,
		IdentityDerived:  derived,
		MaxItems:         EffectiveFanOutMaxItems(spec),
		AuthoredMaxItems: spec.MaxItems,
		MaxItemsSet:      spec.MaxItemsSet,
	}, nil
}

func (b *WorkflowContractBundle) resolveFanOutCollectionType(flowID, eventType string, path paths.Path) (CatalogTypeReference, error) {
	if b == nil {
		return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from requires a loaded contract bundle")
	}
	field := strings.TrimSpace(path.Segments[0])
	switch path.Root {
	case paths.RootPayload:
		ref, ok := ResolveEventFieldType(b, flowID, eventType, field)
		if !ok {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references undeclared payload field %s for event %s in flow %s", field, defaultFanOutEventLabel(eventType), defaultPrimaryEntityFlowLabel(flowID))
		}
		return ref, nil
	case paths.RootEntity:
		primary, err := b.ResolveFlowPrimaryEntity(flowID)
		if err != nil {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references entity but flow %s has no primary entity contract: %w", defaultPrimaryEntityFlowLabel(flowID), err)
		}
		decl, ok := primary.Contract.Fields[field]
		if !ok {
			return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from references undeclared entity field %s", field)
		}
		return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(primary.Types)}, nil
	default:
		return CatalogTypeReference{}, fmt.Errorf("fan_out.items_from must use payload or entity scope")
	}
}

func defaultFanOutEventLabel(eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return "<unknown>"
	}
	return eventType
}

func resolveFanOutCollectionItemType(ref CatalogTypeReference) (ResolvedCatalogType, error) {
	resolved, err := ref.Resolve()
	if err == nil && resolved.Kind == CatalogTypeList && resolved.Element != nil {
		return *resolved.Element, nil
	}
	raw := strings.TrimSpace(ref.Type)
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "array<") && strings.HasSuffix(lower, ">") {
		inner := strings.TrimSpace(raw[len("array<") : len(raw)-1])
		if inner == "" {
			return ResolvedCatalogType{}, fmt.Errorf("must reference a collection with a declared item type")
		}
		return ref.ResolveReference(inner)
	}
	if lower == "array" || strings.HasPrefix(lower, "array ") || strings.HasPrefix(lower, "array(") {
		return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
	}
	if err != nil {
		return ResolvedCatalogType{}, fmt.Errorf("must reference a collection field: %v", err)
	}
	return ResolvedCatalogType{}, fmt.Errorf("must reference a list/array collection field; field has type %q", raw)
}

func fanOutScalarItemKind(kind CatalogTypeKind) bool {
	switch kind {
	case CatalogTypeText, CatalogTypeInteger, CatalogTypeNumber, CatalogTypeBoolean:
		return true
	default:
		return false
	}
}
