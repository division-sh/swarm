package contracts

import (
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func ResolveEventFieldType(bundle *WorkflowContractBundle, flowID, eventType, field string) (CatalogTypeReference, bool) {
	field = strings.TrimSpace(field)
	if field == "" {
		return CatalogTypeReference{}, false
	}
	entry, _, catalog, ok := effectiveEventDeclarationForFlowEvent(bundle, flowID, eventType)
	if !ok {
		return CatalogTypeReference{}, false
	}
	return captureCatalogEventFieldReference(entry, catalog, field)
}

func ResolveExecutableNodeEventFieldType(bundle *WorkflowContractBundle, node runtimeidentity.ExecutableNode, eventType, field string) (CatalogTypeReference, bool) {
	field = strings.TrimSpace(field)
	if bundle == nil || !node.Valid() || field == "" {
		return CatalogTypeReference{}, false
	}
	entry, _, catalog, ok := bundle.resolveEffectiveExecutableNodeEventDeclaration(node, eventType)
	if !ok {
		entry, _, ok = PlatformEventCatalogEntry(bundle.Platform, eventType)
		catalog = bundle.RootTypeCatalog()
	}
	if !ok {
		return CatalogTypeReference{}, false
	}
	return captureCatalogEventFieldReference(entry, catalog, field)
}

func captureCatalogEventFieldReference(entry EventCatalogEntry, catalog TypeCatalogDocument, field string) (CatalogTypeReference, bool) {
	decl, ok := entry.Payload.Properties[field]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false
	}
	return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(eventDeclarationCatalog(entry, catalog))}, true
}
