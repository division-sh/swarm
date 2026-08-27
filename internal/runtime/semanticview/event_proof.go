package semanticview

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

type FlowEventProof struct {
	Node       runtimeidentity.ExecutableNode `json:"-"`
	FlowID     string
	Authored   string
	Local      string
	Canonical  string
	CatalogKey string
	Entry      runtimecontracts.EventCatalogEntry
	HasSchema  bool
}

func ResolveExecutableNodeEventProof(source Source, node runtimeidentity.ExecutableNode, eventType string) FlowEventProof {
	authored := runtimeeventidentity.Normalize(eventType)
	proof := FlowEventProof{Node: node, FlowID: node.FlowID(), Authored: authored, Local: authored, Canonical: authored}
	if source == nil || !node.Valid() || authored == "" {
		return proof
	}
	if canonical := runtimeeventidentity.Normalize(source.ResolveExecutableNodeEventReference(node, authored)); canonical != "" {
		proof.Canonical = canonical
	}
	if semanticScope, err := ResolveExecutableNodeSemanticScope(source, node); err == nil {
		if scope, ok := semanticScope.OwningFlow(); ok {
			if local := localizeExecutableNodeEventForProof(source, scope, proof.Canonical); local != "" {
				proof.Local = local
			}
		}
	}
	for _, candidate := range uniqueNormalizedProofCandidates(proof.Local, proof.Authored, proof.Canonical) {
		entry, key, ok := source.ResolveExecutableNodeEventCatalogEntry(node, candidate)
		if !ok {
			continue
		}
		proof.Entry, proof.CatalogKey, proof.HasSchema = entry, strings.TrimSpace(key), true
		break
	}
	if !proof.HasSchema {
		for _, candidate := range uniqueNormalizedProofCandidates(proof.Canonical, proof.Authored, proof.Local) {
			entry, key, ok := runtimecontracts.PlatformEventCatalogEntry(source.PlatformSpec(), candidate)
			if !ok {
				continue
			}
			proof.Entry, proof.CatalogKey, proof.Canonical, proof.Local, proof.HasSchema = entry, strings.TrimSpace(key), strings.TrimSpace(key), strings.TrimSpace(key), true
			break
		}
	}
	return proof
}

func ResolveFlowEventProof(source Source, flowID, eventType string) FlowEventProof {
	flowID = strings.TrimSpace(flowID)
	authored := runtimeeventidentity.Normalize(eventType)
	proof := FlowEventProof{
		FlowID:    flowID,
		Authored:  authored,
		Local:     authored,
		Canonical: authored,
	}
	if source == nil || authored == "" {
		return proof
	}

	if canonical := runtimeeventidentity.Normalize(source.ResolveFlowEventReference(flowID, authored)); canonical != "" {
		proof.Canonical = canonical
	}

	if local := localizeFlowEventForProof(source, flowID, proof.Canonical); local != "" {
		proof.Local = local
	}

	candidates := uniqueNormalizedProofCandidates(proof.Local, proof.Authored, proof.Canonical)
	for _, candidate := range candidates {
		entry, key, ok := source.ResolveFlowEventCatalogEntry(flowID, candidate)
		if !ok {
			continue
		}
		proof.Entry = entry
		proof.CatalogKey = strings.TrimSpace(key)
		proof.HasSchema = true
		if proof.Local == "" {
			proof.Local = proof.CatalogKey
		}
		break
	}
	if !proof.HasSchema {
		for _, candidate := range candidates {
			entry, key, ok := runtimecontracts.PlatformEventCatalogEntry(source.PlatformSpec(), candidate)
			if !ok {
				continue
			}
			proof.Entry = entry
			proof.CatalogKey = strings.TrimSpace(key)
			proof.Canonical = proof.CatalogKey
			proof.Local = proof.CatalogKey
			proof.HasSchema = true
			break
		}
	}
	return proof
}

func (p FlowEventProof) DisplayName() string {
	switch {
	case strings.TrimSpace(p.Canonical) != "":
		return strings.TrimSpace(p.Canonical)
	case strings.TrimSpace(p.Authored) != "":
		return strings.TrimSpace(p.Authored)
	default:
		return strings.TrimSpace(p.Local)
	}
}

func (p FlowEventProof) EventKey() string {
	if canonical := strings.TrimSpace(p.Canonical); canonical != "" {
		return canonical
	}
	return p.DisplayName()
}

// IsAuthored reports whether the proof's exact canonical event identity is an
// authored declaration. Concrete template-instance identities are runtime
// occurrences, not additional authored declarations.
func (p FlowEventProof) IsAuthored(source Source) bool {
	if source == nil || !p.HasSchema {
		return false
	}
	canonical := runtimeeventidentity.Normalize(p.Canonical)
	if canonical == "" {
		return false
	}
	authored := source.AuthoredResolvedEventCatalog()
	if _, ok := source.AuthoredEventEntries()[canonical]; ok {
		return true
	}
	for _, scope := range source.FlowScopes() {
		for eventType := range scope.Events {
			if _, ok := authored[runtimeeventidentity.Normalize(eventType)]; !ok {
				continue
			}
			declared := runtimeeventidentity.Normalize(source.ResolveFlowEventReference(scope.ID, eventType))
			if declared == canonical {
				return true
			}
		}
	}
	for _, scope := range source.ProjectScopes() {
		for eventType := range scope.Events {
			if _, ok := authored[runtimeeventidentity.Normalize(eventType)]; !ok {
				continue
			}
			declared := runtimeeventidentity.Normalize(eventType)
			if strings.TrimSpace(scope.OwningFlowID) != "" {
				declared = runtimeeventidentity.Normalize(source.ResolveFlowEventReference(scope.OwningFlowID, eventType))
			}
			if declared == canonical {
				return true
			}
		}
	}
	for eventType := range authored {
		if runtimeeventidentity.Normalize(eventType) == canonical {
			return true
		}
	}
	return false
}

func (p FlowEventProof) CrossesDeclaredOutputBoundary(source Source) bool {
	if source == nil {
		return false
	}
	canonical := strings.TrimSpace(p.Canonical)
	if canonical == "" {
		return false
	}
	flowID := strings.TrimSpace(p.FlowID)
	if flowID == "" {
		for _, output := range source.FlowOutputEvents("") {
			if source.ResolveFlowEventReference("", output) == canonical {
				return true
			}
		}
		return false
	}
	for _, output := range source.FlowOutputEvents(flowID) {
		if source.ResolveFlowEventReference(flowID, output) == canonical {
			return true
		}
	}
	return false
}

func localizeFlowEventForProof(source Source, flowID, canonical string) string {
	if source == nil {
		return ""
	}
	canonical = runtimeeventidentity.Normalize(canonical)
	if canonical == "" {
		return ""
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return canonical
	}
	scope, ok := FlowScopeByID(source, flowID)
	if !ok {
		return canonical
	}
	localNames := flowScopeEventNamesForProof(scope)
	if local := concreteTemplateInstanceLocalEventForProof(source, scope, canonical, localNames); local != "" {
		return local
	}
	return runtimeeventidentity.LocalizeForFlow(scope.Path, localNames, canonical)
}

func localizeExecutableNodeEventForProof(source Source, scope FlowScope, canonical string) string {
	canonical = runtimeeventidentity.Normalize(canonical)
	if canonical == "" {
		return ""
	}
	localNames := flowScopeEventNamesForProof(scope)
	if local := concreteTemplateInstanceLocalEventForProof(source, scope, canonical, localNames); local != "" {
		return local
	}
	return runtimeeventidentity.LocalizeForFlow(scope.Path, localNames, canonical)
}

func flowScopeEventNamesForProof(scope FlowScope) []string {
	localNames := make([]string, 0, len(scope.Events))
	for name := range scope.Events {
		name = runtimeeventidentity.Normalize(name)
		if name == "" {
			continue
		}
		localNames = append(localNames, name)
	}
	sort.SliceStable(localNames, func(i, j int) bool {
		if len(localNames[i]) != len(localNames[j]) {
			return len(localNames[i]) > len(localNames[j])
		}
		return localNames[i] < localNames[j]
	})
	return localNames
}

func concreteTemplateInstanceLocalEventForProof(source Source, scope FlowScope, canonical string, localNames []string) string {
	if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
		return ""
	}
	scopePath := runtimeeventidentity.Normalize(scope.Path)
	canonical = runtimeeventidentity.Normalize(canonical)
	if scopePath == "" || canonical == "" || !strings.HasPrefix(canonical, scopePath+"/") {
		return ""
	}
	remainder := strings.TrimPrefix(canonical, scopePath+"/")
	if remainder == "" || !strings.Contains(remainder, "/") {
		return ""
	}
	if eventProofRemainderTargetsDescendantScope(source, scopePath, remainder) {
		return ""
	}
	for _, local := range localNames {
		if local == "" {
			continue
		}
		if remainder == local || strings.HasSuffix(remainder, "/"+local) {
			return local
		}
	}
	return ""
}

func eventProofRemainderTargetsDescendantScope(source Source, scopePath, remainder string) bool {
	if source == nil {
		return false
	}
	scopePath = runtimeeventidentity.Normalize(scopePath)
	remainder = runtimeeventidentity.Normalize(remainder)
	if scopePath == "" || remainder == "" {
		return false
	}
	for _, descendant := range source.FlowScopes() {
		descendantPath := runtimeeventidentity.Normalize(descendant.Path)
		if descendantPath == "" || descendantPath == scopePath || !strings.HasPrefix(descendantPath, scopePath+"/") {
			continue
		}
		relativePath := strings.TrimPrefix(descendantPath, scopePath+"/")
		if relativePath != "" && (remainder == relativePath || strings.HasPrefix(remainder, relativePath+"/")) {
			return true
		}
	}
	return false
}

func uniqueNormalizedProofCandidates(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = runtimeeventidentity.Normalize(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
