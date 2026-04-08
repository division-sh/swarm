package semanticview

import (
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeeventidentity "swarm/internal/runtime/core/eventidentity"
)

type FlowEventProof struct {
	FlowID     string
	Authored   string
	Local      string
	Canonical  string
	CatalogKey string
	Entry      runtimecontracts.EventCatalogEntry
	HasSchema  bool
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
		if bundle, ok := Bundle(source); ok && bundle != nil && bundle.RootSchema != nil {
			for _, output := range bundle.RootSchema.Pins.Outputs.Events {
				if source.ResolveFlowEventReference("", output) == canonical {
					return true
				}
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
	localNames := make([]string, 0, len(scope.Events))
	for name := range scope.Events {
		name = runtimeeventidentity.Normalize(name)
		if name == "" {
			continue
		}
		localNames = append(localNames, name)
	}
	return runtimeeventidentity.LocalizeForFlow(scope.Path, localNames, canonical)
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
