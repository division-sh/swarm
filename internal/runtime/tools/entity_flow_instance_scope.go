package tools

import (
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func normalizeEntityToolFlowInstance(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func entityToolExistingFlowInstanceMatches(source semanticview.Source, requested, stored string) bool {
	requested = normalizeEntityToolFlowInstance(requested)
	stored = normalizeEntityToolFlowInstance(stored)
	if requested == "" {
		return true
	}
	if stored == "" {
		return false
	}
	if requested == stored {
		return true
	}
	root, ok := entityToolDeclaredFlowScopeRoot(source, requested)
	if !ok {
		return false
	}
	return stored == root || strings.HasPrefix(stored, root+"/")
}

func entityToolDeclaredFlowScopeRoot(source semanticview.Source, requested string) (string, bool) {
	requested = normalizeEntityToolFlowInstance(requested)
	if source == nil || requested == "" {
		return "", false
	}
	for flowID := range source.FlowSchemaEntries() {
		if root := normalizeEntityToolFlowInstance(runtimeflowidentity.ScopeKey(source, flowID)); root == requested {
			return root, true
		}
	}
	return "", false
}
