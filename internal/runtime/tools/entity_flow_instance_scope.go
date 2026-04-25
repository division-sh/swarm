package tools

import (
	"fmt"
	"strings"

	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/semanticview"
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

func appendEntityToolExistingFlowInstanceFilter(source semanticview.Source, clauses []string, args []any, requested string) ([]string, []any) {
	requested = normalizeEntityToolFlowInstance(requested)
	if requested == "" {
		return clauses, args
	}
	if root, ok := entityToolDeclaredFlowScopeRoot(source, requested); ok {
		args = append(args, root, root+"/%")
		clauses = append(clauses, fmt.Sprintf("(flow_instance = $%d OR flow_instance LIKE $%d)", len(args)-1, len(args)))
		return clauses, args
	}
	args = append(args, requested)
	clauses = append(clauses, fmt.Sprintf("flow_instance = $%d", len(args)))
	return clauses, args
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
