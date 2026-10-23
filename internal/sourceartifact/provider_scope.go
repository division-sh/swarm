package sourceartifact

import (
	"fmt"
	"sort"
	"strings"
)

// ProviderDirectoriesInScope selects whole directories before any role lookup.
// A nearer directory shadows every role of the same ancestor key.
func (a *AdmittedSourceArtifact) ProviderDirectoriesInScope(flowPath string) ([]ProviderDirectory, error) {
	ancestors, err := a.providerScopeAncestors(flowPath)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]ProviderDirectory)
	for index := len(ancestors) - 1; index >= 0; index-- {
		for _, directory := range ancestors[index].providers {
			key, err := a.ProviderKey(directory)
			if err != nil {
				return nil, err
			}
			if _, nearer := selected[key]; !nearer {
				selected[key] = a.providers[directory]
			}
		}
	}
	keys := make([]string, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	providers := make([]ProviderDirectory, 0, len(keys))
	for _, key := range keys {
		providers = append(providers, selected[key])
	}
	return providers, nil
}

func (a *AdmittedSourceArtifact) providerScopeAncestors(flowPath string) ([]*FlowNode, error) {
	if a == nil || a.root == nil {
		return nil, fmt.Errorf("provider scope requires an admitted flow tree")
	}
	ancestors := []*FlowNode{a.root}
	if flowPath == "." {
		return ancestors, nil
	}
	segments := strings.Split(flowPath, "/")
	current := a.root
	for index, segment := range segments {
		if err := ValidateFlowSegment(segment); err != nil {
			return nil, fmt.Errorf("provider scope requires an exact admitted flow path %q: %w", flowPath, err)
		}
		wanted := strings.Join(segments[:index+1], "/")
		var next *FlowNode
		for _, child := range current.children {
			if child.flowPath == wanted {
				next = child
				break
			}
		}
		if next == nil {
			return nil, fmt.Errorf("provider scope flow %q is not in the admitted flow tree", flowPath)
		}
		ancestors = append(ancestors, next)
		current = next
	}
	return ancestors, nil
}
