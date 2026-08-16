package semanticview

import runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"

func ExecutableNodeEffectiveProduces(source Source, node runtimeidentity.ExecutableNode) []string {
	if source == nil || !node.Valid() {
		return nil
	}
	return append([]string(nil), source.ExecutableNodeEffectiveProduces(node)...)
}

func ExecutableNodeEffectiveSubscriptions(source Source, node runtimeidentity.ExecutableNode) []string {
	if source == nil || !node.Valid() {
		return nil
	}
	return append([]string(nil), source.ExecutableNodeRuntimeSubscriptions(node)...)
}
