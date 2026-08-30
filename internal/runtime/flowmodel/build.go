package flowmodel

import (
	"sort"
)

type BuildNode[T any] struct {
	View     T
	Children []*BuildNode[T]
}

func Materialize[T any](node *BuildNode[T], reset func(*T, int), addChild func(*T, T)) T {
	var zero T
	if node == nil {
		return zero
	}
	out := node.View
	if reset != nil {
		reset(&out, len(node.Children))
	}
	for _, child := range node.Children {
		if addChild == nil {
			continue
		}
		addChild(&out, Materialize(child, reset, addChild))
	}
	return out
}

func IndexAndPopulateScopedURIs[T any, N any, A any, E any](
	root *T,
	tree *Tree[T],
	registry *URIRegistry,
	id func(*T) string,
	children func(*T) []*T,
	normalizeParent func(*T) *T,
	setParent func(*T, *T),
	setPath func(*T, string),
	setURI func(*T, string),
	flowID func(*T) string,
	flowPath func(*T) string,
	nodeEntries func(*T) map[string]N,
	agentEntries func(*T) map[string]A,
	eventEntries func(*T) map[string]E,
	nodeURIs func(*T) *map[string]string,
	agentURIs func(*T) *map[string]string,
	eventURIs func(*T) *map[string]string,
) {
	if root == nil || tree == nil {
		return
	}
	IndexTree(root, nil, "", tree, registry, id, children, normalizeParent, setParent, setPath, setURI)
	Walk(root, children, func(node *T) {
		PopulateScopedURIs(
			node,
			registry,
			flowID,
			flowPath,
			nodeEntries,
			agentEntries,
			eventEntries,
			nodeURIs,
			agentURIs,
			eventURIs,
		)
	})
}

func PopulateScopedURIs[T any, N any, A any, E any](
	node *T,
	registry *URIRegistry,
	flowID func(*T) string,
	flowPath func(*T) string,
	nodeEntries func(*T) map[string]N,
	agentEntries func(*T) map[string]A,
	eventEntries func(*T) map[string]E,
	nodeURIs func(*T) *map[string]string,
	agentURIs func(*T) *map[string]string,
	eventURIs func(*T) *map[string]string,
) {
	if node == nil || registry == nil {
		return
	}
	if target := nodeURIs(node); target != nil && *target == nil {
		*target = map[string]string{}
	}
	if target := agentURIs(node); target != nil && *target == nil {
		*target = map[string]string{}
	}
	if target := eventURIs(node); target != nil && *target == nil {
		*target = map[string]string{}
	}
	currentFlowID := flowID(node)
	currentFlowPath := flowPath(node)
	if target := nodeURIs(node); target != nil {
		for _, id := range sortedKeys(nodeEntries(node)) {
			RegisterURI(registry, target, "node", currentFlowID, currentFlowPath, id)
		}
	}
	if target := agentURIs(node); target != nil {
		for _, id := range sortedKeys(agentEntries(node)) {
			RegisterURI(registry, target, "agent", currentFlowID, currentFlowPath, id)
		}
	}
	if target := eventURIs(node); target != nil {
		for _, id := range sortedKeys(eventEntries(node)) {
			RegisterURI(registry, target, "event", currentFlowID, currentFlowPath, id)
		}
	}
}

func sortedKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
