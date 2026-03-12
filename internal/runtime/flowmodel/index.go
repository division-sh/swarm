package flowmodel

import "strings"

func IndexTree[T any](
	node *T,
	parent *T,
	parentPath string,
	tree *Tree[T],
	registry *URIRegistry,
	id func(*T) string,
	children func(*T) []*T,
	normalizeParent func(*T) *T,
	setParent func(*T, *T),
	setPath func(*T, string),
	setURI func(*T, string),
) {
	if node == nil || tree == nil {
		return
	}
	if setParent != nil {
		setParent(node, parent)
	}
	currentPath := strings.Trim(strings.TrimSpace(parentPath), "/")
	if setPath != nil {
		setPath(node, currentPath)
	}
	if setURI != nil {
		setURI(node, FullURI(registry, currentPath))
	}
	if flowID := strings.TrimSpace(id(node)); flowID != "" {
		resolvedParent := parent
		if normalizeParent != nil {
			resolvedParent = normalizeParent(parent)
		}
		if setParent != nil {
			setParent(node, resolvedParent)
		}
		if currentPath == "" {
			currentPath = flowID
		} else {
			currentPath = currentPath + "/" + flowID
		}
		if tree.ByPath == nil {
			tree.ByPath = map[string]*T{}
		}
		if tree.ByID == nil {
			tree.ByID = map[string]*T{}
		}
		if setPath != nil {
			setPath(node, currentPath)
		}
		if setURI != nil {
			setURI(node, FullURI(registry, currentPath))
		}
		tree.ByPath[currentPath] = node
		tree.ByID[flowID] = node
	}
	for _, child := range children(node) {
		IndexTree(child, node, currentPath, tree, registry, id, children, normalizeParent, setParent, setPath, setURI)
	}
}
