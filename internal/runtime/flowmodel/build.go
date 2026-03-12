package flowmodel

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func AssemblePackageTree[T any, P any, F any](
	packages []P,
	packageKey func(P) string,
	packageParentKey func(P) string,
	packageDir func(P) string,
	packageFlows func(P) []F,
	flowID func(F) string,
	flowRef func(F) string,
	flowDir func(F) string,
	packageNode func(P) *BuildNode[T],
	flowNode func(F) *BuildNode[T],
) (*BuildNode[T], error) {
	packageNodes := map[string]*BuildNode[T]{}
	packagesByKey := map[string]P{}
	packageFlowNodes := map[string]map[string]*BuildNode[T]{}
	packageFlowOrder := map[string][]*BuildNode[T]{}

	for _, pkg := range packages {
		key := strings.TrimSpace(packageKey(pkg))
		if key == "" {
			continue
		}
		packagesByKey[key] = pkg
		if node := packageNode(pkg); node != nil {
			packageNodes[key] = node
		}
	}

	for _, pkg := range packages {
		key := strings.TrimSpace(packageKey(pkg))
		pkgNode := packageNodes[key]
		if key == "" || pkgNode == nil {
			continue
		}
		for _, flow := range packageFlows(pkg) {
			child := flowNode(flow)
			if child == nil {
				continue
			}
			pkgNode.Children = append(pkgNode.Children, child)
			if packageFlowNodes[key] == nil {
				packageFlowNodes[key] = map[string]*BuildNode[T]{}
			}
			if id := strings.TrimSpace(flowID(flow)); id != "" {
				packageFlowNodes[key][id] = child
			}
			if ref := strings.TrimSpace(flowRef(flow)); ref != "" {
				packageFlowNodes[key][ref] = child
			}
			packageFlowOrder[key] = append(packageFlowOrder[key], child)
		}
	}

	var root *BuildNode[T]
	for _, pkg := range packages {
		key := strings.TrimSpace(packageKey(pkg))
		pkgNode := packageNodes[key]
		if key == "" || pkgNode == nil {
			continue
		}
		parent := strings.TrimSpace(packageParentKey(pkg))
		if parent == "" {
			if root == nil {
				root = pkgNode
			}
			continue
		}
		parentPkg, ok := packagesByKey[parent]
		if !ok {
			return nil, fmt.Errorf("flow tree build missing parent package %q for %q", parent, key)
		}
		parentNode := ResolvePackageParentNode(
			parentPkg,
			pkg,
			packageNodes[parent],
			packageFlowNodes[parent],
			packageFlowOrder[parent],
			packageDir,
			packageFlows,
			flowDir,
			flowID,
		)
		if parentNode == nil {
			return nil, fmt.Errorf("flow tree build missing parent node for package %q", key)
		}
		parentNode.Children = append(parentNode.Children, pkgNode)
	}
	if root == nil {
		return nil, fmt.Errorf("flow tree build found no root package")
	}
	return root, nil
}

func ResolvePackageParentNode[T any, P any, F any](
	parentPkg P,
	childPkg P,
	parentPackageNode *BuildNode[T],
	parentFlowNodes map[string]*BuildNode[T],
	orderedParentFlows []*BuildNode[T],
	packageDir func(P) string,
	packageFlows func(P) []F,
	flowDir func(F) string,
	flowID func(F) string,
) *BuildNode[T] {
	if parentPackageNode == nil {
		return nil
	}
	childDir := filepath.Clean(strings.TrimSpace(packageDir(childPkg)))
	for _, flow := range packageFlows(parentPkg) {
		dir := filepath.Clean(strings.TrimSpace(flowDir(flow)))
		if dir == "." || dir == "" {
			continue
		}
		if childDir == dir || strings.HasPrefix(childDir, dir+string(os.PathSeparator)) {
			if node := parentFlowNodes[strings.TrimSpace(flowID(flow))]; node != nil {
				return node
			}
		}
	}
	if len(orderedParentFlows) == 1 {
		return orderedParentFlows[0]
	}
	return parentPackageNode
}
