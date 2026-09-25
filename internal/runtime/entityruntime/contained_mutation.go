package entityruntime

import (
	"fmt"
	"strings"
)

func applyContainedOperationToMetadata(metadata map[string]any, target ContainedOperationTarget, op, key string, hasIndex bool, index int, value any) error {
	if target.MapScoped {
		return applyMapScopedOperation(metadata, target, op, key, hasIndex, index, value)
	}
	return applyListScopedOperation(metadata, target, op, hasIndex, index, value)
}

func applyMapScopedOperation(metadata map[string]any, target ContainedOperationTarget, op, key string, hasIndex bool, index int, value any) error {
	raw, present := metadata[target.RootField]
	root, ok := raw.(map[string]any)
	if present && (!ok || root == nil) {
		return fmt.Errorf("map field %s is present but not a map", target.RootField)
	}
	if !present {
		if op != ContainedOperationSet || len(target.MapValuePath) != 0 {
			return fmt.Errorf("map field %s is missing or not a map", target.RootField)
		}
		root = map[string]any{}
		metadata[target.RootField] = root
	}

	switch op {
	case ContainedOperationSet:
		if len(target.MapValuePath) == 0 {
			root[key] = value
			return nil
		}
		entry, ok := root[key].(map[string]any)
		if !ok || entry == nil {
			return fmt.Errorf("map key %q does not exist", key)
		}
		parent, leaf, err := mutationParent(entry, target.MapValuePath)
		if err != nil {
			return err
		}
		parent[leaf] = value
		return nil
	case ContainedOperationMerge:
		targetMap, err := mapScopedObjectTarget(root, key, target.MapValuePath)
		if err != nil {
			return err
		}
		for mergeKey, mergeValue := range value.(map[string]any) {
			targetMap[mergeKey] = mergeValue
		}
		return nil
	case ContainedOperationDelete:
		if _, exists := root[key]; !exists {
			return fmt.Errorf("map key %q does not exist", key)
		}
		delete(root, key)
		return nil
	case ContainedOperationAppend, ContainedOperationUpdate:
		list, setter, err := mapScopedListTarget(root, key, target.MapValuePath)
		if err != nil {
			return err
		}
		next, err := applyListMutation(list, op, hasIndex, index, value)
		if err != nil {
			return err
		}
		setter(next)
		return nil
	default:
		return fmt.Errorf("unsupported contained operation %q", op)
	}
}

func applyListScopedOperation(metadata map[string]any, target ContainedOperationTarget, op string, hasIndex bool, index int, value any) error {
	path := strings.Split(target.Path, ".")
	list, setter, err := nestedListTarget(metadata, path, op == ContainedOperationAppend)
	if err != nil {
		return err
	}
	next, err := applyListMutation(list, op, hasIndex, index, value)
	if err != nil {
		return err
	}
	setter(next)
	return nil
}

func applyListMutation(list []any, op string, hasIndex bool, index int, value any) ([]any, error) {
	switch op {
	case ContainedOperationAppend:
		return append(list, value), nil
	case ContainedOperationUpdate:
		if !hasIndex {
			return nil, fmt.Errorf("op update requires index")
		}
		if index < 0 || index >= len(list) {
			return nil, fmt.Errorf("list index %d is missing", index)
		}
		next := append([]any(nil), list...)
		next[index] = value
		return next, nil
	default:
		return nil, fmt.Errorf("op %s is not a list operation", op)
	}
}

func mapScopedObjectTarget(root map[string]any, key string, path []string) (map[string]any, error) {
	entry, ok := root[key].(map[string]any)
	if !ok || entry == nil {
		return nil, fmt.Errorf("map key %q does not exist", key)
	}
	if len(path) == 0 {
		return entry, nil
	}
	current := entry
	for _, segment := range path {
		segment = strings.TrimSpace(segment)
		next, ok := current[segment].(map[string]any)
		if !ok || next == nil {
			return nil, fmt.Errorf("map key %q path %s is missing or not an object", key, strings.Join(path, "."))
		}
		current = next
	}
	return current, nil
}

func mapScopedListTarget(root map[string]any, key string, path []string) ([]any, func([]any), error) {
	if len(path) == 0 {
		list, ok := root[key].([]any)
		if !ok {
			return nil, nil, fmt.Errorf("map key %q is missing or not a list", key)
		}
		return list, func(next []any) { root[key] = next }, nil
	}
	entry, ok := root[key].(map[string]any)
	if !ok || entry == nil {
		return nil, nil, fmt.Errorf("map key %q does not exist", key)
	}
	return nestedListTarget(entry, path, false)
}

func nestedListTarget(root map[string]any, path []string, createLeaf bool) ([]any, func([]any), error) {
	if len(path) == 0 {
		return nil, nil, fmt.Errorf("list path is required")
	}
	if len(path) == 1 {
		leaf := strings.TrimSpace(path[0])
		raw, ok := root[leaf]
		if !ok && createLeaf {
			list := []any{}
			root[leaf] = list
			return list, func(next []any) { root[leaf] = next }, nil
		}
		list, ok := raw.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("path %s is missing or not a list", strings.Join(path, "."))
		}
		return list, func(next []any) { root[leaf] = next }, nil
	}
	current := root
	for _, segment := range path[:len(path)-1] {
		segment = strings.TrimSpace(segment)
		next, ok := current[segment].(map[string]any)
		if !ok || next == nil {
			return nil, nil, fmt.Errorf("path %s is missing or not an object", strings.Join(path, "."))
		}
		current = next
	}
	leaf := strings.TrimSpace(path[len(path)-1])
	raw, ok := current[leaf]
	if !ok && createLeaf {
		list := []any{}
		current[leaf] = list
		return list, func(next []any) { current[leaf] = next }, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("path %s is missing or not a list", strings.Join(path, "."))
	}
	return list, func(next []any) { current[leaf] = next }, nil
}
