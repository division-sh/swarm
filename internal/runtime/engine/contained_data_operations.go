package engine

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func (e *Executor) applyContainedDataOperation(frame *executionFrame, current BaseContext, write runtimecontracts.WorkflowDataWrite) error {
	contract, ok := entityruntime.ResolveForFlow(e.deps.Source, frame.req.FlowID.String())
	if !ok {
		return fmt.Errorf("flow %s has no declared entity contract", strings.TrimSpace(frame.req.FlowID.String()))
	}
	op := strings.TrimSpace(string(write.Operation))
	target, err := entityruntime.ResolveContainedOperationTarget(contract, write.Target(), op, !write.Key.IsZero(), !write.Index.IsZero())
	if err != nil {
		return err
	}

	key := ""
	if !write.Key.IsZero() {
		rawKey, err := evalRequiredDataOperationExpression(current, frame.state, write.Key, "key")
		if err != nil {
			return err
		}
		key, err = entityruntime.NormalizeContainedOperationKey(contract, target.MapKeyType, rawKey)
		if err != nil {
			return err
		}
	}

	hasIndex := !write.Index.IsZero()
	index := 0
	if hasIndex {
		rawIndex, err := evalRequiredDataOperationExpression(current, frame.state, write.Index, "index")
		if err != nil {
			return err
		}
		index, err = entityruntime.NormalizeContainedOperationIndex(rawIndex)
		if err != nil {
			return err
		}
	}

	var value any
	if write.Operation != runtimecontracts.WorkflowDataOperationDelete {
		rawValue, err := evalRequiredDataOperationExpression(current, frame.state, write.Value, "value")
		if err != nil {
			return err
		}
		value, err = entityruntime.NormalizeContainedOperationValue(contract, target, op, rawValue)
		if err != nil {
			return err
		}
	}

	if frame.state.State.StateCarrier.Metadata == nil {
		frame.state.State.StateCarrier.Metadata = map[string]any{}
	}
	if err := applyContainedOperationToMetadata(frame.state.State.StateCarrier.Metadata, target, op, key, hasIndex, index, value); err != nil {
		return err
	}
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	return nil
}

func evalRequiredDataOperationExpression(base BaseContext, state ExecutionState, expr runtimecontracts.ExpressionValue, label string) (any, error) {
	value, ok, err := evalExpressionValue(base, state, expr, workflowexpr.ValueExpressionOptions{})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s expression did not resolve", strings.TrimSpace(label))
	}
	return value, nil
}

func applyContainedOperationToMetadata(metadata map[string]any, target entityruntime.ContainedOperationTarget, op, key string, hasIndex bool, index int, value any) error {
	if target.MapScoped {
		return applyMapScopedOperation(metadata, target, op, key, hasIndex, index, value)
	}
	return applyListScopedOperation(metadata, target, op, hasIndex, index, value)
}

func applyMapScopedOperation(metadata map[string]any, target entityruntime.ContainedOperationTarget, op, key string, hasIndex bool, index int, value any) error {
	root, ok := metadata[target.RootField].(map[string]any)
	if !ok || root == nil {
		if op != entityruntime.ContainedOperationSet || len(target.MapValuePath) != 0 {
			return fmt.Errorf("map field %s is missing or not a map", target.RootField)
		}
		root = map[string]any{}
		metadata[target.RootField] = root
	}

	switch op {
	case entityruntime.ContainedOperationSet:
		if len(target.MapValuePath) == 0 {
			root[key] = value
			return nil
		}
		entry, ok := root[key].(map[string]any)
		if !ok || entry == nil {
			return fmt.Errorf("map key %q does not exist", key)
		}
		setNestedMapValue(entry, target.MapValuePath, value)
		return nil
	case entityruntime.ContainedOperationMerge:
		targetMap, err := mapScopedObjectTarget(root, key, target.MapValuePath)
		if err != nil {
			return err
		}
		for mergeKey, mergeValue := range value.(map[string]any) {
			targetMap[mergeKey] = mergeValue
		}
		return nil
	case entityruntime.ContainedOperationDelete:
		if _, exists := root[key]; !exists {
			return fmt.Errorf("map key %q does not exist", key)
		}
		delete(root, key)
		return nil
	case entityruntime.ContainedOperationAppend, entityruntime.ContainedOperationUpdate:
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

func applyListScopedOperation(metadata map[string]any, target entityruntime.ContainedOperationTarget, op string, hasIndex bool, index int, value any) error {
	path := strings.Split(target.Path, ".")
	list, setter, err := metadataListTarget(metadata, path, op == entityruntime.ContainedOperationAppend)
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
	case entityruntime.ContainedOperationAppend:
		return append(list, value), nil
	case entityruntime.ContainedOperationUpdate:
		if !hasIndex {
			return nil, fmt.Errorf("op update requires index")
		}
		if index >= len(list) {
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

func metadataListTarget(metadata map[string]any, path []string, createLeaf bool) ([]any, func([]any), error) {
	return nestedListTarget(metadata, path, createLeaf)
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

func setNestedMapValue(root map[string]any, path []string, value any) {
	current := root
	for _, segment := range path[:len(path)-1] {
		segment = strings.TrimSpace(segment)
		next, _ := current[segment].(map[string]any)
		if next == nil {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[strings.TrimSpace(path[len(path)-1])] = value
}
