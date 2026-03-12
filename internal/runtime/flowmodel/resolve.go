package flowmodel

import (
	"sort"
	"strings"
)

func ViewsByPath[T any](tree Tree[T], id func(*T) string, path func(*T) string, children func(*T) []*T) []T {
	if tree.Root == nil {
		return nil
	}
	views := make([]T, 0, len(tree.ByID))
	Walk(tree.Root, children, func(view *T) {
		if view == nil || strings.TrimSpace(id(view)) == "" {
			return
		}
		views = append(views, *view)
	})
	sort.SliceStable(views, func(i, j int) bool {
		return strings.TrimSpace(path(&views[i])) < strings.TrimSpace(path(&views[j]))
	})
	return views
}

func ResolvePolicyByID[T any](
	base PolicyDocument,
	tree Tree[T],
	flowID string,
	id func(*T) string,
	policy func(*T) PolicyDocument,
	children func(*T) []*T,
) PolicyDocument {
	out := ClonePolicyDocument(base)
	if out.Values == nil {
		out.Values = map[string]PolicyValue{}
	}
	if tree.Root == nil {
		return out
	}
	var pathViews []*T
	if strings.TrimSpace(flowID) == "" {
		pathViews = []*T{tree.Root}
	} else {
		pathViews = CollectPathByID(tree.Root, flowID, id, children)
		if len(pathViews) == 0 {
			return out
		}
	}
	for _, view := range pathViews {
		if view == nil {
			continue
		}
		for key, value := range policy(view).Values {
			out.Values[key] = value
		}
	}
	return out
}

func PathForID[T any](tree Tree[T], flowID string, path func(*T) string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	view, ok := tree.ByID[flowID]
	if !ok || view == nil {
		return ""
	}
	return strings.Trim(strings.TrimSpace(path(view)), "/")
}

func ResolveEntries[T any, V any](tree Tree[T], children func(*T) []*T, entries func(*T) map[string]V) map[string]V {
	if tree.Root == nil {
		return nil
	}
	out := map[string]V{}
	Walk(tree.Root, children, func(view *T) {
		if view == nil {
			return
		}
		for key, value := range entries(view) {
			out[strings.TrimSpace(key)] = value
		}
	})
	return out
}
