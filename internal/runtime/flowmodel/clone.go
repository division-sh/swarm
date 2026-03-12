package flowmodel

import "strings"

func ApplyPolicyOverrides(doc *PolicyDocument, overrides map[string]any) {
	if doc == nil || len(overrides) == 0 {
		return
	}
	if doc.Values == nil {
		doc.Values = map[string]PolicyValue{}
	}
	for key, value := range overrides {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		item := doc.Values[key]
		item.Value = value
		doc.Values[key] = item
	}
}

func CloneViewTree[P, S, N, E, A, T any](
	root *View[P, S, N, E, A, T],
	id func(*View[P, S, N, E, A, T]) string,
	mutate func(*View[P, S, N, E, A, T]),
) (*View[P, S, N, E, A, T], map[string]*View[P, S, N, E, A, T], map[string]*View[P, S, N, E, A, T]) {
	return cloneViewTree(root, id, mutate, nil)
}

func cloneViewTree[P, S, N, E, A, T any](
	view *View[P, S, N, E, A, T],
	id func(*View[P, S, N, E, A, T]) string,
	mutate func(*View[P, S, N, E, A, T]),
	parent *View[P, S, N, E, A, T],
) (*View[P, S, N, E, A, T], map[string]*View[P, S, N, E, A, T], map[string]*View[P, S, N, E, A, T]) {
	if view == nil {
		return nil, nil, nil
	}
	cloned := *view
	cloned.Parent = parent
	cloned.Nodes = cloneMap(view.Nodes)
	cloned.Events = cloneMap(view.Events)
	cloned.Agents = cloneMap(view.Agents)
	cloned.Tools = cloneMap(view.Tools)
	cloned.Policy = ClonePolicyDocument(view.Policy)
	cloned.NodeURIs = cloneMap(view.NodeURIs)
	cloned.AgentURIs = cloneMap(view.AgentURIs)
	cloned.EventURIs = cloneMap(view.EventURIs)
	cloned.Children = make([]View[P, S, N, E, A, T], 0, len(view.Children))
	if mutate != nil {
		mutate(&cloned)
	}
	byPath := map[string]*View[P, S, N, E, A, T]{}
	byID := map[string]*View[P, S, N, E, A, T]{}
	if path := strings.TrimSpace(cloned.Path); path != "" {
		byPath[path] = &cloned
	}
	if id != nil {
		if value := strings.TrimSpace(id(&cloned)); value != "" {
			byID[value] = &cloned
		}
	}
	for i := range view.Children {
		child, childByPath, childByID := cloneViewTree(&view.Children[i], id, mutate, &cloned)
		if child != nil {
			cloned.Children = append(cloned.Children, *child)
		}
		for key, value := range childByPath {
			byPath[key] = value
		}
		for key, value := range childByID {
			byID[key] = value
		}
	}
	for i := range cloned.Children {
		cloned.Children[i].Parent = &cloned
		child := &cloned.Children[i]
		if path := strings.TrimSpace(child.Path); path != "" {
			byPath[path] = child
		}
		if id != nil {
			if value := strings.TrimSpace(id(child)); value != "" {
				byID[value] = child
			}
		}
	}
	return &cloned, byPath, byID
}

func cloneMap[K comparable, V any](in map[K]V) map[K]V {
	if len(in) == 0 {
		return nil
	}
	out := make(map[K]V, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
