package flowmodel

import "strings"

func ClonePolicyDocument(in PolicyDocument) PolicyDocument {
	out := PolicyDocument{Values: map[string]PolicyValue{}}
	for key, value := range in.Values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Values[key] = value
	}
	return out
}

func Walk[T any](node *T, children func(*T) []*T, fn func(*T)) {
	if node == nil || fn == nil {
		return
	}
	fn(node)
	if children == nil {
		return
	}
	for _, child := range children(node) {
		Walk(child, children, fn)
	}
}

func CollectPathByID[T any](root *T, targetID string, id func(*T) string, children func(*T) []*T) []*T {
	path := make([]*T, 0, 8)
	if collectPathByID(root, strings.TrimSpace(targetID), id, children, &path) {
		return path
	}
	return nil
}

func collectPathByID[T any](node *T, targetID string, id func(*T) string, children func(*T) []*T, path *[]*T) bool {
	if node == nil || path == nil {
		return false
	}
	*path = append(*path, node)
	if id != nil && strings.TrimSpace(id(node)) == targetID {
		return true
	}
	if children != nil {
		for _, child := range children(node) {
			if collectPathByID(child, targetID, id, children, path) {
				return true
			}
		}
	}
	*path = (*path)[:len(*path)-1]
	return false
}

func NearestAncestor[T any](node *T, parent func(*T) *T, include func(*T) bool) *T {
	for node != nil {
		if include != nil && include(node) {
			return node
		}
		if parent == nil {
			return nil
		}
		node = parent(node)
	}
	return nil
}

func RegistryKey(flowID, localID string) string {
	if strings.TrimSpace(flowID) == "" {
		return strings.TrimSpace(localID)
	}
	return strings.TrimSpace(flowID) + "/" + strings.TrimSpace(localID)
}

func AbsoluteURI(flowPath, localID string) string {
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	localID = strings.TrimSpace(localID)
	switch {
	case flowPath == "":
		return localID
	case localID == "":
		return flowPath
	default:
		return flowPath + "/" + localID
	}
}

func FullURI(registry *URIRegistry, absolute string) string {
	absolute = strings.Trim(strings.TrimSpace(absolute), "/")
	if absolute == "" {
		return ""
	}
	if registry == nil || strings.TrimSpace(registry.Scheme) == "" {
		return absolute
	}
	return strings.TrimSpace(registry.Scheme) + "://" + absolute
}

func RegisterURI(registry *URIRegistry, target *map[string]string, kind, flowID, flowPath, localID string) {
	if registry == nil || target == nil {
		return
	}
	localID = strings.TrimSpace(localID)
	if localID == "" {
		return
	}
	ref := URIRef{
		Kind:     strings.TrimSpace(kind),
		FlowID:   strings.TrimSpace(flowID),
		LocalID:  localID,
		Path:     flowPath,
		Absolute: AbsoluteURI(flowPath, localID),
		Full:     FullURI(registry, AbsoluteURI(flowPath, localID)),
	}
	switch ref.Kind {
	case "node":
		registry.Nodes[RegistryKey(ref.FlowID, ref.LocalID)] = ref
	case "agent":
		registry.Agents[RegistryKey(ref.FlowID, ref.LocalID)] = ref
	case "event":
		registry.Events[RegistryKey(ref.FlowID, ref.LocalID)] = ref
	}
	registry.ByURI[ref.Absolute] = ref
	registry.ByURI[ref.Full] = ref
	(*target)[ref.LocalID] = ref.Full
}
