package semanticview

// SourceCapability finds one capability through transparent semantic-source
// wrappers. Consumers use this owner instead of asserting only the outermost
// concrete source and silently changing semantics when a wrapper is added.
func SourceCapability[T any](source Source) (T, bool) {
	var zero T
	current := source
	for depth := 0; current != nil && depth < 64; depth++ {
		if capability, ok := any(current).(T); ok {
			return capability, true
		}
		wrapper, ok := current.(interface{ BaseSemanticSource() Source })
		if !ok {
			return zero, false
		}
		current = wrapper.BaseSemanticSource()
	}
	return zero, false
}
