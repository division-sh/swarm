package entityruntime

import (
	"maps"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// AssignmentFacts records proven paths, not declared optionality. In particular,
// a bare root may be unassigned and an optional root may be assigned.
type AssignmentFacts map[string]struct{}

// MutationRequiredPaths identifies structural field reads needed to apply an
// operation. Dynamic key/index membership is still checked against the draft;
// it is not a static membership theorem.
func MutationRequiredPaths(write runtimecontracts.WorkflowDataWrite) []string {
	path, owned, err := EntityWritePath(write.Target())
	if err != nil || !owned || write.Operation == runtimecontracts.WorkflowDataOperationClear {
		return nil
	}
	if write.IsContainedOperation() {
		if !write.Key.IsZero() {
			root, _, nested := strings.Cut(path, ".")
			if write.Operation == runtimecontracts.WorkflowDataOperationSet && !nested {
				return nil
			}
			// The remaining target selects within the separately supplied key,
			// not a CEL member of the map. Membership stays a runtime check.
			return []string{root}
		}
		if write.Operation != runtimecontracts.WorkflowDataOperationAppend {
			return []string{path}
		}
	}
	if index := strings.LastIndex(path, "."); index >= 0 {
		return []string{path[:index]}
	}
	return nil
}

// ObservedAssignmentFacts records keys in the current typed draft without
// expanding required members or evaluating initializers. Runtime reads use this
// after every operation, so clearing a field immediately removes its proof.
func ObservedAssignmentFacts(entity runtimecontracts.ResolvedCatalogType, fields map[string]any) AssignmentFacts {
	facts := AssignmentFacts{}
	var visit func(string, runtimecontracts.ResolvedCatalogType, map[string]any)
	visit = func(prefix string, typ runtimecontracts.ResolvedCatalogType, values map[string]any) {
		for _, field := range typ.Fields {
			value, present := values[field.Name]
			if !present || value == nil {
				continue
			}
			path := prefix + field.Name
			facts[path] = struct{}{}
			if record, ok := value.(map[string]any); ok {
				visit(path+".", field.Type, record)
			}
		}
	}
	visit("", entity, fields)
	return facts
}

func (f AssignmentFacts) Clone() AssignmentFacts {
	if f == nil {
		return AssignmentFacts{}
	}
	return maps.Clone(f)
}

func (f AssignmentFacts) Has(path string) bool {
	_, ok := f[path]
	return ok
}

func (f AssignmentFacts) Intersect(other AssignmentFacts) AssignmentFacts {
	out := AssignmentFacts{}
	for path := range f {
		if other.Has(path) {
			out[path] = struct{}{}
		}
	}
	return out
}

// Forget also invalidates descendants: replacing a record does not preserve an
// earlier proof about an optional member of the old value.
func (f AssignmentFacts) Forget(path string) {
	for known := range f {
		if known == path || strings.HasPrefix(known, path+".") {
			delete(f, known)
		}
	}
}

// Assign consumes the exact declared structural type after a successful typed
// assignment. Required record members follow from value admission; map keys and
// list elements never become statically present by assigning their container.
func (f AssignmentFacts) Assign(path string, typ runtimecontracts.ResolvedCatalogType) {
	f.Forget(path)
	f[path] = struct{}{}
	for _, field := range typ.Fields {
		if !field.IsOptional {
			f.Assign(path+"."+field.Name, field.Type)
		}
	}
}

// AssignValue additionally preserves optional members actually supplied by an
// admitted literal/initializer, without fabricating missing members.
func (f AssignmentFacts) AssignValue(path string, typ runtimecontracts.ResolvedCatalogType, value any) {
	f.Assign(path, typ)
	record, ok := value.(map[string]any)
	if !ok {
		return
	}
	for _, field := range typ.Fields {
		if member, present := record[field.Name]; present {
			f.AssignValue(path+"."+field.Name, field.Type, member)
		}
	}
}

// TransferWrite describes only a successfully executed operation. Its operands
// and target admission must be checked before these facts may justify a read.
func (f AssignmentFacts) TransferWrite(entity runtimecontracts.ResolvedCatalogType, write runtimecontracts.WorkflowDataWrite) {
	path, owned, err := EntityWritePath(write.Target())
	if err != nil || !owned {
		return
	}
	field, ok := entity.FieldPath(path)
	if !ok {
		return
	}
	if write.Operation == runtimecontracts.WorkflowDataOperationClear {
		f.Forget(path)
		return
	}
	if write.IsContainedOperation() {
		// A successful constructive operation proves its container, not any
		// particular key/index. Other contained operations preserve presence.
		if write.Operation == runtimecontracts.WorkflowDataOperationSet || write.Operation == runtimecontracts.WorkflowDataOperationAppend {
			f[path] = struct{}{}
		}
		return
	}
	if write.Value.HasLiteralValue() {
		f.AssignValue(path, field.Type, write.Value.Literal)
	} else {
		f.Assign(path, field.Type)
	}
}
