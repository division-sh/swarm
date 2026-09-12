package entityruntime

import (
	"fmt"
	"reflect"
	"strings"
)

// Mutation is an evaluated operation. Expressions are resolved by their existing
// execution owner against Draft before each operation, not against a stale read.
// An empty Operation is field assignment; named operations are the authored
// collection operations or clear. These are not additional agent tools.
type Mutation struct {
	Operation        string
	Target           string
	Value            any
	Key              any
	HasKey           bool
	Index            any
	HasIndex         bool
	ProjectionSource string
}

const MutationClear = "clear"

// MutationPlan owns a private ordered candidate and structural conflict history.
// Validate ends one executed operation list; the same plan can then continue to
// retain conflicts across later actions and projections in the handler batch.
type MutationPlan struct {
	contract Contract
	draft    map[string]any
	writes   []mutationWrite
	err      error
}

type mutationWrite struct {
	path  string
	clear bool
}

func NewMutationPlan(contract Contract, state map[string]any) (*MutationPlan, error) {
	normalized, err := NormalizeState(contract, state)
	if err != nil {
		return nil, err
	}
	return &MutationPlan{contract: contract, draft: normalized}, nil
}

func (p *MutationPlan) Draft() map[string]any { return cloneMap(p.draft) }

func (p *MutationPlan) Append(op Mutation) error {
	if p.err != nil {
		return p.err
	}
	next := cloneMap(p.draft)
	path, err := p.apply(next, op)
	if err != nil {
		p.err = err
		return err
	}
	clear := op.Operation == MutationClear
	for _, previous := range p.writes {
		if clear != previous.clear && pathsOverlap(previous.path, path) {
			p.err = fmt.Errorf("entity mutation set/clear conflict between %s and %s", previous.path, path)
			return p.err
		}
	}
	p.writes = append(p.writes, mutationWrite{path: path, clear: clear})
	p.draft = next
	return nil
}

func (p *MutationPlan) Validate() (map[string]any, error) {
	if p.err != nil {
		return nil, p.err
	}
	normalized, err := NormalizeState(p.contract, p.draft)
	if err != nil {
		p.err = err
		return nil, err
	}
	p.draft = normalized
	return p.Draft(), nil
}

func ApplyMutations(contract Contract, state map[string]any, operations []Mutation) (map[string]any, error) {
	p, err := NewMutationPlan(contract, state)
	if err != nil {
		return nil, err
	}
	for _, op := range operations {
		if err := p.Append(op); err != nil {
			return nil, err
		}
	}
	return p.Validate()
}

func (p *MutationPlan) apply(next map[string]any, op Mutation) (string, error) {
	path, entity, err := EntityWritePath(op.Target)
	if err != nil {
		return "", err
	}
	if !entity {
		return "", fmt.Errorf("mutation target %s is not an entity field", op.Target)
	}
	root, _, _ := strings.Cut(path, ".")
	decl, err := FieldDecl(p.contract, root)
	if err != nil {
		return "", err
	}
	if decl.MaterializeFrom != op.ProjectionSource {
		return "", fmt.Errorf("field %s is owned by materialize_from %q, not %q", root, decl.MaterializeFrom, op.ProjectionSource)
	}
	if op.ProjectionSource != "" && (op.Operation != "" || path != root) {
		return "", fmt.Errorf("projection must replace its declared root field %s", root)
	}
	if op.Operation == "" || op.Operation == MutationClear {
		if op.HasKey || op.HasIndex {
			return "", fmt.Errorf("field mutation must not declare key or index")
		}
		field, err := ResolveFieldPath(p.contract, path)
		if err != nil {
			return "", err
		}
		if op.Operation == MutationClear {
			if op.Value != nil {
				return "", fmt.Errorf("clear must not carry a value")
			}
			if !field.IsOptional {
				return "", fmt.Errorf("cannot clear bare entity field %s", path)
			}
			if decl.Immutable {
				return "", fmt.Errorf("cannot clear immutable entity field %s", path)
			}
			if err := clearMutationField(next, strings.Split(path, ".")); err != nil {
				return "", err
			}
		} else {
			parent, leaf, err := mutationParent(next, strings.Split(path, "."))
			if err != nil {
				return "", err
			}
			value, err := normalizeFieldValue(p.contract, path, op.Value, true)
			if err != nil {
				return "", err
			}
			parent[leaf] = value
		}
	} else {
		target, err := ResolveContainedOperationTarget(p.contract, "entity."+path, op.Operation, op.HasKey, op.HasIndex)
		if err != nil {
			return "", err
		}
		key, index := "", 0
		if op.HasKey {
			key, err = NormalizeContainedOperationKey(p.contract, target.MapKeyType, op.Key)
			if err != nil {
				return "", err
			}
		}
		if op.HasIndex {
			index, err = NormalizeContainedOperationIndex(op.Index)
			if err != nil {
				return "", err
			}
		}
		var value any
		if op.Operation != ContainedOperationDelete {
			value, err = NormalizeContainedOperationValue(p.contract, target, op.Operation, op.Value)
			if err != nil {
				return "", err
			}
		} else if op.Value != nil {
			return "", fmt.Errorf("delete must not carry a value")
		}
		if err := applyContainedOperationToMetadata(next, target, op.Operation, key, op.HasIndex, index, value); err != nil {
			return "", err
		}
	}
	if previous, exists := p.draft[root]; exists && decl.Immutable {
		value, remains := next[root]
		if !remains || !reflect.DeepEqual(previous, value) {
			return "", fmt.Errorf("immutable entity field %s cannot change", root)
		}
	}
	return path, nil
}

func clearMutationField(fields map[string]any, path []string) error {
	parent := fields
	for _, segment := range path[:len(path)-1] {
		raw, present := parent[segment]
		if !present {
			return nil
		}
		next, ok := raw.(map[string]any)
		if !ok || next == nil {
			return fmt.Errorf("entity parent %s is present but not a record", segment)
		}
		parent = next
	}
	delete(parent, path[len(path)-1])
	return nil
}

func pathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}

func mutationParent(fields map[string]any, path []string) (map[string]any, string, error) {
	if len(path) == 0 {
		return nil, "", fmt.Errorf("entity field path is required")
	}
	parent := fields
	for _, segment := range path[:len(path)-1] {
		next, ok := parent[segment].(map[string]any)
		if !ok || next == nil {
			return nil, "", fmt.Errorf("entity parent %s is missing or not a record", segment)
		}
		parent = next
	}
	return parent, path[len(path)-1], nil
}
