package workflowexpr

import (
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/cel-go/cel"
)

// StructuralPredicateEnv adapts an existing reader namespace to the same
// structural type provider and checked presence analysis as workflow readers.
// The caller still owns namespace/selector admission and row activation.
type StructuralPredicateEnv struct {
	*cel.Env
	provider *workflowStructuralTypeProvider
}

func NewEntityPredicateEnv(entity runtimecontracts.ResolvedCatalogType, extra ...cel.EnvOption) (*StructuralPredicateEnv, error) {
	env, err := cel.NewEnv(append(extra, cel.OptionalTypes())...)
	if err != nil {
		return nil, err
	}
	provider, err := newWorkflowStructuralTypeProvider(env.CELTypeProvider(), ValueExpressionOptions{EntityType: &entity})
	if err != nil {
		return nil, err
	}
	root, ok := provider.rootType("entity")
	if !ok || provider.nodes[root.TypeName()] == nil {
		return nil, fmt.Errorf("predicate entity must be a structural record")
	}
	// Bare aliases reuse the entity owner's field types, including its existing
	// top-level nullable scalar convention; nested records remain exact.
	node := provider.nodes[root.TypeName()]
	options := []cel.EnvOption{cel.CustomTypeProvider(provider)}
	if _, shadowsEnvelope := node.fields["fields"]; !shadowsEnvelope {
		provider.rootTypes["fields"] = root
		provider.rootIdentifiers["fields"] = struct{}{}
		options = append(options, cel.Variable("fields", root))
	}
	for _, field := range entity.Fields {
		typeValue := node.fields[field.Name].celType
		provider.rootTypes[field.Name] = typeValue
		provider.rootIdentifiers[field.Name] = struct{}{}
		options = append(options, cel.Variable(field.Name, typeValue))
	}
	env, err = env.Extend(options...)
	if err != nil {
		return nil, err
	}
	return &StructuralPredicateEnv{Env: env, provider: provider}, nil
}

func (e *StructuralPredicateEnv) CompilePredicate(expression string) (cel.Program, error) {
	compiled, issues := e.Env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if compiled.OutputType() != cel.BoolType && compiled.OutputType() != cel.DynType {
		return nil, fmt.Errorf("predicate must return bool, got %s", compiled.OutputType())
	}
	if err := validateWorkflowOptionalReads(compiled, e.provider); err != nil {
		return nil, err
	}
	return e.Env.Program(compiled)
}
