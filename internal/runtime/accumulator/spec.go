package accumulator

import (
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ValidateSpecForHandler binds the complete payload to its declared state list
// and authored business key, never to pin policy or a separate buffer.
func ValidateSpecForHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string, spec *runtimecontracts.AccumulateSpec) error {
	if spec == nil {
		return nil
	}
	if _, err := spec.ItemKeyPath(); err != nil {
		return err
	}
	resolution := semanticview.ResolveEventSchema(source, node.FlowPath(), handlerEvent)
	if !resolution.HasSchema || !resolution.HasStructural || len(resolution.UnresolvedTypes) != 0 {
		return fmt.Errorf("accumulate payload %s requires its exact admitted schema", handlerEvent)
	}
	field, ok := resolution.Field(spec.Key)
	if !ok || field.IsOptional {
		return fmt.Errorf("accumulate.key %q must name a required scalar item field on %s", spec.Key, handlerEvent)
	}
	switch field.Type.Kind {
	case runtimecontracts.CatalogTypeText, runtimecontracts.CatalogTypeInteger, runtimecontracts.CatalogTypeNumber, runtimecontracts.CatalogTypeBoolean:
	default:
		return fmt.Errorf("accumulate.key %q must name a required scalar item field on %s, not %s", spec.Key, handlerEvent, runtimecontracts.StructuralCatalogTypeSyntax(field.Type))
	}
	target, err := spec.TargetField()
	if err != nil {
		return err
	}
	contract, ok := entityruntime.ResolveForFlow(source, node.FlowPath())
	if !ok {
		return fmt.Errorf("accumulate target %q requires a declared state contract on flow %s", target, node.FlowPath())
	}
	decl, err := entityruntime.FieldDecl(contract, target)
	if err != nil {
		return fmt.Errorf("accumulate target %q: %w", target, err)
	}
	subject := fmt.Sprintf("accumulate target %s: %s for event %s", target, decl.Type, handlerEvent)
	return runtimecontracts.ValidateCatalogListItemSupply(subject, resolution.Schema.Schema, decl, contract.Types)
}
