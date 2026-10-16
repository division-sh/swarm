package accumulator

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ValidateSpecForHandler admits only the authored business key, never pin policy.
func ValidateSpecForHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string, spec *runtimecontracts.AccumulateSpec) error {
	if spec == nil {
		return nil
	}
	if _, err := spec.ItemKeyPath(); err != nil {
		return err
	}
	resolution := semanticview.ResolveEventSchema(source, node.FlowPath(), handlerEvent)
	field, ok := resolution.Field(spec.Key)
	if !ok || field.IsOptional {
		return fmt.Errorf("accumulate.key %q must name a required scalar item field on %s", spec.Key, handlerEvent)
	}
	switch field.Type.Kind {
	case runtimecontracts.CatalogTypeText, runtimecontracts.CatalogTypeInteger, runtimecontracts.CatalogTypeNumber, runtimecontracts.CatalogTypeBoolean:
		return nil
	default:
		return fmt.Errorf("accumulate.key %q must name a required scalar item field on %s, not %s", spec.Key, handlerEvent, runtimecontracts.StructuralCatalogTypeSyntax(field.Type))
	}
}

func keyPath(key string) (paths.Path, error) {
	path := paths.Parse(key)
	if path.Root != paths.RootPayload || len(path.Segments) == 0 || key != "payload."+strings.Join(path.Segments, ".") {
		return paths.Path{}, fmt.Errorf("accumulate.key %q must be an exact payload field path", key)
	}
	return path, nil
}
