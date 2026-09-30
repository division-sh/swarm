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
	if spec == nil || spec.Key == "" {
		return nil
	}
	path, err := keyPath(spec.Key)
	if err != nil {
		return err
	}
	resolution := semanticview.ResolveEventSchema(source, node.FlowPath(), handlerEvent)
	field, ok := resolution.StructuralType.FieldPath(strings.Join(path.Segments, "."))
	if !resolution.HasStructural || !ok || field.Type.Kind != runtimecontracts.CatalogTypeText {
		return fmt.Errorf("accumulate.key %q must select a catalog-admitted text payload field", spec.Key)
	}
	return nil
}

func keyPath(key string) (paths.Path, error) {
	path := paths.Parse(key)
	if path.Root != paths.RootPayload || len(path.Segments) == 0 || key != "payload."+strings.Join(path.Segments, ".") {
		return paths.Path{}, fmt.Errorf("accumulate.key %q must be an exact payload field path", key)
	}
	return path, nil
}
