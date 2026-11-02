package accumulator

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ValidateSpecForHandler admits only the authored business key, never pin policy.
func ValidateSpecForHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEvent string, spec *runtimecontracts.AccumulateSpec) error {
	if spec == nil {
		return nil
	}
	bundle, _ := semanticview.Bundle(source)
	return bundle.ValidateAccumulator(node, handlerEvent, spec)
}
