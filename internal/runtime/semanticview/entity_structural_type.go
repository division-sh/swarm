package semanticview

import (
	"fmt"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func ResolveEntityStructuralType(source Source, flowPath string) (*runtimecontracts.ResolvedCatalogType, error) {
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("entity structural type requires an admitted source")
	}
	primary, err := bundle.ResolveFlowPrimaryEntity(flowPath)
	if err != nil {
		return nil, err
	}
	resolved, err := primary.StructuralType()
	if err != nil {
		return nil, err
	}
	return &resolved, nil
}
