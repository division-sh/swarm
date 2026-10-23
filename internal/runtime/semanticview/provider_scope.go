package semanticview

import (
	"fmt"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

// ProviderDirectoriesInScope consumes the frozen compiler input, not mutable
// exported source indexes or an optional caller-supplied scope implementation.
func ProviderDirectoriesInScope(source Source, flowID string) ([]sourceartifact.ProviderDirectory, error) {
	bundle, ok := Bundle(source)
	if !ok {
		return nil, fmt.Errorf("provider scope requires a compiled rooted source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, fmt.Errorf("provider scope requires a compiled rooted source: %w", err)
	}
	return rooted.Artifact().ProviderDirectoriesInScope(flowID)
}
