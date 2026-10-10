package semanticview

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

// Reconstructed sources and overlays must retain the declaration invariant.
func ValidateToolDeclarationNames(source Source) error {
	if source == nil {
		return nil
	}
	if bundle, ok := Bundle(source); ok {
		if err := bundle.ValidateToolDeclarationNames(); err != nil {
			return err
		}
	}
	if err := contracts.ValidateToolDeclarationNames(source.ToolEntries()); err != nil {
		return err
	}
	for _, scope := range source.FlowScopes() {
		if err := contracts.ValidateToolDeclarationNames(scope.Tools); err != nil {
			return fmt.Errorf("tool declarations in scope %q: %w", scope.ID, err)
		}
	}
	return nil
}
