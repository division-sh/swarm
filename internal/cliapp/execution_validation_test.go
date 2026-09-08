package cliapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Execution fixtures exercise runtime admission, not the structural CLI.
func validateExecutionFixture(ctx context.Context, source semanticview.Source, posture executionposture.Posture) error {
	credentials, err := BuildCredentialStore()
	if err != nil {
		return fmt.Errorf("configure credentials: %w", err)
	}
	managed, err := BuildManagedCredentialStore()
	if err != nil {
		return fmt.Errorf("configure managed credentials: %w", err)
	}
	opts := runtime.DefaultWorkflowContractValidationOptions(credentials, posture)
	opts.ManagedCredentials = managed
	_, err = runtime.ValidateWorkflowContractSurface(ctx, source, opts)
	return err
}
