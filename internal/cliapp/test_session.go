package cliapp

import (
	"context"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// TestSessionRequest carries admitted source and LLM configuration, never deployment
// connection, store, credential or listener settings. The workspace selector
// belongs only to this private execution, never the deployment configuration.
type TestSessionRequest struct {
	Bundle           *runtimecontracts.WorkflowContractBundle
	SourceRoot       string
	PlatformPackBase *packartifact.PlatformPackInventory
	LLM              config.LLMConfig
	WorkspaceBackend string
}

type TestSessionEndpoint struct {
	APIServer     string
	Token         string
	StartScenario func(context.Context, map[string]any) (string, error)
}

// TestSessionRunner owns acquisition and joined cleanup around one ready callback.
// Only the closed scenario-start operation accompanies public API access;
// no store or runtime handle escapes.
type TestSessionRunner func(context.Context, TestSessionRequest, func(context.Context, TestSessionEndpoint) error) error
