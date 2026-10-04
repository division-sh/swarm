package cliapp

import (
	"context"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

// TestSessionRequest carries admitted source and model policy, never deployment
// connection, store, credential or listener settings. The workspace selector
// belongs only to this private execution, never the deployment configuration.
type TestSessionRequest struct {
	Bundle           *runtimecontracts.WorkflowContractBundle
	SourceRoot       string
	PlatformSpecPath string
	PlatformPackBase *packartifact.PlatformPackInventory
	LiveBackend      string
	ModelAliases     llmselection.ModelAliases
	WorkspaceBackend string
}

type TestSessionEndpoint struct {
	APIServer string
	Token     string
}

// TestSessionRunner owns acquisition and joined cleanup around one ready callback.
// The callback has public API access only; no store or runtime handle escapes.
type TestSessionRunner func(context.Context, TestSessionRequest, func(context.Context, TestSessionEndpoint) error) error
