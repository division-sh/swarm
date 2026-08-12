package tools

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func wrapRootAgentBundle(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return semanticviewtest.WrapRootAgents(bundle)
}
