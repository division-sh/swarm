package semanticviewtest

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// WrapRootAgents gives direct in-memory root declarations the owner facts that
// the contract loader normally supplies. It is for tests that intentionally
// bypass loading, not a runtime identity fallback.
func WrapRootAgents(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	if bundle == nil {
		return nil
	}
	if bundle.URIRegistry.Agents == nil {
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.URIRegistry.ByURI == nil {
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	}
	for rawLocalID := range bundle.Agents {
		localID := strings.TrimSpace(rawLocalID)
		if localID == "" {
			continue
		}
		uri := "swarm-test://root/agents/" + localID
		ref := runtimecontracts.ContractURIRef{Kind: "agent", LocalID: localID, Full: uri}
		bundle.URIRegistry.Agents[localID] = ref
		bundle.URIRegistry.ByURI[uri] = ref
	}
	return semanticview.Wrap(bundle)
}
