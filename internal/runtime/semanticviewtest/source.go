package semanticviewtest

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type rootAgentsSource struct {
	semanticview.Source
	scope semanticview.ProjectScope
}

func (s rootAgentsSource) ProjectScopes() []semanticview.ProjectScope {
	return []semanticview.ProjectScope{s.scope}
}

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
	agentURIs := make(map[string]string, len(bundle.Agents))
	for rawLocalID := range bundle.Agents {
		localID := strings.TrimSpace(rawLocalID)
		if localID == "" {
			continue
		}
		uri := "swarm-test://root/agents/" + localID
		ref := runtimecontracts.ContractURIRef{Kind: "agent", LocalID: localID, Full: uri}
		bundle.URIRegistry.Agents[localID] = ref
		bundle.URIRegistry.ByURI[uri] = ref
		agentURIs[localID] = uri
	}
	base := semanticview.Wrap(bundle)
	return rootAgentsSource{Source: base, scope: semanticview.ProjectScope{
		Key: ".", Manifest: runtimecontracts.ProjectPackageDocument{
			Name: "semanticview-test-root", Version: "1.0.0", PlatformVersion: ">=0.7.0 <0.8.0",
		}, Nodes: bundle.NodeEntries(), Events: bundle.EventEntries(),
		Agents: runtimecontracts.EffectiveAgentRegistryEntries(bundle.Agents), AgentURIs: agentURIs,
		Tools: bundle.ToolEntries(), Policy: bundle.Policy,
	}}
}
