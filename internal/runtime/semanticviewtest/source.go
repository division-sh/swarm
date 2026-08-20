package semanticviewtest

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type rootAgentsSource struct {
	semanticview.Source
	scopes []semanticview.ProjectScope
}

func (s rootAgentsSource) ProjectScopes() []semanticview.ProjectScope {
	return append([]semanticview.ProjectScope(nil), s.scopes...)
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
	scopes := base.ProjectScopes()
	rootScope := -1
	for index := range scopes {
		key := strings.Trim(strings.TrimSpace(scopes[index].Key), "/")
		if key == "" {
			key = "."
		}
		if key == "." && strings.TrimSpace(scopes[index].OwningFlowID) == "" {
			rootScope = index
			break
		}
	}
	if rootScope >= 0 {
		scopes[rootScope].Agents = runtimecontracts.EffectiveAgentRegistryEntries(bundle.Agents)
		scopes[rootScope].AgentURIs = agentURIs
		return rootAgentsSource{Source: base, scopes: scopes}
	}

	nodes := make(map[string]runtimecontracts.SystemNodeContract)
	for _, record := range base.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			panic(fmt.Sprintf("semanticview test root node %q has invalid identity: %v", record.LogicalID, err))
		}
		if node.PackageKey() != "." || node.FlowID() != "" {
			continue
		}
		if _, duplicate := nodes[node.NodeID()]; duplicate {
			panic(fmt.Sprintf("semanticview test root node %q has duplicate exact identity", record.LogicalID))
		}
		nodes[node.NodeID()] = record.Entry
	}
	scopes = append(scopes, semanticview.ProjectScope{
		Key: ".", Manifest: runtimecontracts.ProjectPackageDocument{
			Name: "semanticview-test-root", Version: "1.0.0", PlatformVersion: ">=0.7.0 <0.8.0",
		}, Nodes: nodes, Events: bundle.EventEntries(),
		Agents: runtimecontracts.EffectiveAgentRegistryEntries(bundle.Agents), AgentURIs: agentURIs,
		Tools: bundle.ToolEntries(), Policy: bundle.Policy,
	})
	return rootAgentsSource{Source: base, scopes: scopes}
}
