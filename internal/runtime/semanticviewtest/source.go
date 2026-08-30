package semanticviewtest

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type rootAgentsSource struct {
	semanticview.Source
	scopes []semanticview.FlowScope
}

func (s rootAgentsSource) FlowScopes() []semanticview.FlowScope {
	return append([]semanticview.FlowScope(nil), s.scopes...)
}

func (s rootAgentsSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	id = strings.TrimSpace(id)
	for _, scope := range s.scopes {
		if strings.TrimSpace(scope.ID) == id {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}

// WrapRootAgents gives direct in-memory root declarations the owner facts that
// the contract loader normally supplies. It is for tests that intentionally
// bypass loading, not a runtime identity fallback.
func WrapRootAgents(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	if bundle == nil {
		return nil
	}
	root := bundle.FlowTree.Root
	if root == nil {
		root = &runtimecontracts.FlowContractView{
			Path:  ".",
			Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		}
		bundle.FlowTree.Root = root
	}
	if strings.TrimSpace(root.Path) == "" {
		root.Path = "."
	}
	if strings.TrimSpace(root.Paths.FlowPath) == "" {
		root.Paths.FlowPath = "."
	}
	if root.Nodes == nil {
		root.Nodes = bundle.Nodes
	}
	if root.Events == nil {
		root.Events = bundle.Events
	}
	if root.Tools == nil {
		root.Tools = bundle.Tools
	}
	if len(root.Policy.Values) == 0 && len(root.Policy.Criteria) == 0 && len(root.Policy.Validation) == 0 && len(root.Policy.Modules) == 0 {
		root.Policy = bundle.Policy
	}
	if bundle.RootSchema != nil {
		root.Schema = *bundle.RootSchema
	}
	if bundle.FlowTree.ByID == nil {
		bundle.FlowTree.ByID = map[string]*runtimecontracts.FlowContractView{}
	}
	bundle.FlowTree.ByID["."] = root
	root.Agents = runtimecontracts.EffectiveAgentRegistryEntries(bundle.Agents)
	if root.AgentURIs == nil {
		root.AgentURIs = map[string]string{}
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
		root.AgentURIs[localID] = uri
	}
	base := semanticview.Wrap(bundle)
	scopes := base.FlowScopes()
	rootIndex := -1
	for index := range scopes {
		path := strings.TrimSpace(scopes[index].Path)
		if path == "." || strings.Trim(path, "/") == "" {
			rootIndex = index
			break
		}
	}
	if rootIndex < 0 {
		nodes := make(map[string]runtimecontracts.SystemNodeContract)
		for _, record := range base.ExecutableNodeRecords() {
			node, err := record.Identity()
			if err != nil {
				panic(fmt.Sprintf("semanticview test root node %q has invalid identity: %v", record.LogicalID, err))
			}
			if node.FlowPath() == "." {
				nodes[node.NodeID()] = record.Entry
			}
		}
		scopes = append(scopes, semanticview.FlowScope{ID: ".", Path: ".", Nodes: nodes})
		rootIndex = len(scopes) - 1
	}
	scopes[rootIndex].Agents = runtimecontracts.EffectiveAgentRegistryEntries(bundle.Agents)
	scopes[rootIndex].AgentURIs = agentURIs
	scopes[rootIndex].Tools = bundle.ToolEntries()
	scopes[rootIndex].Policy = bundle.Policy
	return rootAgentsSource{Source: base, scopes: scopes}
}
