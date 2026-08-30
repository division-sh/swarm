package tools

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func wrapRootAgentBundle(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return semanticviewtest.WrapRootAgents(bundle)
}

func toolTestSourceWithDeclaredAgent(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, agentID, flowID string) semanticview.Source {
	t.Helper()
	toolTestDeclareAgent(t, bundle, agentID, flowID)
	if bundle.FlowSchemas == nil {
		bundle.FlowSchemas = map[string]runtimecontracts.FlowSchemaDocument{}
	}
	if bundle.FlowSources == nil {
		bundle.FlowSources = map[string]runtimecontracts.FlowSource{}
	}
	for id, view := range bundle.FlowTree.ByID {
		if view != nil {
			bundle.FlowSchemas[id] = view.Schema
			bundle.FlowSources[id] = runtimecontracts.FlowSource{
				FlowPath: id,
				Schema:   "swarm-test/" + id + "/schema.yaml",
			}
		}
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile tool-test workflow semantics: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func toolTestDeclareAgent(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, agentID, flowID string) {
	t.Helper()
	if bundle == nil {
		t.Fatal("tool test agent declaration requires bundle")
	}
	agentID = strings.TrimSpace(agentID)
	flowID = strings.TrimSpace(flowID)
	if agentID == "" {
		t.Fatal("tool test agent declaration requires agent id")
	}
	if flowID == "" {
		t.Fatal("tool test agent declaration requires canonical flow path")
	}
	view := toolTestAttachFlowView(bundle, flowID)
	if view == nil {
		t.Fatalf("tool test agent declaration requires flow %q", flowID)
	}
	if view.Agents == nil {
		view.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
	}
	localID := agentID
	for candidate, entry := range view.Agents {
		publicID, err := runtimecontracts.DeclaredAgentID(candidate, entry)
		if err == nil && publicID == agentID {
			localID = candidate
			break
		}
	}
	if _, ok := view.Agents[localID]; !ok {
		view.Agents[localID] = runtimecontracts.EffectiveAgentRegistryEntry(localID, runtimecontracts.AgentRegistryEntry{ID: agentID, Role: agentID})
	}
	if strings.TrimSpace(view.Paths.FlowPath) == "" {
		view.Paths.FlowPath = flowID
	}
	if strings.TrimSpace(view.Paths.AgentsFile) == "" {
		view.Paths.AgentsFile = "swarm-test/" + flowID + "/agents.yaml"
	}
	if view.AgentURIs == nil {
		view.AgentURIs = map[string]string{}
	}
	owner := "swarm-test://" + flowID + "/" + agentID
	if flowID == "." {
		owner = "swarm-test://root/agents/" + agentID
	}
	view.AgentURIs[localID] = owner
	if bundle.URIRegistry.Agents == nil {
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.URIRegistry.ByURI == nil {
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	}
	ref := runtimecontracts.ContractURIRef{Kind: "agent", FlowID: flowID, LocalID: localID, Full: owner}
	bundle.URIRegistry.Agents[flowID+"/"+localID] = ref
	bundle.URIRegistry.ByURI[owner] = ref
}

func toolTestAttachFlowView(bundle *runtimecontracts.WorkflowContractBundle, flowID string) *runtimecontracts.FlowContractView {
	if flowID == "." && bundle.FlowTree.Root == nil {
		rootSchema := runtimecontracts.FlowSchemaDocument{}
		if bundle.RootSchema != nil {
			rootSchema = *bundle.RootSchema
		}
		bundle.FlowTree.Root = &runtimecontracts.FlowContractView{
			Path:   ".",
			Paths:  runtimecontracts.FlowContractPaths{FlowPath: "."},
			Schema: rootSchema,
			Events: bundle.Events,
		}
		bundle.Events = nil
	}
	var find func(*runtimecontracts.FlowContractView) *runtimecontracts.FlowContractView
	find = func(view *runtimecontracts.FlowContractView) *runtimecontracts.FlowContractView {
		if view == nil {
			return nil
		}
		if strings.TrimSpace(view.Paths.FlowPath) == flowID {
			return view
		}
		for index := range view.Children {
			if match := find(&view.Children[index]); match != nil {
				return match
			}
		}
		return nil
	}
	if view := find(bundle.FlowTree.Root); view != nil {
		toolTestReindexFlowTree(bundle)
		return view
	}
	detached := bundle.FlowTree.ByID[flowID]
	if detached == nil {
		return nil
	}
	if bundle.FlowTree.Root == nil {
		bundle.FlowTree.Root = detached
	} else {
		bundle.FlowTree.Root.Children = append(bundle.FlowTree.Root.Children, *detached)
	}
	toolTestReindexFlowTree(bundle)
	return find(bundle.FlowTree.Root)
}

func toolTestReindexFlowTree(bundle *runtimecontracts.WorkflowContractBundle) {
	if bundle.FlowTree.ByID == nil {
		bundle.FlowTree.ByID = map[string]*runtimecontracts.FlowContractView{}
	}
	if bundle.FlowTree.ByPath == nil {
		bundle.FlowTree.ByPath = map[string]*runtimecontracts.FlowContractView{}
	}
	var index func(*runtimecontracts.FlowContractView)
	index = func(view *runtimecontracts.FlowContractView) {
		if view == nil {
			return
		}
		if id := strings.TrimSpace(view.Paths.FlowPath); id != "" {
			bundle.FlowTree.ByID[id] = view
		}
		if path := strings.TrimSpace(view.Path); path != "" {
			bundle.FlowTree.ByPath[path] = view
		}
		for childIndex := range view.Children {
			index(&view.Children[childIndex])
		}
	}
	index(bundle.FlowTree.Root)
}
