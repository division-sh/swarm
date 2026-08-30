package contracts

import (
	"fmt"
	"sort"
	"strings"

	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func flowViewChildren(view *FlowContractView) []*FlowContractView {
	if view == nil || len(view.Children) == 0 {
		return nil
	}
	children := make([]*FlowContractView, 0, len(view.Children))
	for i := range view.Children {
		children = append(children, &view.Children[i])
	}
	return children
}
func loadFlowContractViewFromSource(artifact *sourceartifact.AdmittedSourceArtifact, source FlowSource, schema FlowSchemaDocument) (FlowContractView, error) {
	paths := FlowContractPaths{
		FlowPath: source.FlowPath, SchemaFile: source.Schema, TypesFile: source.Types,
		EntitiesFile: source.Entities, NodesFile: source.Nodes, EventsFile: source.Events,
		AgentsFile: source.Agents, ToolsFile: source.Tools, PolicyFile: source.Policy,
	}
	view := FlowContractView{
		Paths: paths, Schema: schema,
		Nodes: map[string]SystemNodeContract{}, Events: map[string]EventCatalogEntry{},
		Agents: map[string]AgentRegistryEntry{}, Tools: map[string]ToolSchemaEntry{},
		Policy:   PolicyDocument{Values: map[string]PolicyValue{}},
		NodeURIs: map[string]string{}, AgentURIs: map[string]string{}, EventURIs: map[string]string{},
	}
	var err error
	if view.Nodes, err = loadOptionalNodeDeclarationsFromSource(artifact, source.Nodes); err != nil {
		return view, err
	}
	if view.Events, err = loadOptionalEventCatalogFromSource(artifact, source.Events); err != nil {
		return view, err
	}
	if view.Agents, err = loadOptionalAgentDeclarationsFromSource(artifact, source.Agents); err != nil {
		return view, err
	}
	view.Agents, err = normalizeAgentRegistryEntries(view.Agents, source.Agents)
	if err != nil {
		return view, err
	}
	declaration := ContractItemSource{FlowPath: source.FlowPath, Family: "agents", File: source.Agents}
	view.Agents, err = materializeAgentIntentsFromSource(artifact, declaration, view.Agents)
	if err != nil {
		return view, err
	}
	view.Agents, err = materializeAgentMockPerformancesFromSource(artifact, declaration, view.Agents)
	if err != nil {
		return view, err
	}
	if view.Tools, err = loadOptionalToolDeclarationsFromSource(artifact, source.Tools); err != nil {
		return view, err
	}
	if view.Policy, err = loadOptionalPolicyDeclarationsFromSource(artifact, source.Policy); err != nil {
		return view, err
	}
	return view, nil
}

func buildFilesystemFlowTree(bundle *WorkflowContractBundle, views map[string]FlowContractView) error {
	if bundle == nil || bundle.FlowSources == nil {
		return fmt.Errorf("filesystem flow tree requires indexed source topology")
	}
	rootSource, ok := bundle.FlowSources["."]
	if !ok {
		return fmt.Errorf("filesystem flow tree is missing selected root")
	}
	var build func(FlowSource) (FlowContractView, error)
	build = func(source FlowSource) (FlowContractView, error) {
		view, exists := views[source.FlowPath]
		if !exists {
			return FlowContractView{}, fmt.Errorf("filesystem flow tree is missing view %q", source.FlowPath)
		}
		view.Children = make([]FlowContractView, 0, len(source.Children))
		for _, childPath := range source.Children {
			childSource, exists := bundle.FlowSources[childPath]
			if !exists {
				return FlowContractView{}, fmt.Errorf("filesystem flow tree is missing child source %q", childPath)
			}
			child, err := build(childSource)
			if err != nil {
				return FlowContractView{}, err
			}
			view.Children = append(view.Children, child)
		}
		return view, nil
	}
	root, err := build(rootSource)
	if err != nil {
		return err
	}
	tree := FlowTree{Root: &root, ByPath: map[string]*FlowContractView{}, ByID: map[string]*FlowContractView{}}
	registry := ContractURIRegistry{Scheme: "swarm", Nodes: map[string]ContractURIRef{}, Agents: map[string]ContractURIRef{}, Events: map[string]ContractURIRef{}, ByURI: map[string]ContractURIRef{}}
	var index func(*FlowContractView, *FlowContractView)
	index = func(view, parent *FlowContractView) {
		view.Parent = parent
		flowPath := strings.TrimSpace(view.Paths.FlowPath)
		uriPath := flowPath
		if flowPath == "." {
			uriPath = ""
		}
		view.Path = uriPath
		view.URI = flowmodel.FullURI(&registry, uriPath)
		tree.ByPath[flowPath] = view
		tree.ByID[flowPath] = view
		flowmodel.PopulateScopedURIs(
			view, &registry,
			func(v *FlowContractView) string { return strings.TrimSpace(v.Paths.FlowPath) },
			func(v *FlowContractView) string {
				if strings.TrimSpace(v.Paths.FlowPath) == "." {
					return ""
				}
				return strings.TrimSpace(v.Paths.FlowPath)
			},
			func(v *FlowContractView) map[string]SystemNodeContract { return v.Nodes },
			func(v *FlowContractView) map[string]AgentRegistryEntry { return v.Agents },
			func(v *FlowContractView) map[string]EventCatalogEntry { return v.Events },
			func(v *FlowContractView) *map[string]string { return &v.NodeURIs },
			func(v *FlowContractView) *map[string]string { return &v.AgentURIs },
			func(v *FlowContractView) *map[string]string { return &v.EventURIs },
		)
		for i := range view.Children {
			index(&view.Children[i], view)
		}
	}
	index(tree.Root, nil)
	bundle.FlowTree = tree
	bundle.URIRegistry = registry
	return nil
}

func populateMergedFlowViews(bundle *WorkflowContractBundle) error {
	views := bundle.FlowViews()
	sort.Slice(views, func(i, j int) bool { return views[i].Paths.FlowPath < views[j].Paths.FlowPath })
	for _, view := range views {
		flowPath := strings.TrimSpace(view.Paths.FlowPath)
		if err := mergeNodeContracts(bundle, view.Nodes, ContractItemSource{FlowPath: flowPath, Family: "nodes", File: view.Paths.NodesFile}); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, ContractItemSource{FlowPath: flowPath, Family: "events", File: view.Paths.EventsFile}); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, ContractItemSource{FlowPath: flowPath, Family: "agents", File: view.Paths.AgentsFile}); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, ContractItemSource{FlowPath: flowPath, Family: "tools", File: view.Paths.ToolsFile}); err != nil {
			return err
		}
	}
	return nil
}

func normalizeAgentRegistryEntries(entries map[string]AgentRegistryEntry, sourceFile string) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return map[string]AgentRegistryEntry{}, nil
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	effectiveOwners := make(map[string]string, len(entries))
	for _, key := range sortedContractKeys(entries) {
		entry := entries[key]
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		if err := validateAgentRegistryMapKey(trimmedKey, sourceFile); err != nil {
			return nil, err
		}
		effective := EffectiveAgentRegistryEntry(trimmedKey, entry)
		effectiveID, err := DeclaredAgentID(trimmedKey, effective)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", strings.TrimSpace(sourceFile), err)
		}
		if previous, exists := effectiveOwners[effectiveID]; exists {
			return nil, fmt.Errorf(
				"%s agents %q and %q derive the same effective agent id %q; use distinct declaration map keys or literal id overrides",
				strings.TrimSpace(sourceFile),
				previous,
				trimmedKey,
				effectiveID,
			)
		}
		effectiveOwners[effectiveID] = trimmedKey
		out[trimmedKey] = effective
	}
	return out, nil
}

func validateAgentRegistryMapKey(key, sourceFile string) error {
	switch strings.TrimSpace(key) {
	case "agent_defaults", "agent_profiles", "profiles":
		return fmt.Errorf("UNSUPPORTED: %s key %q is reserved for future platform-defaults support and is not accepted by Layer 1 platform defaults", strings.TrimSpace(sourceFile), key)
	default:
		return nil
	}
}
