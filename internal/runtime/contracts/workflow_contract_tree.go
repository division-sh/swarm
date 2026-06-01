package contracts

import (
	"fmt"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	"strings"
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
func loadProjectContractView(paths ProjectPackagePaths, manifest ProjectPackageDocument) (ProjectContractView, error) {
	view := ProjectContractView{
		Paths:    paths,
		Manifest: manifest,
		Nodes:    map[string]SystemNodeContract{},
		Events:   map[string]EventCatalogEntry{},
		Agents:   map[string]AgentRegistryEntry{},
		Tools:    map[string]ToolSchemaEntry{},
		Policy:   PolicyDocument{Values: map[string]PolicyValue{}},
	}
	if err := loadOptionalYAMLMap(paths.ProjectNodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectEventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectAgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectPolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}
func loadFlowContractView(paths FlowContractPaths, schema FlowSchemaDocument) (FlowContractView, error) {
	view := FlowContractView{
		Paths:     paths,
		Schema:    schema,
		Nodes:     map[string]SystemNodeContract{},
		Events:    map[string]EventCatalogEntry{},
		Agents:    map[string]AgentRegistryEntry{},
		Tools:     map[string]ToolSchemaEntry{},
		Policy:    PolicyDocument{Values: map[string]PolicyValue{}},
		NodeURIs:  map[string]string{},
		AgentURIs: map[string]string{},
		EventURIs: map[string]string{},
		Children:  nil,
		Parent:    nil,
	}
	if err := loadOptionalYAMLMap(paths.NodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.EventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.AgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.PolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}
func buildFlowTree(bundle *WorkflowContractBundle, flowViewsByID map[string]FlowContractView) error {
	if bundle == nil {
		return nil
	}
	tree := FlowTree{
		ByPath: map[string]*FlowContractView{},
		ByID:   map[string]*FlowContractView{},
	}
	registry := ContractURIRegistry{
		Scheme: flowTreeURIScheme(bundle),
		Nodes:  map[string]ContractURIRef{},
		Agents: map[string]ContractURIRef{},
		Events: map[string]ContractURIRef{},
		ByURI:  map[string]ContractURIRef{},
	}
	if len(flowViewsByID) == 0 {
		bundle.FlowTree = tree
		bundle.URIRegistry = registry
		return nil
	}

	hasPackageNodes := false
	for _, pkg := range bundle.PackageTree {
		if _, ok := bundle.ProjectViewByKey(pkg.Key); ok {
			hasPackageNodes = true
			break
		}
	}
	if !hasPackageNodes {
		root := &FlowContractView{Children: make([]FlowContractView, 0, len(bundle.Paths.Flows))}
		for _, flow := range bundle.Paths.Flows {
			view, ok := flowViewsByID[flow.ID]
			if !ok {
				continue
			}
			root.Children = append(root.Children, view)
		}
		tree.Root = root
		flowmodel.IndexAndPopulateScopedURIs(
			tree.Root,
			&tree,
			&registry,
			func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
			flowViewChildren,
			nearestFlowTreeAncestor,
			func(view *FlowContractView, parent *FlowContractView) { view.Parent = parent },
			func(view *FlowContractView, path string) { view.Path = path },
			func(view *FlowContractView, uri string) { view.URI = uri },
			func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
			func(view *FlowContractView) string { return strings.Trim(strings.TrimSpace(view.Path), "/") },
			func(view *FlowContractView) map[string]SystemNodeContract { return view.Nodes },
			func(view *FlowContractView) map[string]AgentRegistryEntry { return view.Agents },
			func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
			func(view *FlowContractView) *map[string]string { return &view.NodeURIs },
			func(view *FlowContractView) *map[string]string { return &view.AgentURIs },
			func(view *FlowContractView) *map[string]string { return &view.EventURIs },
		)
		if len(tree.ByPath) == 0 {
			return fmt.Errorf("flow tree build produced no indexed paths")
		}
		bundle.FlowTree = tree
		bundle.URIRegistry = registry
		return nil
	}

	rootNode, err := flowmodel.AssemblePackageTree(
		bundle.PackageTree,
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.Key) },
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.ParentKey) },
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.Paths.Dir) },
		func(pkg LoadedProjectPackage) []FlowContractPaths { return pkg.Paths.Flows },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.ID) },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.Flow) },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.Dir) },
		func(pkg LoadedProjectPackage) *flowmodel.BuildNode[FlowContractView] {
			view, ok := bundle.ProjectViewByKey(pkg.Key)
			if !ok {
				return nil
			}
			return &flowmodel.BuildNode[FlowContractView]{View: flowmodel.ProjectAsFlowView[
				ProjectPackagePaths,
				ProjectPackageDocument,
				FlowContractPaths,
				FlowSchemaDocument,
				SystemNodeContract,
				EventCatalogEntry,
				AgentRegistryEntry,
				ToolSchemaEntry,
			](
				FlowContractPaths{
					PackageKey: pkg.Key,
					PackageDir: pkg.Paths.Dir,
					Dir:        pkg.Paths.Dir,
				},
				view,
			)}
		},
		func(flow FlowContractPaths) *flowmodel.BuildNode[FlowContractView] {
			view, ok := flowViewsByID[flow.ID]
			if !ok {
				return nil
			}
			return &flowmodel.BuildNode[FlowContractView]{View: view}
		},
	)
	if err != nil {
		return err
	}

	root := materializeFlowTree(rootNode)
	tree.Root = &root
	flowmodel.IndexAndPopulateScopedURIs(
		tree.Root,
		&tree,
		&registry,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		flowViewChildren,
		nearestFlowTreeAncestor,
		func(view *FlowContractView, parent *FlowContractView) { view.Parent = parent },
		func(view *FlowContractView, path string) { view.Path = path },
		func(view *FlowContractView, uri string) { view.URI = uri },
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) string { return strings.Trim(strings.TrimSpace(view.Path), "/") },
		func(view *FlowContractView) map[string]SystemNodeContract { return view.Nodes },
		func(view *FlowContractView) map[string]AgentRegistryEntry { return view.Agents },
		func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
		func(view *FlowContractView) *map[string]string { return &view.NodeURIs },
		func(view *FlowContractView) *map[string]string { return &view.AgentURIs },
		func(view *FlowContractView) *map[string]string { return &view.EventURIs },
	)
	if len(tree.ByPath) == 0 {
		return fmt.Errorf("flow tree build produced no indexed paths")
	}
	bundle.FlowTree = tree
	bundle.URIRegistry = registry
	return nil
}
func materializeFlowTree(node *flowmodel.BuildNode[FlowContractView]) FlowContractView {
	return flowmodel.Materialize(
		node,
		func(view *FlowContractView, children int) {
			view.Children = make([]FlowContractView, 0, children)
			view.Parent = nil
		},
		func(view *FlowContractView, child FlowContractView) {
			view.Children = append(view.Children, child)
		},
	)
}
func flowTreeURIScheme(bundle *WorkflowContractBundle) string {
	if bundle == nil {
		return "swarm"
	}
	for _, candidate := range []string{bundle.Package.Name, bundle.Semantics.Name} {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" {
			return candidate
		}
	}
	return "swarm"
}
func nearestFlowTreeAncestor(node *FlowContractView) *FlowContractView {
	return flowmodel.NearestAncestor(
		node,
		func(view *FlowContractView) *FlowContractView { return view.Parent },
		func(view *FlowContractView) bool { return strings.TrimSpace(view.Paths.ID) != "" },
	)
}
func populateMergedPackageViews(bundle *WorkflowContractBundle, flowViewsByID map[string]FlowContractView) error {
	for _, view := range bundle.RootProjectViews() {
		pkgKey := strings.TrimSpace(view.Paths.Key)
		if pkgKey == "" {
			continue
		}
		if err := mergeNodeContracts(bundle, view.Nodes, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectNodesFile}); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectEventsFile}); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectAgentsFile}); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectToolsFile}); err != nil {
			return err
		}
	}
	for _, flow := range bundle.Paths.Flows {
		view, ok := flowViewsByID[flow.ID]
		if !ok {
			continue
		}
		sourcePrefix := ContractItemSource{PackageKey: flow.PackageKey, FlowID: flow.ID, Layer: "flow"}
		if err := mergeNodeContracts(bundle, view.Nodes, contractSourceWithFile(sourcePrefix, view.Paths.NodesFile)); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, contractSourceWithFile(sourcePrefix, view.Paths.EventsFile)); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, contractSourceWithFile(sourcePrefix, view.Paths.AgentsFile)); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, contractSourceWithFile(sourcePrefix, view.Paths.ToolsFile)); err != nil {
			return err
		}
	}
	return nil
}
