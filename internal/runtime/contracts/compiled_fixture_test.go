package contracts

// Direct root-only component fixtures still need the declaration owner normally
// supplied by the loader before their semantic consumers may inspect schemas.
func compileRootContractTestFixture(bundle *WorkflowContractBundle) {
	root := &FlowContractView{
		Path: ".", Paths: FlowContractPaths{FlowPath: "."},
		Events: bundle.Events, Nodes: bundle.Nodes, Tools: bundle.Tools,
	}
	if bundle.RootSchema != nil {
		root.Schema = *bundle.RootSchema
	}
	bundle.FlowTree = FlowTree{Root: root, ByID: map[string]*FlowContractView{".": root}, ByPath: map[string]*FlowContractView{".": root}}
	if err := CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
}
