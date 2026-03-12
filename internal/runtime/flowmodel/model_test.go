package flowmodel

import "testing"

type testNode struct {
	ID       string
	Path     string
	URI      string
	Policy   PolicyDocument
	Parent   *testNode
	Children []testNode
}

func testChildren(node *testNode) []*testNode {
	if node == nil || len(node.Children) == 0 {
		return nil
	}
	out := make([]*testNode, 0, len(node.Children))
	for i := range node.Children {
		out = append(out, &node.Children[i])
	}
	return out
}

func TestResolvePolicyByID_WalksAncestorChain(t *testing.T) {
	root := &testNode{
		ID: "root",
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"root":   {Value: "yes"},
			"shared": {Value: "root"},
		}},
		Children: []testNode{{
			ID: "child",
			Policy: PolicyDocument{Values: map[string]PolicyValue{
				"child":  {Value: "yes"},
				"shared": {Value: "child"},
			}},
		}},
	}
	tree := Tree[testNode]{
		Root: root,
		ByID: map[string]*testNode{
			"root":  root,
			"child": &root.Children[0],
		},
	}
	base := PolicyDocument{Values: map[string]PolicyValue{
		"base": {Value: "yes"},
	}}

	got := ResolvePolicyByID(
		base,
		tree,
		"child",
		func(node *testNode) string { return node.ID },
		func(node *testNode) PolicyDocument { return node.Policy },
		testChildren,
	)

	if got.Values["base"].Value != "yes" {
		t.Fatalf("expected base policy to survive, got %#v", got.Values["base"].Value)
	}
	if got.Values["root"].Value != "yes" {
		t.Fatalf("expected root policy to merge, got %#v", got.Values["root"].Value)
	}
	if got.Values["child"].Value != "yes" {
		t.Fatalf("expected child policy to merge, got %#v", got.Values["child"].Value)
	}
	if got.Values["shared"].Value != "child" {
		t.Fatalf("expected child override for shared policy, got %#v", got.Values["shared"].Value)
	}
}

func TestMaterialize_RebuildsRecursiveValueTree(t *testing.T) {
	root := &BuildNode[testNode]{
		View: testNode{ID: "root"},
		Children: []*BuildNode[testNode]{
			{View: testNode{ID: "child-a"}},
			{
				View: testNode{ID: "child-b"},
				Children: []*BuildNode[testNode]{
					{View: testNode{ID: "grandchild"}},
				},
			},
		},
	}

	out := Materialize(
		root,
		func(node *testNode, children int) {
			node.Children = make([]testNode, 0, children)
			node.Parent = nil
		},
		func(node *testNode, child testNode) {
			node.Children = append(node.Children, child)
		},
	)

	if out.ID != "root" {
		t.Fatalf("expected root id, got %q", out.ID)
	}
	if len(out.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(out.Children))
	}
	if out.Children[0].ID != "child-a" {
		t.Fatalf("expected first child id child-a, got %q", out.Children[0].ID)
	}
	if out.Children[1].ID != "child-b" {
		t.Fatalf("expected second child id child-b, got %q", out.Children[1].ID)
	}
	if len(out.Children[1].Children) != 1 || out.Children[1].Children[0].ID != "grandchild" {
		t.Fatalf("expected nested child grandchild, got %#v", out.Children[1].Children)
	}
}

func TestPopulateScopedURIs_RegistersKindsByFlowScope(t *testing.T) {
	type scopedNode struct {
		FlowID    string
		Path      string
		Nodes     map[string]struct{}
		Agents    map[string]struct{}
		Events    map[string]struct{}
		NodeURIs  map[string]string
		AgentURIs map[string]string
		EventURIs map[string]string
	}

	node := &scopedNode{
		FlowID: "child",
		Path:   "root/child",
		Nodes:  map[string]struct{}{"n1": {}},
		Agents: map[string]struct{}{"a1": {}},
		Events: map[string]struct{}{"evt.created": {}},
	}
	registry := &URIRegistry{
		Scheme: "mas",
		Nodes:  map[string]URIRef{},
		Agents: map[string]URIRef{},
		Events: map[string]URIRef{},
		ByURI:  map[string]URIRef{},
	}

	PopulateScopedURIs(
		node,
		registry,
		func(node *scopedNode) string { return node.FlowID },
		func(node *scopedNode) string { return node.Path },
		func(node *scopedNode) map[string]struct{} { return node.Nodes },
		func(node *scopedNode) map[string]struct{} { return node.Agents },
		func(node *scopedNode) map[string]struct{} { return node.Events },
		func(node *scopedNode) *map[string]string { return &node.NodeURIs },
		func(node *scopedNode) *map[string]string { return &node.AgentURIs },
		func(node *scopedNode) *map[string]string { return &node.EventURIs },
	)

	if got := node.NodeURIs["n1"]; got != "mas://root/child/n1" {
		t.Fatalf("expected node URI, got %q", got)
	}
	if got := node.AgentURIs["a1"]; got != "mas://root/child/a1" {
		t.Fatalf("expected agent URI, got %q", got)
	}
	if got := node.EventURIs["evt.created"]; got != "mas://root/child/evt.created" {
		t.Fatalf("expected event URI, got %q", got)
	}
	if _, ok := registry.Nodes["child/n1"]; !ok {
		t.Fatal("expected node registry entry")
	}
	if _, ok := registry.Agents["child/a1"]; !ok {
		t.Fatal("expected agent registry entry")
	}
	if _, ok := registry.Events["child/evt.created"]; !ok {
		t.Fatal("expected event registry entry")
	}
}

func TestIndexAndPopulateScopedURIs_AssignsTreeAndScopedURIs(t *testing.T) {
	type scopedTreeNode struct {
		ID        string
		Path      string
		URI       string
		Parent    *scopedTreeNode
		Children  []scopedTreeNode
		Nodes     map[string]struct{}
		Agents    map[string]struct{}
		Events    map[string]struct{}
		NodeURIs  map[string]string
		AgentURIs map[string]string
		EventURIs map[string]string
	}

	children := func(node *scopedTreeNode) []*scopedTreeNode {
		if node == nil || len(node.Children) == 0 {
			return nil
		}
		out := make([]*scopedTreeNode, 0, len(node.Children))
		for i := range node.Children {
			out = append(out, &node.Children[i])
		}
		return out
	}

	root := &scopedTreeNode{
		Children: []scopedTreeNode{{
			ID:     "parent",
			Nodes:  map[string]struct{}{"n1": {}},
			Agents: map[string]struct{}{"a1": {}},
			Events: map[string]struct{}{"evt.parent": {}},
			Children: []scopedTreeNode{{
				ID:     "child",
				Nodes:  map[string]struct{}{"n2": {}},
				Agents: map[string]struct{}{"a2": {}},
				Events: map[string]struct{}{"evt.child": {}},
			}},
		}},
	}
	tree := &Tree[scopedTreeNode]{}
	registry := &URIRegistry{
		Scheme: "mas",
		Nodes:  map[string]URIRef{},
		Agents: map[string]URIRef{},
		Events: map[string]URIRef{},
		ByURI:  map[string]URIRef{},
	}

	IndexAndPopulateScopedURIs(
		root,
		tree,
		registry,
		func(node *scopedTreeNode) string { return node.ID },
		children,
		func(node *scopedTreeNode) *scopedTreeNode {
			return NearestAncestor(
				node,
				func(current *scopedTreeNode) *scopedTreeNode { return current.Parent },
				func(current *scopedTreeNode) bool { return current.ID != "" },
			)
		},
		func(node *scopedTreeNode, parent *scopedTreeNode) { node.Parent = parent },
		func(node *scopedTreeNode, path string) { node.Path = path },
		func(node *scopedTreeNode, uri string) { node.URI = uri },
		func(node *scopedTreeNode) string { return node.ID },
		func(node *scopedTreeNode) string { return node.Path },
		func(node *scopedTreeNode) map[string]struct{} { return node.Nodes },
		func(node *scopedTreeNode) map[string]struct{} { return node.Agents },
		func(node *scopedTreeNode) map[string]struct{} { return node.Events },
		func(node *scopedTreeNode) *map[string]string { return &node.NodeURIs },
		func(node *scopedTreeNode) *map[string]string { return &node.AgentURIs },
		func(node *scopedTreeNode) *map[string]string { return &node.EventURIs },
	)

	parent := &root.Children[0]
	child := &root.Children[0].Children[0]
	if parent.Path != "parent" || child.Path != "parent/child" {
		t.Fatalf("unexpected indexed paths parent=%q child=%q", parent.Path, child.Path)
	}
	if child.URI != "mas://parent/child" {
		t.Fatalf("expected child URI, got %q", child.URI)
	}
	if got := parent.NodeURIs["n1"]; got != "mas://parent/n1" {
		t.Fatalf("expected parent node URI, got %q", got)
	}
	if got := child.EventURIs["evt.child"]; got != "mas://parent/child/evt.child" {
		t.Fatalf("expected child event URI, got %q", got)
	}
	if tree.ByID["child"] != child {
		t.Fatal("expected child to be indexed by id")
	}
}

func TestIndexTree_AssignsPathsAndNearestEligibleParent(t *testing.T) {
	root := &testNode{
		Children: []testNode{{
			ID: "parent",
			Children: []testNode{{
				ID: "child",
			}},
		}},
	}
	tree := &Tree[testNode]{}
	registry := &URIRegistry{
		Scheme: "mas",
		Nodes:  map[string]URIRef{},
		Agents: map[string]URIRef{},
		Events: map[string]URIRef{},
		ByURI:  map[string]URIRef{},
	}

	IndexTree(
		root,
		nil,
		"",
		tree,
		registry,
		func(node *testNode) string { return node.ID },
		testChildren,
		func(node *testNode) *testNode {
			return NearestAncestor(
				node,
				func(current *testNode) *testNode { return current.Parent },
				func(current *testNode) bool { return current.ID != "" },
			)
		},
		func(node *testNode, parent *testNode) { node.Parent = parent },
		func(node *testNode, path string) { node.Path = path },
		func(node *testNode, uri string) { node.URI = uri },
	)

	parent := &root.Children[0]
	child := &root.Children[0].Children[0]

	if tree.ByID["parent"] != parent {
		t.Fatal("expected parent to be indexed by id")
	}
	if got := parent.Path; got != "parent" {
		t.Fatalf("expected parent path %q, got %q", "parent", got)
	}
	if got := child.Path; got != "parent/child" {
		t.Fatalf("expected child path %q, got %q", "parent/child", got)
	}
	if child.Parent != parent {
		t.Fatal("expected child parent to resolve to nearest eligible ancestor")
	}
	if got := child.URI; got != "mas://parent/child" {
		t.Fatalf("expected child URI %q, got %q", "mas://parent/child", got)
	}
}

func TestCloneViewTree_ClonesIndexesPolicyAndParents(t *testing.T) {
	type previewPaths struct {
		ID string
	}
	type previewView = View[previewPaths, struct{}, string, string, string, string]

	root := &previewView{
		Paths:    previewPaths{ID: "root"},
		Path:     "root",
		NodeURIs: map[string]string{"n1": "mas://root/n1"},
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"shared": {Value: "root"},
		}},
		Children: []previewView{{
			Paths: previewPaths{ID: "child"},
			Path:  "root/child",
			Policy: PolicyDocument{Values: map[string]PolicyValue{
				"child": {Value: "yes"},
			}},
		}},
	}

	cloned, byPath, byID := CloneViewTree(
		root,
		func(view *previewView) string { return view.Paths.ID },
		func(view *previewView) {
			ApplyPolicyOverrides(&view.Policy, map[string]any{"shared": "override"})
		},
	)

	if cloned == nil {
		t.Fatal("expected cloned tree")
	}
	if got := cloned.Policy.Values["shared"].Value; got != "override" {
		t.Fatalf("expected cloned root policy override, got %#v", got)
	}
	if got := root.Policy.Values["shared"].Value; got != "root" {
		t.Fatalf("expected original root policy unchanged, got %#v", got)
	}
	if len(cloned.Children) != 1 {
		t.Fatalf("expected 1 cloned child, got %d", len(cloned.Children))
	}
	child := &cloned.Children[0]
	if child.Parent != cloned {
		t.Fatal("expected cloned child parent to point at cloned root")
	}
	if byPath["root"] != cloned || byPath["root/child"] != child {
		t.Fatal("expected cloned nodes to be indexed by path")
	}
	if byID["root"] != cloned || byID["child"] != child {
		t.Fatal("expected cloned nodes to be indexed by id")
	}
	child.NodeURIs = map[string]string{"n2": "mas://root/child/n2"}
	cloned.NodeURIs["n1"] = "changed"
	if got := root.NodeURIs["n1"]; got != "mas://root/n1" {
		t.Fatalf("expected original URI map unchanged, got %q", got)
	}
}
