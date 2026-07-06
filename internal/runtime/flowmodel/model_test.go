package flowmodel

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

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

func TestPolicyDocumentCriteriaIsTypedSectionNotGenericValue(t *testing.T) {
	var doc PolicyDocument
	if err := yaml.Unmarshal([]byte(`
threshold: 12
criteria:
  feasibility_exclusions:
    classes:
      hard: {disposition: cto.spec_vetoed}
    rules:
      - id: FX-HARD-01
        class: hard
        text: Requires regulated real-time integration.
        params:
          max_features: policy.max_features
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal PolicyDocument: %v", err)
	}

	if _, ok := doc.Values["criteria"]; ok {
		t.Fatalf("criteria leaked into generic policy values: %#v", doc.Values["criteria"])
	}
	if got := doc.Values["threshold"].Value; got != 12 {
		t.Fatalf("threshold value = %#v, want 12", got)
	}
	set, ok := doc.Criteria["feasibility_exclusions"]
	if !ok {
		t.Fatalf("criteria set missing: %#v", doc.Criteria)
	}
	if got := set.Classes["hard"].Disposition; got != "cto.spec_vetoed" {
		t.Fatalf("hard disposition = %q, want cto.spec_vetoed", got)
	}
	if len(set.Rules) != 1 || set.Rules[0].ID != "FX-HARD-01" {
		t.Fatalf("rules = %#v, want FX-HARD-01", set.Rules)
	}
	if got := set.Rules[0].Params["max_features"].Value; got != "policy.max_features" {
		t.Fatalf("param value = %#v, want policy.max_features", got)
	}
}

func TestPolicyDocumentValidationIsTypedSectionNotGenericValue(t *testing.T) {
	var doc PolicyDocument
	if err := yaml.Unmarshal([]byte(`
threshold: 12
validation:
  deploy_manifest:
    classes:
      invalid: {disposition: deploy.manifest_invalid}
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        check:
          equal: {left: input.source_ref, right: input.manifest_source_ref}
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal PolicyDocument: %v", err)
	}

	if _, ok := doc.Values["validation"]; ok {
		t.Fatalf("validation leaked into generic policy values: %#v", doc.Values["validation"])
	}
	set, ok := doc.Validation["deploy_manifest"]
	if !ok {
		t.Fatalf("validation set missing: %#v", doc.Validation)
	}
	if got := set.Classes["invalid"].Disposition; got != "deploy.manifest_invalid" {
		t.Fatalf("invalid disposition = %q, want deploy.manifest_invalid", got)
	}
	if got := set.Inputs["source_ref"]; got != "string" {
		t.Fatalf("input source_ref type = %q, want string", got)
	}
	if len(set.Rules) != 1 || set.Rules[0].ID != "VR-001" {
		t.Fatalf("rules = %#v, want VR-001", set.Rules)
	}
	if set.Rules[0].PinCandidate == nil || !*set.Rules[0].PinCandidate {
		t.Fatalf("pin_candidate = %#v, want true", set.Rules[0].PinCandidate)
	}
}

func TestPolicyDocumentValidationRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{
			name: "set unknown field",
			body: `
validation:
  deploy_manifest:
    schema: {}
    classes:
      invalid: {disposition: deploy.manifest_invalid}
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        check:
          equal: {left: input.source_ref, right: input.manifest_source_ref}
`,
			contains: `unsupported field "schema"`,
		},
		{
			name: "class unknown field",
			body: `
validation:
  deploy_manifest:
    classes:
      invalid:
        disposition: deploy.manifest_invalid
        retry: never
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        check:
          equal: {left: input.source_ref, right: input.manifest_source_ref}
`,
			contains: `unsupported field "retry"`,
		},
		{
			name: "rule unknown field",
			body: `
validation:
  deploy_manifest:
    classes:
      invalid: {disposition: deploy.manifest_invalid}
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        emit: deploy.manifest_invalid
        check:
          equal: {left: input.source_ref, right: input.manifest_source_ref}
`,
			contains: `unsupported field "emit"`,
		},
		{
			name: "check extra predicate",
			body: `
validation:
  deploy_manifest:
    classes:
      invalid: {disposition: deploy.manifest_invalid}
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        check:
          equal: {left: input.source_ref, right: input.manifest_source_ref}
          regex: {input: input.source_ref, pattern: "^[a-f0-9]+$"}
`,
			contains: `unsupported field "regex"`,
		},
		{
			name: "equal unknown field",
			body: `
validation:
  deploy_manifest:
    classes:
      invalid: {disposition: deploy.manifest_invalid}
    inputs:
      source_ref: string
      manifest_source_ref: string
    rules:
      - id: VR-001
        class: invalid
        text: Manifest source ref must match request source ref.
        pin_candidate: true
        check:
          equal:
            left: input.source_ref
            right: input.manifest_source_ref
            normalize: true
`,
			contains: `unsupported field "normalize"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var doc PolicyDocument
			err := yaml.Unmarshal([]byte(tt.body), &doc)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestResolvePolicyByID_MergesCriteriaAlongAncestorChain(t *testing.T) {
	root := &testNode{
		ID: "root",
		Policy: PolicyDocument{
			Criteria: map[string]PolicyCriteriaSet{
				"root_set": {
					Classes: map[string]PolicyCriteriaClass{"hard": {Disposition: "root.blocked"}},
					Rules:   []PolicyCriteriaRule{{ID: "ROOT-1", Class: "hard", Text: "Root rule."}},
				},
				"shared": {
					Classes: map[string]PolicyCriteriaClass{"soft": {Disposition: "root.revise"}},
					Rules:   []PolicyCriteriaRule{{ID: "ROOT-SHARED", Class: "soft", Text: "Root shared."}},
				},
			},
		},
		Children: []testNode{{
			ID: "child",
			Policy: PolicyDocument{
				Criteria: map[string]PolicyCriteriaSet{
					"child_set": {
						Classes: map[string]PolicyCriteriaClass{"allow": {Disposition: "none"}},
						Rules:   []PolicyCriteriaRule{{ID: "CHILD-1", Class: "allow", Text: "Child rule."}},
					},
					"shared": {
						Classes: map[string]PolicyCriteriaClass{"hard": {Disposition: "child.blocked"}},
						Rules:   []PolicyCriteriaRule{{ID: "CHILD-SHARED", Class: "hard", Text: "Child shared."}},
					},
				},
			},
		}},
	}
	tree := Tree[testNode]{
		Root: root,
		ByID: map[string]*testNode{
			"root":  root,
			"child": &root.Children[0],
		},
	}
	base := PolicyDocument{Criteria: map[string]PolicyCriteriaSet{
		"base_set": {
			Classes: map[string]PolicyCriteriaClass{"soft": {Disposition: "base.revise"}},
			Rules:   []PolicyCriteriaRule{{ID: "BASE-1", Class: "soft", Text: "Base rule."}},
		},
	}}

	got := ResolvePolicyByID(
		base,
		tree,
		"child",
		func(node *testNode) string { return node.ID },
		func(node *testNode) PolicyDocument { return node.Policy },
		testChildren,
	)

	for _, name := range []string{"base_set", "root_set", "child_set", "shared"} {
		if _, ok := got.Criteria[name]; !ok {
			t.Fatalf("merged criteria missing %q: %#v", name, got.Criteria)
		}
	}
	if gotID := got.Criteria["shared"].Rules[0].ID; gotID != "CHILD-SHARED" {
		t.Fatalf("shared criteria rule = %q, want child override", gotID)
	}
}

func TestResolvePolicyByID_MergesValidationAlongAncestorChain(t *testing.T) {
	falseValue := false
	trueValue := true
	root := &testNode{
		ID: "root",
		Policy: PolicyDocument{
			Validation: map[string]PolicyValidationSet{
				"root_set": {
					Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "root.invalid"}},
					Inputs:  map[string]string{"left": "string", "right": "string"},
					Rules: []PolicyValidationRule{{
						ID:           "ROOT-1",
						Class:        "invalid",
						Text:         "Root validation.",
						PinCandidate: &falseValue,
						Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.left", Right: "input.right"}},
					}},
				},
				"shared": {
					Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "root.shared_invalid"}},
					Inputs:  map[string]string{"left": "string", "right": "string"},
					Rules: []PolicyValidationRule{{
						ID:           "ROOT-SHARED",
						Class:        "invalid",
						Text:         "Root shared.",
						PinCandidate: &falseValue,
						Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.left", Right: "input.right"}},
					}},
				},
			},
		},
		Children: []testNode{{
			ID: "child",
			Policy: PolicyDocument{
				Validation: map[string]PolicyValidationSet{
					"child_set": {
						Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "child.invalid"}},
						Inputs:  map[string]string{"left": "string", "right": "string"},
						Rules: []PolicyValidationRule{{
							ID:           "CHILD-1",
							Class:        "invalid",
							Text:         "Child validation.",
							PinCandidate: &trueValue,
							Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.left", Right: "input.right"}},
						}},
					},
					"shared": {
						Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "child.shared_invalid"}},
						Inputs:  map[string]string{"left": "string", "right": "string"},
						Rules: []PolicyValidationRule{{
							ID:           "CHILD-SHARED",
							Class:        "invalid",
							Text:         "Child shared.",
							PinCandidate: &trueValue,
							Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.left", Right: "input.right"}},
						}},
					},
				},
			},
		}},
	}
	tree := Tree[testNode]{
		Root: root,
		ByID: map[string]*testNode{
			"root":  root,
			"child": &root.Children[0],
		},
	}
	base := PolicyDocument{Validation: map[string]PolicyValidationSet{
		"base_set": {
			Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "base.invalid"}},
			Inputs:  map[string]string{"left": "string", "right": "string"},
			Rules: []PolicyValidationRule{{
				ID:           "BASE-1",
				Class:        "invalid",
				Text:         "Base validation.",
				PinCandidate: &falseValue,
				Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.left", Right: "input.right"}},
			}},
		},
	}}

	got := ResolvePolicyByID(
		base,
		tree,
		"child",
		func(node *testNode) string { return node.ID },
		func(node *testNode) PolicyDocument { return node.Policy },
		testChildren,
	)

	for _, name := range []string{"base_set", "root_set", "child_set", "shared"} {
		if _, ok := got.Validation[name]; !ok {
			t.Fatalf("merged validation missing %q: %#v", name, got.Validation)
		}
	}
	if gotID := got.Validation["shared"].Rules[0].ID; gotID != "CHILD-SHARED" {
		t.Fatalf("shared validation rule = %q, want child override", gotID)
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
		Scheme: "swarm",
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

	if got := node.NodeURIs["n1"]; got != "swarm://root/child/n1" {
		t.Fatalf("expected node URI, got %q", got)
	}
	if got := node.AgentURIs["a1"]; got != "swarm://root/child/a1" {
		t.Fatalf("expected agent URI, got %q", got)
	}
	if got := node.EventURIs["evt.created"]; got != "swarm://root/child/evt.created" {
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
		Scheme: "swarm",
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
	if child.URI != "swarm://parent/child" {
		t.Fatalf("expected child URI, got %q", child.URI)
	}
	if got := parent.NodeURIs["n1"]; got != "swarm://parent/n1" {
		t.Fatalf("expected parent node URI, got %q", got)
	}
	if got := child.EventURIs["evt.child"]; got != "swarm://parent/child/evt.child" {
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
		Scheme: "swarm",
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
	if got := child.URI; got != "swarm://parent/child" {
		t.Fatalf("expected child URI %q, got %q", "swarm://parent/child", got)
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
		NodeURIs: map[string]string{"n1": "swarm://root/n1"},
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
	child.NodeURIs = map[string]string{"n2": "swarm://root/child/n2"}
	cloned.NodeURIs["n1"] = "changed"
	if got := root.NodeURIs["n1"]; got != "swarm://root/n1" {
		t.Fatalf("expected original URI map unchanged, got %q", got)
	}
}
