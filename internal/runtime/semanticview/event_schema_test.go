package semanticview

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestResolveEventSchema_ReportsUnresolvedTypesAfterBundleResolution(t *testing.T) {
	root := &runtimecontracts.FlowContractView{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"handoff.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"evidence": {Type: "NotDeclared"},
					},
					Required: []string{"evidence"},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
		},
	}

	resolution := ResolveEventSchema(Wrap(bundle), "", "handoff.completed")
	if !resolution.HasSchema {
		t.Fatal("expected event schema resolution")
	}
	if len(resolution.UnresolvedTypes) != 1 || resolution.UnresolvedTypes[0] != "NotDeclared" {
		t.Fatalf("UnresolvedTypes = %#v, want [NotDeclared]", resolution.UnresolvedTypes)
	}
	if err := resolution.UnresolvedTypeError(); err == nil || !strings.Contains(err.Error(), "NotDeclared") {
		t.Fatalf("UnresolvedTypeError = %v, want NotDeclared", err)
	}
}

func TestResolveFlowEventProof_TemplateInstanceOutputUsesTemplateCatalog(t *testing.T) {
	root := runtimecontracts.FlowContractView{}
	root.Children = []runtimecontracts.FlowContractView{{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", Mode: "template"},
		Path:  "child",
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"child.done"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.done": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"step": {Type: "string"},
					},
					Required: []string{"step"},
				},
			},
		},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0],
			},
		},
	}

	proof := ResolveFlowEventProof(Wrap(bundle), "child", "child/inst-1/child.done")
	if !proof.HasSchema {
		t.Fatal("expected concrete template instance event to resolve template schema proof")
	}
	if proof.Local != "child.done" {
		t.Fatalf("Local = %q, want child.done", proof.Local)
	}
	if proof.CatalogKey != "child.done" {
		t.Fatalf("CatalogKey = %q, want child.done", proof.CatalogKey)
	}
	if proof.EventKey() != "child/inst-1/child.done" {
		t.Fatalf("EventKey = %q, want concrete event identity", proof.EventKey())
	}
	if proof.Entry.Payload.Properties["step"].Type != "string" {
		t.Fatalf("Entry payload = %#v, want step string", proof.Entry.Payload)
	}
}

func TestResolveFlowEventProof_TemplateDescendantPathDoesNotBecomeInstanceLocalEvent(t *testing.T) {
	root := runtimecontracts.FlowContractView{}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", Mode: "template"},
		Path:  "child",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild", Mode: "static"},
		Path:  "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child.Children = []runtimecontracts.FlowContractView{grandchild}
	root.Children = []runtimecontracts.FlowContractView{child}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"child":            &root.Children[0],
				"child/grandchild": &root.Children[0].Children[0],
			},
		},
	}

	proof := ResolveFlowEventProof(Wrap(bundle), "child", "child/grandchild/micro.done")
	if proof.Local == "micro.done" {
		t.Fatalf("Local = %q, want descendant path to remain non-local for child proof", proof.Local)
	}
}
