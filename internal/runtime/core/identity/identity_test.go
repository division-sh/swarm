package identity

import "testing"

func TestNormalizeIDs(t *testing.T) {
	if got := NormalizeEntityID(" ent-1 ").String(); got != "ent-1" {
		t.Fatalf("entity id = %q, want ent-1", got)
	}
	if got := NormalizeNodeID(" node-a ").String(); got != "node-a" {
		t.Fatalf("node id = %q, want node-a", got)
	}
	if got := NormalizeFlowID(" flow/root ").String(); got != "flow/root" {
		t.Fatalf("flow id = %q, want flow/root", got)
	}
}

func TestWorkflowURIResolve(t *testing.T) {
	base := MustParseWorkflowURI("root/child")
	got, err := base.Resolve("../sibling/node")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.String() != "root/sibling/node" {
		t.Fatalf("resolved uri = %q, want root/sibling/node", got.String())
	}
}

func TestWorkflowURIParent(t *testing.T) {
	uri := MustParseWorkflowURI("root/child/node")
	if got := uri.Parent().String(); got != "root/child" {
		t.Fatalf("parent = %q, want root/child", got)
	}
}
