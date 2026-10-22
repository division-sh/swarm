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
