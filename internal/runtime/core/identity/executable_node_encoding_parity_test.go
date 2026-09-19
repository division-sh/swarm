package identity

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestIdentityEncodingRetainsAdmissionAndBytes(t *testing.T) {
	flows := []string{"", ".", "child", "parent/child", "../child", "/child", " child", "child/", "child//nested", "child\\nested", "Child", "child/\x00", "child/\u00e9", strings.Repeat("a/", 31) + "b", strings.Repeat("a/", 32) + "b", strings.Repeat("a", 100), strings.Repeat("a", 101)}
	families := []string{"", "node", "fan_out", " node", "Node", "node/other", "node\x00"}
	paths := []string{"", "worker", "worker/child", `nodes["worker"].rules[0]`, "worker name", "\u00e9", " worker", "worker\r", "worker\n", "worker\x00"}
	for _, flow := range flows {
		for _, family := range families {
			for _, path := range paths {
				assertIdentityEncodingParity(t, flow, family, path)
			}
		}
	}
}

func FuzzIdentityEncodingAdmissionParity(f *testing.F) {
	for _, seed := range [][3]string{{".", "node", "worker"}, {"a/b/c", "node", "worker"}, {"/bad", "node", "worker"}, {".", "fan_out", "worker\x00"}, {"", "", ""}} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, flow, family, path string) {
		assertIdentityEncodingParity(t, flow, family, path)
	})
}

func assertIdentityEncodingParity(t *testing.T, flow, family, path string) {
	t.Helper()
	d := DeclarationIdentity{flow: FlowIdentity{value: flow}, family: family, semanticPath: path}
	// The former accessor-based validation is an independent reference, including
	// invalid private values that cannot be constructed through public admission.
	parsed, err := AdmitDeclarationIdentity(d.flow.String(), family, path)
	wantValid := err == nil && parsed == d
	if d.Valid() != wantValid {
		t.Fatalf("declaration validation changed for %q/%q/%q", flow, family, path)
	}
	encode := func(parts ...string) string {
		for i := range parts {
			parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
		}
		return strings.Join(parts, ".")
	}
	wantDeclaration, wantNode, wantFlow, wantID := "", "", "", ""
	if wantValid {
		wantDeclaration = encode(flow, family, path)
		if family == "node" {
			wantNode, wantFlow, wantID = encode(flow, path), flow, path
		}
	}
	node := ExecutableNode{declaration: d}
	if d.Key() != wantDeclaration || node.Key() != wantNode || node.FlowPath() != wantFlow || node.NodeID() != wantID || node.Valid() != (wantValid && family == "node") {
		t.Fatalf("identity encoding changed for %q/%q/%q: declaration=%q node=%q flow=%q id=%q", flow, family, path, d.Key(), node.Key(), node.FlowPath(), node.NodeID())
	}
	if wantDeclaration != "" {
		restored, err := ParseDeclarationIdentityKey(wantDeclaration)
		if err != nil || restored != d {
			t.Fatalf("declaration key no longer round trips: %v", err)
		}
	}
	if wantNode != "" {
		restored, err := ParseExecutableNodeKey(wantNode)
		if err != nil || restored != node {
			t.Fatalf("node key no longer round trips: %v", err)
		}
	}
}
