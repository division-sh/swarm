package flowidentity

import (
	"strings"
	"testing"
)

func TestSemanticScopeFromInstancePath(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{path: "", want: ""},
		{path: "child", want: ""},
		{path: "child/inst-1", want: "child"},
		{path: "child/grandchild/inst-1", want: "child/grandchild"},
	}
	for _, tc := range cases {
		if got := SemanticScopeFromInstancePath(tc.path); got != tc.want {
			t.Fatalf("SemanticScopeFromInstancePath(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestLogicalInstanceID(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{path: "", want: ""},
		{path: "child", want: "child"},
		{path: "child/inst-1", want: "inst-1"},
		{path: "child/grandchild/inst-1", want: "inst-1"},
	}
	for _, tc := range cases {
		if got := LogicalInstanceID(tc.path); got != tc.want {
			t.Fatalf("LogicalInstanceID(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestOwnedByFlow_UsesExactSemanticScope(t *testing.T) {
	if !OwnedByFlow(nil, "child", "child/inst-1") {
		t.Fatal("expected child to own child/inst-1")
	}
	if OwnedByFlow(nil, "child", "child/grandchild/inst-1") {
		t.Fatal("did not expect child to own child/grandchild/inst-1")
	}
	if OwnedByFlow(nil, "validation", "scoring/inst-1") {
		t.Fatal("did not expect validation to own scoring/inst-1")
	}
}

func TestOwnedByScope_UsesExactSemanticScope(t *testing.T) {
	if !OwnedByScope("child", "child/inst-1") {
		t.Fatal("expected child scope to own child/inst-1")
	}
	if OwnedByScope("child", "child/grandchild/inst-1") {
		t.Fatal("did not expect child scope to own child/grandchild/inst-1")
	}
	if !OwnedByScope("child/grandchild/great", "child/grandchild/great/inst-1") {
		t.Fatal("expected deep exact scope match to be owned")
	}
	if OwnedByScope("child/grandchild", "child/other/inst-1") {
		t.Fatal("did not expect different branch to be owned")
	}
}

func TestIsDescendant_DepthSafe(t *testing.T) {
	if IsDescendant("child", "child/inst-1") {
		t.Fatal("same flow instance must not count as descendant")
	}
	if !IsDescendant("child", "child/grandchild/inst-1") {
		t.Fatal("direct descendant should count as descendant")
	}
	if !IsDescendant("child", "child/grandchild/great/inst-1") {
		t.Fatal("deep descendant should count as descendant")
	}
}

func TestRecursiveIdentitySemantics_ArbitraryDepth(t *testing.T) {
	scope := "root"
	instancePath := "root/inst-1"
	if got := SemanticScopeFromInstancePath(instancePath); got != scope {
		t.Fatalf("depth=1 SemanticScopeFromInstancePath(%q) = %q, want %q", instancePath, got, scope)
	}
	if !OwnedByScope(scope, instancePath) {
		t.Fatalf("depth=1 OwnedByScope(%q, %q) = false, want true", scope, instancePath)
	}

	segments := []string{"root"}
	for depth := 2; depth <= 8; depth++ {
		segments = append(segments, "level")
		scope = strings.Join(segments, "/")
		instancePath = scope + "/inst-1"

		if got := SemanticScopeFromInstancePath(instancePath); got != scope {
			t.Fatalf("depth=%d SemanticScopeFromInstancePath(%q) = %q, want %q", depth, instancePath, got, scope)
		}
		if !OwnedByScope(scope, instancePath) {
			t.Fatalf("depth=%d OwnedByScope(%q, %q) = false, want true", depth, scope, instancePath)
		}

		parentScope := strings.Join(segments[:len(segments)-1], "/")
		if parentScope != "" && !IsDescendant(parentScope, instancePath) {
			t.Fatalf("depth=%d IsDescendant(%q, %q) = false, want true", depth, parentScope, instancePath)
		}
		if OwnedByScope(parentScope, instancePath) {
			t.Fatalf("depth=%d OwnedByScope(%q, %q) = true, want false", depth, parentScope, instancePath)
		}
	}
}
