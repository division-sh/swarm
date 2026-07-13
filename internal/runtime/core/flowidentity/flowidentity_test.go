package flowidentity

import (
	"strings"
	"testing"
)

func TestEntityID_UsesCanonicalRefDerivation(t *testing.T) {
	const uuidRef = "11111111-1111-1111-1111-111111111111"
	if got := EntityID(uuidRef); got != uuidRef {
		t.Fatalf("EntityID(%q) = %q, want preserved uuid", uuidRef, got)
	}

	if got := EntityID("child"); got == "" || got == "child" {
		t.Fatalf("EntityID(child) = %q, want canonical hashed id", got)
	}

	pathID := EntityID("child/inst-1")
	if pathID == "" || pathID == "child/inst-1" {
		t.Fatalf("EntityID(child/inst-1) = %q, want canonical hashed id", pathID)
	}
	if got := EntityID("/child/inst-1/"); got != pathID {
		t.Fatalf("EntityID(/child/inst-1/) = %q, want %q", got, pathID)
	}
}

func TestLookupKeys_UsesCanonicalEntityID(t *testing.T) {
	cases := []struct {
		name string
		ref  string
		want []string
	}{
		{
			name: "uuid ref",
			ref:  "11111111-1111-1111-1111-111111111111",
			want: []string{"11111111-1111-1111-1111-111111111111"},
		},
		{
			name: "bare non-uuid ref",
			ref:  "child",
			want: []string{EntityID("child")},
		},
		{
			name: "path ref",
			ref:  "child/inst-1",
			want: []string{EntityID("child/inst-1")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := LookupKeys(tc.ref)
			if len(got) != len(tc.want) {
				t.Fatalf("LookupKeys(%q) len = %d, want %d (%v)", tc.ref, len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("LookupKeys(%q)[%d] = %q, want %q", tc.ref, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestStandingForService_UsesCanonicalStableFlowInstanceRoute(t *testing.T) {
	serviceID := StandingServiceID("root", "service")
	instance := StandingForService(nil, "service", serviceID)
	if instance.ScopeKey != "service" || instance.InstanceID != serviceID || instance.InstancePath != "service/"+serviceID {
		t.Fatalf("standing identity = %#v, want canonical service route", instance)
	}
	if instance.EntityID != EntityID(instance.InstancePath) || !OwnedByFlow(nil, "service", instance.InstancePath) {
		t.Fatalf("standing route ownership = entity:%q owned:%v", instance.EntityID, OwnedByFlow(nil, "service", instance.InstancePath))
	}
}

func TestSemanticScope_DistinguishesStaticAndInstancedPaths(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{path: "", want: ""},
		{path: "child", want: "child"},
		{path: "child/inst-1", want: "child"},
		{path: "child/grandchild/inst-1", want: "child/grandchild"},
	}
	for _, tc := range cases {
		if got := SemanticScope(tc.path); got != tc.want {
			t.Fatalf("SemanticScope(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestSemanticScopeFromFlowInstanceRef_DistinguishesStaticScopeFromRootID(t *testing.T) {
	cases := []struct {
		ref  string
		want string
	}{
		{ref: "", want: ""},
		{ref: "provider", want: "provider"},
		{ref: "provider/inst-1", want: "provider"},
		{ref: "parent/provider/inst-1", want: "parent/provider"},
		{ref: "11111111-1111-4111-8111-111111111111", want: ""},
	}
	for _, tc := range cases {
		if got := SemanticScopeFromFlowInstanceRef(tc.ref); got != tc.want {
			t.Fatalf("SemanticScopeFromFlowInstanceRef(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

func TestStoredCoordinates_SeparateScopeFromConcretePath(t *testing.T) {
	cases := []struct {
		name         string
		workflowName string
		flowPath     string
		wantScope    string
		wantPath     string
	}{
		{
			name:         "static flow row uses semantic scope as path",
			workflowName: "child",
			wantScope:    "child",
			wantPath:     "child",
		},
		{
			name:         "template flow row keeps concrete path",
			workflowName: "grandchild",
			flowPath:     "child/grandchild/inst-1",
			wantScope:    "child/grandchild",
			wantPath:     "child/grandchild/inst-1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Stored(nil, tc.workflowName, tc.flowPath, "", "", "")
			if got.ScopeKey != tc.wantScope {
				t.Fatalf("Stored(%q, %q).ScopeKey = %q, want %q", tc.workflowName, tc.flowPath, got.ScopeKey, tc.wantScope)
			}
			if got.InstancePath != tc.wantPath {
				t.Fatalf("Stored(%q, %q).InstancePath = %q, want %q", tc.workflowName, tc.flowPath, got.InstancePath, tc.wantPath)
			}
			if got.HasStoredPath != (tc.flowPath != "") {
				t.Fatalf("Stored(%q, %q).HasStoredPath = %v, want %v", tc.workflowName, tc.flowPath, got.HasStoredPath, tc.flowPath != "")
			}
		})
	}
}

func TestStoredPersisted_KeepsStorageRefSeparateFromSemanticIdentity(t *testing.T) {
	got, err := StoredPersisted(nil, "child", "11111111-1111-1111-1111-111111111111", "", "11111111-1111-1111-1111-111111111111", "", "")
	if err != nil {
		t.Fatalf("StoredPersisted(static): %v", err)
	}
	if got.StorageRef != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("StorageRef = %q, want preserved uuid storage ref", got.StorageRef)
	}
	if got.ScopeKey != "child" {
		t.Fatalf("ScopeKey = %q, want child", got.ScopeKey)
	}
	if got.InstancePath != "child" {
		t.Fatalf("InstancePath = %q, want child", got.InstancePath)
	}
	if got.InstanceID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("InstanceID = %q, want preserved logical id", got.InstanceID)
	}
}

func TestStoredPersisted_RejectsInstanceIDThatDisagreesWithConcretePath(t *testing.T) {
	_, err := StoredPersisted(nil, "grandchild", "child/grandchild/inst-1", "child/grandchild/inst-1", "inst-2", "", "")
	if err == nil {
		t.Fatal("expected StoredPersisted to reject mismatched instance id")
	}
	if !strings.Contains(err.Error(), "disagrees with flow_instance_path") {
		t.Fatalf("StoredPersisted error = %v, want disagreement message", err)
	}
}

func TestStoredRoute_CanonicalizesScopeInstanceAndPath(t *testing.T) {
	route := StoredRoute("", "", "child/grandchild/inst-1")
	if !route.Valid() {
		t.Fatal("expected stored route from instance path to be valid")
	}
	if route.ScopeKey != "child/grandchild" {
		t.Fatalf("StoredRoute scope = %q, want child/grandchild", route.ScopeKey)
	}
	if route.InstanceID != "inst-1" {
		t.Fatalf("StoredRoute instance id = %q, want inst-1", route.InstanceID)
	}
	if route.InstancePath != "child/grandchild/inst-1" {
		t.Fatalf("StoredRoute instance path = %q, want child/grandchild/inst-1", route.InstancePath)
	}

	derived := DeriveRoute("child/grandchild", "inst-1")
	if derived != route {
		t.Fatalf("DeriveRoute = %#v, want %#v", derived, route)
	}

	instance := Stored(nil, "grandchild", "child/grandchild/inst-1", "inst-1", "", "")
	if got := instance.Route(); got != route {
		t.Fatalf("Instance.Route() = %#v, want %#v", got, route)
	}
}

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
