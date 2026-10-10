package checkoutsource

import "testing"

func TestFixturePackageRoleUsesCanonicalInternalPaths(t *testing.T) {
	for _, test := range []struct {
		path string
		want bool
	}{
		{"internal/store/selected/selectedtest", true},
		{"internal/store/storetest", true},
		{"internal/store/testsql", true},
		{"internal/store/eventfixture", true},
		{"internal/store/testutil/nested", true},
		{"internal/testutil/nested", true},
		{"internal/runtime/testfixtures/nested", true},
		{"internal/runtime/ordinary", false},
		{"internal/store/selected/selectedtesting", false},
		{"internal/store/selected/selectedtestextra", false},
		{"internal/store/internal/selectedtest", false},
		{"selectedtest", false},
		{"store/selected/selectedtest", false},
		{"external/store/selected/selectedtest", false},
	} {
		t.Run(test.path, func(t *testing.T) {
			if got := IsTestFixturePackage(test.path); got != test.want {
				t.Fatalf("repository role=%v want=%v", got, test.want)
			}
			if got := IsTestFixturePackage("github.com/division-sh/swarm/" + test.path); got != test.want {
				t.Fatalf("module role=%v want=%v", got, test.want)
			}
			if IsTestFixturePackage("example.com/foreign/" + test.path) {
				t.Fatal("foreign module acquired an internal fixture role")
			}
		})
	}
}
