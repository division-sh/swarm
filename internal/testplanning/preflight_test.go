package testplanning

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPreflightSelectionRefusesMissingDuplicateAndForeignRoots(t *testing.T) {
	root := TestRoot{Package: "github.com/division-sh/swarm/proof", Name: "TestGuard"}
	build := BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1", GOVERSION: "go1.26.8"}
	inventory := RootInventory{BuildContext: build, Packages: map[string]PackageRoots{
		root.Package: {Package: root.Package, HasTestFiles: true, Roots: []string{root.Name}},
	}}
	for _, name := range []string{"valid", "missing", "duplicate", "foreign", "child", "unbound-build"} {
		t.Run(name, func(t *testing.T) {
			selected := []RequiredTest{{TestRoot: root, Children: []string{"positive", "refusal"}}}
			current := inventory
			switch name {
			case "missing":
				selected[0].Name = "TestMissing"
			case "duplicate":
				selected = append(selected, selected[0])
			case "foreign":
				selected[0].Package = "foreign"
			case "child":
				selected[0].Name += "/positive"
			case "unbound-build":
				current.BuildContext.GOVERSION = ""
			}
			plan, err := bindPreflightSelection(strings.Repeat("a", 40), strings.Repeat("b", 40), current, selected)
			if name == "valid" {
				if err != nil || plan.Validate() != nil || len(plan.Packages) != 1 || len(plan.Packages[0].Required) != 1 {
					t.Fatalf("valid finite selection: %+v / %v", plan, err)
				}
			} else if err == nil {
				t.Fatalf("accepted %s selection", name)
			}
		})
	}
}

func TestPreflightPlanIntegrityAndSourceRefusals(t *testing.T) {
	root := TestRoot{Package: "github.com/division-sh/swarm/proof", Name: "TestGuard"}
	inventory := RootInventory{BuildContext: BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1", GOVERSION: "go1.26.8"}, Packages: map[string]PackageRoots{
		root.Package: {Package: root.Package, HasTestFiles: true, Roots: []string{root.Name}},
	}}
	plan, err := bindPreflightSelection(strings.Repeat("a", 40), strings.Repeat("b", 40), inventory, []RequiredTest{{TestRoot: root}})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"source", "base", "build", "selector", "digest", "empty", "nonhex-source"} {
		t.Run(change, func(t *testing.T) {
			mutant := plan
			mutant.Packages = append([]PreflightPackage(nil), plan.Packages...)
			switch change {
			case "source":
				mutant.HeadSHA = ""
			case "base":
				mutant.BaseSHA = ""
			case "build":
				mutant.BuildContext.GOVERSION = "go1.25.0"
			case "selector":
				mutant.Packages[0].Run = "^Test.*$"
			case "digest":
				mutant.Digest = strings.Repeat("0", 64)
			case "empty":
				mutant.Packages = nil
			case "nonhex-source":
				mutant.HeadSHA = strings.Repeat("x", 40)
				mutant.Digest = mutant.digest()
			}
			if mutant.Validate() == nil {
				t.Fatalf("accepted %s mutation", change)
			}
		})
	}
	if TierRank("preflight") != 0 {
		t.Fatal("local preflight became a hosted tier")
	}
	if tier, _ := CITier("CI-Tier: preflight"); tier != ProfileFull {
		t.Fatal("invalid hosted tier no longer fails conservatively")
	}
}

func TestPreflightStructuralSelectionUsesCanonicalOwner(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := StaticAuthorityGuardRoots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, staticAuthorityGuardRoots(t, repo)) || len(actual) != 81 {
		t.Fatalf("canonical structural selection changed: %d", len(actual))
	}
	if len(preflightCandidates) != 31 {
		t.Fatalf("reviewed candidate count changed: %d", len(preflightCandidates))
	}
	for _, root := range preflightCandidates {
		for _, excluded := range []string{"TestPersistenceAuthorityDebtRatchet", "TestReviewedSnapshotCandidateOverlayTypeChecks", "TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners"} {
			if root.Name == excluded {
				t.Fatalf("unadmitted root %s entered preflight", root.Name)
			}
		}
	}
	copyRoot := t.TempDir()
	for dir, files := range staticAuthorityGuardFiles {
		if err := os.MkdirAll(filepath.Join(copyRoot, dir), 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range files {
			raw, err := os.ReadFile(filepath.Join(repo, dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(copyRoot, dir, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if roots, err := StaticAuthorityGuardRoots(copyRoot); err != nil || !reflect.DeepEqual(actual, roots) {
		t.Fatalf("exact source copy differs: %v", err)
	}
	path := filepath.Join(copyRoot, "internal/runtime", staticAuthorityGuardFiles["internal/runtime"][0])
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := StaticAuthorityGuardRoots(copyRoot); err == nil {
		t.Fatal("missing canonical source silently shrank selection")
	}
	if err := os.WriteFile(path, []byte("package broken\nfunc"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := StaticAuthorityGuardRoots(copyRoot); err == nil {
		t.Fatal("malformed canonical source granted selection")
	}
}
