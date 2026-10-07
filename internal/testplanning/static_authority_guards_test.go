package testplanning

import (
	"fmt"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

const staticAuthorityGuardUnit = "core-structural-owner-guards"

var staticAuthorityGuardFiles = map[string][]string{
	"internal/store/internal/runtimepersistence": {
		"event_boundary_guard_test.go", "delivery_lifecycle_boundary_guard_test.go",
		"run_lifecycle_ownership_guard_test.go", "candidate_handoff_retry_guard_test.go",
		"run_fork_revision_exact_contributor_guard_test.go", "run_fork_revision_retry_owner_guard_test.go",
		"run_bundle_identity_eventbus_group_guard_test.go", "standing_restart_disposition_guard_test.go",
		"store_abstraction_guard_matrix_test.go", "dynamic_agent_topology_static_test.go",
	},
	"internal/runtime": {
		"channel_activation_ownership_guard_test.go", "context_visibility_owner_guard_test.go",
		"event_schema_presence_owner_guard_test.go", "persistence_ownership_guard_test.go",
		"run_scoped_live_identity_guard_test.go", "semantic_ownership_guard_test.go",
	},
	"internal/serveapp":   {"retired_builder_transport_guard_test.go", "retired_dashboard_transport_guard_test.go"},
	"internal/releasee2e": {"boundary_test.go"},
}

func staticAuthorityGuardRoots(t *testing.T, repo string) []TestRoot {
	t.Helper()
	roots := []TestRoot{{Package: "github.com/division-sh/swarm/internal/store/internal/runtimepersistence", Name: "TestRunForkRevisionStateAccessorInventoryIsClosed"}}
	for dir, files := range staticAuthorityGuardFiles {
		for _, name := range files {
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repo, dir, name), nil, parser.ParseComments|parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range executableTestRoots(file) {
				roots = append(roots, TestRoot{Package: "github.com/division-sh/swarm/" + dir, Name: name})
			}
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].Package+roots[i].Name < roots[j].Package+roots[j].Name
	})
	if len(roots) != 79 {
		t.Fatalf("static authority source census changed: %d roots, want 79", len(roots))
	}
	return roots
}

func staticAuthorityGuardSelection(roots []TestRoot) string {
	var names []string
	for _, root := range roots {
		names = append(names, root.Name)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	return "^(" + strings.Join(names, "|") + ")$"
}

func assertStaticAuthorityGuardCoverage(t *testing.T, repo string, plan RunPlan) {
	t.Helper()
	if err := validateStaticAuthorityGuardCoverage(plan, staticAuthorityGuardRoots(t, repo)); err != nil {
		t.Fatal(err)
	}
}

func validateStaticAuthorityGuardCoverage(plan RunPlan, roots []TestRoot) error {
	selected, required := map[TestRoot]int{}, map[TestRoot]int{}
	for _, unit := range plan.Units {
		for _, root := range unit.SelectedRoots {
			selected[root]++
		}
		for _, root := range unit.RequiredTests {
			required[root.TestRoot]++
		}
	}
	for _, root := range roots {
		if selected[root] != 1 || required[root] != 1 {
			return fmt.Errorf("%s static guard %s/%s has selected=%d required=%d owners, want one complete proof", plan.Profile, root.Package, root.Name, selected[root], required[root])
		}
	}
	unit, err := plan.Unit(staticAuthorityGuardUnit)
	if plan.Profile != ProfileCore {
		if err == nil {
			return fmt.Errorf("%s duplicated its existing guard owners with the core-only unit", plan.Profile)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return validateStaticAuthorityGuardEnvelope(unit, roots)
}

func validateStaticAuthorityGuardEnvelope(unit ProofUnit, roots []TestRoot) error {
	wantPackages := []string{
		"github.com/division-sh/swarm/internal/releasee2e", "github.com/division-sh/swarm/internal/runtime",
		"github.com/division-sh/swarm/internal/serveapp", "github.com/division-sh/swarm/internal/store/internal/runtimepersistence",
	}
	if !slices.Equal(unit.Packages, wantPackages) || unit.Run != staticAuthorityGuardSelection(roots) ||
		unit.Skip != "" || unit.CountMode != "count-1" || unit.GoTimeout != "" ||
		unit.EnvironmentID != "ci-postgres-gateway-empty-v1" || unit.BudgetClass != "broad" ||
		len(unit.SelectedRoots) != len(roots) || len(unit.RequiredTests) != len(roots) || len(unit.DeferredTests) != 0 || len(unit.RequiredChildren) != 0 {
		return fmt.Errorf("static authority guard unit changed its finite complete-proof envelope: %+v", unit)
	}
	return nil
}

func TestStaticAuthorityGuardCoverageRejectsMissingDuplicateAndDeferredProof(t *testing.T) {
	root := TestRoot{Package: "github.com/division-sh/swarm/internal/runtime", Name: "TestClosedOwner"}
	for _, change := range []string{"missing", "missing-name", "extra-name", "duplicate", "deferred", "broad", "partial", "cached", "budget", "timeout"} {
		t.Run(change, func(t *testing.T) {
			unit := ProofUnit{
				ID: staticAuthorityGuardUnit, Packages: []string{
					"github.com/division-sh/swarm/internal/releasee2e", "github.com/division-sh/swarm/internal/runtime",
					"github.com/division-sh/swarm/internal/serveapp", "github.com/division-sh/swarm/internal/store/internal/runtimepersistence",
				}, Run: staticAuthorityGuardSelection([]TestRoot{root}), CountMode: "count-1",
				EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "broad",
				SelectedRoots: []TestRoot{root}, RequiredTests: []RequiredTest{{TestRoot: root}},
			}
			plan := RunPlan{Profile: ProfileCore, Units: []ProofUnit{unit}}
			if err := validateStaticAuthorityGuardCoverage(plan, []TestRoot{root}); err != nil {
				t.Fatalf("valid negative-control premise: %v", err)
			}
			switch change {
			case "missing":
				plan.Units = nil
			case "missing-name":
				plan.Units[0].Run = "^$"
			case "extra-name":
				plan.Units[0].Run = "^(TestClosedOwner|TestForeignWork)$"
			case "duplicate":
				plan.Units = append(plan.Units, unit)
			case "deferred":
				plan.Units[0].RequiredTests = nil
				plan.Units[0].DeferredTests = []DeferredTest{{TestRoot: root, Reason: "not execution"}}
			case "broad":
				plan.Units[0].Run = "^Test.*$"
			case "partial":
				plan.Units[0].Run += "/sqlite"
			case "cached":
				plan.Units[0].CountMode = "cache-default"
			case "budget":
				plan.Units[0].BudgetClass = "full"
			case "timeout":
				plan.Units[0].GoTimeout = "30m"
			}
			if validateStaticAuthorityGuardCoverage(plan, []TestRoot{root}) == nil {
				t.Fatal("invalid static guard coverage or execution envelope was accepted")
			}
		})
	}
}
