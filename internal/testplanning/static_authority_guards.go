package testplanning

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
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

// StaticAuthorityGuardRoots is the source-owned inventory shared by canonical
// tier coverage and local preflight. It carries no execution credit.
func StaticAuthorityGuardRoots(repo string) ([]TestRoot, error) {
	roots := []TestRoot{{Package: "github.com/division-sh/swarm/internal/store/internal/runtimepersistence", Name: "TestRunForkRevisionStateAccessorInventoryIsClosed"}}
	for dir, files := range staticAuthorityGuardFiles {
		for _, name := range files {
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repo, dir, name), nil, parser.ParseComments|parser.AllErrors)
			if err != nil {
				return nil, err
			}
			for _, name := range executableTestRoots(file) {
				roots = append(roots, TestRoot{Package: "github.com/division-sh/swarm/" + dir, Name: name})
			}
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].Package+roots[i].Name < roots[j].Package+roots[j].Name
	})
	return roots, nil
}
