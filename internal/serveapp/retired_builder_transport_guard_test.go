package serveapp

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestRetiredBuilderSemanticReferencesStayExplicit(t *testing.T) {
	repoRoot := repoRootForTest()
	cmd := exec.Command("git", "-C", repoRoot, "ls-files", "-z")
	tracked, err := cmd.Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}

	semanticTokens := []string{
		"internal/" + "builder",
		"SWARM_" + "BUILDER_AUTH_TOKEN",
		"builder_" + "api",
		"dashboard/" + "Builder",
		"Dashboard/" + "Builder",
		"Builder/" + "operator",
		"Builder Web" + "Socket",
		"Builder " + "transport",
		"Builder " + "credentials",
		"Builder " + "API",
		"builder " + "RPC",
		"builder " + "state.get_entity",
		"builder " + "summaries",
		"Builder " + "state.get_entity",
		"Builder " + "run-summary",
	}
	expectedCounts := map[string]int{
		".github/audit-artifacts/issue-2378-failure-class.yaml":                        14,
		"internal/apiv1/handler_test.go":                                               1,
		"internal/apiv1/testdata/openrpc_compliance_matrix.yaml":                       1,
		"internal/apiv1/testdata/public_surface_backend_matrix.yaml":                   1,
		"internal/cliapp/api_consumption_boundary_test.go":                             3,
		"internal/cliapp/env_guard.go":                                                 1,
		"internal/cliapp/main_test.go":                                                 1,
		"internal/dashboard/server/observability_sql_test.go":                          1,
		"internal/runtime/destructivereset/contracts.go":                               1,
		"internal/runtime/destructivereset/coordinator_test.go":                        1,
		"internal/runtime/manager/runtime_reset_test.go":                               2,
		"internal/runtime/runforkexecution/readiness_classifier.go":                    1,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": 5,
		"openrpc.json":       2,
		"platform-spec.yaml": 47,
	}
	classificationDocuments := map[string]bool{
		".github/audit-artifacts/issue-2378-failure-class.yaml": true,
		"platform-spec.yaml": true,
	}
	rawRetirementFixtures := map[string]bool{
		"internal/apiv1/handler_test.go":                                               true,
		"internal/cliapp/api_consumption_boundary_test.go":                             true,
		"internal/runtime/manager/runtime_reset_test.go":                               true,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": true,
	}
	classificationMarkers := []string{
		"retir", "legacy", "removed", "historical", "fail-closed", "must not",
		"no direct", "not accepted", "invalid", "rejected", "consumer only", "consumers only",
	}

	actualCounts := map[string]int{}
	var violations []string
	for _, rawPath := range bytes.Split(tracked, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		relative := filepath.ToSlash(string(rawPath))
		body, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if readErr != nil {
			t.Fatalf("read tracked file %s: %v", relative, readErr)
		}
		if bytes.IndexByte(body, 0) >= 0 {
			continue
		}
		for index, line := range strings.Split(string(body), "\n") {
			matched := false
			for _, token := range semanticTokens {
				if strings.Contains(line, token) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			actualCounts[relative]++
			if classificationDocuments[relative] || rawRetirementFixtures[relative] {
				continue
			}
			lower := strings.ToLower(line)
			classified := false
			for _, marker := range classificationMarkers {
				if strings.Contains(lower, marker) {
					classified = true
					break
				}
			}
			if !classified {
				violations = append(violations, fmt.Sprintf("%s:%d: %s", relative, index+1, strings.TrimSpace(line)))
			}
		}
	}
	paths := make([]string, 0, len(actualCounts)+len(expectedCounts))
	seen := map[string]bool{}
	for path := range actualCounts {
		paths = append(paths, path)
		seen[path] = true
	}
	for path := range expectedCounts {
		if !seen[path] {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		if actualCounts[path] != expectedCounts[path] {
			violations = append(violations, fmt.Sprintf("%s: semantic reference count=%d, want %d", path, actualCounts[path], expectedCounts[path]))
		}
	}
	if len(violations) > 0 {
		t.Fatalf("retired Builder semantic references escaped the exact tracked-file classification:\n%s", strings.Join(violations, "\n"))
	}
}
