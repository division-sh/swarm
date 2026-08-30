package serveapp

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var retiredBuilderCandidatePattern = regexp.MustCompile(`(?i)builder`)

type retiredBuilderCandidate struct {
	line int
	text string
}

func TestRetiredBuilderSemanticReferencesStayExplicit(t *testing.T) {
	repoRoot := repoRootForTest()
	cmd := exec.Command("git", "-C", repoRoot, "ls-files", "-z")
	tracked, err := cmd.Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}

	expectedCounts := map[string]int{
		".github/audit-artifacts/issue-2007-failure-class.yaml":                        14,
		".github/audit-artifacts/issue-2378-failure-class.yaml":                        82,
		"internal/apiv1/handler_test.go":                                               2,
		"internal/apiv1/testdata/openrpc_compliance_matrix.yaml":                       1,
		"internal/apiv1/testdata/public_surface_backend_matrix.yaml":                   1,
		"internal/cliapp/api_consumption_boundary_test.go":                             3,
		"internal/cliapp/env_guard.go":                                                 1,
		"internal/cliapp/main_test.go":                                                 1,
		"internal/dashboard/server/server_test.go":                                     1,
		"internal/dashboard/server/observability_sql_test.go":                          1,
		"internal/runtime/destructivereset/contracts.go":                               1,
		"internal/runtime/destructivereset/coordinator_test.go":                        2,
		"internal/runtime/manager/runtime_reset_test.go":                               3,
		"internal/runtime/runforkexecution/readiness_classifier.go":                    1,
		"internal/runtime/runforkexecution/runtime_container.go":                       1,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": 6,
		"openrpc.json":       2,
		"platform-spec.yaml": 51,
	}
	expectedUnrelatedTextCounts := map[string]int{
		"internal/events/construction_test.go":                                      3,
		"internal/runtime/conformance/repo_validation_snapshot_test.go":             1,
		"internal/runtime/contracts/mock_performance_ownership_guard_test.go":       1,
		"internal/runtime/llm/runtime_resolver.go":                                  1,
		"internal/runtime/pythonmodule/artifact_manifest_generated.go":              2,
		"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go": 5,
		"internal/store/selected/boundary_test.go":                                  1,
		"internal/store/testdata/persistence_authority_findings.tsv":                1,
		"internal/userfacing/human_code_projection_cli_test.go":                     5,
	}
	classificationDocuments := map[string]bool{
		".github/audit-artifacts/issue-2007-failure-class.yaml": true,
		".github/audit-artifacts/issue-2378-failure-class.yaml": true,
		"platform-spec.yaml": true,
	}
	rawRetirementFixtures := map[string]bool{
		"internal/apiv1/handler_test.go":                                               true,
		"internal/cliapp/api_consumption_boundary_test.go":                             true,
		"internal/dashboard/server/server_test.go":                                     true,
		"internal/runtime/destructivereset/coordinator_test.go":                        true,
		"internal/runtime/manager/runtime_reset_test.go":                               true,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": true,
	}
	classificationMarkers := []string{
		"retir", "legacy", "removed", "historical", "fail-closed", "must not",
		"no direct", "not accepted", "invalid", "rejected", "consumer only", "consumers only",
	}

	actualCounts := map[string]int{}
	actualUnrelatedTextCounts := map[string]int{}
	var violations []string
	for _, rawPath := range bytes.Split(tracked, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		relative := filepath.ToSlash(string(rawPath))
		if relative == "internal/serveapp/retired_builder_transport_guard_test.go" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(relative), "internal/builder/") {
			violations = append(violations, fmt.Sprintf("%s: tracked path retains a Builder product candidate", relative))
		}
		body, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if readErr != nil {
			t.Fatalf("read tracked file %s: %v", relative, readErr)
		}
		if bytes.IndexByte(body, 0) >= 0 {
			continue
		}
		candidates, candidateErr := retiredBuilderCandidates(relative, body)
		if candidateErr != nil {
			t.Fatalf("classify tracked file %s: %v", relative, candidateErr)
		}
		for _, candidate := range candidates {
			line := candidate.text
			if isExplicitUnrelatedBuilderText(relative, line) {
				actualUnrelatedTextCounts[relative]++
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
				violations = append(violations, fmt.Sprintf("%s:%d: %s", relative, candidate.line, strings.TrimSpace(line)))
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
	for path, expected := range expectedUnrelatedTextCounts {
		if actualUnrelatedTextCounts[path] != expected {
			violations = append(violations, fmt.Sprintf("%s: unrelated Builder text count=%d, want %d", path, actualUnrelatedTextCounts[path], expected))
		}
	}
	for path, count := range actualUnrelatedTextCounts {
		if _, ok := expectedUnrelatedTextCounts[path]; !ok {
			violations = append(violations, fmt.Sprintf("%s: unexpected unrelated Builder text count=%d", path, count))
		}
	}
	if len(violations) > 0 {
		t.Fatalf("retired Builder semantic references escaped the exact tracked-file classification:\n%s", strings.Join(violations, "\n"))
	}
}

func retiredBuilderCandidates(relative string, body []byte) ([]retiredBuilderCandidate, error) {
	if filepath.Ext(relative) != ".go" {
		return retiredBuilderCandidateLines(body, nil), nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, relative, body, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	type byteRange struct{ start, end int }
	var semanticRanges []byteRange
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		semanticRanges = append(semanticRanges, byteRange{
			start: fset.Position(literal.Pos()).Offset,
			end:   fset.Position(literal.End()).Offset,
		})
		return true
	})
	for _, group := range file.Comments {
		for _, comment := range group.List {
			semanticRanges = append(semanticRanges, byteRange{
				start: fset.Position(comment.Pos()).Offset,
				end:   fset.Position(comment.End()).Offset,
			})
		}
	}
	return retiredBuilderCandidateLines(body, func(start, end int) bool {
		for _, semanticRange := range semanticRanges {
			if start < semanticRange.end && end > semanticRange.start {
				return true
			}
		}
		return false
	}), nil
}

func retiredBuilderCandidateLines(body []byte, include func(start, end int) bool) []retiredBuilderCandidate {
	var candidates []retiredBuilderCandidate
	offset := 0
	for index, rawLine := range bytes.SplitAfter(body, []byte("\n")) {
		line := string(bytes.TrimSuffix(rawLine, []byte("\n")))
		matched := false
		for _, match := range retiredBuilderCandidatePattern.FindAllStringIndex(line, -1) {
			if include == nil || include(offset+match[0], offset+match[1]) {
				matched = true
				break
			}
		}
		if matched {
			candidates = append(candidates, retiredBuilderCandidate{line: index + 1, text: line})
		}
		offset += len(rawLine)
	}
	return candidates
}

func isExplicitUnrelatedBuilderText(relative, line string) bool {
	lower := strings.ToLower(line)
	switch {
	case relative == "internal/events/construction_test.go" && strings.Contains(lower, "fixture builder"):
		return true
	case relative == "internal/runtime/conformance/repo_validation_snapshot_test.go" && strings.Contains(lower, "snapshot builder returned"):
		return true
	case relative == "internal/runtime/contracts/mock_performance_ownership_guard_test.go" && strings.Contains(lower, "bundlehashentrybuilder"):
		return true
	case relative == "internal/runtime/llm/runtime_resolver.go" && strings.Contains(lower, "runtime builder is not configured"):
		return true
	case relative == "internal/runtime/pythonmodule/artifact_manifest_generated.go" &&
		(strings.Contains(lower, "expatbuilder.py") || strings.Contains(lower, "xmlbuilder.py")):
		return true
	case relative == "internal/store/internal/runtimepersistence/run_debug_read_surface_test.go" && strings.Contains(lower, `"builder"`):
		return true
	case relative == "internal/store/selected/boundary_test.go" && strings.Contains(lower, "apioptionalcapabilitybuilder"):
		return true
	case relative == "internal/store/testdata/persistence_authority_findings.tsv" && strings.Contains(lower, "runtimefactorybuilder"):
		return true
	case relative == "internal/userfacing/human_code_projection_cli_test.go" && strings.Contains(lower, "strings.builder"):
		return true
	default:
		return false
	}
}
