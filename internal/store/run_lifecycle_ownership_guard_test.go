package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestRunLifecycleOwnershipBoundaryGuard(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	allowedWrites := map[string]bool{
		"internal/store/run_lifecycle_candidates.go":       true,
		"internal/store/run_lifecycle_mutation_adapter.go": true,
		"internal/store/run_lifecycle_state_adapter.go":    true,
		"internal/testutil/runlifecyclefixture/fixture.go": true,
	}
	allowedCandidateColumns := map[string]bool{
		"internal/store/run_lifecycle_candidates.go":    true,
		"internal/store/run_lifecycle_state_adapter.go": true,
	}
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	var violations []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source := string(raw)
		if runWrite.MatchString(source) && !allowedWrites[relative] {
			violations = append(violations, relative+": writes runs outside the private lifecycle adapters")
		}
		if (strings.Contains(source, "completion_due_at") || strings.Contains(source, "completion_revision")) &&
			!allowedCandidateColumns[relative] {
			violations = append(violations, relative+": accesses private completion candidate columns")
		}
		for _, legacy := range []string{
			"ConvergeNormalRunCompletion",
			"ConvergeStandaloneRuntimePlatformRun",
			"MarkRunTerminal(",
			"RunLifecyclePersistence",
			"internal/store/runlifecycle",
			"status IN ('running', 'paused')",
		} {
			if strings.Contains(source, legacy) {
				violations = append(violations, relative+": retains legacy lifecycle owner "+legacy)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("run lifecycle ownership violations:\n%s", strings.Join(violations, "\n"))
	}
}

func TestSemanticRunFixturesUseLifecycleOwner(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	var violations []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "internal/store/run_lifecycle_ownership_guard_test.go" ||
			relative == "internal/runtime/pipeline/run_lifecycle_test_adapter_test.go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !runWrite.MatchString(value) {
				return true
			}
			if allowedSemanticRunFixtureLiteral(relative, value) {
				return true
			}
			violations = append(violations, relative+": "+compactSQLForLifecycleGuard(value))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("semantic run fixtures bypass the lifecycle owner:\n%s", strings.Join(violations, "\n"))
	}
}

func allowedSemanticRunFixtureLiteral(relative string, value string) bool {
	compact := compactSQLForLifecycleGuard(value)
	switch relative {
	case "internal/store/schema_compatibility_bootstrap_test.go":
		for _, legacyRunID := range []string{
			"00000000-0000-0000-0000-000000002055",
			"00000000-0000-0000-0000-000000002057",
		} {
			if strings.Contains(compact, legacyRunID) {
				return true
			}
		}
	case "internal/cliapp/api_consumption_boundary_test.go":
		return compact == "UPDATE runs"
	case "internal/store/run_terminal_delivery_lock_order_test.go":
		return compact == "UPDATE RUNS"
	}
	return false
}

func compactSQLForLifecycleGuard(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
