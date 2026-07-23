package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var executableDeliverySQL = regexp.MustCompile(`(?is)\b(?:from|join|into|update|delete\s+from|on)\s+(?:event_deliveries|event_delivery_attempts|event_delivery_outcomes)\b`)

var executableDeliverySQLOwners = map[string]string{
	"internal/runtime/deliverylifecycle/adapter.go":                   "canonical executable-delivery lifecycle adapter",
	"internal/runtime/deliverylifecycle/read_projections.go":          "canonical bounded executable-delivery read projections",
	"internal/runtime/runforkrevision/revision.go":                    "immutable fork-revision fact capture",
	"internal/store/destructive_reset_cleanup.go":                     "named destructive-reset physical cleanup",
	"internal/store/run_fork_selected_contract_execution_mutation.go": "selected-fork physical cleanup after typed terminalization",
	"internal/store/testsql/event.go":                                 "named hostile rollback injection used only by tests",
}

func TestRetiredGenericDeliveryReadersHaveNoProductionConsumers(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	retired := []string{"SnapshotsForRun", "SnapshotsForAgent", "EligibleAgentSnapshots"}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, name := range retired {
				if regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).Match(contents) {
					relative, relErr := filepath.Rel(repoRoot, path)
					if relErr != nil {
						return relErr
					}
					t.Errorf("%s consumes retired generic delivery reader %s", filepath.ToSlash(relative), name)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
}

func TestExecutableDeliverySQLHasClosedOwners(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	found := map[string]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				raw, err := strconv.Unquote(literal.Value)
				if err != nil || !executableDeliverySQL.MatchString(raw) {
					return true
				}
				found[relative]++
				if _, allowed := executableDeliverySQLOwners[relative]; !allowed {
					t.Errorf("%s:%d owns executable-delivery SQL outside the closed lifecycle boundary", relative, fset.Position(literal.Pos()).Line)
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
	for path, reason := range executableDeliverySQLOwners {
		if found[path] == 0 {
			t.Errorf("closed executable-delivery SQL owner %s (%s) has no classified SQL", path, reason)
		}
	}
}

func TestReplayScopesAreNotExecutableDeliveries(t *testing.T) {
	for _, source := range []string{
		"internal/runtime/deliverylifecycle/adapter.go",
		"internal/store/delivery_lifecycle.go",
	} {
		contents, err := os.ReadFile(filepath.Join(eventBoundaryRepositoryRoot(t), source))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), "committed_replay_scopes") {
			t.Fatalf("%s conflates committed replay scope with executable delivery lifecycle", source)
		}
	}
}
