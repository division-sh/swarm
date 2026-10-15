package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestConversationForkHistoricalConstructionHasClosedConsumers(t *testing.T) {
	root := pipeline.WorkflowRepoRoot()
	want := map[string]map[string]int{
		"CommitConversationForkConstructionForTest":     {"internal/store/internal/runtimepersistence/test_conversation_fork_source.go": 2},
		"InsertConversationForkConstructionDiffForTest": {"internal/store/internal/backend/pipelinepersistence/workflow_engine_mutation_commit.go": 1},
		"PrepareConversationForkConstructionForTest":    {"internal/store/storetest/conversation_fork.go": 1},
	}
	got := map[string]map[string]int{}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || want[selector.Sel.Name] == nil {
				return true
			}
			if got[selector.Sel.Name] == nil {
				got[selector.Sel.Name] = map[string]int{}
			}
			got[selector.Sel.Name][filepath.ToSlash(relative)]++
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("historical construction escaped its finite fixture consumers: got=%v want=%v", got, want)
	}
	forwarders, err := os.ReadFile("facade_forwarders_generated.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(forwarders), "CommitConversationForkConstructionForTest") {
		t.Fatal("historical fixture entrypoint was exposed through the runtime facade")
	}
}
