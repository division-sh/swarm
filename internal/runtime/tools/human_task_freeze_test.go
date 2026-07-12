package tools

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestHumanTaskInterpreterRemainsFrozenToIssue1995ClosedSet(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	tokens := []string{"human_task_request", "human_task_decide", "CreateHumanTask", "DecideHumanTask", "HumanTaskPersistence", "human_task."}
	want := []string{
		"internal/builder/runs_runtime.go",
		"internal/runtime/agents/agent_llm.go",
		"internal/runtime/authority/provider.go",
		"internal/runtime/authority/source_provider.go",
		"internal/runtime/runforkexecution/agent_runtime_materialization.go",
		"internal/runtime/runtime.go",
		"internal/runtime/testfixtures/templatereply/fixture.go",
		"internal/runtime/tools/contracts.go",
		"internal/runtime/tools/deps.go",
		"internal/runtime/tools/executor.go",
		"internal/runtime/tools/executor_human_tasks.go",
		"internal/runtime/tools/handler_registry.go",
		"internal/runtime/tools/permissions.go",
		"internal/runtime/tools/persistence.go",
		"internal/runtime/tools/tool_input_normalization.go",
		"internal/runtime/tools/usage.go",
		"internal/store/tool_persistence.go",
	}
	var got []string
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, token := range tokens {
			if strings.Contains(string(raw), token) {
				rel, _ := filepath.Rel(repoRoot, path)
				got = append(got, filepath.ToSlash(rel))
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan frozen human-task interpreter: %v", err)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("#1995 frozen human-task production set changed\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
