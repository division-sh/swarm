package tools

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredHumanTaskInterpreterHasNoProductionSurvivors(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	forbidden := []string{
		"human_task_decide",
		"HumanTaskPersistence",
		"CreateHumanTask(",
		"DecideHumanTask(",
		"human_task.completed",
		"requesting_agent",
		`json:"deadline"`,
		`json:"deadline_rfc3339"`,
		`json:"deadline_hours"`,
	}
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, err error) error {
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
			for _, token := range forbidden {
				if strings.Contains(string(raw), token) {
					rel, _ := filepath.Rel(repoRoot, path)
					t.Fatalf("retired human-task interpreter token %q survives in %s", token, filepath.ToSlash(rel))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range forbidden {
		if strings.Contains(string(raw), token) {
			t.Fatalf("retired human-task interpreter token %q survives in platform-spec.yaml", token)
		}
	}
}
