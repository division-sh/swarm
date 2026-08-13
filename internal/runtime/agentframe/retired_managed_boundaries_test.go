package agentframe

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRetiredManagedTurnOwnersAreAbsentFromProduction(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve guard source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
	forbidden := map[string]string{
		"New" + "Conversation(":                "raw conversation constructor",
		".Step" + "(":                          "raw managed step",
		"Step" + "WithRole":                    "raw role-selected managed step",
		"Continue" + "Session(":                "raw adapter continuation",
		"Append" + "Result(":                   "raw managed result mutation",
		"Append" + "Feedback(":                 "raw managed feedback mutation",
		"format" + "EventForAgent":             "local event formatter",
		"boardDirective" + "RemediationPrompt": "local remediation formatter",
		"resolve" + "PromptForMode":            "retired prompt-mode owner",
		"cliExecution" + "ToolSurfaceForActor": "retired CLI capability owner",
	}
	violations := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source := string(raw)
		for token, owner := range forbidden {
			if strings.Contains(source, token) {
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					relative = path
				}
				violations = append(violations, relative+": "+owner+" still contains "+token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan production Go sources: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("retired managed-turn owners remain live:\n%s", strings.Join(violations, "\n"))
	}
}
