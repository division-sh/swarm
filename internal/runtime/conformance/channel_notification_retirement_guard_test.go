package conformance

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestNoRetiredChannelCriticalNotifier(t *testing.T) {
	root := conformanceRepoRoot(t)
	forbidden := []string{
		"ListUnnotifiedCriticalMailboxItems",
		"CriticalNotifier",
		"criticalNotifier",
	}
	var violations []string
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, symbol := range forbidden {
			if strings.Contains(string(raw), symbol) {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				violations = append(violations, relative+": "+symbol)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("retired channel notification paths remain:\n%s", strings.Join(violations, "\n"))
	}
}
