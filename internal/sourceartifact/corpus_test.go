package sourceartifact

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestTrackedManifestRootsUseFiniteSourceGrammar(t *testing.T) {
	repo := filepath.Clean(filepath.Join(testWorkingDirectory(t), "..", ".."))
	roots := make([]string, 0)
	err := filepath.WalkDir(repo, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == ".swarm") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && entry.Name() == "manifest.yaml" {
			root := filepath.Dir(current)
			if strings.Contains(filepath.ToSlash(root), "/internal/runtime/scenarioderivation/testdata/hostile") {
				return nil
			}
			roots = append(roots, root)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(roots)
	if len(roots) < 200 {
		t.Fatalf("manifest roots = %d, want migrated corpus", len(roots))
	}
	for _, root := range roots {
		t.Run(strings.TrimPrefix(filepath.ToSlash(root), filepath.ToSlash(repo)+"/"), func(t *testing.T) {
			if _, err := AdmitDirectory(root); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testWorkingDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
