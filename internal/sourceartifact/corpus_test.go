package sourceartifact

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestTrackedManifestRootsUseFiniteSourceGrammar(t *testing.T) {
	repo := filepath.Clean(filepath.Join(testWorkingDirectory(t), "..", ".."))
	roots, err := checkedManifestRoots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) < 200 {
		t.Fatalf("manifest roots = %d, want migrated corpus", len(roots))
	}
	for _, root := range roots {
		t.Run(strings.TrimPrefix(filepath.ToSlash(root), filepath.ToSlash(repo)+"/"), func(t *testing.T) {
			artifact, err := AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			if metadata, present := artifact.RootManifest(); !present || metadata.Name == "" || metadata.Version == "" || metadata.PlatformVersion == "" {
				t.Fatalf("tracked positive manifest bypassed strict metadata: %#v", metadata)
			}
		})
	}
}

func TestManifestCorpusExcludesNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	local := filepath.Join(repo, "examples", "local")
	foreign := filepath.Join(repo, "examples", "nested", "foreign")
	for _, dir := range []string{local, foreign} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte("invalid: ["), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "examples", "nested", ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	roots, err := checkedManifestRoots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0] != local {
		t.Fatalf("manifest corpus = %v, want only local %s", roots, local)
	}
	if _, err := AdmitDirectory(local); err == nil {
		t.Fatal("invalid current-checkout manifest was admitted")
	}
}

func checkedManifestRoots(repo string) ([]string, error) {
	roots := make([]string, 0)
	err := checkoutsource.WalkDir(repo, repo, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".swarm") {
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
		return nil, err
	}
	sort.Strings(roots)
	return roots, nil
}

func testWorkingDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
