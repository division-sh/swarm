package checkoutsource

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCheckoutMembershipExcludesNestedGitBoundaries(t *testing.T) {
	for _, rootMarker := range []string{"none", "file", "directory"} {
		t.Run(rootMarker, func(t *testing.T) {
			root := t.TempDir()
			if rootMarker != "none" {
				marker(t, root, rootMarker)
			}
			write(t, filepath.Join(root, "internal", "current.go"))
			write(t, filepath.Join(root, "internal", ".ignored", "untracked.go"))
			for _, shape := range []string{"file", "directory"} {
				foreign := filepath.Join(root, "internal", "arbitrary", shape, "deep")
				marker(t, foreign, shape)
				write(t, filepath.Join(foreign, "stale.go"))
				write(t, filepath.Join(foreign, "deeper", "stale.go"))
			}
			for _, scan := range []string{root, filepath.Join(root, "internal")} {
				var got []string
				err := WalkDir(root, scan, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if strings.HasSuffix(path, ".go") {
						got = append(got, filepath.Base(path))
					}
					return nil
				})
				if err != nil || !slices.Equal(got, []string{"untracked.go", "current.go"}) {
					t.Fatalf("WalkDir(%s) = %v, %v", scan, got, err)
				}
			}
			member, err := Member(root, filepath.Join(root, "internal", "arbitrary", "file", "deep", "stale.go"))
			if err != nil || member {
				t.Fatalf("foreign member = %v, %v", member, err)
			}
		})
	}
}

func TestCheckoutReadDirExcludesForeignTierAndFixture(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "tests", "tier1", "current", "tests", "expected.yaml"))
	marker(t, filepath.Join(root, "tests", "tier2"), "file")
	write(t, filepath.Join(root, "tests", "tier2", "foreign", "tests", "expected.yaml"))
	marker(t, filepath.Join(root, "tests", "tier1", "foreign"), "directory")
	write(t, filepath.Join(root, "tests", "tier1", "foreign", "tests", "expected.yaml"))
	tiers, err := ReadDir(root, filepath.Join(root, "tests"))
	if err != nil || len(tiers) != 1 || tiers[0].Name() != "tier1" {
		t.Fatalf("tiers = %v, %v", names(tiers), err)
	}
	fixtures, err := ReadDir(root, filepath.Join(root, "tests", "tier1"))
	if err != nil || len(fixtures) != 1 || fixtures[0].Name() != "current" {
		t.Fatalf("fixtures = %v, %v", names(fixtures), err)
	}
}

func TestCheckoutMembershipFailsClosedOnInvalidRoots(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "current.go"))
	for _, scan := range []string{filepath.Join(root, "missing"), t.TempDir()} {
		if err := WalkDir(root, scan, func(string, fs.DirEntry, error) error { return nil }); err == nil {
			t.Fatalf("WalkDir(%s) accepted invalid root", scan)
		}
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(t.TempDir(), link); err != nil {
		t.Fatal(err)
	}
	if err := WalkDir(root, link, func(string, fs.DirEntry, error) error { return nil }); err == nil {
		t.Fatal("symlink scan root accepted")
	}
	if _, err := ReadDir(root, link); err == nil {
		t.Fatal("symlink directory accepted")
	}
	if _, err := ReadDir(root, filepath.Join(root, "current.go")); err == nil {
		t.Fatal("regular file was accepted as a directory")
	}
	if _, err := Member(root, filepath.Join(root, "current.go", "child")); err == nil {
		t.Fatal("inspection failure below a regular file was hidden")
	}
	want := errors.New("consumer read failed")
	if err := WalkDir(root, root, func(path string, _ fs.DirEntry, _ error) error {
		if path == filepath.Join(root, "current.go") {
			return want
		}
		return nil
	}); !errors.Is(err, want) {
		t.Fatalf("consumer error = %v, want %v", err, want)
	}
	if err := Walk(root, root, func(_ string, _ os.FileInfo, err error) error { return err }); err != nil {
		t.Fatal(err)
	}
}

func marker(t *testing.T, dir, shape string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ".git")
	if shape == "file" {
		if err := os.WriteFile(path, []byte("gitdir: elsewhere\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	} else if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.Name()
	}
	return out
}
