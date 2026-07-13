package canonicalrouting

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	RootIngress             = "root-ingress"
	ParentConnect           = "parent-connect"
	TemplateSelectExisting  = "template-select-existing"
	TemplateSelectOrCreate  = "template-select-or-create"
	TemplateReply           = "template-reply"
	TemplateCreateMintedKey = "template-create-minted-key"
)

// ExampleRoot returns the checked-in positive authoring owner for a routing pattern.
func ExampleRoot(t testing.TB, name string) string {
	t.Helper()
	return filepath.Join(RepoRoot(t), "examples", "routing", name)
}

// CopyExample materializes a canonical example for a focused overlay or negative mutation.
func CopyExample(t testing.TB, name string) string {
	t.Helper()
	target := t.TempDir()
	CopyTree(t, ExampleRoot(t, name), target)
	return target
}

func CopyTree(t testing.TB, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, contents, 0o644)
	})
	if err != nil {
		t.Fatalf("copy canonical routing example %s: %v", source, err)
	}
}

func ReplaceFile(t testing.TB, path, old, replacement string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(contents), old) {
		t.Fatalf("canonical mutation target missing in %s", path)
	}
	updated := strings.Replace(string(contents), old, replacement, 1)
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func WriteFile(t testing.TB, root, relativePath, contents string) {
	t.Helper()
	path := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func RepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
