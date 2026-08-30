//go:build linux || darwin

package sourceartifact

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDirectoryMetadataRemainsBoundToOpenHandleAfterPathReplacement(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "source")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, "schema.yaml", "name: original\n")
	if err := os.Mkdir(filepath.Join(root, "child"), 0o755); err != nil {
		t.Fatal(err)
	}

	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := os.Rename(root, filepath.Join(parent, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, "replacement.yaml", "name: replacement\n")

	names, err := readDirectoryNames(fd, "not/a/host/path")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "child" || names[1] != "schema.yaml" {
		t.Fatalf("open-handle names = %v", names)
	}
	if directory, err := inspectChildFD(fd, "child", "child"); err != nil || !directory {
		t.Fatalf("inspect original child directory = %t, %v", directory, err)
	}
	if directory, err := inspectChildFD(fd, "schema.yaml", "schema.yaml"); err != nil || directory {
		t.Fatalf("inspect original schema file = %t, %v", directory, err)
	}
	if _, err := inspectChildFD(fd, "replacement.yaml", "replacement.yaml"); err == nil {
		t.Fatal("replacement path leaked into open-handle metadata")
	}
}
