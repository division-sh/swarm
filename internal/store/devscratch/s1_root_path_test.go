package devscratch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestS1ScratchCoordinateExactSourcePath(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shop-a", "shop-a ", "shop-a\t", "shop-a\n"} {
		root := filepath.Join(parent, name)
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		coordinate, err := Resolve(root)
		if err != nil || coordinate.ProjectRoot != root || filepath.Dir(filepath.Dir(coordinate.DatabasePath)) != filepath.Join(root, ".swarm") {
			t.Errorf("scratch coordinate retargeted source: root=%q coordinate=%+v err=%v", root, coordinate, err)
		}
	}
}
