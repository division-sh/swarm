package sourceartifact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestS1RootAdmissionExactWhitespaceSibling(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "parent with spaces ")
	writeTestFile(t, parent, "shop-a/nodes.yaml", "original: {type: procedure}\n")
	t.Chdir(parent)
	for _, name := range []string{" shop-a", "\tshop-a", "\nshop-a", "shop-a ", "shop-a\t", "shop-a\n", " "} {
		writeTestFile(t, parent, name+"/nodes.yaml", "different: {type: procedure}\n")
		for _, selected := range []string{name, filepath.Join(parent, name)} {
			t.Run(selected, func(t *testing.T) {
				artifact, err := AdmitDirectory(selected)
				if err == nil || artifact != nil || !strings.Contains(err.Error(), "rename it") {
					t.Fatalf("invalid selected sibling was replaced: selected=%q artifact=%v err=%v", selected, artifact, err)
				}
			})
		}
	}
	for _, selected := range []string{"shop-a", "./shop-a", filepath.Join(parent, "shop-a")} {
		artifact, err := AdmitDirectory(selected)
		if err != nil {
			t.Fatalf("valid root inside whitespace parent refused: %q: %v", selected, err)
		}
		admission, present := artifact.RootAdmission()
		entry, member := artifact.Entry("nodes.yaml")
		if !present || !member || admission.RootDirectory() != "shop-a" || string(entry.Bytes()) != "original: {type: procedure}\n" {
			t.Fatal("ordinary relative/absolute selection changed its root or content")
		}
	}
	t.Chdir(filepath.Join(parent, "shop-a"))
	artifact, err := AdmitDirectory(".")
	if err != nil {
		t.Fatal(err)
	}
	if admission, present := artifact.RootAdmission(); !present || admission.RootDirectory() != "shop-a" {
		t.Fatal("dot selection lost the captured directory")
	}
}

func TestS1RootAdmissionPreservesParentTraversalBeforeOpen(t *testing.T) {
	parent := t.TempDir()
	writeTestFile(t, parent, "shop-a/nodes.yaml", "wrong: {type: procedure}\n")
	writeTestFile(t, parent, "outside/shop-a/nodes.yaml", "selected: {type: procedure}\n")
	if err := os.Mkdir(filepath.Join(parent, "outside", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(parent, "outside", "nested"), filepath.Join(parent, "link")); err != nil {
		t.Fatal(err)
	}
	t.Chdir(parent)
	for _, operand := range []string{"link/../shop-a", parent + "/link/../shop-a"} {
		artifact, err := AdmitDirectory(operand)
		if err != nil {
			t.Fatal(err)
		}
		entry, present := artifact.Entry("nodes.yaml")
		if !present || string(entry.Bytes()) != "selected: {type: procedure}\n" {
			t.Fatalf("source path was lexically rewritten before opening: %q", operand)
		}
	}
	for _, operand := range []string{"link", "link/"} {
		if artifact, err := AdmitDirectory(operand); err == nil || artifact != nil {
			t.Fatalf("terminal root symlink acquired admission: %q", operand)
		}
	}
}
