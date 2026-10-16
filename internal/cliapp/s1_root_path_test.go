package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestS1CLIExactSourcePath(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shop-a", " shop-a", "shop-a ", "shop-a\t", "shop-a\n"} {
		root := filepath.Join(parent, name)
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("description: "+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		invocation, err := NewInvocationRoot(root)
		if err != nil || invocation.Path() != root {
			t.Errorf("invocation selected a different sibling: %q -> %q, %v", root, invocation.Path(), err)
		}
		for _, operand := range []string{name, root} {
			selected, err := ResolveSourceRoot(parent, operand)
			if err != nil || selected != root {
				t.Errorf("source selector changed operand: %q -> %q, %v", operand, selected, err)
			}
			normalized, err := NormalizeSourceRoot(root)
			if err != nil || normalized != root {
				t.Errorf("serve source normalizer selected another sibling: %q -> %q, %v", root, normalized, err)
			}
			if name != "shop-a" {
				base := mustInvocationRootForTest(parent)
				if hash, err := admitCLIIdentitySource(base, operand); err == nil || hash != "" || !strings.Contains(err.Error(), "rename it") {
					t.Errorf("identity selector admitted an invalid sibling: %q -> %q, %v", operand, hash, err)
				}
			}
		}
	}
}

func TestS1CLIWhitespaceParentIsNotAnotherInvocation(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"parent", "parent "} {
		if err := os.MkdirAll(filepath.Join(base, name, "shop-a"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	parent := filepath.Join(base, "parent ")
	invocation, err := NewInvocationRoot(parent)
	if err != nil || invocation.Path() != parent {
		t.Fatalf("whitespace parent changed: %q, %v", invocation.Path(), err)
	}
	selected, err := ResolveSourceRoot(invocation.Path(), "shop-a")
	if err != nil || selected != filepath.Join(parent, "shop-a") {
		t.Fatalf("valid child escaped the actual whitespace parent: %q, %v", selected, err)
	}
	project := resolveLocalRuntimeStateProject(parent, CLISourcePlatformSpecPaths{SourceRoot: parent})
	if project.ProjectRoot != parent || project.CanonicalProjectRoot != parent {
		t.Fatalf("local state retargeted selected root: %+v", project)
	}
}
