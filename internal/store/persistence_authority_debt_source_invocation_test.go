package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistenceAuthorityDebtSourceInvocationRejectsEveryChangedImage(t *testing.T) {
	root := t.TempDir()
	gitdir, err := debtGit(persistenceAuthorityRepoRoot(t), "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, root, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
	const path = "internal/serveapp/current.go"
	const source = "package proof\nfunc current() { selectedOwner() }\n"
	for _, mutation := range []string{"body", "extra-caller", "missing", "extra-file", "manifest"} {
		t.Run(mutation, func(t *testing.T) {
			debtWriteModuleSource(t, root, path, source)
			debtWriteModuleSource(t, root, "tools/fixture-codemod/pipeline-observations/manifest.json", "[]")
			image, err := debtCaptureServedSourceInvocation(root)
			if err != nil || image.recheck(root) != nil {
				t.Fatal("fresh image refused:", err)
			}
			switch mutation {
			case "body":
				debtWriteModuleSource(t, root, path, strings.Replace(source, "selectedOwner", "foreignOwner", 1))
			case "extra-caller":
				debtWriteModuleSource(t, root, path, source+"func extra() { rawCaller() }\n")
			case "missing":
				if err := os.Remove(filepath.Join(root, path)); err != nil {
					t.Fatal(err)
				}
			case "extra-file":
				debtWriteModuleSource(t, root, "internal/serveapp/extra.go", "package proof\n")
				t.Cleanup(func() { _ = os.Remove(filepath.Join(root, "internal/serveapp/extra.go")) })
			case "manifest":
				debtWriteModuleSource(t, root, "tools/fixture-codemod/pipeline-observations/manifest.json", "[{}]")
			}
			if image.recheck(root) == nil {
				t.Fatal("changed live image passed its end check:", mutation)
			}
			changed, err := debtCaptureServedSourceInvocation(root)
			if err != nil || maps.Equal(changed.files, image.files) || changed.recheck(root) != nil {
				t.Fatal("the changed case did not receive its own fresh image:", mutation, err)
			}
		})
	}
}
