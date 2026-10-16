package store_test

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistenceAuthorityDebtSourceInvocationRejectsEveryChangedImage(t *testing.T) {
	t.Run("bound-present-and-fallback", proveServedInvocationBoundSetupKeys)
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

func proveServedInvocationBoundSetupKeys(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	setup, err := debtServedSetupSourcePins(copy)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[[2]string]bool{}
	for _, pin := range setup {
		listed[[2]string{pin.File, pin.Function}] = true
	}
	keys := [][2]string{{setup[0].File, setup[0].Function}}
	for _, key := range debtServedObservationOlderSourceKeys {
		if !listed[key] {
			keys = append(keys, key)
			break
		}
	}
	if len(keys) != 2 {
		t.Fatal("bound proof requires one listed and one fallback key")
	}
	previous, recheck, err := debtServedObservationInvocation(copy)
	if err != nil || recheck() != nil {
		t.Fatal("complete bound source image refused:", err)
	}
	for _, key := range keys {
		decls, err := debtServedDebugDeclarations(filepath.Join(copy, key[0]))
		if err != nil {
			t.Fatal(err)
		}
		actual := decls[key[1]]
		want, err := debtServedObservationSourcePrevious(copy, key[0], key[1], actual)
		got, boundErr := previous(key[0], key[1], actual)
		if err != nil || boundErr != nil || actual == "" || got != want {
			t.Fatalf("bound present/fallback semantics changed: %v: %v %v", key, err, boundErr)
		}
		if _, err := previous(key[0], key[1], strings.Replace(actual, "{", "{foreignBoundBody();", 1)); err == nil {
			t.Fatal("listed/fallback key admitted altered actual source:", key)
		}
		if got, err := previous("foreign.go", key[1], actual); err != nil || got != actual {
			t.Fatal("foreign key received a bound inverse:", err)
		}
	}
	proveServedInvocationFreshCopiedInputs(t, copy, keys[1], recheck)
}

func proveServedInvocationFreshCopiedInputs(t *testing.T, root string, key [2]string, recheck func() error) {
	path := key[0]
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	decls, err := debtServedDebugDeclarations(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	body := decls[key[1]]
	mutant := strings.Replace(body, "{", "{foreignBoundFixture();", 1)
	if body == "" || mutant == body {
		t.Fatal("protected fallback declaration mutation did not apply")
	}
	debtSourceBlobCopyDeclaration(t, root, path, key[1], mutant)
	if recheck() == nil {
		t.Fatal("bound proof hid changed input after authentication")
	}
	if _, _, err := debtServedObservationInvocation(root); err == nil {
		t.Fatal("fresh proof reused acceptance of a changed copied image")
	}
	debtWriteModuleSource(t, root, path, string(raw))
	_, end, err := debtServedObservationInvocation(root)
	if err != nil || end() != nil {
		t.Fatal("restored actual image did not authenticate freshly:", err)
	}
	if err := os.Remove(filepath.Join(root, path)); err != nil {
		t.Fatal(err)
	}
	if end() == nil {
		t.Fatal("bound proof hid a missing copied source")
	}
	if _, _, err := debtServedObservationInvocation(root); err == nil {
		t.Fatal("warm inputs hid a missing copied source from a fresh proof")
	}
}
