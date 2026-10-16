package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourceblob"
)

func sourceBlobLivePins(t *testing.T) []sourceblob.Pin {
	t.Helper()
	return sourceBlobDeclarationsForInvocation(t).pins
}

func sourceBlobInvocationPins(t *testing.T, image sourceBlobInvocation, setup, batch []runphysical.Pin) []sourceblob.Pin {
	t.Helper()
	pins, err := sourceblob.Decode(image.files[sourceblob.Path])
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		raw := image.files[pin.File]
		body, err := servedsettlement.UniqueBody(raw, pin.Function)
		if err != nil {
			t.Fatal(err)
		}
		current := servedSetupPredecessorSource(t, pin.File, pin.Function, body)
		for _, stage := range [][]runphysical.Pin{setup, batch} {
			current, err = servedsettlement.EarlierSource(stage, pin.File, pin.Function, current)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := sourceblob.ValidateCurrentSource(pin, []byte("package proof\n"+current)); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := selectedreadrole.CatchUp2542HistoricalSource("../../..", "internal/serveapp/main_runtime_test.go", image.files["internal/serveapp/main_runtime_test.go"])
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceblob.ValidateFixtureRole(raw); err != nil {
		t.Fatal(err)
	}
	return pins
}

type sourceBlobInvocation struct {
	files map[string][]byte
	head  []byte
	owned bool
}

func sourceBlobOwnedInvocationPaths(root string) ([]string, error) {
	var paths []string
	err := checkoutsource.WalkDir(root, root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !(strings.HasPrefix(rel, "internal/") || strings.HasPrefix(rel, "tools/fixture-codemod/pipeline-observations/") || strings.HasPrefix(rel, ".github/audit-artifacts/")) {
			return nil
		}
		switch filepath.Ext(rel) {
		case ".go", ".json", ".tsv", ".yaml":
			paths = append(paths, rel)
		}
		return nil
	})
	return paths, err
}

type sourceBlobDeclarationInvocation struct {
	setup, batch []runphysical.Pin
	pins         []sourceblob.Pin
}

func sourceBlobDeclarationsForInvocation(t *testing.T) sourceBlobDeclarationInvocation {
	t.Helper()
	paths, err := sourceBlobOwnedInvocationPaths("../../..")
	if err != nil {
		t.Fatal(err)
	}
	image, err := captureSourceBlobInvocation("../../..", paths)
	if err != nil {
		t.Fatal(err)
	}
	image.owned = true
	t.Cleanup(func() {
		if err := image.recheck("../../.."); err != nil {
			t.Error(err)
		}
	})
	setup, err := servedsettlement.ReadSetupPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := servedsettlement.ReadBatchPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := servedsettlement.ValidateBatchCurrentSource("../../..", batch); err != nil {
		t.Fatal(err)
	}
	pins := sourceBlobInvocationPins(t, image, setup, batch)
	if err := image.recheck("../../.."); err != nil {
		t.Fatal(err)
	}
	return sourceBlobDeclarationInvocation{setup: setup, batch: batch, pins: pins}
}

func captureSourceBlobInvocation(root string, paths []string) (sourceBlobInvocation, error) {
	image := sourceBlobInvocation{files: map[string][]byte{}}
	var err error
	image.head, err = exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return image, err
	}
	for _, path := range paths {
		if image.files[path] != nil {
			continue
		}
		image.files[path], err = os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return image, err
		}
	}
	return image, nil
}

func (image sourceBlobInvocation) recheck(root string) error {
	head, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil || !bytes.Equal(head, image.head) {
		return fmt.Errorf("source-blob invocation changed its head: %v", err)
	}
	if image.owned {
		paths, err := sourceBlobOwnedInvocationPaths(root)
		if err != nil || len(paths) != len(image.files) {
			return fmt.Errorf("source-blob invocation changed source inventory membership: %v", err)
		}
		for _, path := range paths {
			if _, found := image.files[path]; !found {
				return fmt.Errorf("source-blob invocation added an unobserved source: %s", path)
			}
		}
	}
	for path, before := range image.files {
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !bytes.Equal(before, current) {
			return fmt.Errorf("source-blob input changed during invocation: %s: %v", path, err)
		}
	}
	return nil
}

func TestFamily145SourceBlobInvocationRejectsChangesBeforeItsEndCheck(t *testing.T) {
	root := t.TempDir()
	gitdir, err := exec.Command("git", "-C", "../../..", "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	const path = "internal/serveapp/current.go"
	const manifest = "tools/fixture-codemod/pipeline-observations/manifest.json"
	for _, path := range []string{path, manifest} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	original := []byte("package proof\nfunc current(){ actualOwner() }\n")
	for _, mutation := range []string{"body", "extra-caller", "missing", "manifest", "extra-file"} {
		t.Run(mutation, func(t *testing.T) {
			for name, raw := range map[string][]byte{path: original, manifest: []byte(`{"exact":"source"}`)} {
				if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			image, err := captureSourceBlobInvocation(root, []string{path, manifest})
			image.owned = true
			if err != nil || image.recheck(root) != nil {
				t.Fatal("fresh unchanged invocation failed:", err)
			}
			target, replacement := path, bytes.Replace(original, []byte("actualOwner"), []byte("foreignOwner"), 1)
			switch mutation {
			case "extra-caller":
				replacement = append(bytes.Clone(original), []byte("func escaped(){ rawCaller() }\n")...)
			case "manifest":
				target, replacement = manifest, []byte(`{"exact":"foreign"}`)
			case "extra-file":
				target, replacement = "internal/serveapp/extra.go", []byte("package proof\nfunc escaped(){ rawCaller() }\n")
				t.Cleanup(func() { _ = os.Remove(filepath.Join(root, target)) })
			case "missing":
				err = os.Remove(filepath.Join(root, target))
			}
			if mutation != "missing" {
				err = os.WriteFile(filepath.Join(root, target), replacement, 0600)
			}
			if err != nil || image.recheck(root) == nil {
				t.Fatal("a mid-invocation change survived its end check:", mutation, err)
			}
		})
	}
}

func sourceBlobPreviousDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedObservationEarlierSource(t, file, function, current)
	for _, pin := range sourceBlobInventoryPins {
		if pin.File == file && pin.Function == function {
			sourceBlobLivePins(t)
			if current != pin.After {
				t.Fatal("source blob exact current declaration changed:", function)
			}
			return pin.Before
		}
	}
	return current
}

func sourceBlobPinnedInventory() []sourceblob.Pin {
	pins, err := sourceblob.Decode(sourceBlobBytes)
	if err != nil {
		panic(err)
	}
	return pins
}

var sourceBlobInventoryPins = sourceBlobPinnedInventory()

var servedObservationInventoryPins = servedObservationPinnedInventory()

func servedObservationPinnedInventory() []runphysical.Pin {
	pins, err := servedsettlement.ReadBatchPins("../../..")
	if err != nil {
		panic(err)
	}
	return pins
}
