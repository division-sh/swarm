package servedsettlement

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/globalname"
)

func TestImmutableSetupCacheReauthenticatesCopiedInputsAndIsolatesCallers(t *testing.T) {
	root, copy := filepath.Clean("../../../../.."), t.TempDir()
	for _, stage := range setupStages {
		raw, err := os.ReadFile(filepath.Join(root, stage.path))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(copy, stage.path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	pins, err := ReadSetupPins(copy)
	if err != nil || len(pins) != 104 {
		t.Fatal("exact copied input refused:", err)
	}
	expected := pins[0]
	pins[0].After = "foreign caller"
	again, err := ReadSetupPins(copy)
	if err != nil || again[0] != expected {
		t.Fatal("caller changed immutable decoded evidence:", err)
	}
	for _, stage := range setupStages {
		path := filepath.Join(copy, stage.path)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(bytes.Clone(raw), ' '), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSetupPins(copy); err == nil {
			t.Fatal("warm cache accepted changed copied evidence")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSetupPins(copy); err == nil {
			t.Fatal("warm cache accepted missing copied evidence")
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestImmutableBatchCacheRejectsChangedStagesAndCallerMutation(t *testing.T) {
	root := filepath.Clean("../../../../..")
	inputs := make([][]byte, 3)
	for i, path := range []string{globalname.Path, Path, LookupPath} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		inputs[i] = raw
	}
	pins, err := BatchPins(inputs[0], inputs[1], inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	expected := append(pins[:0:0], pins...)
	pins[0].After = "foreign caller"
	again, err := BatchPins(inputs[0], inputs[1], inputs[2])
	if err != nil || !reflect.DeepEqual(again, expected) {
		t.Fatal("caller changed cached batch evidence:", err)
	}
	for i := range inputs {
		changed := append(inputs[:0:0], inputs...)
		changed[i] = append(bytes.Clone(inputs[i]), ' ')
		if _, err := BatchPins(changed[0], changed[1], changed[2]); err == nil {
			t.Fatal("warm cache accepted changed stage:", i)
		}
	}
}
