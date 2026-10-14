package selectedreadrole

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadRoleCompletePredecessorAndCurrentnessRefuseEveryForeignInput(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, pin := range pins {
		if before[pin.File] == nil {
			raw, err := exec.Command("git", "-C", root, "show", Predecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
			before[pin.File] = raw
		}
		body, err := Body(before[pin.File], pin.Function)
		if err != nil || body != pin.Before {
			t.Fatalf("complete signed predecessor changed: %s %v", pin.Function, err)
		}
		got, err := EarlierSource(root, pin.File, pin.Function, pin.After)
		if err != nil || got != pin.Before {
			t.Fatalf("current endpoint cannot invert: %s %v", pin.Function, err)
		}
		if _, err := EarlierSource(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("supplied older source was accepted")
		}
		if got, err := EarlierSource(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign source received inverse:", err)
		}
	}
}

func TestReadRoleHistoryPreservesEveryFieldAndRefusesChangedOrder(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := PriorHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, err := ComposeHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		got, err := EarlierHistory(root, current, recipes)
		if err != nil || !bytes.Equal(got, prior) {
			t.Fatal("history did not restore exact bytes:", err)
		}
		for _, field := range []string{"Before", "After", "Successor", "File", "Function", "Removed", "Mechanical", "Family"} {
			var rows []map[string]any
			if err := json.Unmarshal(current, &rows); err != nil {
				t.Fatal(err)
			}
			rows[0][field] = "foreign"
			raw, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := EarlierHistory(root, raw, recipes); err == nil {
				t.Fatal("history erased changed metadata:", field)
			}
		}
		var rows []map[string]any
		if err := json.Unmarshal(current, &rows); err != nil {
			t.Fatal(err)
		}
		rows[0], rows[1] = rows[1], rows[0]
		raw, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := EarlierHistory(root, raw, recipes); err == nil {
			t.Fatal("history ordering change was erased")
		}
	}
}

func TestReadRoleWarmSourceCacheDoesNotHideMissingOrChangedSiblings(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := t.TempDir()
	paths := map[string]bool{Path: true}
	for _, pin := range pins {
		paths[pin.File] = true
	}
	for path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copy, path), raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, pins[0].File)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutant := range []string{strings.Replace(string(raw), pins[0].Function, "missingSibling", 1), strings.Replace(string(raw), "t.Helper()", "t.Helper();foreignAssertion()", 1)} {
		if err := os.WriteFile(path, []byte(mutant), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := EarlierSource(copy, pins[len(pins)-1].File, pins[len(pins)-1].Function, pins[len(pins)-1].After); err == nil {
			t.Fatal("unchanged supplied endpoint hid mutated sibling")
		}
	}
}
