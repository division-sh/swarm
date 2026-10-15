package selectedreadrole

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProcessReadCompleteSignedSourceAndCurrentEndpoints(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadProcessPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProcessCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	removed, unchanged := 0, 0
	for _, pin := range pins {
		if before[pin.File] == nil {
			before[pin.File], err = exec.Command("git", "-C", root, "show", ProcessPredecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
		}
		body, err := ProcessBody(before[pin.File], pin.Function)
		if err != nil || body != pin.Before {
			t.Fatal("process-read pin changed its complete signed source:", pin.Function, err)
		}
		got, err := EarlierProcessSource(root, pin.File, pin.Function, pin.After)
		if err != nil || got != pin.Before {
			t.Fatal("process-read endpoint cannot invert:", pin.Function, err)
		}
		if _, err := EarlierProcessSource(root, pin.File, pin.Function, pin.After+"\nforeignAssertion()"); err == nil {
			t.Fatal("foreign supplied source received inverse:", pin.Function)
		}
		if pin.Before == pin.After {
			unchanged++
		}
		if pin.After == "" {
			removed++
		}
	}
	if len(pins) != 19 || removed != 2 || unchanged != 1 {
		t.Fatalf("process-read complete declaration inventory changed: %d / %d / %d", len(pins), removed, unchanged)
	}
}

func TestProcessReadHistoryOnlyAppendsAndRefusesForeignFields(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := PriorProcessHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, err := ComposeProcessHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		got, err := EarlierProcessHistory(root, current, recipes)
		if err != nil || !bytes.Equal(got, prior) {
			t.Fatal("process history did not retain exact signed predecessor:", err)
		}
		var old, rows []map[string]any
		if json.Unmarshal(prior, &old) != nil || json.Unmarshal(current, &rows) != nil || len(rows) != len(old)+18 || !reflect.DeepEqual(rows[:len(old)], old) {
			t.Fatal("process history changed original metadata/order or its eighteen appends")
		}
		assertProcessHistoryRejectsMutations(t, root, current, recipes)
	}
}

func assertProcessHistoryRejectsMutations(t *testing.T, root string, current []byte, recipes bool) {
	t.Helper()
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
		if _, err := EarlierProcessHistory(root, raw, recipes); err == nil {
			t.Fatal("process history erased changed original metadata:", field)
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
	if _, err := EarlierProcessHistory(root, raw, recipes); err == nil {
		t.Fatal("process history erased changed original ordering")
	}
}

func TestProcessReadSourceRefusesMissingOrChangedSiblingAndManifest(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadProcessPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := t.TempDir()
	paths := map[string]bool{ProcessPath: true}
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
	if err := ValidateProcessCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, pins[0].File)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(raw), pins[0].After, strings.Replace(pins[0].After, "t.Helper()", "t.Helper();foreignAssertion()", 1), 1)
	if mutated == string(raw) {
		t.Fatal("hostile control did not modify the observed sibling")
	}
	if err := os.WriteFile(path, []byte(mutated), 0644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProcessCurrent(copy, pins); err == nil {
		t.Fatal("changed actual sibling passed source validation")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProcessCurrent(copy, pins); err == nil {
		t.Fatal("missing sibling passed source validation")
	}
	if err := os.WriteFile(filepath.Join(copy, ProcessPath), []byte("[]\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadProcessPins(copy); err == nil {
		t.Fatal("changed immutable manifest passed validation")
	}
}
