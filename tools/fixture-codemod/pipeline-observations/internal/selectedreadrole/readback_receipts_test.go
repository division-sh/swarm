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

func TestReadbackCompleteSignedSourceAndExactCurrentInverse(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadReadbackPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReadbackCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, pin := range pins {
		if files[pin.File] == nil {
			files[pin.File], err = exec.Command("git", "-C", root, "show", ReadbackPredecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
		}
		before, err := Body(files[pin.File], pin.Function)
		if err != nil || before != pin.Before {
			t.Fatal("readback changed its complete signed input:", pin.Function, err)
		}
		current, err := EarlierReadbackSource(root, pin.File, pin.Function, pin.After)
		if err != nil || current != pin.Before {
			t.Fatal("readback inverse lost the signed input:", pin.Function, err)
		}
		for _, foreign := range []string{pin.Before, pin.After + "\nforeignAssertion()"} {
			if _, err := EarlierReadbackSource(root, pin.File, pin.Function, foreign); err == nil {
				t.Fatal("historical or foreign source pretended to be actual current source:", pin.Function)
			}
		}
	}
	pins[0].After += "foreign"
	if err := ValidateReadbackCurrent(root, pins); err == nil {
		t.Fatal("mutated caller inventory inherited a warm source-cache pass")
	}
	authenticated, err := ReadReadbackPins(root)
	if err != nil || authenticated[0].After == pins[0].After {
		t.Fatal("returned pin mutation corrupted the immutable decoded cache:", err)
	}
}

func TestReadbackHistoryConservesSignedMetadataAndRejectsForeignChanges(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{true, false} {
		prior, err := PriorReadbackHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		next, err := ComposeReadbackHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := EarlierReadbackHistory(root, next, recipes)
		if err != nil || !bytes.Equal(previous, prior) {
			t.Fatal("readback did not recover the verbatim signed receipt:", err)
		}
		var old, rows []map[string]any
		if json.Unmarshal(prior, &old) != nil || json.Unmarshal(next, &rows) != nil {
			t.Fatal("invalid readback history")
		}
		appends, advances, field := 8, 24, "After"
		if recipes {
			appends, advances, field = 6, 26, "Successor"
		}
		if len(rows) != len(old)+appends {
			t.Fatal("readback append multiplicity changed")
		}
		changed := assertReadbackConservedRows(t, old, rows, field)
		if changed != advances {
			t.Fatalf("readback exact advance multiplicity=%d want=%d", changed, advances)
		}
		assertReadbackHistoryHostileChanges(t, root, next, recipes)
	}
}

func assertReadbackConservedRows(t *testing.T, old, rows []map[string]any, field string) int {
	t.Helper()
	changed := 0
	for index, before := range old {
		for key, value := range before {
			if key != field && !reflect.DeepEqual(value, rows[index][key]) {
				t.Fatalf("readback changed unrelated field %d/%s", index, key)
			}
		}
		if !reflect.DeepEqual(before[field], rows[index][field]) {
			changed++
			if !ReadbackFunction(before["File"].(string), before["Function"].(string)) {
				t.Fatal("readback advanced an unenumerated history owner")
			}
		}
	}
	return changed
}

func assertReadbackHistoryHostileChanges(t *testing.T, root string, raw []byte, recipes bool) {
	t.Helper()
	for _, field := range []string{"Before", "After", "Successor", "File", "Function", "Family", "Mechanical", "Removed"} {
		var rows []map[string]any
		if err := json.Unmarshal(raw, &rows); err != nil {
			t.Fatal(err)
		}
		rows[0][field] = "foreign"
		changed, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := EarlierReadbackHistory(root, changed, recipes); err == nil {
			t.Fatal("foreign history field inherited readback authority:", field)
		}
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	rows[0], rows[1] = rows[1], rows[0]
	changed, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EarlierReadbackHistory(root, changed, recipes); err == nil {
		t.Fatal("reordered readback history received original-source credit")
	}
}

func TestReadbackWarmCacheRefusesChangedManifestAndActualSibling(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadReadbackPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{ReadbackPath: true}
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
	if err := ValidateReadbackCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, pins[0].File)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(original), "edge-create", "foreign-edge-create", 1)
	if mutated == string(original) {
		t.Fatal("source hostile control did not mutate its selected actual declaration")
	}
	if err := os.WriteFile(path, []byte(mutated), 0644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReadbackCurrent(copy, pins); err == nil {
		t.Fatal("changed actual sibling inherited the warm readback cache")
	}
	if err := os.WriteFile(path, original, 0644); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(copy, ReadbackPath)
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, append(raw, ' '), 0644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReadbackCurrent(copy, pins); err == nil {
		t.Fatal("changed manifest inherited the warm readback cache")
	}
}
