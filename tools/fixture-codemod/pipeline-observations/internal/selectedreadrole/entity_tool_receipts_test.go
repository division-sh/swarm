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

func TestEntityToolCompleteSignedSourceAndOneTimeInverse(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateEntityToolCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	retired := 0
	for _, pin := range pins {
		before, err := EarlierEntityToolSource(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatal("exact current inverse lost its signed input:", pin.Function, err)
		}
		if pin.Removed {
			retired++
		}
		for _, foreign := range []string{pin.Before, pin.After + "\nforeignAssertion()"} {
			if _, err := EarlierEntityToolSource(root, pin.File, pin.Function, foreign); err == nil {
				t.Fatal("historical or foreign text acquired live-source authority:", pin.Function)
			}
		}
	}
	if retired != 4 {
		t.Fatal("complete retirement inventory changed:", retired)
	}
	pins[0].After += "foreign"
	if err := ValidateEntityToolCurrent(root, pins); err == nil {
		t.Fatal("supplied inventory bypassed manifest authentication after a warm pass")
	}
}

func TestEntityToolHistoryPreservesEverySignedFieldAndExactMultiplicities(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{true, false} {
		prior, err := PriorEntityToolHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		next, err := ComposeEntityToolHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := EarlierEntityToolHistory(root, next, recipes)
		if err != nil || !bytes.Equal(restored, prior) {
			t.Fatal("complete original history was not recovered verbatim:", err)
		}
		var old, current []map[string]any
		if json.Unmarshal(prior, &old) != nil || json.Unmarshal(next, &current) != nil {
			t.Fatal("invalid entity-tool history")
		}
		appends, advances, retirements, field := 37, 6, 0, "After"
		if recipes {
			appends, advances, retirements, field = 29, 12, 2, "Successor"
		}
		if len(current) != len(old)+appends {
			t.Fatal("entity-tool append multiplicity changed:", len(current)-len(old))
		}
		changed, removed := entityToolConservedRows(t, old, current, field)
		if changed != advances || removed != retirements {
			t.Fatalf("entity-tool history advances/retirements=%d/%d want=%d/%d", changed, removed, advances, retirements)
		}
		assertEntityToolHistoryHostileChanges(t, root, prior, next, recipes)
	}
}

func entityToolConservedRows(t *testing.T, old, current []map[string]any, field string) (changed, removed int) {
	t.Helper()
	for index, before := range old {
		after := current[index]
		for key, value := range before {
			if key != field && !reflect.DeepEqual(value, after[key]) {
				t.Fatalf("entity-tool changed retained historical field %d/%s", index, key)
			}
		}
		for key := range after {
			if _, existed := before[key]; !existed && key != field && key != "Removed" {
				t.Fatalf("entity-tool added unrelated metadata %d/%s", index, key)
			}
		}
		if !reflect.DeepEqual(before[field], after[field]) {
			changed++
		}
		if after["Removed"] == true && before["Removed"] != true {
			removed++
		}
	}
	return changed, removed
}

func assertEntityToolHistoryHostileChanges(t *testing.T, root string, prior, next []byte, recipes bool) {
	t.Helper()
	for _, field := range []string{"Before", "After", "Successor", "File", "Function", "Family", "Mechanical", "Removed"} {
		var rows []map[string]any
		if err := json.Unmarshal(next, &rows); err != nil {
			t.Fatal(err)
		}
		rows[0][field] = "foreign"
		changed, _ := json.Marshal(rows)
		if _, err := EarlierEntityToolHistory(root, changed, recipes); err == nil {
			t.Fatal("foreign historical field accepted:", field)
		}
	}
	for _, raw := range [][]byte{prior, append(append([]byte(nil), prior[:len(prior)-1]...), []byte(",{}]")...)} {
		if _, err := EarlierEntityToolHistory(root, raw, recipes); err == nil {
			t.Fatal("missing batch membership accepted")
		}
	}
	var rows []map[string]any
	if err := json.Unmarshal(next, &rows); err != nil {
		t.Fatal(err)
	}
	rows[0], rows[1] = rows[1], rows[0]
	changed, _ := json.Marshal(rows)
	if _, err := EarlierEntityToolHistory(root, changed, recipes); err == nil {
		t.Fatal("reordered signed history accepted")
	}
	if _, err := ComposeEntityToolHistory(root, changed, recipes); err == nil {
		t.Fatal("foreign predecessor accepted for composition")
	}
}

func entityToolProbeCheckout(t *testing.T, root string, pins []EntityToolPin) string {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{EntityToolPath: true}
	for _, pin := range pins {
		paths[pin.File] = true
	}
	for path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copy, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return copy
}

func TestEntityToolWarmCacheRefusesMissingStaleAndDuplicateSiblings(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := entityToolProbeCheckout(t, root, pins)
	if err := ValidateEntityToolCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	pin := pins[len(pins)-1]
	path := filepath.Join(copy, pin.File)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{[]byte("package serveapp\n"), append(append([]byte(nil), original...), []byte("\n"+pin.After)...)} {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := EarlierEntityToolSource(copy, pins[0].File, pins[0].Function, pins[0].After); err == nil {
			t.Fatal("one changed sibling was hidden by supplied endpoint or warm cache")
		}
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(copy, EntityToolPath)
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]byte{append(raw, ' '), []byte("[]"), bytes.Replace(raw, []byte(`"Function":`), []byte(`"Foreign": true, "Function":`), 1)} {
		if err := os.WriteFile(manifest, invalid, 0600); err != nil {
			t.Fatal(err)
		}
		if err := ValidateEntityToolCurrent(copy, pins); err == nil {
			t.Fatal("changed complete manifest inherited a cached pass")
		}
	}
}

func TestEntityToolRetirementRefusesResurrectionAndExcludedInitializerReferences(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := entityToolProbeCheckout(t, root, pins)
	for _, pin := range pins {
		if !pin.Removed {
			continue
		}
		path := filepath.Join(copy, "internal/runtime/tools/foreign_reference_test.go")
		for _, body := range []string{pin.Before, "var foreign = " + pin.Function} {
			if err := os.WriteFile(path, []byte("//go:build never\n\npackage tools\n"+body+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := EarlierEntityToolSource(copy, pin.File, pin.Function, ""); err == nil {
				t.Fatal("supplied absence hid actual retired declaration/reference:", pin.Function)
			}
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateEntityToolCurrent(copy, pins); err != nil {
		t.Fatal("retired source did not restore after actual reference removal:", err)
	}
}

func TestEntityToolSourceOwnershipAndEnumerationFailClosed(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := entityToolProbeCheckout(t, root, pins)
	if err := ValidateEntityToolCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(copy, "internal/runtime/tools/.git")
	if err := os.WriteFile(marker, []byte("gitdir: foreign\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEntityToolCurrent(copy, pins); err == nil {
		t.Fatal("foreign checkout supplied the canonical source pins")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	unresolved := filepath.Join(copy, "internal/runtime/tools/parse_failure_test.go")
	if err := os.WriteFile(unresolved, []byte("//go:build never\n\npackage tools\nvar broken ="), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEntityToolCurrent(copy, pins); err == nil {
		t.Fatal("unresolved excluded source inherited a warm complete-enumeration pass")
	}
	if err := os.Remove(unresolved); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(copy, pins[0].File)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEntityToolCurrent(copy, pins); err == nil {
		t.Fatal("missing owned source inherited a cached pass")
	}
}
