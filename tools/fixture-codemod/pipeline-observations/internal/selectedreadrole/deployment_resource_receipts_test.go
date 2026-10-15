package selectedreadrole

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDeploymentResourceCompleteSignedSourceAndOnceOnlyInverse(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeploymentResourceCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		before, err := EarlierDeploymentResourceSource(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatal("exact signed predecessor lost:", pin.Function, err)
		}
		for _, bad := range []string{pin.Before, pin.After + "\nforeignAssertion()"} {
			if _, err := EarlierDeploymentResourceSource(root, pin.File, pin.Function, bad); err == nil {
				t.Fatal("historical or foreign text acquired live-source authority:", pin.Function)
			}
		}
	}
	pins[0].After += "foreign"
	if err := ValidateDeploymentResourceCurrent(root, pins); err == nil {
		t.Fatal("supplied inventory bypassed complete manifest authentication")
	}
}

func TestDeploymentResourceHistoryPreservesAllFieldsAndRejectsForeignChanges(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := PriorDeploymentResourceHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		next, err := ComposeDeploymentResourceHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := EarlierDeploymentResourceHistory(root, next, recipes)
		if err != nil || !bytes.Equal(restored, prior) {
			t.Fatal("complete signed history was not restored verbatim:", err)
		}
		var old, current []map[string]any
		if json.Unmarshal(prior, &old) != nil || json.Unmarshal(next, &current) != nil {
			t.Fatal("invalid complete history")
		}
		field := "After"
		if recipes {
			field = "Successor"
		}
		changed, removed := entityToolConservedRows(t, old, current, field)
		if removed != 0 || changed+len(current)-len(old) != 32 {
			t.Fatalf("complete32 retained declarations not conserved: advances=%d appends=%d retirements=%d", changed, len(current)-len(old), removed)
		}
		assertDeploymentResourceHistoryHostile(t, root, prior, next, recipes)
	}
}

func assertDeploymentResourceHistoryHostile(t *testing.T, root string, prior, next []byte, recipes bool) {
	t.Helper()
	for _, field := range []string{"Before", "After", "Successor", "File", "Function", "Family", "Mechanical", "Removed"} {
		var rows []map[string]any
		if err := json.Unmarshal(next, &rows); err != nil {
			t.Fatal(err)
		}
		rows[0][field] = "foreign"
		bad, _ := json.Marshal(rows)
		if _, err := EarlierDeploymentResourceHistory(root, bad, recipes); err == nil {
			t.Fatal("foreign historical field accepted:", field)
		}
	}
	var rows []map[string]any
	if err := json.Unmarshal(next, &rows); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]map[string]any{rows[:len(rows)-1], append(append([]map[string]any(nil), rows...), rows[len(rows)-1])} {
		raw, _ := json.Marshal(bad)
		if _, err := EarlierDeploymentResourceHistory(root, raw, recipes); err == nil {
			t.Fatal("missing or duplicate batch membership accepted")
		}
	}
	rows[0], rows[1] = rows[1], rows[0]
	bad, _ := json.Marshal(rows)
	if _, err := EarlierDeploymentResourceHistory(root, bad, recipes); err == nil {
		t.Fatal("reordered history accepted")
	}
	if _, err := ComposeDeploymentResourceHistory(root, next, recipes); err == nil {
		t.Fatal("composition repeated or accepted a foreign predecessor")
	}
	if _, err := EarlierDeploymentResourceHistory(root, prior, recipes); err == nil {
		t.Fatal("batch missing entirely")
	}
}

func TestDeploymentResourceWarmCacheCannotHideForeignMissingOrChangedSource(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := entityToolProbeCheckout(t, root, pins)
	manifest, err := os.ReadFile(filepath.Join(root, DeploymentResourcePath))
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(copy, DeploymentResourcePath)
	if err := os.WriteFile(manifestPath, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeploymentResourceCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	pin := pins[len(pins)-1]
	path := filepath.Join(copy, pin.File)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{[]byte("package conformance\n"), []byte("package conformance\nvar broken ="), append(append([]byte(nil), raw...), []byte("\n"+pin.After)...)} {
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := EarlierDeploymentResourceSource(copy, pins[0].File, pins[0].Function, pins[0].After); err == nil {
			t.Fatal("one changed sibling was hidden by warm cache or supplied endpoint")
		}
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(filepath.Dir(path), ".git")
	if err := os.WriteFile(marker, []byte("gitdir: foreign\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeploymentResourceCurrent(copy, pins); err == nil {
		t.Fatal("foreign checkout supplied source authority")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeploymentResourceCurrent(copy, pins); err == nil {
		t.Fatal("missing owned source inherited a warm pass")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, append(manifest, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDeploymentResourceCurrent(copy, pins); err == nil {
		t.Fatal("manifest bytes changed without a new inventory")
	}
	if err := os.WriteFile(manifestPath, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if actual, err := ReadDeploymentResourcePins(copy); err != nil || !reflect.DeepEqual(actual, pins) {
		t.Fatal("exact manifest did not restore:", err)
	}
}
