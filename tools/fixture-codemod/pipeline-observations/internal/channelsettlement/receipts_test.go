package channelsettlement

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChannelSettlementHistoryRefusesForeignEvidence(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, recipes := range []bool{false, true} {
		prior, err := PriorHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, err := ComposeHistory(prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := RestoreHistory(current, prior, pins, recipes)
		if err != nil || !bytes.Equal(restored, prior) {
			t.Fatal("original metadata/order/source lost:", err)
		}
		for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Removed", "Family"} {
			var rows []map[string]any
			if err := json.Unmarshal(current, &rows); err != nil {
				t.Fatal(err)
			}
			rows[0][field] = "foreign"
			mutant, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RestoreHistory(mutant, prior, pins, recipes); err == nil {
				t.Fatalf("changed history accepted: %s", field)
			}
		}
		if _, err := ComposeHistory(append(prior, ' '), pins, recipes); err == nil {
			t.Fatal("changed signed history accepted")
		}
	}
}

func TestChannelSettlementLiveSourceRefusesForeignEvidence(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if _, err := EarlierSource(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("historical substitution accepted:", pin.Function)
		}
	}
	copy := t.TempDir()
	for _, pin := range pins {
		path := filepath.Join(copy, pin.File)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(root, pin.File))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	pin := pins[0]
	path := filepath.Join(copy, pin.File)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(raw), pin.Function, "foreignEndpoint", 1)
	if err := os.WriteFile(path, []byte(changed), 0644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCurrent(copy, pins); err == nil {
		t.Fatal("warm parse cache hid changed actual source")
	}
}
