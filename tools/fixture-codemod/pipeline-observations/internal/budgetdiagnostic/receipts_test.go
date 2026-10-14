package budgetdiagnostic

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBudgetHistoryIsOneExactAppendAndAdvanceWithoutMetadataLoss(t *testing.T) {
	root := filepath.Clean("../../../../..")
	for _, recipes := range []bool{true, false} {
		prior, err := PriorHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, err := ComposeHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := EarlierHistory(root, current, recipes)
		if err != nil || !bytes.Equal(previous, prior) {
			t.Fatal("exact historical metadata/order was not restored:", err)
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
				t.Fatal("changed original history accepted:", field)
			}
		}
		if _, err := ComposeHistory(root, append(prior, ' '), recipes); err == nil {
			t.Fatal("unsigned predecessor bytes accepted")
		}
	}
}

func TestBudgetSourceInverseRequiresEveryActualCurrentDeclaration(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		previous, err := EarlierSource(root, pin.File, pin.Function, pin.After)
		if err != nil || previous != pin.Before {
			t.Fatal("exact current inverse refused:", err)
		}
		if _, err := EarlierSource(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("supplied historical body replaced current source")
		}
		if got, err := EarlierSource(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign declaration received an inverse:", err)
		}
	}
	copy := t.TempDir()
	for _, path := range []string{Path, Source} {
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
	raw, err := os.ReadFile(filepath.Join(copy, Source))
	if err != nil {
		t.Fatal(err)
	}
	mutant := strings.Replace(string(raw), pins[0].Function, "foreignEndpoint", 1)
	if err := os.WriteFile(filepath.Join(copy, Source), []byte(mutant), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := EarlierSource(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("unchanged supplied string hid a missing actual declaration")
	}
}
