package budgetdiagnostic

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
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
		canonical := budgetReadRoleBoundary(t, root, current, recipes)
		current, err = selectedreadrole.ComposeHistory(root, canonical, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current = budgetLaterReadBoundaries(t, root, current, recipes)
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

func budgetLaterReadBoundaries(t *testing.T, root string, current []byte, recipes bool) []byte {
	t.Helper()
	for _, stage := range []struct {
		prior   func(string, bool) ([]byte, error)
		compose func(string, []byte, bool) ([]byte, error)
	}{
		{selectedreadrole.PriorProcessHistory, selectedreadrole.ComposeProcessHistory},
		{selectedreadrole.PriorReadbackHistory, selectedreadrole.ComposeReadbackHistory},
	} {
		signed, err := stage.prior(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var generated, trusted []map[string]any
		if json.Unmarshal(current, &generated) != nil || json.Unmarshal(signed, &trusted) != nil || !reflect.DeepEqual(generated, trusted) {
			t.Fatal("budget output differs from a complete later signed read boundary")
		}
		current, err = stage.compose(root, signed, recipes)
		if err != nil {
			t.Fatal(err)
		}
	}
	return current
}

func budgetReadRoleBoundary(t *testing.T, root string, current []byte, recipes bool) []byte {
	t.Helper()
	canonical, err := selectedreadrole.PriorHistory(root, recipes)
	if err != nil {
		t.Fatal(err)
	}
	var generated, signed []map[string]any
	if err := json.Unmarshal(current, &generated); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(canonical, &signed); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(generated, signed) {
		t.Fatal("budget generation differs from the complete signed read-role boundary")
	}
	return canonical
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
