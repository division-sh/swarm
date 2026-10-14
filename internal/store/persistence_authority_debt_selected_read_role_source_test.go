package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const debtSelectedReadRoleManifest = "tools/fixture-codemod/pipeline-observations/family145-selected-read-role-preimages.json"

func debtSelectedReadRolePins(root string) ([]debtRunIdentitySourceBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, debtSelectedReadRoleManifest))
	if err != nil || debtBaselineChecksum(raw) != "dd9421b12197694434ac1134f95ce1d28e4ab9fad9e5530e830ae9a0393aa5be" {
		return nil, fmt.Errorf("selected read-role immutable source inventory changed: %v", err)
	}
	var pins []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 43 {
		return nil, fmt.Errorf("selected read-role exact declaration count changed")
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || pin.Before == pin.After || pin.After == "" || debtBaselineChecksum([]byte(pin.Before)) != pin.SHA256 {
			return nil, fmt.Errorf("selected read-role duplicate/changed predecessor: %s", key)
		}
		seen[key] = true
	}
	return pins, nil
}

func debtSelectedReadRoleLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	pins, err := debtSelectedReadRolePins(root)
	if err != nil {
		return nil, err
	}
	files := map[string]map[string]string{}
	for _, pin := range pins {
		if files[pin.File] == nil {
			files[pin.File], err = debtServedSetupDeclarations(filepath.Join(root, pin.File))
			if err != nil {
				return nil, err
			}
		}
		if files[pin.File][pin.Function] != pin.After {
			return nil, fmt.Errorf("selected read-role actual source changed: %s", pin.Function)
		}
	}
	return pins, nil
}

// Source-evidence ingress only: no occurrence identity or classifier changes.
func debtSelectedReadRoleSourcePrevious(root, file, function, current string) (string, error) {
	pins, err := debtSelectedReadRolePins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if _, err := debtSelectedReadRoleLiveSources(root); err != nil {
			return "", err
		}
		if pin.After != current {
			return "", fmt.Errorf("selected read-role supplied source is not actual current source: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func debtSelectedReadRoleCopySources(t *testing.T, root, copy string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, debtSelectedReadRoleManifest))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtSelectedReadRoleManifest, string(raw))
	pins, err := debtSelectedReadRoleLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
	}
}

func TestPersistenceAuthorityDebtSelectedReadRoleSourceRefusesHistoricalForeignAndStaleEvidence(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtSelectedReadRoleLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		before, err := debtSelectedReadRoleSourcePrevious(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatalf("exact source inverse refused %s: %v", pin.Function, err)
		}
		if _, err := debtSelectedReadRoleSourcePrevious(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("historical supplied source accepted as current")
		}
		if got, err := debtSelectedReadRoleSourcePrevious(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign source received inverse:", err)
		}
	}
	copy := t.TempDir()
	debtSelectedReadRoleCopySources(t, root, copy)
	debtSourceBlobCopyDeclaration(t, copy, pins[0].File, pins[0].Function, pins[0].Before)
	if _, err := debtSelectedReadRoleSourcePrevious(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("supplied current string hid a stale actual sibling")
	}
	if _, err := debtSelectedReadRoleLiveSources(t.TempDir()); err == nil {
		t.Fatal("missing evidence accepted")
	}
}
