package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const debtChannelSettlementManifest = "tools/fixture-codemod/pipeline-observations/family145-channel-settlement-preimages.json"

func debtChannelSettlementPins(root string) ([]debtRunIdentitySourceBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, debtChannelSettlementManifest))
	if err != nil || debtBaselineChecksum(raw) != "b4e5394d408d8fdfffc4f997a366d45896abfdd71e19c893d8a06baac3c8168f" {
		return nil, fmt.Errorf("channel settlement exact source inventory changed: %v", err)
	}
	const digest = "b4e5394d408d8fdfffc4f997a366d45896abfdd71e19c893d8a06baac3c8168f"
	if cached, ok := debtSelectedReadRolePinCache.Load(digest); ok {
		return slices.Clone(cached.([]debtRunIdentitySourceBody)), nil
	}
	var pins []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 38 {
		return nil, fmt.Errorf("channel settlement source inventory changed")
	}
	debtSelectedReadRolePinCache.Store(digest, slices.Clone(pins))
	return pins, nil
}

func debtChannelSettlementLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	pins, err := debtChannelSettlementPins(root)
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
		current := files[pin.File][pin.Function]
		if current != pin.After {
			current, err = debtChannelProcessSourcePrevious(root, pin.File, pin.Function, current)
		}
		if err != nil || current != pin.After {
			return nil, fmt.Errorf("channel settlement actual source changed: %s", pin.Function)
		}
	}
	return pins, nil
}

func debtChannelSettlementSourcePrevious(root, file, function, current string) (string, error) {
	pins, err := debtChannelSettlementPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if _, err := debtChannelSettlementLiveSources(root); err != nil {
			return "", err
		}
		if current != pin.After {
			current, err = debtChannelProcessSourcePrevious(root, file, function, current)
			if err != nil {
				return "", err
			}
		}
		if pin.After != current {
			return "", fmt.Errorf("channel settlement supplied historical instead of actual source: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func debtChannelSettlementCopySources(t *testing.T, root, copy string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, debtChannelSettlementManifest))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtChannelSettlementManifest, string(raw))
	pins, err := debtChannelSettlementLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
	}
}

func TestPersistenceAuthorityDebtChannelSettlementSourceInverseRefusesForeignEvidence(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtChannelSettlementLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		previous, err := debtChannelSettlementSourcePrevious(root, pin.File, pin.Function, pin.After)
		if err != nil || previous != pin.Before {
			t.Fatalf("exact source inverse failed for %s: %v", pin.Function, err)
		}
		if _, err := debtChannelSettlementSourcePrevious(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("historical source accepted as current:", pin.Function)
		}
		if got, err := debtChannelSettlementSourcePrevious(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign source received an inverse:", err)
		}
	}
	copy := t.TempDir()
	debtChannelSettlementCopySources(t, root, copy)
	if _, err := debtChannelSettlementLiveSources(copy); err != nil {
		t.Fatal(err)
	}
	debtSourceBlobCopyDeclaration(t, copy, pins[0].File, pins[0].Function, pins[0].Before)
	if _, err := debtChannelSettlementSourcePrevious(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("supplied current string hid another stale actual source")
	}
	if _, err := debtChannelSettlementLiveSources(t.TempDir()); err == nil {
		t.Fatal("missing source evidence accepted")
	}
}
