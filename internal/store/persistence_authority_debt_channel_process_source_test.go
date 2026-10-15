package store_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtChannelProcessManifest = "tools/fixture-codemod/pipeline-observations/family145-channel-process-preimages.json"

// Source-only composition under6182382945. This never changes a site,
// occurrence, role, signature, allowed ceiling or collector identity.
func debtChannelProcessSourcePrevious(root, file, function, current string) (string, error) {
	if !strings.HasPrefix(file, "internal/serveapp/channel_") {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, debtChannelProcessManifest))
	if err != nil || debtBaselineChecksum(raw) != "1e94cca2eaa12fedf807b37ed910371a79d0ba5df00df9f79afb4db5de879334" {
		return "", fmt.Errorf("channel process signed source composition changed: %v", err)
	}
	var rows []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 45 {
		return "", fmt.Errorf("channel process complete source inventory changed: %v", err)
	}
	declarations, err := debtServedDebugDeclarations(filepath.Join(root, file))
	if err != nil {
		return "", err
	}
	for _, row := range rows {
		if row.File != file || row.Function != function {
			continue
		}
		if current != row.After || declarations[function] != row.After {
			return "", fmt.Errorf("channel process supplied/live source is not the exact endpoint")
		}
		return row.Before, nil
	}
	return current, nil
}

func TestPersistenceAuthorityDebtChannelProcessSourceInverseIsExactAndAccountingNeutral(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pin := debtSourceBlobPins[3]
	bodies, err := debtServedDebugDeclarations(filepath.Join(root, pin.File))
	if err != nil {
		t.Fatal(err)
	}
	current := bodies[pin.Function]
	prior, err := debtChannelProcessSourcePrevious(root, pin.File, pin.Function, current)
	if err != nil || prior == "" || prior == current {
		t.Fatal("channel source inverse failed:", err)
	}
	for _, changed := range []string{current + "\n", prior, ""} {
		if _, err := debtChannelProcessSourcePrevious(root, pin.File, pin.Function, changed); err == nil {
			t.Fatal("foreign/stale/missing source was accepted")
		}
	}
	if unchanged, err := debtChannelProcessSourcePrevious(root, pin.File, pin.Function+"Foreign", current); err != nil || unchanged != current {
		t.Fatal("channel inverse widened its finite declaration scope")
	}
}
