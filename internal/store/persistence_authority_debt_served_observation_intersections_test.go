package store_test

import (
	"path/filepath"
	"testing"
)

// Report the whole older provenance inventory, not merely its first conflict.
func TestPersistenceAuthorityDebtServedObservationBatchReportsEveryOlderSourceIntersection(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	bodies, err := debtServedDebugBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	needed := debtServedDebugRequiredSources()
	for _, body := range bodies {
		if !needed[body.File+"/"+body.Function] && !needed["callee/"+body.Function] && !needed["capture/"+body.Function] {
			continue
		}
		decls, err := debtServedDebugDeclarations(filepath.Join(root, body.File))
		if err != nil {
			t.Error(body.Function, err)
			continue
		}
		current, err := debtServedObservationSourcePrevious(root, body.File, body.Function, decls[body.Function])
		if err == nil {
			current, err = debtSourceForkSourcePrevious(root, body.File, body.Function, current)
		}
		if err == nil {
			current, err = debtRunIdentitySourcePrevious(root, body.File, body.Function, current)
		}
		if err == nil {
			current, err = debtWorkflowStateSourcePrevious(root, body.File, body.Function, current)
		}
		if err == nil {
			current, err = debtRequestKeySourcePrevious(root, body.File, body.Function, current)
		}
		if err == nil {
			current, err = debtReceiverReadbackSourcePrevious(root, body.File, body.Function, current)
		}
		if err != nil || current != body.After {
			t.Errorf("uncomposed older source: %s/%s: current=%s expected=%s error=%v", body.File, body.Function, debtBaselineChecksum([]byte(current)), debtBaselineChecksum([]byte(body.After)), err)
		}
	}
}
