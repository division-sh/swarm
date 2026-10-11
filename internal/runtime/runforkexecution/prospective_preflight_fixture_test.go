package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestProspectiveComponentDoesNotSupplyFixedHistoryCertificate(t *testing.T) {
	plan := runfork.RunForkPlan{ForkPoint: runfork.RunForkPoint{Revision: 1}, ExecutionReady: true}
	fingerprint, err := runfork.SelectedPreparationPlanFingerprint(plan,
		runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "prospective-component")
	if err == nil || fingerprint != "" || !strings.Contains(err.Error(), "admitted fixed-revision history") {
		t.Fatalf("prospective component supplied executable fixed-history evidence: %q/%v", fingerprint, err)
	}
}
