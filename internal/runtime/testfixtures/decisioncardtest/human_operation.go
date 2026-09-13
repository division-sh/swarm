package decisioncardtest

import (
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"testing"
)

func HumanOperation(t testing.TB, runID, logicalCall string) decisioncard.HumanTaskOperationID {
	t.Helper()
	id, err := decisioncard.NewHumanTaskOperationID(runID, logicalCall)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
