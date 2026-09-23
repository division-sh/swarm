package runforkrevision

import (
	"testing"

	"github.com/google/uuid"
)

func TestWholeParentDeletionDiscardsOnlyDeletedRunEffects(t *testing.T) {
	deletedRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	effects := NewEffects()
	if err := effects.AddFact(deletedRunID, FamilyEvents, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if err := effects.AddFact(otherRunID, FamilyEvents, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if err := effects.DiscardDeletedRun(deletedRunID); err == nil {
		t.Fatal("whole-parent deletion discarded another run's contributions")
	}
	if len(effects.byRun) != 2 {
		t.Fatalf("failed discard changed contributions: %d runs", len(effects.byRun))
	}
	delete(effects.byRun, otherRunID)
	if err := effects.DiscardDeletedRun(deletedRunID); err != nil {
		t.Fatal(err)
	}
	if len(effects.byRun) != 0 {
		t.Fatalf("deleted parent retained %d historical contributions", len(effects.byRun))
	}
}
