package runforkrevision

import "testing"

func TestEffectsHasDeclarationsTracksDiscardAndRetryReset(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000241"
	const eventID = "00000000-0000-0000-0000-000000000242"
	var missing *Effects
	if missing.HasDeclarations() {
		t.Fatal("nil effects declared a revision")
	}
	effects := NewEffects()
	if effects.HasDeclarations() {
		t.Fatal("new effects declared a revision")
	}
	if err := effects.AddFact(runID, FamilyEvents, eventID); err != nil || !effects.HasDeclarations() {
		t.Fatalf("exact declaration: present=%v err=%v", effects.HasDeclarations(), err)
	}
	effects = NewEffects()
	reset := effects.AttemptReset()
	if err := effects.Add(runID, FamilyEvents); err != nil || !effects.HasDeclarations() {
		t.Fatalf("whole-family declaration: present=%v err=%v", effects.HasDeclarations(), err)
	}
	reset()
	if effects.HasDeclarations() {
		t.Fatal("rolled-back declaration survived retry reset")
	}
	if err := effects.Add(runID, FamilyEvents); err != nil {
		t.Fatal(err)
	}
	if err := effects.DiscardDeletedRun(runID); err != nil || effects.HasDeclarations() {
		t.Fatalf("discarded run retained declarations: present=%v err=%v", effects.HasDeclarations(), err)
	}
}
