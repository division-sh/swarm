package packs

import (
	"testing"
)

func TestA9CapabilitySubjectDeclarationIdentity(t *testing.T) {
	left := effectiveTriggerSubject(RequirementStatusUnbound)
	left.TriggerAdmission.Alias = "shared"
	right := CloneSubjects([]Subject{left})[0]
	right.TriggerAdmission.FlowPath = "other"
	var err error
	right.ID, err = IngressSubjectID(right.TriggerAdmission.BundleHash, right.TriggerAdmission.FlowPath, right.Provider)
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := NormalizeSubjects([]Subject{left, right})
	if err != nil || len(subjects) != 2 || subjects[0].ID == subjects[1].ID {
		t.Fatalf("same alias collapsed distinct declarations: %+v err=%v", subjects, err)
	}
	for _, flow := range []string{"", "other", "/chat", "chat/", "chat//child"} {
		invalid := CloneSubjects([]Subject{left})[0]
		invalid.TriggerAdmission.FlowPath = flow
		if _, err := NormalizeSubjects([]Subject{invalid}); err == nil {
			t.Fatalf("missing/foreign/noncanonical owner %q accepted with ID %q", flow, invalid.ID)
		}
	}
	changed := CloneSubjects([]Subject{left})[0]
	changed.TriggerAdmission.Alias = "changed"
	if _, err := NormalizeSubjects([]Subject{changed}); err != nil {
		t.Fatalf("endpoint override changed the declaring identity: %v", err)
	}
}
