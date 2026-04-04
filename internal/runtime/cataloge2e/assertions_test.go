package cataloge2e

import (
	"context"
	"testing"

	"swarm/internal/testutil"
)

func TestCatalogSubjectEntityIDs_UsesResolvedSubjectID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	rootID := "11111111-1111-1111-1111-111111111111"
	childID := "22222222-2222-2222-2222-222222222222"
	grandchildID := "33333333-3333-3333-3333-333333333333"

	for _, stmt := range []struct {
		entityID  string
		subjectID string
		flow      string
		state     string
	}{
		{entityID: rootID, subjectID: rootID, flow: rootID, state: "done"},
		{entityID: childID, subjectID: rootID, flow: "child", state: "completed"},
		{entityID: grandchildID, subjectID: rootID, flow: "grandchild", state: "finished"},
	} {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO entity_state (
				entity_id, subject_id, flow_instance, entity_type, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3, 'default', $4, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
			)
		`, stmt.entityID, stmt.subjectID, stmt.flow, stmt.state); err != nil {
			t.Fatalf("insert entity_state %s: %v", stmt.entityID, err)
		}
	}

	gotFromRoot := catalogSubjectEntityIDs(t, db, rootID)
	if len(gotFromRoot) != 3 {
		t.Fatalf("root subject entity ids len = %d, want 3 (%v)", len(gotFromRoot), gotFromRoot)
	}
	gotFromChild := catalogSubjectEntityIDs(t, db, childID)
	if len(gotFromChild) != 3 {
		t.Fatalf("child subject entity ids len = %d, want 3 (%v)", len(gotFromChild), gotFromChild)
	}
	for _, candidate := range []string{rootID, childID, grandchildID} {
		if _, ok := gotFromChild[candidate]; !ok {
			t.Fatalf("child subject entity ids missing %s (%v)", candidate, gotFromChild)
		}
	}
}
