package failures

import (
	"errors"
	"fmt"
	"testing"
)

func TestStateContentionRequiresOnlyExactUncommittedConflictBranches(t *testing.T) {
	conflict := New(ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "test", "commit", nil)
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"exact", conflict, true},
		{"wrapped", fmt.Errorf("commit: %w", conflict), true},
		{"two conflicts", errors.Join(conflict, conflict), true},
		{"cleanup failed", errors.Join(conflict, errors.New("cleanup")), false},
		{"other lifecycle", New(ClassLifecycleConflict, "lifecycle_transition_conflict", "test", "commit", nil), false},
		{"wrong class", New(ClassAuthorizationDenied, "workflow_engine_state_revision_conflict", "test", "commit", nil), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsStateContention(tc.err); got != tc.want {
				t.Fatalf("contention=%v want=%v err=%v", got, tc.want, tc.err)
			}
		})
	}
}
