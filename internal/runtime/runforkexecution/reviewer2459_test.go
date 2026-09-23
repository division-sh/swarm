package runforkexecution

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestReviewer2459NativeCleanupJoinRetainsForkAcknowledgement(t *testing.T) {
	cleanup := errors.Join(errors.New("connection cleanup failed"))
	t.Run("materialization", func(t *testing.T) {
		committed := selectedMaterializationCommitTestError{value: runfork.RunForkMaterialization{ForkRunID: "fork"}, cause: cleanup}
		if _, ok := isolatedSelectedForkMaterializationCommit(committed); !ok {
			t.Fatal("discarded acknowledged materialization solely because native cleanup uses errors.Join")
		}
	})
	t.Run("source-events", func(t *testing.T) {
		committed := selectedSourceEventsCommitTestError{sourceRunID: "source", forkRunID: "fork", cause: cleanup}
		if _, _, _, ok := isolatedSelectedForkSourceEventsCommit(committed); !ok {
			t.Fatal("discarded acknowledged source-event copy solely because native cleanup uses errors.Join")
		}
	})
}
