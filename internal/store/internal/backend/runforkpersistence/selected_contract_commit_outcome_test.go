package runforkpersistence

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractPostCommitErrorsPreserveExactOutputAndCause(t *testing.T) {
	cause := errors.New("postcommit cleanup failed")
	materialization := runfork.RunForkMaterialization{ForkRunID: "fork"}
	materialized := &selectedForkMaterializationPostCommitError{value: materialization, cause: cause}
	if !errors.Is(materialized, cause) || materialized.Error() != cause.Error() || materialized.SelectedForkMaterializationCommit().ForkRunID != materialization.ForkRunID {
		t.Fatalf("materialization postcommit error lost output or cause: %v", materialized)
	}
	events := []runfork.RunForkSelectedContractSourceEvent{{SourceEventID: "source-event"}}
	loaded := &selectedForkSourceEventsPostCommitError{sourceRunID: "source", forkRunID: "fork", value: events, cause: cause}
	sourceRunID, forkRunID, committed := loaded.SelectedForkSourceEventsCommit()
	if !errors.Is(loaded, cause) || loaded.Error() != cause.Error() || sourceRunID != "source" || forkRunID != "fork" || len(committed) != 1 || committed[0].SourceEventID != "source-event" {
		t.Fatalf("source-event postcommit error lost exact scope, output, or cause: %v", loaded)
	}
}
