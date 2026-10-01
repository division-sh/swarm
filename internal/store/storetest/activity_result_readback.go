package storetest

import (
	"context"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ActivityResultPublicationStorage = private.ActivityResultPublicationStorage

func ActivityJournalCleanupPersistenceFault(t testing.TB, selected any, fault error) (runtimepipeline.WorkflowPersistence, func() int32) {
	t.Helper()
	persistence, count, err := private.ActivityJournalCleanupPersistenceFaultForTest(selected, fault)
	if err != nil {
		t.Fatalf("install exact selected activity journal cleanup fault: %v", err)
	}
	return persistence, count
}

func ObserveActivityResultPublicationStorage(t testing.TB, ctx context.Context, selected any) ActivityResultPublicationStorage {
	t.Helper()
	observed, err := private.ObserveActivityResultPublicationStorageForTest(ctx, selected)
	if err != nil {
		t.Fatalf("read activity result publication storage: %v", err)
	}
	return observed
}
