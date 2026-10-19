package storetest

import (
	"context"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ActivityResultPublicationStorage = private.ActivityResultPublicationStorage

type ProposedEffectRunExecutionStorage = private.ProposedEffectRunExecutionStorage

func ReadProposedEffectRunExecutionStorage(ctx context.Context, selected any, runID string) (ProposedEffectRunExecutionStorage, error) {
	return private.ReadProposedEffectRunExecutionStorageForTest(ctx, selected, runID)
}

type ActivityAttemptStorageEvidence = private.ActivityAttemptStorageEvidence

func ReadActivityAttemptStorage(ctx context.Context, selected any, run string) ([]ActivityAttemptStorageEvidence, error) {
	return private.ReadActivityAttemptStorageForTest(ctx, selected, run)
}

func ActivityJournalCleanupBoundFault(t testing.TB, selected any, original runtimepipeline.ActivityAttemptJournal, fault error) (runtimepipeline.ActivityAttemptJournal, func() int32) {
	t.Helper()
	journal, count, err := private.ActivityJournalCleanupBoundFaultForTest(selected, original, fault)
	if err != nil {
		t.Fatalf("install captured original activity journal cleanup fault: %v", err)
	}
	return journal, count
}

func ObserveActivityResultPublicationStorage(t testing.TB, ctx context.Context, selected any) ActivityResultPublicationStorage {
	t.Helper()
	observed, err := private.ObserveActivityResultPublicationStorageForTest(ctx, selected)
	if err != nil {
		t.Fatalf("read activity result publication storage: %v", err)
	}
	return observed
}
