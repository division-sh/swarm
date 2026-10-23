package pipeline

import (
	"context"

	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type capturedActivityJournalProbe struct {
	ActivityAttemptJournal
	claims    atomic.Int32
	terminals atomic.Int32
	mu        sync.Mutex
	record    *ActivityAttemptRecord
}

func (j *capturedActivityJournalProbe) LoadActivityAttempt(_ context.Context, id string) (ActivityAttemptRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.record == nil || j.record.RequestEventID != id {
		return ActivityAttemptRecord{}, false, nil
	}
	return *j.record, true, nil
}

func (j *capturedActivityJournalProbe) ClaimActivityAttemptForLoopGeneration(_ context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	j.claims.Add(1)
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.record != nil {
		return *j.record, false, nil
	}
	record.StartedAt = time.Now().UTC()
	j.record = &record
	return record, true, nil
}

func (j *capturedActivityJournalProbe) CompleteActivityAttempt(_ context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	j.terminals.Add(1)
	j.mu.Lock()
	defer j.mu.Unlock()
	completed := time.Now().UTC()
	record.CompletedAt = &completed
	j.record = &record
	return record, true, nil
}

func (j *capturedActivityJournalProbe) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	return j.CompleteActivityAttempt(ctx, record)
}

type unusedActivityInstanceReader struct {
	WorkflowInstancePersistenceReader
}

// These HTTP/payload unit controls do not supply native execution permission.
func newJournaledActivityHTTPComponentForTest(t *testing.T, bus Bus, options PipelineCoordinatorOptions) *PipelineCoordinator {
	t.Helper()
	pc := newPreviewPipelineCoordinatorForTest(bus, options)
	pc.workflowStore = &workflowInstanceStore{instanceReader: &unusedActivityInstanceReader{}}
	if err := pc.InstallActivityJournal(&capturedActivityJournalProbe{}); err != nil {
		t.Fatal(err)
	}
	return pc
}
