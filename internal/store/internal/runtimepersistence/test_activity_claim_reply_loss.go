package runtimepersistence

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// Lose only the reply to the exact native claim after its actual successful
// insert. No transaction, commit verdict or persisted record is reconstructed.
func ActivityClaimReplyLossPersistenceFaultForTest(ctx context.Context, selected any, run, request string, fault error) (pipeline.WorkflowPersistence, func() int32, error) {
	cut, err := activityClaimReplyLossFaultForTest(ctx, selected, run, request, fault)
	if err != nil {
		return pipeline.WorkflowPersistence{}, nil, err
	}
	return pipeline.NewWorkflowPersistence(cut), cut.lost.Load, nil
}

func ActivityClaimReplyLossBoundFaultForTest(ctx context.Context, selected any, original pipeline.ActivityAttemptJournal, run, request string, fault error) (pipeline.ActivityAttemptJournal, func() int32, error) {
	if original == nil {
		return nil, nil, fmt.Errorf("claim reply-loss fault requires its captured original journal")
	}
	cut, err := activityClaimReplyLossFaultForTest(ctx, selected, run, request, fault)
	if err != nil {
		return nil, nil, err
	}
	cut.original = original
	return cut, cut.lost.Load, nil
}

func activityClaimReplyLossFaultForTest(ctx context.Context, selected any, run, request string, fault error) (*activityClaimReplyLossFault, error) {
	for _, key := range []string{run, request} {
		id, err := uuid.Parse(key)
		if err != nil || id == uuid.Nil || id.String() != key {
			return nil, fmt.Errorf("claim reply-loss fault requires canonical run/request identity")
		}
	}
	if fault == nil {
		return nil, fmt.Errorf("claim reply-loss fault requires an error")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	owner, ok := selected.(pipeline.WorkflowPersistenceOwner)
	if !ok {
		return nil, fmt.Errorf("claim reply-loss fault requires the original workflow owner")
	}
	if _, found, err := owner.LoadActivityAttempt(ctx, request); err != nil {
		return nil, err
	} else if found {
		return nil, fmt.Errorf("claim reply-loss fault requires an absent native receipt")
	}
	return &activityClaimReplyLossFault{WorkflowPersistenceOwner: owner, original: owner, run: run, request: request, fault: fault}, nil
}

type activityClaimReplyLossFault struct {
	pipeline.WorkflowPersistenceOwner
	original     pipeline.ActivityAttemptJournal
	run, request string
	fault        error
	lost         atomic.Int32
}

func (f *activityClaimReplyLossFault) ClaimActivityAttemptForLoopGeneration(ctx context.Context, request pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	stored, inserted, err := f.original.ClaimActivityAttemptForLoopGeneration(ctx, request)
	if err != nil || !inserted || request.RunID != f.run || request.RequestEventID != f.request || !f.lost.CompareAndSwap(0, 1) {
		return stored, inserted, err
	}
	return pipeline.ActivityAttemptRecord{}, false, f.fault
}

func (f *activityClaimReplyLossFault) StartActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return f.original.StartActivityAttempt(ctx, record)
}

func (f *activityClaimReplyLossFault) CompleteActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return f.original.CompleteActivityAttempt(ctx, record)
}

func (f *activityClaimReplyLossFault) MarkActivityAttemptUncertain(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return f.original.MarkActivityAttemptUncertain(ctx, record)
}

func (f *activityClaimReplyLossFault) LoadActivityAttempt(ctx context.Context, request string) (pipeline.ActivityAttemptRecord, bool, error) {
	return f.original.LoadActivityAttempt(ctx, request)
}
