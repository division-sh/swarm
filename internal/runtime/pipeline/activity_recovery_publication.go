package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// RecoveredHTTPActivity retains content read by the native recovery owner. It
// conveys neither transaction acknowledgement nor execution permission.
type RecoveredHTTPActivity struct {
	ActivityAttemptRecord
	Request    events.Event
	BundleHash string
}

// PublishRecoveredHTTPActivity consumes acknowledged recovery through the
// existing result owner. An uncommitted candidate cannot replace journal facts.
func (pc *PipelineCoordinator) PublishRecoveredHTTPActivity(ctx context.Context, recovered RecoveredHTTPActivity) error {
	if ctx == nil {
		return errors.New("activity recovery publication requires its original context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pc == nil || pc.bus == nil || pc.workflowStore == nil || pc.workflowStore.activityJournal == nil {
		return errors.New("activity recovery publication requires its original result and journal owners")
	}
	if recovered.Status == ActivityAttemptStatusStarted {
		return errors.New("activity recovery publication requires an acknowledged terminal result")
	}
	if _, err := DisplacedHTTPActivityResult(recovered.Request, recovered.ActivityAttemptRecord, recovered.BundleHash); err != nil {
		return err
	}
	stored, found, err := pc.workflowStore.LoadActivityAttempt(ctx, recovered.RequestEventID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("activity recovery terminal result is not present in its original journal")
	}
	if err := requireRecoveredHTTPActivityReadback(stored, recovered.ActivityAttemptRecord); err != nil {
		return err
	}
	intent, err := activityIntentFromRequestEvent(recovered.Request)
	if err != nil {
		return err
	}
	ctx, err = activityExecutionContext(ctx, intent)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return (pipelineActivityDispatcher{coordinator: pc}).publishExistingActivityAttempt(ctx, intent, stored)
}

func requireRecoveredHTTPActivityReadback(stored, expected ActivityAttemptRecord) error {
	if err := ValidateActivityAttemptExecutionIdentity(stored, expected); err != nil {
		return err
	}
	if err := ValidateActivityAttemptTerminal(stored); err != nil {
		return err
	}
	actual, err := canonicaljson.MarshalPreservingNumberKinds(normalizeRecoveredHTTPActivityTimes(stored))
	if err != nil {
		return err
	}
	want, err := canonicaljson.MarshalPreservingNumberKinds(normalizeRecoveredHTTPActivityTimes(expected))
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, want) {
		return fmt.Errorf("activity recovery result %s differs from its durable journal", expected.RequestEventID)
	}
	return nil
}

func normalizeRecoveredHTTPActivityTimes(record ActivityAttemptRecord) ActivityAttemptRecord {
	record = NormalizeActivityAttemptRecord(record)
	record.StartedAt, record.UpdatedAt = record.StartedAt.UTC(), record.UpdatedAt.UTC()
	if record.CompletedAt != nil {
		completed := record.CompletedAt.UTC()
		record.CompletedAt = &completed
	}
	return record
}
