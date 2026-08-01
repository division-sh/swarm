package pipeline

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const (
	ActivityAttemptStatusStarted   = "started"
	ActivityAttemptStatusSucceeded = "succeeded"
	ActivityAttemptStatusFailed    = "failed"
	ActivityAttemptStatusUncertain = "uncertain"
)

type ActivityAttemptRecord struct {
	RequestEventID  string
	RunID           string
	ExecutionMode   runtimeeffects.ExecutionMode
	SourceEventID   string
	ParentEventID   string
	EntityID        string
	FlowInstance    string
	NodeID          string
	HandlerEventKey string
	ActivityID      string
	Tool            string
	EffectClass     string
	Attempt         int
	Status          string
	SuccessEvent    string
	FailureEvent    string
	ResultEventID   string
	ResultEventType string
	ResultPayload   map[string]any
	Failure         *runtimefailures.Envelope
	InputHash       string
	ReplyContextID  string
	Generation      attemptgeneration.Generation
	LoopStage       string
	StartedAt       time.Time
	CompletedAt     *time.Time
	UpdatedAt       time.Time
}

func NormalizeActivityAttemptRecord(rec ActivityAttemptRecord) ActivityAttemptRecord {
	rec.RequestEventID = strings.TrimSpace(rec.RequestEventID)
	rec.RunID = strings.TrimSpace(rec.RunID)
	rec.ExecutionMode = runtimeeffects.ExecutionMode(strings.TrimSpace(string(rec.ExecutionMode)))
	rec.SourceEventID = strings.TrimSpace(rec.SourceEventID)
	rec.ParentEventID = strings.TrimSpace(rec.ParentEventID)
	rec.EntityID = strings.TrimSpace(rec.EntityID)
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.NodeID = strings.TrimSpace(rec.NodeID)
	rec.HandlerEventKey = strings.TrimSpace(rec.HandlerEventKey)
	rec.ActivityID = strings.TrimSpace(rec.ActivityID)
	rec.Tool = strings.TrimSpace(rec.Tool)
	rec.EffectClass = strings.TrimSpace(rec.EffectClass)
	rec.Status = strings.TrimSpace(rec.Status)
	rec.SuccessEvent = strings.TrimSpace(rec.SuccessEvent)
	rec.FailureEvent = strings.TrimSpace(rec.FailureEvent)
	rec.ResultEventID = strings.TrimSpace(rec.ResultEventID)
	rec.ResultEventType = strings.TrimSpace(rec.ResultEventType)
	rec.Failure = runtimefailures.CloneEnvelope(rec.Failure)
	rec.InputHash = strings.TrimSpace(rec.InputHash)
	rec.ReplyContextID = strings.TrimSpace(rec.ReplyContextID)
	rec.Generation = rec.Generation.Normalize()
	rec.LoopStage = strings.TrimSpace(rec.LoopStage)
	if rec.Attempt <= 0 {
		rec.Attempt = 1
	}
	if rec.ResultPayload == nil && rec.Status != ActivityAttemptStatusStarted {
		rec.ResultPayload = map[string]any{}
	}
	return rec
}

func (rec ActivityAttemptRecord) normalized() ActivityAttemptRecord {
	return NormalizeActivityAttemptRecord(rec)
}

func ValidateActivityAttemptStart(rec ActivityAttemptRecord) error {
	rec = NormalizeActivityAttemptRecord(rec)
	if err := validateActivityAttemptUUID(rec.RequestEventID, "request_event_id"); err != nil {
		return err
	}
	if err := validateActivityAttemptUUID(rec.RunID, "run_id"); err != nil {
		return err
	}
	if !rec.ExecutionMode.Valid() {
		return fmt.Errorf("activity attempt execution_mode %q is invalid", rec.ExecutionMode)
	}
	for field, value := range map[string]string{
		"source_event_id": rec.SourceEventID,
		"parent_event_id": rec.ParentEventID,
		"entity_id":       rec.EntityID,
	} {
		if err := validateOptionalActivityAttemptUUID(value, field); err != nil {
			return err
		}
	}
	if rec.ActivityID == "" || rec.Tool == "" || rec.EffectClass == "" || rec.SuccessEvent == "" || rec.FailureEvent == "" || rec.InputHash == "" {
		return fmt.Errorf("activity attempt %s is missing required identity fields", rec.RequestEventID)
	}
	if rec.EffectClass != "non_idempotent_write" {
		return fmt.Errorf("activity attempt effect_class %q is not supported by the non-idempotent journal", rec.EffectClass)
	}
	if rec.Attempt != 1 {
		return fmt.Errorf("activity attempt attempt = %d, want 1 for non-idempotent journal", rec.Attempt)
	}
	return nil
}

func ValidateActivityAttemptTerminal(rec ActivityAttemptRecord) error {
	rec = NormalizeActivityAttemptRecord(rec)
	if err := validateActivityAttemptUUID(rec.RequestEventID, "request_event_id"); err != nil {
		return err
	}
	if !rec.ExecutionMode.Valid() {
		return fmt.Errorf("activity attempt execution_mode %q is invalid", rec.ExecutionMode)
	}
	if rec.Status != ActivityAttemptStatusSucceeded && rec.Status != ActivityAttemptStatusFailed && rec.Status != ActivityAttemptStatusUncertain {
		return fmt.Errorf("activity attempt status %q is not terminal", rec.Status)
	}
	if err := validateActivityAttemptUUID(rec.ResultEventID, "result_event_id"); err != nil {
		return err
	}
	if rec.ResultEventType == "" {
		return fmt.Errorf("activity attempt terminal result_event_type is required")
	}
	if rec.ResultPayload == nil {
		return fmt.Errorf("activity attempt terminal result_payload is required")
	}
	if rec.Status == ActivityAttemptStatusSucceeded && rec.Failure != nil {
		return fmt.Errorf("successful activity attempt must not carry failure")
	}
	if (rec.Status == ActivityAttemptStatusFailed || rec.Status == ActivityAttemptStatusUncertain) && rec.Failure == nil {
		return fmt.Errorf("failed or uncertain activity attempt requires canonical failure")
	}
	return nil
}

func ValidateActivityAttemptClaimIdentity(actual, expected ActivityAttemptRecord) error {
	actual, expected = NormalizeActivityAttemptRecord(actual), NormalizeActivityAttemptRecord(expected)
	if actual.RequestEventID == expected.RequestEventID && actual.RunID == expected.RunID &&
		actual.EntityID == expected.EntityID && actual.NodeID == expected.NodeID &&
		actual.HandlerEventKey == expected.HandlerEventKey && actual.ActivityID == expected.ActivityID &&
		actual.Tool == expected.Tool && actual.EffectClass == expected.EffectClass && actual.ExecutionMode == expected.ExecutionMode &&
		actual.SuccessEvent == expected.SuccessEvent && actual.FailureEvent == expected.FailureEvent &&
		actual.InputHash == expected.InputHash && actual.LoopStage == expected.LoopStage &&
		((!actual.Generation.Valid() && !expected.Generation.Valid()) || actual.Generation.Equal(expected.Generation)) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "activity_loop_claim_conflict", "activity-runtime", "claim_activity_attempt", map[string]any{
		"activity_id": expected.ActivityID, "request_event_id": expected.RequestEventID,
	})
}

func validateActivityAttemptClaimIdentity(actual, expected ActivityAttemptRecord) error {
	return ValidateActivityAttemptClaimIdentity(actual, expected)
}

func ValidateActivityAttemptTerminalMode(actual, expected ActivityAttemptRecord) error {
	if actual.ExecutionMode == expected.ExecutionMode {
		return nil
	}
	return fmt.Errorf("activity attempt %s execution_mode conflict: stored %q, terminal %q", expected.RequestEventID, actual.ExecutionMode, expected.ExecutionMode)
}

func validateActivityAttemptUUID(value, field string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("activity attempt %s is required", field)
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("activity attempt %s must be a UUID: %w", field, err)
	}
	return nil
}

func validateOptionalActivityAttemptUUID(value, field string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("activity attempt %s must be a UUID when present: %w", field, err)
	}
	return nil
}
