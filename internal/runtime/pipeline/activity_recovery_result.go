package pipeline

import (
	"bytes"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// DisplacedHTTPActivityResult prepares journal content, not execution or recovery
// authority. The native owner must prove displacement before writing this value.
func DisplacedHTTPActivityResult(request events.Event, record ActivityAttemptRecord, bundleHash string) (ActivityAttemptRecord, error) {
	intent, err := admitActivityRecoveryRequest(request, record, bundleHash)
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	if record.Status != ActivityAttemptStatusStarted {
		if err := validateActivityRecoveryTerminal(intent, record); err != nil {
			return ActivityAttemptRecord{}, err
		}
		return record, nil
	}
	if record.ResultEventID != "" || record.ResultEventType != "" || record.ResultPayload != nil || record.Failure != nil || record.CompletedAt != nil {
		return ActivityAttemptRecord{}, fmt.Errorf("started activity recovery has contradictory terminal evidence")
	}
	publication, err := admitActivityPublication(intent, intent.FailureEvent)
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	cause := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "activity_provider_outcome_uncertain", "activity-runtime", "recover_displaced_http_activity", map[string]any{
		"activity_id": intent.ActivityID, "tool": intent.Tool,
	})
	failure := runtimefailures.Normalize(cause, "activity-runtime", "recover_displaced_http_activity")
	return record.withTerminal(ActivityAttemptStatusUncertain, publication.eventID, string(publication.eventType), activityFailurePayload(intent, cause), &failure), nil
}

func admitActivityRecoveryRequest(request events.Event, record ActivityAttemptRecord, bundleHash string) (runtimeengine.ActivityIntent, error) {
	if request.Type() != activityRequestEventType || request.ID() != record.RequestEventID || request.RunID() != record.RunID ||
		request.ExecutionMode() != runtimeeffects.ExecutionModeLive || record.ExecutionMode != runtimeeffects.ExecutionModeLive {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity recovery requires its exact original live request")
	}
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	if bundleHash == "" || intent.BundleHash != bundleHash || intent.WorkflowVersion == "" || record.StartedAt.IsZero() {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity recovery differs from its admitted source or start evidence")
	}
	constructed, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	expected := constructed.Event
	actualPayload, err := canonicaljson.Decode(request.Payload())
	if err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	canonicalPayload, err := canonicaljson.Encode(actualPayload)
	if err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	if request.ID() != expected.ID() || request.RunID() != expected.RunID() || request.ParentEventID() != expected.ParentEventID() ||
		request.TaskID() != expected.TaskID() || request.ChainDepth() != expected.ChainDepth() || !request.Producer().Equal(expected.Producer()) ||
		request.RoutingSource() != expected.RoutingSource() || !bytes.Equal(canonicalPayload, expected.Payload()) {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity recovery request differs from canonical construction")
	}
	expectedRecord := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	expectedRecord.ExecutionBinding = record.ExecutionBinding
	if err := record.ExecutionBinding.Validate(); err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	if err := ValidateActivityAttemptExecutionIdentity(record, expectedRecord); err != nil {
		return runtimeengine.ActivityIntent{}, err
	}
	return intent, nil
}

func validateActivityRecoveryTerminal(intent runtimeengine.ActivityIntent, record ActivityAttemptRecord) error {
	if err := ValidateActivityAttemptTerminal(record); err != nil {
		return err
	}
	if record.CompletedAt == nil || record.CompletedAt.IsZero() || record.CompletedAt.Before(record.StartedAt) {
		return fmt.Errorf("activity recovery requires its original terminal completion time")
	}
	resultType := intent.FailureEvent
	var expectedPayload map[string]any
	if record.Status == ActivityAttemptStatusSucceeded {
		resultType = intent.SuccessEvent
		result, found := record.ResultPayload["result"]
		if !found {
			return fmt.Errorf("activity recovery success has no result")
		}
		expectedPayload = activitySuccessPayload(intent, result)
	} else {
		if err := runtimefailures.ValidateEnvelope(*record.Failure); err != nil {
			return err
		}
		expectedPayload = activityFailurePayload(intent, runtimefailures.FromEnvelope(*record.Failure))
	}
	publication, err := admitActivityPublication(intent, resultType)
	if err != nil {
		return err
	}
	actual, err := canonicaljson.MarshalPreservingNumberKinds(record.ResultPayload)
	if err != nil {
		return err
	}
	expected, err := canonicaljson.MarshalPreservingNumberKinds(expectedPayload)
	if err != nil {
		return err
	}
	if record.ResultEventID != publication.eventID || record.ResultEventType != string(publication.eventType) || !bytes.Equal(actual, expected) {
		return fmt.Errorf("activity recovery terminal result differs from its original request")
	}
	return nil
}
