package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func activityRecoveryContentForTest(t *testing.T) (events.Event, ActivityAttemptRecord, string) {
	t.Helper()
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	intent := NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString())
	intent.BundleHash, intent.WorkflowVersion = bundle, "recovery-content-v1"
	request, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		t.Fatal(err)
	}
	record := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	record.StartedAt = time.Date(2026, 10, 16, 0, 0, 0, 123456000, time.UTC)
	record.ExecutionBinding = processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
	return request.Event, record, bundle
}

func TestDisplacedActivityResultPreservesOriginalIdentityWithoutPublication(t *testing.T) {
	request, started, bundle := activityRecoveryContentForTest(t)
	terminal, err := DisplacedHTTPActivityResult(request, started, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateActivityAttemptExecutionIdentity(terminal, started); err != nil {
		t.Fatal(err)
	}
	if terminal.Status != ActivityAttemptStatusUncertain || terminal.Failure == nil || terminal.Failure.Detail.Code != "activity_provider_outcome_uncertain" ||
		terminal.CompletedAt != nil || terminal.StartedAt != started.StartedAt || terminal.ExecutionBinding != started.ExecutionBinding {
		t.Fatalf("recovery preparation changed native attribution/time or lost uncertainty: %+v", terminal)
	}
	if !reflect.DeepEqual(started.ResultPayload, map[string]any(nil)) || started.Status != ActivityAttemptStatusStarted {
		t.Fatal("recovery preparation changed the input record")
	}
	completed := started.StartedAt.Add(time.Second)
	terminal.CompletedAt = &completed
	for range 3 {
		replayed, err := DisplacedHTTPActivityResult(request, terminal, bundle)
		if err != nil || !reflect.DeepEqual(replayed, terminal) {
			t.Fatalf("exact terminal recovery changed durable result: %+v %v", replayed, err)
		}
	}
}

func TestDisplacedActivityResultRefusesForeignAndIncompleteEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ActivityAttemptRecord)
	}{
		{"request", func(r *ActivityAttemptRecord) { r.RequestEventID = uuid.NewString() }},
		{"run", func(r *ActivityAttemptRecord) { r.RunID = uuid.NewString() }},
		{"mode", func(r *ActivityAttemptRecord) { r.ExecutionMode = "mock" }},
		{"source", func(r *ActivityAttemptRecord) { r.SourceEventID = uuid.NewString() }},
		{"parent", func(r *ActivityAttemptRecord) { r.ParentEventID = uuid.NewString() }},
		{"entity", func(r *ActivityAttemptRecord) { r.EntityID = uuid.NewString() }},
		{"instance", func(r *ActivityAttemptRecord) { r.FlowInstance = "other" }},
		{"input", func(r *ActivityAttemptRecord) { r.InputHash = "other" }},
		{"reply", func(r *ActivityAttemptRecord) { r.ReplyContextID = uuid.NewString() }},
		{"native_attempt", func(r *ActivityAttemptRecord) { r.Attempt = 2 }},
		{"legacy_binding", func(r *ActivityAttemptRecord) { r.ExecutionBinding = processbinding.ActivityExecutionBinding{} }},
		{"start_time", func(r *ActivityAttemptRecord) { r.StartedAt = time.Time{} }},
		{"started_with_result", func(r *ActivityAttemptRecord) { r.ResultEventID = uuid.NewString() }},
		{"started_with_payload", func(r *ActivityAttemptRecord) { r.ResultPayload = map[string]any{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, record, bundle := activityRecoveryContentForTest(t)
			tc.change(&record)
			if _, err := DisplacedHTTPActivityResult(request, record, bundle); err == nil {
				t.Fatal("inexact recovery evidence produced a result")
			}
		})
	}
	request, record, _ := activityRecoveryContentForTest(t)
	for _, bundle := range []string{"", "bundle-v2:sha256:" + strings.Repeat("b", 64)} {
		if _, err := DisplacedHTTPActivityResult(request, record, bundle); err == nil {
			t.Fatal("recovery accepted foreign source evidence")
		}
	}
}

func TestDisplacedActivityResultRefusesChangedTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ActivityAttemptRecord)
	}{
		{"event", func(r *ActivityAttemptRecord) { r.ResultEventID = uuid.NewString() }},
		{"type", func(r *ActivityAttemptRecord) { r.ResultEventType = "other" }},
		{"missing_time", func(r *ActivityAttemptRecord) { r.CompletedAt = nil }},
		{"early_time", func(r *ActivityAttemptRecord) { early := r.StartedAt.Add(-time.Second); r.CompletedAt = &early }},
		{"payload", func(r *ActivityAttemptRecord) { r.ResultPayload = map[string]any{"activity_id": "different"} }},
		{"failure", func(r *ActivityAttemptRecord) { r.Failure = nil }},
		{"status", func(r *ActivityAttemptRecord) { r.Status = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, record, bundle := activityRecoveryContentForTest(t)
			terminal, err := DisplacedHTTPActivityResult(request, record, bundle)
			if err != nil {
				t.Fatal(err)
			}
			completed := record.StartedAt.Add(time.Second)
			terminal.CompletedAt = &completed
			tc.change(&terminal)
			if _, err := DisplacedHTTPActivityResult(request, terminal, bundle); err == nil {
				t.Fatal("changed terminal evidence was reused")
			}
		})
	}
}

func TestDisplacedActivityResultReusesKnownTerminalOutcomes(t *testing.T) {
	request, record, bundle := activityRecoveryContentForTest(t)
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		t.Fatal(err)
	}
	completed := record.StartedAt.Add(time.Second)
	for _, success := range []bool{false, true} {
		terminal := record
		resultType := intent.FailureEvent
		if success {
			resultType = intent.SuccessEvent
		}
		publication, err := admitActivityPublication(intent, resultType)
		if err != nil {
			t.Fatal(err)
		}
		if success {
			terminal = terminal.withTerminal(ActivityAttemptStatusSucceeded, publication.eventID, string(publication.eventType), activitySuccessPayload(intent, map[string]any{"business": "original"}), nil)
		} else {
			cause := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "provider_unavailable", "activity-runtime", "http_response", nil)
			failure := runtimefailures.Normalize(cause, "activity-runtime", "http_response")
			terminal = terminal.withTerminal(ActivityAttemptStatusFailed, publication.eventID, string(publication.eventType), activityFailurePayload(intent, cause), &failure)
		}
		terminal.CompletedAt = &completed
		replayed, err := DisplacedHTTPActivityResult(request, terminal, bundle)
		if err != nil || !reflect.DeepEqual(replayed, terminal) {
			t.Fatalf("known outcome changed into uncertainty: %+v %v", replayed, err)
		}
	}
}
