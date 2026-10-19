package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func recoveredHTTPActivityForTest(t *testing.T, status string) RecoveredHTTPActivity {
	t.Helper()
	request, record, bundle := activityRecoveryContentForTest(t)
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		t.Fatal(err)
	}
	resultType := intent.FailureEvent
	if status == ActivityAttemptStatusSucceeded {
		resultType = intent.SuccessEvent
	}
	publication, err := admitActivityPublication(intent, resultType)
	if err != nil {
		t.Fatal(err)
	}
	if status == ActivityAttemptStatusSucceeded {
		record = record.withTerminal(status, publication.eventID, string(publication.eventType), activitySuccessPayload(intent, map[string]any{"business": "original"}), nil)
	} else {
		class := runtimefailures.ClassConnectorFailure
		code := "provider_http_status"
		if status == ActivityAttemptStatusUncertain {
			class, code = runtimefailures.ClassOutcomeUncertain, "activity_provider_outcome_uncertain"
		}
		cause := runtimefailures.New(class, code, "activity-runtime", "recover_displaced_http_activity", nil)
		failure := runtimefailures.Normalize(cause, "activity-runtime", "recover_displaced_http_activity")
		record = record.withTerminal(status, publication.eventID, string(publication.eventType), activityFailurePayload(intent, cause), &failure)
	}
	completed := record.StartedAt.Add(time.Second)
	record.CompletedAt, record.UpdatedAt = &completed, completed
	return RecoveredHTTPActivity{ActivityAttemptRecord: record, Request: request, BundleHash: bundle}
}

func TestRecoveredHTTPActivityPublicationUsesExactDurableResult(t *testing.T) {
	for _, status := range []string{ActivityAttemptStatusSucceeded, ActivityAttemptStatusFailed, ActivityAttemptStatusUncertain} {
		t.Run(status, func(t *testing.T) {
			recovered := recoveredHTTPActivityForTest(t, status)
			journal := &capturedActivityJournalProbe{record: &recovered.ActivityAttemptRecord}
			bus := &recordingPipelineBus{}
			pc := &PipelineCoordinator{bus: bus, workflowStore: &workflowInstanceStore{activityJournal: journal}}
			for range 2 {
				if err := pc.PublishRecoveredHTTPActivity(context.Background(), recovered); err != nil {
					t.Fatal(err)
				}
			}
			if journal.claims.Load() != 0 || journal.terminals.Load() != 0 || len(bus.publishes) != 2 {
				t.Fatalf("readback executed or settled an activity: claims=%d terminals=%d publications=%d", journal.claims.Load(), journal.terminals.Load(), len(bus.publishes))
			}
			first := bus.publishes[0]
			if first.ID() != recovered.ResultEventID || string(first.Type()) != recovered.ResultEventType || !first.CreatedAt().Equal(*recovered.CompletedAt) || !reflect.DeepEqual(first, bus.publishes[1]) {
				t.Fatalf("recovery reminted its exact result: %+v %+v", first, bus.publishes[1])
			}
		})
	}
}

func TestRecoveredHTTPActivityPublicationRefusesMissingOrChangedReadback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ActivityAttemptRecord)
	}{
		{"missing", nil},
		{"not_committed", func(r *ActivityAttemptRecord) { r.Status = ActivityAttemptStatusStarted }},
		{"other_grant", func(r *ActivityAttemptRecord) { r.ExecutionBinding.GrantID = uuid.NewString() }},
		{"other_input", func(r *ActivityAttemptRecord) { r.InputHash = "different" }},
		{"other_payload", func(r *ActivityAttemptRecord) { r.ResultPayload = map[string]any{"business": "different"} }},
		{"other_time", func(r *ActivityAttemptRecord) { at := r.CompletedAt.Add(time.Second); r.CompletedAt = &at }},
		{"missing_time", func(r *ActivityAttemptRecord) { r.CompletedAt = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recovered := recoveredHTTPActivityForTest(t, ActivityAttemptStatusSucceeded)
			journal := &capturedActivityJournalProbe{}
			if tc.change != nil {
				stored := recovered.ActivityAttemptRecord
				tc.change(&stored)
				journal.record = &stored
			}
			bus := &recordingPipelineBus{}
			pc := &PipelineCoordinator{bus: bus, workflowStore: &workflowInstanceStore{activityJournal: journal}}
			if err := pc.PublishRecoveredHTTPActivity(context.Background(), recovered); err == nil || len(bus.publishes) != 0 || journal.claims.Load() != 0 || journal.terminals.Load() != 0 {
				t.Fatalf("unacknowledged or changed recovery escaped: publications=%d err=%v", len(bus.publishes), err)
			}
		})
	}
}

func TestRecoveredHTTPActivityPublicationPreservesCancellationAndSource(t *testing.T) {
	recovered := recoveredHTTPActivityForTest(t, ActivityAttemptStatusSucceeded)
	journal := &capturedActivityJournalProbe{record: &recovered.ActivityAttemptRecord}
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{bus: bus, workflowStore: &workflowInstanceStore{activityJournal: journal}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pc.PublishRecoveredHTTPActivity(ctx, recovered); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
	for _, bundle := range []string{"", "different-source"} {
		candidate := recovered
		candidate.BundleHash = bundle
		if err := pc.PublishRecoveredHTTPActivity(context.Background(), candidate); err == nil {
			t.Fatal("foreign source reached publication")
		}
	}
	if len(bus.publishes) != 0 {
		t.Fatal("canceled or foreign-source recovery published")
	}
}
