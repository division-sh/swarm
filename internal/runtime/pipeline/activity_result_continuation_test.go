package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func publishedActivityAccountingFixture(t *testing.T) (runtimeengine.ActivityIntent, ActivityAttemptRecord, activityresult.PublishedContinuation, context.Context) {
	t.Helper()
	intent := testActivityIntent("https://input.invalid")
	intent.SourceRunID, intent.SourceEventID = uuid.NewString(), uuid.NewString()
	intent.EntityID = identity.NormalizeEntityID(uuid.NewString())
	publication, err := admitActivityPublication(intent, intent.SuccessEvent)
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Unix(100, 0).UTC()
	record := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	record.Status, record.ResultEventID, record.ResultEventType = ActivityAttemptStatusSucceeded, publication.eventID, string(publication.eventType)
	record.ResultPayload, record.CompletedAt = activitySuccessPayload(intent, map[string]any{"value": "recorded"}), &completed
	record.ReplyContextID = "reply-v1:exact-child"
	raw, err := canonicaljson.MarshalPreservingNumberKinds(record.ResultPayload)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := activityresult.NewPublishedContinuation(activityIdentityFact(intent), uuid.NewString(), record.ResultEventType, raw,
		record.ExecutionMode, record.InputHash, record.ReplyContextID, completed)
	if err != nil {
		t.Fatal(err)
	}
	executionID := uuid.NewString()
	ctx := runtimeeffects.WithAuthority(context.Background(), runtimeeffects.Authority{Kind: runtimeeffects.AuthoritySelectedContractFork,
		ID: executionID, ExecutionOwner: "selected-accounting-proof", LeaseExpiresAt: time.Now().Add(time.Hour), FenceGeneration: 1, ExecutionMode: intent.ExecutionMode,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{ExecutionID: executionID, ForkRunID: intent.SourceRunID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "configuration"}})
	return intent, record, retained, ctx
}

func TestSelectedPublishedActivityResultLeavesInputAccountingWithoutEmission(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, published := range []bool{false, true} {
			intent, record, retained, ctx := publishedActivityAccountingFixture(t)
			var continuations []activityresult.PublishedContinuation
			if published {
				continuations = append(continuations, retained)
			}
			ctx, err := WithSelectedActivityPublications(ctx, intent.SourceRunID, continuations)
			if err != nil {
				t.Fatal(err)
			}
			bus := &recordingPipelineBus{}
			emissions := &pipelineEmissionPlan{}
			dispatcher := pipelineActivityDispatcher{coordinator: &PipelineCoordinator{bus: bus}}
			if deferred {
				dispatcher.emissions = emissions
			}
			if err := dispatcher.publishExistingActivityAttempt(ctx, intent, record); err != nil {
				t.Fatal(err)
			}
			count := len(bus.publishes) + len(emissions.immutableEvents())
			want := 1
			if published {
				want = 0
			}
			if count != want {
				t.Fatalf("deferred=%t published=%t emissions=%d want=%d", deferred, published, count, want)
			}
		}
	}
}

func TestSelectedActivityAccountingRejectsCrossedJournalAndAuthority(t *testing.T) {
	for _, fault := range []string{"exact", "request", "run", "result", "type", "payload", "input", "reply", "completed", "authority", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			intent, record, retained, ctx := publishedActivityAccountingFixture(t)
			ctx, err := WithSelectedActivityPublications(ctx, intent.SourceRunID, []activityresult.PublishedContinuation{retained})
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "request":
				record.RequestEventID = uuid.NewString()
			case "run":
				record.RunID = uuid.NewString()
			case "result":
				record.ResultEventID = uuid.NewString()
			case "type":
				record.ResultEventType = "unrelated.result"
			case "payload":
				record.ResultPayload = map[string]any{"result": "different"}
			case "input":
				record.InputHash = "different"
			case "reply":
				record.ReplyContextID = "reply-v1:other"
			case "completed":
				completed := record.CompletedAt.Add(time.Second)
				record.CompletedAt = &completed
			case "authority":
				ctx = runtimeeffects.WithAuthority(ctx, runtimeeffects.Authority{})
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			owned, err := selectedActivityPublicationOwnsResult(ctx, intent, record)
			if fault == "exact" {
				if !owned || err != nil {
					t.Fatalf("exact accounting refused: owned=%t err=%v", owned, err)
				}
			} else if err == nil {
				t.Fatal("contradictory journal or scope suppressed a publication")
			}
		})
	}
	intent, _, retained, ctx := publishedActivityAccountingFixture(t)
	for _, inventory := range [][]activityresult.PublishedContinuation{{retained, retained}, {{}}} {
		if _, err := WithSelectedActivityPublications(ctx, intent.SourceRunID, inventory); err == nil {
			t.Fatal("duplicate or empty continuation admitted")
		}
	}
}

func TestRecordedActivityPublicationUsesCanonicalRequestJournalResultLinkage(t *testing.T) {
	intent, record, _, _ := publishedActivityAccountingFixture(t)
	request, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		t.Fatal(err)
	}
	emissions := &pipelineEmissionPlan{}
	if err := (pipelineActivityDispatcher{emissions: emissions}).publishJournaledActivityResult(context.Background(), intent, record); err != nil {
		t.Fatal(err)
	}
	result := emissions.immutableEvents()[0]
	payload, err := json.MarshalIndent(record.ResultPayload, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRecordedActivityPublication(request.Event, result, record.ResultEventID, payload); err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		id      string
		payload json.RawMessage
	}{
		{uuid.NewString(), payload}, {record.ResultEventID, json.RawMessage(`{"result":"contradictory"}`)},
	} {
		if err := ValidateRecordedActivityPublication(request.Event, result, pair.id, pair.payload); err == nil {
			t.Fatal("contradictory result/journal linkage admitted")
		}
	}
	if err := ValidateRecordedActivityPublication(request.Event, events.Event{}, record.ResultEventID, payload); err == nil {
		t.Fatal("missing physical publication became result evidence")
	}
}

func TestSelectedReadOnlyActivityPublicationKeepsExactInputAndScope(t *testing.T) {
	intent, record, _, ctx := publishedActivityAccountingFixture(t)
	raw, err := canonicaljson.MarshalPreservingNumberKinds(record.ResultPayload)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := activityresult.NewPublishedContinuation(activityIdentityFact(intent), uuid.NewString(), record.ResultEventType, raw,
		intent.ExecutionMode, record.InputHash, "", *record.CompletedAt)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = WithSelectedActivityPublications(ctx, intent.SourceRunID, []activityresult.PublishedContinuation{publication})
	if err != nil {
		t.Fatal(err)
	}
	if _, present, err := selectedActivityInputPublication(ctx, intent); err != nil || !present {
		t.Fatalf("exact read-only input accounting: present=%t err=%v", present, err)
	}
	intent.Input = mustActivityInput(map[string]any{"url": "different"})
	if _, _, err := selectedActivityInputPublication(ctx, intent); err == nil {
		t.Fatal("changed read-only input consumed a captured publication")
	}
}
