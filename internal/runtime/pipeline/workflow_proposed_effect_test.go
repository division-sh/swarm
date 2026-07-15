package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestProposedEffectOutcomeEventRoutesExactTypedVerdicts(t *testing.T) {
	now := time.Date(2026, 7, 14, 20, 0, 0, 0, time.UTC)
	parent := eventtest.RuntimeControl(uuid.NewString(), workflowGateDecisionEventType, "platform", "", []byte(`{"card_id":"card-1"}`), 0,
		uuid.NewString(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), now)
	continuation := decisioncard.ProposedEffectContinuation{
		ActivityID: "send_support_reply", Tool: "telegram.send_message", EffectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite,
		EffectContentHash: "sha256:effect", SourceTaskID: "task-1",
		EntityID: parent.EntityID(), FlowInstance: "root",
		ExecutionMode: parent.ExecutionMode(),
		RevisionEvent: "send_support_reply.revision_requested", RejectedEvent: "send_support_reply.rejected",
	}
	for _, tc := range []struct {
		name      string
		verdict   string
		fields    semanticvalue.Value
		wantEvent events.EventType
		wantField string
		wantValue string
	}{
		{
			name: "revise", verdict: "revise", fields: mustProposedEffectFields(t, map[string]any{"feedback": "Remove the customer secret."}),
			wantEvent: "send_support_reply.revision_requested", wantField: "feedback", wantValue: "Remove the customer secret.",
		},
		{
			name: "reject", verdict: "reject", fields: mustProposedEffectFields(t, map[string]any{"reason": "Not authorized."}),
			wantEvent: "send_support_reply.rejected", wantField: "reason", wantValue: "Not authorized.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			card := decisioncard.Card{
				CardID: uuid.NewString(), Verdict: tc.verdict, Fields: tc.fields,
				DecidedBy: "operator", DecidedAt: now,
			}
			got, err := proposedEffectOutcomeEvent(card, parent, continuation)
			if err != nil {
				t.Fatal(err)
			}
			if got.Type() != tc.wantEvent || got.ParentEventID() != parent.ID() || got.EntityID() != parent.EntityID() || got.FlowInstance() != "root" {
				t.Fatalf("outcome identity = type:%s parent:%s entity:%s flow:%s", got.Type(), got.ParentEventID(), got.EntityID(), got.FlowInstance())
			}
			payload, err := canonicaljson.Decode(got.Payload())
			if err != nil {
				t.Fatal(err)
			}
			field, ok := payload.Lookup(tc.wantField)
			value, text := field.String()
			if !ok || !text || value != tc.wantValue {
				t.Fatalf("outcome %s = %q (present=%v text=%v)", tc.wantField, value, ok, text)
			}
			for fieldName, want := range map[string]string{
				"card_id": card.CardID, "activity_id": continuation.ActivityID, "tool": continuation.Tool,
				"effect_class": string(continuation.EffectClass), "effect_content_hash": continuation.EffectContentHash,
				"decided_by": card.DecidedBy, "decided_at": now.Format(time.RFC3339Nano),
			} {
				field, present := payload.Lookup(fieldName)
				value, stringValue := field.String()
				if !present || !stringValue || value != want {
					t.Fatalf("outcome %s = %q (present=%v text=%v), want %q", fieldName, value, present, stringValue, want)
				}
			}
			repeated, err := proposedEffectOutcomeEvent(card, parent, continuation)
			if err != nil || repeated.ID() != got.ID() {
				t.Fatalf("repeated outcome identity = %s, %v; want %s", repeated.ID(), err, got.ID())
			}
		})
	}
}

func TestProposedEffectRevisionRequiresFeedback(t *testing.T) {
	now := time.Date(2026, 7, 14, 20, 30, 0, 0, time.UTC)
	parent := eventtest.RuntimeControl(uuid.NewString(), workflowGateDecisionEventType, "platform", "", []byte(`{"card_id":"card-1"}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, now)
	_, err := proposedEffectOutcomeEvent(decisioncard.Card{
		CardID: uuid.NewString(), Verdict: "revise", Fields: semanticvalue.EmptyObject(), DecidedAt: now,
	}, parent, decisioncard.ProposedEffectContinuation{RevisionEvent: "activity.revision_requested"})
	if err == nil {
		t.Fatal("revision without feedback succeeded")
	}
}

func mustProposedEffectFields(t *testing.T, fields map[string]any) semanticvalue.Value {
	t.Helper()
	value, err := canonicaljson.FromGo(fields)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
