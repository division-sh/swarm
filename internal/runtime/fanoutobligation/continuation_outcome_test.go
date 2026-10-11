package fanoutobligation

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestOutcomeContinuationClosedShapes(t *testing.T) {
	createdAt := time.Unix(1, 0).UTC()
	childEventID, sourceEventID := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		name    string
		outcome Outcome
	}{
		{name: "committed owned", outcome: Outcome{Kind: OutcomeCommitted, EventID: childEventID}},
		{name: "committed inherited success", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID, InheritedDisposition: InheritedSucceeded}},
		{name: "committed inherited failure", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID, InheritedDisposition: InheritedDeadLettered}},
		{name: "committed inherited canceled", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID, InheritedDisposition: InheritedCanceled}},
		{name: "committed inherited no route", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID, InheritedDisposition: InheritedNoRoute}},
		{name: "semantic rejected", outcome: Outcome{Kind: OutcomeSemanticRejected, Failure: continuationSemanticRejection(t)}},
		{name: "continued all recipients unfinished", outcome: Outcome{Kind: OutcomeContinued, EventID: childEventID, SourceEventID: sourceEventID}},
		{name: "continued retained success", outcome: Outcome{Kind: OutcomeContinued, EventID: childEventID, SourceEventID: sourceEventID, InheritedDisposition: InheritedSucceeded}},
		{name: "continued retained failure", outcome: Outcome{Kind: OutcomeContinued, EventID: childEventID, SourceEventID: sourceEventID, InheritedDisposition: InheritedDeadLettered}},
		{name: "continued retained canceled", outcome: Outcome{Kind: OutcomeContinued, EventID: childEventID, SourceEventID: sourceEventID, InheritedDisposition: InheritedCanceled}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := tc.outcome
			outcome.Ordinal, outcome.CreatedAt = 2, createdAt
			if err := outcome.Validate(); err != nil {
				t.Fatalf("valid outcome rejected: %v", err)
			}
			raw, err := json.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			var decoded Outcome
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, outcome) {
				t.Fatalf("outcome facts changed on round trip: %#v", decoded)
			}
			if err := decoded.Validate(); err != nil {
				t.Fatalf("round-tripped outcome rejected: %v", err)
			}
		})
	}
}

func TestOutcomeContinuationRejectsIncompleteOrContradictoryFacts(t *testing.T) {
	failure := continuationSemanticRejection(t)
	for _, disposition := range []InheritedTerminalDisposition{"", InheritedSucceeded, InheritedDeadLettered, InheritedCanceled} {
		name := string(disposition)
		if name == "" {
			name = "no retained contribution"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				mutate func(*Outcome)
			}{
				{name: "missing child", mutate: func(o *Outcome) { o.EventID = "" }},
				{name: "blank child", mutate: func(o *Outcome) { o.EventID = " \t\n" }},
				{name: "missing source", mutate: func(o *Outcome) { o.SourceEventID = "" }},
				{name: "blank source", mutate: func(o *Outcome) { o.SourceEventID = " \t\n" }},
				{name: "missing both events", mutate: func(o *Outcome) { o.EventID, o.SourceEventID = "", "" }},
				{name: "same event", mutate: func(o *Outcome) { o.EventID = o.SourceEventID }},
				{name: "same event with whitespace", mutate: func(o *Outcome) { o.EventID = " " + o.SourceEventID + " " }},
				{name: "typed failure", mutate: func(o *Outcome) { o.Failure = failure }},
				{name: "null failure", mutate: func(o *Outcome) { o.Failure = json.RawMessage(`null`) }},
				{name: "empty object failure", mutate: func(o *Outcome) { o.Failure = json.RawMessage(`{}`) }},
				{name: "blank failure", mutate: func(o *Outcome) { o.Failure = json.RawMessage(" ") }},
				{name: "partial no route", mutate: func(o *Outcome) { o.InheritedDisposition = InheritedNoRoute }},
				{name: "unknown disposition", mutate: func(o *Outcome) { o.InheritedDisposition = "pending" }},
				{name: "noncanonical disposition", mutate: func(o *Outcome) { o.InheritedDisposition = " succeeded " }},
				{name: "negative ordinal", mutate: func(o *Outcome) { o.Ordinal = -1 }},
				{name: "missing creation time", mutate: func(o *Outcome) { o.CreatedAt = time.Time{} }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					outcome := Outcome{Ordinal: 2, Kind: OutcomeContinued, EventID: uuid.NewString(), SourceEventID: uuid.NewString(),
						InheritedDisposition: disposition, CreatedAt: time.Unix(1, 0).UTC()}
					tc.mutate(&outcome)
					if err := outcome.Validate(); err == nil {
						t.Fatalf("hostile continued outcome accepted: %#v", outcome)
					}
				})
			}
		})
	}
}

func TestOutcomeContinuationPreservesExistingOutcomeGuards(t *testing.T) {
	childEventID, sourceEventID := uuid.NewString(), uuid.NewString()
	failure := continuationSemanticRejection(t)
	for _, tc := range []struct {
		name    string
		outcome Outcome
	}{
		{name: "committed both event identities", outcome: Outcome{Kind: OutcomeCommitted, EventID: childEventID, SourceEventID: sourceEventID, InheritedDisposition: InheritedSucceeded}},
		{name: "committed neither event identity", outcome: Outcome{Kind: OutcomeCommitted}},
		{name: "committed owned with disposition", outcome: Outcome{Kind: OutcomeCommitted, EventID: childEventID, InheritedDisposition: InheritedSucceeded}},
		{name: "committed inherited without disposition", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID}},
		{name: "committed inherited unknown disposition", outcome: Outcome{Kind: OutcomeCommitted, SourceEventID: sourceEventID, InheritedDisposition: "pending"}},
		{name: "committed with failure", outcome: Outcome{Kind: OutcomeCommitted, EventID: childEventID, Failure: failure}},
		{name: "rejected without failure", outcome: Outcome{Kind: OutcomeSemanticRejected}},
		{name: "rejected untyped failure", outcome: Outcome{Kind: OutcomeSemanticRejected, Failure: json.RawMessage(`{}`)}},
		{name: "rejected with child", outcome: Outcome{Kind: OutcomeSemanticRejected, EventID: childEventID, Failure: failure}},
		{name: "rejected with source", outcome: Outcome{Kind: OutcomeSemanticRejected, SourceEventID: sourceEventID, Failure: failure}},
		{name: "rejected with both event identities", outcome: Outcome{Kind: OutcomeSemanticRejected, EventID: childEventID, SourceEventID: sourceEventID, InheritedDisposition: InheritedSucceeded, Failure: failure}},
		{name: "rejected with disposition", outcome: Outcome{Kind: OutcomeSemanticRejected, InheritedDisposition: InheritedSucceeded, Failure: failure}},
		{name: "unknown kind", outcome: Outcome{Kind: "continuation", EventID: childEventID, SourceEventID: sourceEventID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := tc.outcome
			outcome.CreatedAt = time.Unix(1, 0).UTC()
			if err := outcome.Validate(); err == nil {
				t.Fatalf("hostile existing outcome shape accepted: %#v", outcome)
			}
		})
	}
}

func continuationSemanticRejection(t *testing.T) json.RawMessage {
	t.Helper()
	envelope, ok := failures.EnvelopeFromError(failures.New(failures.ClassSchemaInvalid, "emit_payload_contract_violation", "runtime.engine", "fan_out.emit",
		map[string]any{"event": "item.ready", "kind": "schema_mismatch", "path": "$.item", "constraint": "type", "expected": "number", "actual": "string", "detail": "$.item must be number"}))
	if !ok {
		t.Fatal("construct typed semantic rejection")
	}
	raw, err := failures.MarshalEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
