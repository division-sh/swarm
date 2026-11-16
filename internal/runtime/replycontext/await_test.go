package replycontext

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func awaitRecord(t *testing.T) Record {
	t.Helper()
	record := a2ReplyOriginRecord(t)
	record.Await = &AwaitBinding{Version: "reply-await.v1", ReplyContextID: record.ID, RequestEventID: record.RequestEventID,
		RunID: record.RunID, OriginLife: record.OriginLife, DeliveryID: "delivery", ClaimVersion: 1, ExecutionOwner: "worker",
		OperationID: "operation", AttemptID: "attempt", BundleHash: "source", PlanFingerprint: "plan", FrameID: "frame",
		ResponseFingerprint: "response", CallID: "call", CallIndex: 0, CompletedResults: "[]"}
	return record
}

// Binding persistence and lifecycle observation cannot substitute execution authority.
func TestReplyAwaitBindingImmutableAndReadyOnly(t *testing.T) {
	original := awaitRecord(t)
	if err := original.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := original.EncodeAwaitBinding()
	if err != nil {
		t.Fatal(err)
	}
	decoded := original.Normalized()
	decoded.Await = nil
	if err := decoded.DecodeAwaitBinding([]byte(encoded.(string))); err != nil || !decoded.SameIdentity(original) {
		t.Fatalf("binding round trip: err=%v record=%+v", err, decoded)
	}
	copy := original.Normalized()
	copy.Await.CallID = "other"
	if original.Await.CallID != "call" || copy.SameIdentity(original) {
		t.Fatal("normalized binding aliases or changes immutable identity")
	}
	terminal := original.Normalized()
	stamp := original.CreatedAt.Add(time.Second)
	terminal.State, terminal.AcceptedReplyEventID, terminal.TerminalAt, terminal.UpdatedAt = StateTerminal, "reply-event", &stamp, stamp
	original.UpdatedAt = original.CreatedAt
	if err := terminal.ValidateContinuationFrom(original, original.CreatedAt); err != nil {
		t.Fatalf("ready reply cannot retain the same immutable binding: %v", err)
	}
	for _, mutate := range []func(*Record){
		func(r *Record) { r.Await.ReplyContextID = "other" },
		func(r *Record) { r.Await.RequestEventID = "other" },
		func(r *Record) { r.Await.RunID = "other" },
		func(r *Record) { r.Await.OriginLife++ },
		func(r *Record) { r.Await.CallIndex++ },
	} {
		hostile := original.Normalized()
		mutate(&hostile)
		if err := hostile.Validate(); err == nil {
			t.Fatalf("contradictory binding admitted: %+v", hostile.Await)
		}
	}
	ordinary := original.Normalized()
	ordinary.Await = nil
	if raw, err := ordinary.EncodeAwaitBinding(); err != nil || raw != nil || ordinary.SameIdentity(original) {
		t.Fatalf("ordinary reply acquired await evidence: raw=%v err=%v", raw, err)
	}
}

// Persisted decoding rejects ambiguity, including zero-valued required members.
func TestReplyAwaitBindingClosedEncoding(t *testing.T) {
	original := awaitRecord(t)
	encoded, err := original.EncodeAwaitBinding()
	if err != nil {
		t.Fatal(err)
	}
	raw := encoded.(string)
	for name, hostile := range map[string]string{
		"null":             "null",
		"unknown":          strings.Replace(raw, `"version":`, `"unknown":1,"version":`, 1),
		"case":             strings.Replace(raw, `"call_index":`, `"Call_Index":`, 1),
		"duplicate":        strings.Replace(raw, `"call_index":0`, `"call_index":0,"call_index":0`, 1),
		"missing_zero":     strings.Replace(raw, `"call_index":0,`, "", 1),
		"null_index":       strings.Replace(raw, `"call_index":0`, `"call_index":null`, 1),
		"string_index":     strings.Replace(raw, `"call_index":0`, `"call_index":"0"`, 1),
		"fractional_index": strings.Replace(raw, `"call_index":0`, `"call_index":0.5`, 1),
		"trailing":         raw + `{}`,
		"prefix_null":      strings.Replace(raw, `"completed_results":"[]"`, `"completed_results":"null"`, 1),
		"prefix_object":    strings.Replace(raw, `"completed_results":"[]"`, `"completed_results":"{}"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			before := original.Normalized()
			candidate := original.Normalized()
			if err := candidate.DecodeAwaitBinding([]byte(hostile)); err == nil || !reflect.DeepEqual(candidate, before) {
				t.Fatalf("malformed binding admitted or changed destination: err=%v candidate=%+v", err, candidate)
			}
		})
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	for name := range fields {
		t.Run("required_"+name, func(t *testing.T) {
			copy := make(map[string]json.RawMessage, len(fields))
			for key, value := range fields {
				if key != name {
					copy[key] = value
				}
			}
			raw, err := json.Marshal(copy)
			if err != nil {
				t.Fatal(err)
			}
			candidate := original.Normalized()
			if err := candidate.DecodeAwaitBinding(raw); err == nil {
				t.Fatal("missing binding member admitted")
			}
		})
	}
}
