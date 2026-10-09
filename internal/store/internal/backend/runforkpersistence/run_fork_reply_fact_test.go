package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func historicalReplyRecord(t *testing.T) replycontext.Record {
	t.Helper()
	declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "collector", "join"), "item.done", "collecting", "items")
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{
		RunID: "11111111-1111-4111-8111-111111111111", FlowScope: "collector", InstanceID: "one", InstancePath: "collector/one",
		EntityID: "22222222-2222-4222-8222-222222222222", Stage: "collecting", Cause: "delivery",
		EventID: "33333333-3333-4333-8333-333333333333", OccurrenceID: "old-delivery", TransitionID: "old-transition",
	}
	bound, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	early, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "collector", "early"), "later.done", "waiting", "later")
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 9, 8, 10, 11, 12, 123456789, time.UTC)
	return replycontext.Record{
		ID: "reply-v1:opaque/request|42", RunID: entry.RunID, RequestEventID: entry.EventID,
		RequesterFlowID: "collector", RequestOutputPin: "request", ReplyInputPin: "return", ProviderFlowID: "worker",
		ProviderInputPin: "request", ProviderOutputPin: "return", RequestCorrelationID: "request-42", CorrelationKey: "item.id",
		Origin:      events.RouteIdentity{FlowID: "collector", FlowInstance: entry.InstancePath, EntityID: entry.EntityID},
		ReturnJoins: []events.JoinAdmissionReceipt{{Ref: bound, Disposition: events.JoinAdmissionBound}, {Ref: early, Disposition: events.JoinAdmissionEarly}},
		State:       replycontext.StateOpen, CreatedAt: stamp, UpdatedAt: stamp.Add(time.Second),
	}
}

func historicalReplyBody(t *testing.T, record replycontext.Record) map[string]any {
	t.Helper()
	origin, err := record.EncodeOrigin()
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"reply_context_id": record.ID, "run_id": record.RunID, "request_event_id": record.RequestEventID,
		"requester_flow_id": record.RequesterFlowID, "request_output_pin": record.RequestOutputPin, "reply_input_pin": record.ReplyInputPin,
		"provider_flow_id": record.ProviderFlowID, "provider_input_pin": record.ProviderInputPin, "provider_output_pin": record.ProviderOutputPin,
		"origin_route": json.RawMessage(origin), "request_correlation_id": record.RequestCorrelationID, "correlation_key": record.CorrelationKey,
		"state": string(record.State), "accepted_reply_event_id": record.AcceptedReplyEventID,
		"created_at": record.CreatedAt, "updated_at": record.UpdatedAt, "terminal_at": record.TerminalAt,
	}
}

func historicalReplyJSON(t *testing.T, body map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func historicalReplyContext(record replycontext.Record) (*runForkRevisionSnapshot, runForkHistoricalFactContext) {
	return &runForkRevisionSnapshot{RunID: record.RunID, Revision: 5}, runForkHistoricalFactContext{
		RunID: record.RunID, Family: runforkrevision.FamilyReplyContexts, Key: record.ID, FirstRevision: 2, Revision: 4,
	}
}

func TestHistoricalReplyFactPreservesCompleteRecord(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "terminal"}[terminal], func(t *testing.T) {
			want := historicalReplyRecord(t)
			if terminal {
				want.State, want.AcceptedReplyEventID = replycontext.StateTerminal, "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
				terminalAt := want.UpdatedAt
				want.TerminalAt = &terminalAt
			}
			snapshot, context := historicalReplyContext(want)
			got, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, historicalReplyBody(t, want)))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Record, want) || !got.Record.SameIdentity(want) ||
				got.FirstRevision != context.FirstRevision || got.Revision != context.Revision {
				t.Fatalf("historical record lost evidence: %#v, want %#v", got, want)
			}
			if snapshot.admittedFacts != nil || len(snapshot.ReplyContexts) != 0 {
				t.Fatal("read-only decoder mutated snapshot")
			}
			newer := want
			newer.ReturnJoins = append([]events.JoinAdmissionReceipt(nil), want.ReturnJoins...)
			entry := newer.ReturnJoins[0].Ref.StageEntry()
			entry.OccurrenceID, entry.TransitionID = "new-delivery", "new-transition"
			ref, err := newer.ReturnJoins[0].Ref.Declaration().BindStageEntry(entry, attemptgeneration.Generation{})
			if err != nil {
				t.Fatal(err)
			}
			newer.ReturnJoins[0].Ref = ref
			if got.Record.SameIdentity(newer) {
				t.Fatal("historical old return arm became newer reply authority")
			}
		})
	}
}

func TestHistoricalReplyFactRejectsContextAndIdentityCorruption(t *testing.T) {
	want := historicalReplyRecord(t)
	for _, scenario := range []struct {
		name string
		edit func(*runForkRevisionSnapshot, *runForkHistoricalFactContext, map[string]any)
	}{
		{"missing_selected_run", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, _ map[string]any) { s.RunID = "" }},
		{"invalid_selected_run", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, _ map[string]any) {
			s.RunID = "invalid"
		}},
		{"missing_selected_revision", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, _ map[string]any) { s.Revision = 0 }},
		{"foreign_wrapper_run", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) {
			c.RunID = want.RequestEventID
		}},
		{"foreign_family", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) {
			c.Family = runforkrevision.FamilyEvents
		}},
		{"missing_key", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) { c.Key = "" }},
		{"wrong_key", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) {
			c.Key = "other-reply"
		}},
		{"missing_first_revision", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) {
			c.FirstRevision = 0
		}},
		{"missing_fact_revision", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) { c.Revision = 0 }},
		{"reversed_revisions", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) {
			c.FirstRevision = 5
		}},
		{"after_cut", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext, _ map[string]any) { c.Revision = 6 }},
		{"foreign_body_run", func(_ *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, b map[string]any) {
			b["run_id"] = want.RequestEventID
		}},
		{"invalid_request_identity", func(_ *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, b map[string]any) {
			b["request_event_id"] = "invalid"
		}},
		{"missing_request_identity", func(_ *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, b map[string]any) {
			b["request_event_id"] = ""
		}},
		{"invalid_accepted_identity", func(_ *runForkRevisionSnapshot, _ *runForkHistoricalFactContext, b map[string]any) {
			b["state"], b["accepted_reply_event_id"], b["terminal_at"] = "terminal", "invalid", want.UpdatedAt
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			snapshot, context := historicalReplyContext(want)
			body := historicalReplyBody(t, want)
			scenario.edit(snapshot, &context, body)
			got, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body))
			if err == nil || !reflect.DeepEqual(got, runForkHistoricalReplyFact{}) || snapshot.admittedFacts != nil || len(snapshot.ReplyContexts) != 0 {
				t.Fatalf("invalid evidence admitted or mutated snapshot: %#v, %v", got, err)
			}
		})
	}
	snapshot, context := historicalReplyContext(want)
	if _, err := decodeRunForkHistoricalReplyFact(nil, context, historicalReplyJSON(t, historicalReplyBody(t, want))); err == nil {
		t.Fatal("nil snapshot admitted")
	}
	snapshot.admittedFacts = map[runForkHistoricalFactKey]struct{}{{family: context.Family, key: context.Key}: {}}
	if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, historicalReplyBody(t, want))); err == nil || len(snapshot.admittedFacts) != 1 {
		t.Fatal("duplicate historical fact admitted or mutated identity inventory")
	}
}

func TestHistoricalReplyFactRequiresCanonicalShape(t *testing.T) {
	want := historicalReplyRecord(t)
	snapshot, context := historicalReplyContext(want)
	for field := range historicalReplyBody(t, want) {
		t.Run("missing_"+field, func(t *testing.T) {
			body := historicalReplyBody(t, want)
			delete(body, field)
			if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body)); err == nil {
				t.Fatalf("missing %s was repaired", field)
			}
		})
	}
	for _, scenario := range []struct {
		name, field string
		value       any
	}{
		{"empty_state", "state", ""}, {"unknown_state", "state", "closed"},
		{"open_with_accepted", "accepted_reply_event_id", want.RequestEventID}, {"open_with_terminal_time", "terminal_at", want.UpdatedAt},
		{"terminal_without_accepted_or_time", "state", "terminal"},
		{"empty_pairing", "provider_output_pin", ""}, {"missing_correlation", "request_correlation_id", ""},
		{"null_correlation_key", "correlation_key", nil}, {"null_accepted_identity", "accepted_reply_event_id", nil},
		{"null_created", "created_at", nil}, {"empty_updated", "updated_at", ""}, {"zero_created", "created_at", time.Time{}},
		{"invalid_updated", "updated_at", "not-a-timestamp"}, {"numeric_created", "created_at", 42},
		{"empty_terminal_time", "terminal_at", ""}, {"zero_terminal_time", "terminal_at", time.Time{}},
		{"worker_claim", "claim_token", "source-claim"}, {"worker", "worker_id", "source-worker"},
		{"session", "session_id", "source-session"}, {"payload_copy", "payload", map[string]any{"request": "copied"}},
		{"alternate_spelling", "STATE", "terminal"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			body := historicalReplyBody(t, want)
			body[scenario.field] = scenario.value
			if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body)); err == nil {
				t.Fatal("invalid reply shape admitted")
			}
		})
	}
	for _, field := range []string{"accepted_reply_event_id", "terminal_at"} {
		t.Run("terminal_missing_"+field, func(t *testing.T) {
			body := historicalReplyBody(t, want)
			body["state"], body["accepted_reply_event_id"], body["terminal_at"] = "terminal", want.RequestEventID, want.UpdatedAt
			body[field] = historicalReplyBody(t, want)[field]
			if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body)); err == nil {
				t.Fatal("partial terminal shape admitted")
			}
		})
	}
}

func TestHistoricalReplyFactDelegatesReturnAdmissionToOwner(t *testing.T) {
	want := historicalReplyRecord(t)
	snapshot, context := historicalReplyContext(want)
	for _, coordinate := range []string{"run", "instance", "entity"} {
		t.Run("foreign_bound_"+coordinate, func(t *testing.T) {
			bad := want
			bad.ReturnJoins = append([]events.JoinAdmissionReceipt(nil), want.ReturnJoins...)
			entry := bad.ReturnJoins[0].Ref.StageEntry()
			switch coordinate {
			case "run":
				entry.RunID = want.RequestEventID
			case "instance":
				entry.InstanceID, entry.InstancePath = "other", "collector/other"
			case "entity":
				entry.EntityID = want.RequestEventID
			}
			ref, err := bad.ReturnJoins[0].Ref.Declaration().BindStageEntry(entry, attemptgeneration.Generation{})
			if err != nil {
				t.Fatal(err)
			}
			bad.ReturnJoins[0].Ref = ref
			if bad.Validate() == nil {
				t.Fatal("fixture does not contradict canonical owner")
			}
			body := historicalReplyBody(t, want)
			body["origin_route"] = map[string]any{
				"flow_id": want.Origin.FlowID, "flow_instance": want.Origin.FlowInstance, "entity_id": want.Origin.EntityID, "join_admissions": bad.ReturnJoins,
			}
			if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body)); err == nil {
				t.Fatal("foreign retained return binding admitted")
			}
		})
	}
	body := historicalReplyBody(t, want)
	body["origin_route"] = map[string]any{
		"flow_id": want.Origin.FlowID, "flow_instance": want.Origin.FlowInstance, "entity_id": want.Origin.EntityID,
		"join_admissions": []events.JoinAdmissionReceipt{want.ReturnJoins[0], want.ReturnJoins[0]},
	}
	if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body)); err == nil {
		t.Fatal("duplicate return admission accepted")
	}
	if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, historicalReplyBody(t, want))); err != nil {
		t.Fatalf("failed admission reserved identity: %v", err)
	}
}

func TestHistoricalReplyFactRejectsHostileJSON(t *testing.T) {
	want := historicalReplyRecord(t)
	snapshot, context := historicalReplyContext(want)
	raw := string(historicalReplyJSON(t, historicalReplyBody(t, want)))
	for _, scenario := range []struct{ name, raw string }{
		{"duplicate_state", strings.TrimSuffix(raw, "}") + `,"state":"terminal"}`},
		{"escaped_duplicate_state", strings.TrimSuffix(raw, "}") + `,"st\u0061te":"terminal"}`},
		{"duplicate_origin_reset", strings.TrimSuffix(raw, "}") + `,"origin_route":null}`},
		{"nested_join_reset", strings.Replace(raw, `"origin_route":{`, `"origin_route":{"join_admissions":null,`, 1)},
		{"nested_case_alias", strings.Replace(raw, `"flow_instance":`, `"FLOW_INSTANCE":`, 1)},
		{"trailing_object", raw + `{}`}, {"null_root", `null`}, {"array_root", `[]`},
		{"invalid_utf8", strings.Replace(raw, "request-42", "request-\xff", 1)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if _, err := decodeRunForkHistoricalReplyFact(snapshot, context, []byte(scenario.raw)); err == nil {
				t.Fatal("hostile JSON erased historical reply evidence")
			}
		})
	}
}

func TestHistoricalReplyFactUsesExistingTimestampCodec(t *testing.T) {
	want := historicalReplyRecord(t)
	snapshot, context := historicalReplyContext(want)
	body := historicalReplyBody(t, want)
	zoned := want.CreatedAt.In(time.FixedZone("MST", -7*3600))
	body["created_at"] = zoned.Format("2006-01-02 15:04:05.999999999 -0700 MST")
	body["updated_at"] = want.UpdatedAt.In(zoned.Location()).Format(time.RFC3339Nano)
	got, err := decodeRunForkHistoricalReplyFact(snapshot, context, historicalReplyJSON(t, body))
	if err != nil || !reflect.DeepEqual(got.Record, want) {
		t.Fatalf("existing timestamp representations lost precision: %#v, %v", got, err)
	}
}
