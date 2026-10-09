package runforkrevision

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
)

func TestReplyContextProjectionCoversCanonicalRecord(t *testing.T) {
	spec := replyContextProjectionSpec()
	fields := map[string]string{
		"ID": "reply_context_id", "RunID": "run_id", "RequestEventID": "request_event_id",
		"RequesterFlowID": "requester_flow_id", "RequestOutputPin": "request_output_pin", "ReplyInputPin": "reply_input_pin",
		"ProviderFlowID": "provider_flow_id", "ProviderInputPin": "provider_input_pin", "ProviderOutputPin": "provider_output_pin",
		"Origin": "origin_route", "ReturnJoins": "origin_route", "RequestCorrelationID": "request_correlation_id",
		"CorrelationKey": "correlation_key", "State": "state", "AcceptedReplyEventID": "accepted_reply_event_id",
		"CreatedAt": "created_at", "UpdatedAt": "updated_at", "TerminalAt": "terminal_at",
	}
	recordType := reflect.TypeOf(replycontext.Record{})
	if recordType.NumField() != len(fields) {
		t.Fatal("canonical reply record changed; audit the historical field inventory")
	}
	kinds := make(map[string]valueKind)
	for _, column := range spec.columns {
		if _, duplicate := kinds[column.name]; duplicate {
			t.Fatalf("duplicate column %s", column.name)
		}
		kinds[column.name] = column.kind
	}
	if len(kinds) != 17 || spec.build != nil {
		t.Fatalf("projection should retain exactly the 17 native columns: %#v", spec)
	}
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i).Name
		column, exists := fields[field]
		if !exists {
			t.Fatalf("canonical field %s has no historical projection", field)
		}
		kind, exists := kinds[column]
		if !exists || !strings.Contains(spec.query, "r."+column) {
			t.Fatalf("canonical field %s loses native column %s", field, column)
		}
		want := valueRaw
		switch column {
		case "origin_route":
			want = valueJSON
		case "created_at", "updated_at", "terminal_at":
			want = valueTime
		}
		if kind != want {
			t.Fatalf("column %s kind = %d, want %d", column, kind, want)
		}
	}
	for _, excluded := range []string{"worker", "session", "claim", "payload", "lease"} {
		if strings.Contains(spec.query, excluded) {
			t.Fatalf("projection imports operational/payload field %s", excluded)
		}
	}
}

func TestReplyContextProjectionUsesExactRunAndOpaqueKey(t *testing.T) {
	spec := replyContextProjectionSpec()
	const runID = "11111111-1111-4111-8111-111111111111"
	const key = "reply-v1:opaque/request|42'"
	ref, err := NewFactRef(FamilyReplyContexts, key)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := selectedProjectionQuery(spec, FamilyReplyContexts, runID, nil)
	if err != nil || query != spec.query || !reflect.DeepEqual(args, []any{runID}) {
		t.Fatalf("whole projection = %q, %#v, %v", query, args, err)
	}
	query, args, err = selectedProjectionQuery(spec, FamilyReplyContexts, runID, []FactRef{ref})
	if err != nil || !strings.Contains(query, "r.run_id = $1") || !strings.Contains(query, "r.reply_context_id=$2") ||
		strings.Contains(query, key) || !reflect.DeepEqual(args, []any{runID, key}) {
		t.Fatalf("exact projection = %q, %#v, %v", query, args, err)
	}
}

func TestReplyContextProjectionNormalizesWithoutLosingOriginOrDisposition(t *testing.T) {
	declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "collector", "join"), "item.done", "collecting", "items")
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{
		RunID: "11111111-1111-4111-8111-111111111111", FlowScope: "collector", InstanceID: "one",
		InstancePath: "collector/one", EntityID: "22222222-2222-4222-8222-222222222222", Stage: "collecting", Cause: "construction",
	}
	bound, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	want := replycontext.Record{
		ID: "reply/opaque", RunID: entry.RunID, RequestEventID: "33333333-3333-4333-8333-333333333333",
		RequesterFlowID: "collector", RequestOutputPin: "request", ReplyInputPin: "return", ProviderFlowID: "worker",
		ProviderInputPin: "request", ProviderOutputPin: "return", RequestCorrelationID: "request-42", CorrelationKey: "item.id",
		Origin:      events.RouteIdentity{FlowID: "collector", FlowInstance: entry.InstancePath, EntityID: entry.EntityID},
		ReturnJoins: []events.JoinAdmissionReceipt{{Ref: bound, Disposition: events.JoinAdmissionBound}}, State: replycontext.StateOpen,
	}
	origin, err := want.EncodeOrigin()
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 9, 8, 10, 11, 12, 123456789, time.UTC)
	zoned := stamp.In(time.FixedZone("fixture", -7*3600))
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "terminal"}[terminal], func(t *testing.T) {
			values := map[string]any{
				"reply_context_id": want.ID, "run_id": want.RunID, "request_event_id": want.RequestEventID,
				"requester_flow_id": want.RequesterFlowID, "request_output_pin": want.RequestOutputPin, "reply_input_pin": want.ReplyInputPin,
				"provider_flow_id": want.ProviderFlowID, "provider_input_pin": want.ProviderInputPin, "provider_output_pin": want.ProviderOutputPin,
				"origin_route": origin, "request_correlation_id": want.RequestCorrelationID, "correlation_key": want.CorrelationKey,
				"state": "open", "accepted_reply_event_id": "", "created_at": zoned, "updated_at": []byte(zoned.Format(time.RFC3339Nano)), "terminal_at": nil,
			}
			if terminal {
				values["state"], values["accepted_reply_event_id"], values["terminal_at"] = "terminal", "44444444-4444-4444-8444-444444444444", zoned.Format(time.RFC3339Nano)
			}
			for _, column := range replyContextProjectionSpec().columns {
				value, err := normalizeProjectionValue(values[column.name], column.kind)
				if err != nil {
					t.Fatal(err)
				}
				values[column.name] = value
			}
			raw, err := json.Marshal(values["origin_route"])
			if err != nil {
				t.Fatal(err)
			}
			got := want
			got.Origin, got.ReturnJoins = events.RouteIdentity{}, nil
			if err := got.DecodeOrigin(raw); err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("origin or return admission lost: %#v, %v", got, err)
			}
			for _, column := range []string{"created_at", "updated_at"} {
				if values[column] != stamp.Format(time.RFC3339Nano) {
					t.Fatalf("%s lost UTC/nanosecond precision: %v", column, values[column])
				}
			}
			if terminal && (values["state"] != "terminal" || values["accepted_reply_event_id"] != "44444444-4444-4444-8444-444444444444" || values["terminal_at"] != stamp.Format(time.RFC3339Nano)) {
				t.Fatalf("terminal disposition lost: %#v", values)
			}
			if !terminal && (values["accepted_reply_event_id"] != "" || values["terminal_at"] != nil) {
				t.Fatalf("open disposition changed: %#v", values)
			}
			key, err := projectionFactKey(FamilyReplyContexts, values)
			if err != nil || key != want.ID {
				t.Fatalf("opaque reply identity changed: %q, %v", key, err)
			}
		})
	}
}
