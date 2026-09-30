package replycontext

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

func TestA2ReplyOriginRetainsItsExactJoinObligation(t *testing.T) {
	record := a2ReplyOriginRecord(t)
	raw, err := record.EncodeOrigin()
	if err != nil {
		t.Fatal(err)
	}
	copy := record
	copy.Origin, copy.ReturnJoins = events.RouteIdentity{}, nil
	if err := copy.DecodeOrigin(raw); err != nil {
		t.Fatal(err)
	}
	if !record.SameIdentity(copy) {
		t.Fatal("return receipt changed across storage")
	}
	copy.ReturnJoins = nil
	if record.SameIdentity(copy) {
		t.Fatal("missing return binding retained the same reply authority")
	}
	entry := record.ReturnJoins[0].Ref.StageEntry()
	entry.Cause, entry.EventID, entry.OccurrenceID, entry.TransitionID = "delivery", "event-2", "delivery-2", "transition"
	newRef, err := record.ReturnJoins[0].Ref.Declaration().BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	copy.ReturnJoins = []events.JoinAdmissionReceipt{{Ref: newRef, Disposition: events.JoinAdmissionBound}}
	if record.SameIdentity(copy) {
		t.Fatal("a newer stage entry replaced immutable reply authority")
	}
	copy.RunID = "other-run"
	if copy.Validate() == nil {
		t.Fatal("foreign run accepted a retained reply receipt")
	}
}

func a2ReplyOriginRecord(t *testing.T) Record {
	t.Helper()
	declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "collector", "join"), "item.done", "collecting", "items")
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{RunID: "run", FlowScope: "collector", InstanceID: "one", InstancePath: "collector/one", EntityID: "entity", Stage: "collecting", Cause: "construction"}
	ref, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	return Record{
		ID: "reply", RunID: "run", RequestEventID: "request", RequesterFlowID: "collector", RequestOutputPin: "request", ReplyInputPin: "return", ProviderFlowID: "worker", ProviderInputPin: "request", ProviderOutputPin: "return",
		Origin: events.RouteIdentity{FlowID: "collector", FlowInstance: "collector/one", EntityID: "entity"}, RequestCorrelationID: "correlation", State: StateOpen, CreatedAt: time.Now().UTC(),
		ReturnJoins: []events.JoinAdmissionReceipt{{Ref: ref, Disposition: events.JoinAdmissionBound}},
	}
}

func TestA2ReplyOriginHostileJSONCannotEraseRetainedEvidence(t *testing.T) {
	record := a2ReplyOriginRecord(t)
	raw, err := record.EncodeOrigin()
	if err != nil {
		t.Fatal(err)
	}
	objectPrefix := strings.TrimSuffix(string(raw), "}")
	for _, scenario := range []struct{ name, raw string }{
		{"duplicate_reset", objectPrefix + `,"join_admissions":null}`},
		{"escaped_duplicate_reset", objectPrefix + `,"join_\u0061dmissions":null}`},
		{"case_alias_reset", objectPrefix + `,"JOIN_ADMISSIONS":null}`},
		{"foreign_field", objectPrefix + `,"unknown":true}`},
		{"nested_duplicate", strings.Replace(string(raw), `"instance_id":"one"`, `"instance_id":"one","instance_id":"foreign"`, 1)},
		{"trailing_value", string(raw) + `{}`},
		{"non_object", `null`},
		{"invalid_utf8", strings.Replace(string(raw), `"entity"`, "\"\xff\"", 1)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			retained := record
			if err := retained.DecodeOrigin([]byte(scenario.raw)); err == nil {
				t.Fatal("hostile reply origin was accepted")
			}
			if !reflect.DeepEqual(record, retained) {
				t.Fatal("failed origin decoding changed retained reply authority")
			}
		})
	}
}
