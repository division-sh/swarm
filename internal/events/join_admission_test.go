package events

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

func TestA2DeliveryContextRetainsExactJoinReceipt(t *testing.T) {
	node := identitytest.FlowNode(t, "collector", "join")
	declaration, err := timeridentity.NewJoinRef(node, "item.done", "collecting", "items")
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{RunID: "run-1", FlowScope: "collector", InstanceID: "one", InstancePath: "collector/one", EntityID: "entity-1", Stage: "collecting", Cause: "delivery", EventID: "entry-event-1", OccurrenceID: "entry-delivery-1", TransitionID: "transition-1"}
	bound, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	context := DeliveryContext{Reply: &ReplyContextRef{ID: "reply-1"}, Joins: []JoinAdmissionReceipt{{Ref: bound, Disposition: JoinAdmissionBound}}}
	raw, err := json.Marshal(context.Normalized())
	if err != nil {
		t.Fatal(err)
	}
	var restored DeliveryContext
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatal(err)
	}
	receipt, found := restored.JoinAdmission(declaration)
	if !found || !receipt.Ref.Equal(bound) || restored.ReplyContextID() != "reply-1" {
		t.Fatalf("receipt changed on readback: %#v found=%v", receipt, found)
	}
	entry.EventID, entry.OccurrenceID = "entry-event-2", "entry-delivery-2"
	if _, err := bound.BindStageEntry(entry, attemptgeneration.Generation{}); err == nil {
		t.Fatal("a retained obligation rebound to the next entry")
	}
	if fresh := restored.ReplyOnly(); len(fresh.Joins) != 0 || fresh.ReplyContextID() != "reply-1" {
		t.Fatal("an unrelated business output inherited ambient join authority")
	}
}

func TestA2JoinReceiptHasClosedAdmissionStates(t *testing.T) {
	node := identitytest.FlowNode(t, "collector", "join")
	declaration, _ := timeridentity.NewJoinRef(node, "item.done", "collecting", "items")
	if err := (JoinAdmissionReceipt{Ref: declaration, Disposition: JoinAdmissionEarly}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, receipt := range []JoinAdmissionReceipt{
		{Ref: declaration, Disposition: JoinAdmissionBound},
		{Ref: declaration, Disposition: "first_target_admission"},
		{Ref: declaration},
		{Disposition: JoinAdmissionEarly},
	} {
		if err := receipt.Validate(); err == nil {
			t.Fatalf("missing or speculative binding accepted: %#v", receipt)
		}
	}
	context := DeliveryContext{Joins: []JoinAdmissionReceipt{{Ref: declaration, Disposition: JoinAdmissionEarly}, {Ref: declaration, Disposition: JoinAdmissionEarly}}}
	if err := context.Validate(); err == nil {
		t.Fatal("two receipts for one declaration accepted")
	}
}
