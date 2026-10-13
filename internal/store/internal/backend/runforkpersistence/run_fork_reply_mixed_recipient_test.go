package runforkpersistence

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

func mixedRequestRecipientFixture(t *testing.T) (*runForkRevisionSnapshot, string, string, pinrouting.ReplyRequestContinuation, []events.DeliveryRoute) {
	t.Helper()
	snapshot, eventID, child, reply, projected := requestContinuationPartitionFixture(t)
	sibling := snapshot.Deliveries[0]
	sibling.Snapshot.Route.Recipient = events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, ".", "observer"))
	sibling.Snapshot.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID})
	setReplayReceiverConstructedHistory(t, snapshot, snapshot.RunID, snapshot.RunID, ".", snapshot.ReplyContexts[0].Record.CreatedAt)
	sibling.Snapshot.Route.Context = events.DeliveryContext{}
	sibling.Snapshot.Route.ConnectClaim = events.ConnectExecutionClaim{}
	var err error
	sibling.Snapshot.DeliveryID, err = deliverylifecycle.DeliveryID(eventID, sibling.Snapshot.Route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = append(snapshot.Deliveries, sibling)
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	event, err := cut.event(eventID)
	if err != nil {
		t.Fatal(err)
	}
	siblingProjected, err := projectSelectedFanOutRecipient(snapshot, event, sibling.Snapshot, child)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, eventID, child, reply, []events.DeliveryRoute{projected, siblingProjected}
}

func TestIssue642RequestPartitionKeepsNonReplyRecipient(t *testing.T) {
	for _, fault := range []string{"exact", "omitted_observer", "extra_observer", "observer_claim", "observer_target"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, eventID, child, reply, routes := mixedRequestRecipientFixture(t)
			switch fault {
			case "omitted_observer":
				routes = routes[:1]
			case "extra_observer":
				routes = append(routes, routes[1])
			case "observer_claim":
				routes[1].ConnectClaim = routes[0].ConnectClaim
			case "observer_target":
				routes[1].Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child, EntityID: uuid.NewString()})
			}
			before := projectionJSON(t, snapshot)
			input := append([]events.DeliveryRoute(nil), routes...)
			err := requireRunForkRequestRecipientPartition(snapshot, eventID, child, []pinrouting.ReplyRequestContinuation{reply}, routes)
			if (err == nil) != (fault == "exact") {
				t.Fatalf("complete mixed recipient partition %s: %v", fault, err)
			}
			if before != projectionJSON(t, snapshot) || !reflect.DeepEqual(input, routes) {
				t.Fatal("mixed partition validation mutated its retained inputs")
			}
		})
	}
}

func TestIssue642SettledReplyDoesNotBlockOtherPendingRecipient(t *testing.T) {
	for _, fault := range []string{"settled_reply", "pending_terminal_reply", "pending_open_reply", "settled_open_reply"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, eventID, _, _, _ := mixedRequestRecipientFixture(t)
			pending := fault == "pending_terminal_reply" || fault == "pending_open_reply"
			if !pending {
				snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDelivered
			}
			row := &snapshot.ReplyContexts[0].Record
			if fault == "settled_reply" || fault == "pending_terminal_reply" {
				row.State = replycontext.StateTerminal
				row.AcceptedReplyEventID = uuid.NewString()
				terminal := row.UpdatedAt
				row.TerminalAt = &terminal
			}
			if err := row.Validate(); err != nil {
				t.Fatal(err)
			}
			before := projectionJSON(t, snapshot)
			got, err := capturedRunForkRequestReplies(context.Background(), nil, snapshot, eventID)
			if fault == "pending_terminal_reply" {
				if err == nil {
					t.Fatal("actual pending request execution accepted a terminal reply")
				}
			} else if err != nil || (len(got) == 1) != pending {
				t.Fatalf("request-bearing unfinished partition %s: rows=%+v error=%v", fault, got, err)
			}
			if before != projectionJSON(t, snapshot) {
				t.Fatal("discovery changed settled history")
			}
		})
	}
}

func TestIssue642NativeRequestPartitionIncludesOnlyUnfinishedRecipients(t *testing.T) {
	for _, status := range []deliverylifecycle.Status{deliverylifecycle.StatusPending, deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusCanceled} {
		t.Run(string(status), func(t *testing.T) {
			snapshot, eventID, child, _, routes := mixedRequestRecipientFixture(t)
			snapshot.Deliveries[0].Snapshot.Status = status
			before := projectionJSON(t, snapshot)
			partitions, err := requestPendingPublications(context.Background(), nil, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{snapshot.Deliveries[1].Snapshot.DeliveryID}
			if !snapshot.Deliveries[0].Snapshot.Terminal() {
				want = append(want, snapshot.Deliveries[0].Snapshot.DeliveryID)
			}
			if len(partitions) != 1 || len(partitions[eventID]) != len(want) {
				t.Fatalf("complete unfinished request partition: %+v want=%v", partitions, want)
			}
			for _, id := range want {
				found := false
				for _, actual := range partitions[eventID] {
					found = found || actual == id
				}
				if !found {
					t.Fatalf("unfinished request delivery omitted: %s", id)
				}
			}
			if snapshot.Deliveries[0].Snapshot.Terminal() {
				projected, err := projectContinuedRequestSourceRoutes(context.Background(), nil, snapshot, eventID, child)
				if err != nil || !reflect.DeepEqual(projected, routes[1:]) {
					t.Fatalf("non-reply observer projection changed: %+v error=%v", projected, err)
				}
				if err := requireRunForkRequestRecipientPartition(snapshot, eventID, child, nil, routes[1:]); err != nil {
					t.Fatalf("settled provider blocked exact unfinished observer: %v", err)
				}
				if err := requireRunForkRequestRecipientPartition(snapshot, eventID, child, nil, routes); err == nil {
					t.Fatal("settled provider was admitted as fresh owed work")
				}
			}
			if before != projectionJSON(t, snapshot) {
				t.Fatal("partition discovery/projection repaired retained history")
			}
		})
	}
}
