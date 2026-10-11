package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// Complete continuation must preserve settled failure evidence in the child
// fold; omitting those routes alone is not a fix.
func TestIssue642FanOutPublishedPendingNodeProjection(t *testing.T) {
	for _, firstStatus := range []deliverylifecycle.Status{deliverylifecycle.StatusPending, deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter} {
		t.Run(string(firstStatus), func(t *testing.T) {
			run, event := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC()
			ledger, err := events.NewConnectEvaluationLedger(nil)
			if err != nil {
				t.Fatal(err)
			}
			settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(settlement)
			if err != nil {
				t.Fatal(err)
			}
			eventRows := map[string]runForkRevisionEvent{event: {RunID: run, EventID: event, RouteSettlement: raw}}
			deliveryRows := map[string][]runForkRevisionDelivery{}
			pendingRows := map[string]runfork.RunForkPendingWork{}
			var wantPending []string
			for i, status := range []deliverylifecycle.Status{firstStatus, deliverylifecycle.StatusPending} {
				node, err := identity.AdmitExecutableNodeDeclaration(".", []string{"first", "second"}[i])
				if err != nil {
					t.Fatal(err)
				}
				target, err := events.NewEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: run})
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}
				routeID, err := route.Identity()
				if err != nil {
					t.Fatal(err)
				}
				deliveryID, err := deliverylifecycle.DeliveryID(event, route)
				if err != nil {
					t.Fatal(err)
				}
				row := deliverylifecycle.Snapshot{DeliveryID: deliveryID, EventID: event, RunID: run, Route: route, RouteIdentity: routeID,
					SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: node.Key(), Status: status,
					CreatedAt: now, UpdatedAt: now, MaxRetries: 3}
				if status.Terminal() {
					row.ClaimVersion, row.SettledAt = 1, now
				} else {
					pendingRows[deliveryID] = runfork.RunForkPendingWork{DeliveryID: deliveryID, EventID: event, SubscriberType: "node", SubscriberID: node.Key(), DeliveryRoute: route,
						Status: "pending", Classification: runfork.RunForkPendingClassificationPending}
					wantPending = append(wantPending, deliveryID)
				}
				deliveryRows[event] = append(deliveryRows[event], runForkRevisionDelivery{Snapshot: row})
			}
			before := append([]runForkRevisionDelivery(nil), deliveryRows[event]...)
			terminal, retained, pending, _, err := fixedRunForkFanOutEventDisposition(event, run, nil, "", eventRows, deliveryRows, nil, pendingRows)
			if !reflect.DeepEqual(before, deliveryRows[event]) {
				t.Fatal("classification changed the source delivery facts")
			}
			sort.Strings(wantPending)
			if err != nil || terminal || !reflect.DeepEqual(pending, wantPending) {
				t.Fatalf("published work must retain exact unfinished recipients: terminal=%t pending=%v want=%v err=%v", terminal, pending, wantPending, err)
			}
			wantRetained := map[deliverylifecycle.Status]fanoutobligation.InheritedTerminalDisposition{
				deliverylifecycle.StatusDelivered:  fanoutobligation.InheritedSucceeded,
				deliverylifecycle.StatusDeadLetter: fanoutobligation.InheritedDeadLettered,
			}[firstStatus]
			if retained != wantRetained {
				t.Fatalf("settled source partition was discarded: got=%s want=%s", retained, wantRetained)
			}
			for _, prior := range []fanoutobligation.InheritedTerminalDisposition{"succeeded", "dead_lettered", "canceled"} {
				terminal, retained, pending, _, err := fixedRunForkFanOutEventDisposition(event, run, nil, prior, eventRows, deliveryRows, nil, pendingRows)
				want := prior
				if firstStatus == deliverylifecycle.StatusDeadLetter {
					want = fanoutobligation.InheritedDeadLettered
				}
				if err != nil || terminal || retained != want || !reflect.DeepEqual(pending, wantPending) {
					t.Fatalf("refork lost unfinished partition or retained contribution: terminal=%t retained=%s pending=%v err=%v", terminal, retained, pending, err)
				}
			}
		})
	}
}
