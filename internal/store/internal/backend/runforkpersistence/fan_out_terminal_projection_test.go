package runforkpersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestIssue642FanOutTerminalRecipientDisposition(t *testing.T) {
	for _, test := range []struct {
		name     string
		statuses []deliverylifecycle.Status
		want     fanoutobligation.InheritedTerminalDisposition
	}{
		{"success", []deliverylifecycle.Status{deliverylifecycle.StatusDelivered}, "succeeded"},
		{"canceled", []deliverylifecycle.Status{deliverylifecycle.StatusCanceled}, "canceled"},
		{"success_and_canceled", []deliverylifecycle.Status{deliverylifecycle.StatusDelivered, deliverylifecycle.StatusCanceled}, "canceled"},
		{"failure_and_canceled", []deliverylifecycle.Status{deliverylifecycle.StatusDeadLetter, deliverylifecycle.StatusCanceled}, "dead_lettered"},
	} {
		t.Run(test.name, func(t *testing.T) {
			run, event := uuid.NewString(), uuid.NewString()
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
			at := time.Now().UTC()
			for i, status := range test.statuses {
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
					CreatedAt: at, UpdatedAt: at, SettledAt: at, MaxRetries: 3, ClaimVersion: 1}
				if status == deliverylifecycle.StatusCanceled {
					row.ReasonCode = string(deliverylifecycle.CancellationTerminate)
				}
				deliveryRows[event] = append(deliveryRows[event], runForkRevisionDelivery{Snapshot: row})
			}
			terminal, disposition, pending, _, err := fixedRunForkFanOutEventDisposition(event, run, nil, "", eventRows, deliveryRows, nil, nil)
			if err != nil || !terminal || disposition != test.want || len(pending) != 0 {
				t.Fatalf("terminal recipient disposition lost: terminal=%t disposition=%q pending=%v err=%v; want %q", terminal, disposition, pending, err, test.want)
			}
			outcome := fanoutobligation.Outcome{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: event, InheritedDisposition: disposition, CreatedAt: at}
			if err := outcome.Validate(); err != nil {
				t.Fatalf("projected terminal evidence is unreadable: %v", err)
			}
		})
	}
}
