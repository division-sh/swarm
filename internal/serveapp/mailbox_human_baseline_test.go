package serveapp

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestHumanTaskRealProducerCompletionBaselineBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newMailboxCompletionFixture(t, backend)
			card := mailboxCompletionAnchorCard(t, f, decisioncard.AnchorKindHumanTask)
			anchor, err := card.Anchor.HumanTask()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(anchor.OperationID.String(), "human-task-operation:v1:") {
				t.Fatalf("real dispatcher operation = %q", anchor.OperationID.String())
			}
			if anchor.Source.Route().EntityID != "" {
				t.Fatalf("regression requires the original entityless requester, got %#v", anchor.Source.Route())
			}
			control, err := card.Anchor.ControlRoutingSource()
			if err != nil || control.Kind() != events.RoutingSourceFlowOwnedControl || control.Route() != anchor.Source.Route() {
				t.Fatalf("control projection = %#v, %v; source %#v", control, err, anchor.Source.Route())
			}
			var result map[string]any
			requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.defer", map[string]any{"card_id": card.CardID, "until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), "idempotency_key": "baseline-human-defer"}, &result)
			waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, card.RunID)
		})
	}
}
