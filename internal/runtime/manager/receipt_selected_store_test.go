package manager_test

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestManagerReceiptOutcomeBothSelectedStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			selected := fixture.Store
			dialect := authoractivityfixture.DialectSQLite
			if backend == "postgres" {
				dialect = authoractivityfixture.DialectPostgres
			}
			for _, status := range []manager.ReceiptStatus{manager.ReceiptStatusProcessed, manager.ReceiptStatusError, manager.ReceiptStatusDeadLetter, manager.ReceiptStatusTerminal} {
				for _, fail := range []bool{false, true} {
					phase := "healthy"
					if fail {
						phase = "handoff_and_continuation_errors"
						if status == manager.ReceiptStatusError {
							phase = "owner_return_and_continuation_errors"
						}
					}
					t.Run(string(status)+"/"+phase, func(t *testing.T) {
						ctx := fixture.Context
						runID := uuid.NewString()
						fixture.RequireRun(t, runID)
						identity := agentidentitytest.RootRuntimeForRun(t, runID, "agent-a", "receipt-selected-store")
						route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
						evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
						storetest.InsertCanonicalEventRecord(t, ctx, storetest.Database(selected), dialect, evt)
						storetest.CommitDeliveryObligationsForPersistedEvent(t, ctx, selected, evt, []events.DeliveryRoute{route})
						claimed, err := storetest.ClaimDelivery(ctx, selected, evt, route)
						if err != nil {
							t.Fatal(err)
						}
						var failure error
						if fail {
							failure = errors.New("independent acknowledged settlement return failure")
						}
						manager.ProveSelectedStoreReceiptOutcome(t, ctx, selected, evt, claimed, status, failure)
					})
				}
			}
		})
	}
}
