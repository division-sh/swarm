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
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestManagerReceiptOutcomeBothSelectedStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				runtimedelivery.Store
				runlifecycle.OperationOwner
				runlifecycle.CandidateStore
				runlifecycle.CandidateRegistrar
			}
			dialect := authoractivityfixture.DialectSQLite
			if backend == "postgres" {
				_, db, _ := testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
				dialect = authoractivityfixture.DialectPostgres
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			original := originalManagerFixtureContext(t, manager.TerminalPanicFixtureContext(), selected.(agentfixture.Store))
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
						ctx := original
						runID := uuid.NewString()
						fixture.RequireRun(t, runID)
						identity := agentidentitytest.RootRuntimeForRun(t, runID, "agent-a", "receipt-selected-store")
						route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
						evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
						fixture.Publish(t, ctx, evt, []events.DeliveryRoute{route}, fixture.Authority)
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
