package manager_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type receiptCandidateSink struct {
	submit func(runlifecycle.Candidate) error
}

func (s *receiptCandidateSink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	return s, nil
}
func (s *receiptCandidateSink) Submit(candidate runlifecycle.Candidate) error {
	return s.submit(candidate)
}
func (*receiptCandidateSink) Cancel() error { return nil }

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
						ctx := manager.TerminalPanicFixtureContext()
						runID := uuid.NewString()
						storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
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
							failure = errors.New("independent committed candidate handoff failure")
						}
						submits := 0
						registration, err := selected.RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: claimed.Snapshot.Authority.SourceArtifact().BundleHash()}, &receiptCandidateSink{submit: func(candidate runlifecycle.Candidate) error {
							submits++
							if candidate.RunID != runID {
								t.Errorf("foreign candidate: %+v", candidate)
							}
							durable, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
							if err != nil {
								return err
							}
							if !durable.MatchesSettlementClaim(claimed.Claim) {
								t.Errorf("handoff before exact durable settlement: %+v", durable)
							}
							return failure
						}})
						if err != nil {
							t.Fatal(err)
						}
						defer registration.Release()
						manager.ProveSelectedStoreReceiptOutcome(t, ctx, selected, evt, claimed, status, failure)
						wantSubmits := 1
						if status == manager.ReceiptStatusError {
							wantSubmits = 0
						}
						if submits != wantSubmits {
							t.Fatalf("candidate submissions=%d want=%d", submits, wantSubmits)
						}
					})
				}
			}
		})
	}
}
