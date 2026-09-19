package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// Unlike the group-flush uncertainty control, this loses the acknowledgement
// of the subsequent real singleton Deferred mutation itself.
func TestB13DeferredSingletonCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "lost_after_commit", "lost_after_rollback"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f, connector := newB12CollectorFixture(t, backend)
				probe := &b12DeferredInterceptor{second: f.events[1].ID(), retryAt: time.Now().UTC().Add(time.Hour), calls: map[string]int{}}
				f.bus.SetInterceptors(probe)
				signals := &b12ContinuationSignals{DeliveryContinuationOwner: f.bus.DeliveryContinuationOwner()}
				if err := f.bus.SetDeliveryContinuationOwner(signals); err != nil {
					t.Fatal(err)
				}
				sink := &b10CandidateSink{runID: f.runID}
				scope, _ := authoractivity.ScopeFromContext(f.ctx)
				registration, err := f.raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, runlifecycle.CandidateScope{BundleHash: scope.BundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registration.Release)
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				lost := errors.New("b13_singleton_deferred_commit_ack_lost")
				observed := &b13AfterGroupSettlement{PublicationGroup: f.group}
				faults := 0
				if phase != "healthy" {
					observed.after = func() {
						connector.arm(func(tx driver.Tx) error {
							faults++
							if phase == "lost_after_commit" {
								return errors.Join(lost, tx.Commit())
							}
							return errors.Join(lost, tx.Rollback())
						})
					}
				}
				revisions := countP16RunRevisions(t, f.db, f.runID)
				err = f.bus.DispatchFanOutPublications(f.ctx, observed, f.committed)
				if phase == "healthy" {
					if err != nil || faults != 0 {
						t.Fatalf("healthy deferred execution: faults=%d err=%v", faults, err)
					}
				} else if !errors.Is(err, lost) || faults != 1 {
					t.Fatalf("missing exact singleton uncertainty: faults=%d err=%v", faults, err)
				}
				if observed.calls != 1 || len(observed.outcome.Results) != 1 || !observed.outcome.Results[0].Outcome.Committed() || !observed.outcome.Results[0].Outcome.DeliveryHandoffCommitted() {
					t.Fatalf("earlier group acknowledgement lost: calls=%d outcome=%+v", observed.calls, observed.outcome)
				}
				if probe.calls[f.events[0].ID()] != 1 || probe.calls[f.events[1].ID()] != 1 || signals.signals != 1 {
					t.Fatalf("duplicate execution or invented handoff: calls=%v signals=%d", probe.calls, signals.signals)
				}
				var status string
				var attempts, receiptCount int
				if err := f.db.QueryRow(`SELECT status,attempt_count FROM decision_card_route_obligations WHERE event_id=$1`, f.events[1].ID()).Scan(&status, &attempts); err != nil {
					t.Fatal(err)
				}
				wantAttempts := 1
				if phase == "lost_after_rollback" {
					wantAttempts = 0
				}
				if status != "pending" || attempts != wantAttempts {
					t.Fatalf("actual Deferred commit state: status=%s attempts=%d want=%d", status, attempts, wantAttempts)
				}
				if err := f.db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform'`, f.events[0].ID()).Scan(&receiptCount); err != nil || receiptCount != 1 {
					t.Fatalf("prior segment receipt lost or duplicated: count=%d err=%v", receiptCount, err)
				}
				if err := f.db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform'`, f.events[1].ID()).Scan(&receiptCount); err != nil || receiptCount != 0 {
					t.Fatalf("Deferred invented terminal receipt: count=%d err=%v", receiptCount, err)
				}
				if got := countP16RunRevisions(t, f.db, f.runID); got != revisions+1 {
					t.Fatalf("Deferred changed historical cut: got=%d want=%d", got, revisions+1)
				}
				counts := collector.Snapshot().ByOperation[transactiontest.PipelineSettlement]
				wantCommits, wantFailures, wantSubmits := uint64(2), uint64(0), 2
				if phase != "healthy" {
					wantCommits, wantFailures, wantSubmits = 1, 1, 1
				}
				if counts.CommitAttempts != 2 || counts.WriteCommits != wantCommits || counts.CommitFailures != wantFailures || sink.submits != wantSubmits {
					t.Fatalf("uncertainty retried or fabricated commit/candidate: counts=%+v submits=%d", counts, sink.submits)
				}
			})
		}
	}
}

type b13AfterGroupSettlement struct {
	pipelineobligation.PublicationGroup
	after   func()
	calls   int
	outcome pipelineobligation.PublicationGroupOutcome
}

func (g *b13AfterGroupSettlement) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	g.calls++
	out, err := g.PublicationGroup.Settle(ctx, members)
	g.outcome = out
	if err == nil && len(out.Results) > 0 && g.after != nil {
		g.after()
	}
	return out, err
}

func TestB13GroupRejectsDeferredBeforeMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			before := f.snapshot(t)
			out, err := f.group.Settle(f.ctx, []pipelineobligation.PublicationSettlementMember{{Claim: f.members[1].Claim, Disposition: pipelineobligation.Deferred("b13_deferred", time.Now().UTC().Add(time.Hour), nil)}})
			if err == nil || !strings.Contains(err.Error(), "only terminal dispositions") || len(out.Results) != 0 {
				t.Fatalf("group admitted nonterminal singleton behavior: %+v err=%v", out, err)
			}
			f.requireSnapshot(t, before)
		})
	}
}
