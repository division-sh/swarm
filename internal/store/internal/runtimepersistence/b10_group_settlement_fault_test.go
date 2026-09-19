package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// B19 covers history-only rollback. These cases additionally inspect route,
// delivery handoff, and live candidate admission through those same boundaries.
func TestB10GroupSettlementFaultsRollbackAllRepresentations(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, point := range []struct {
			name, fault string
			member      int
		}{
			{"receipt_first", "receipt", 0},
			{"route_first", "route", 0},
			{"delivery_handoff_first", "delivery_handoff", 0},
			{"receipt", "receipt", 1},
			{"route", "route", 1},
			{"delivery_handoff", "delivery_handoff", 1},
			{"candidate_write", "candidate_write", 1},
			{"candidate_admission", "candidate_admission", 1},
			{"finalizer", "finalizer", 1},
		} {
			t.Run(backend+"/"+point.name, func(t *testing.T) {
				fault := point.fault
				f := newB10GroupFaultFixture(t, backend)
				injected := errors.New("b10_candidate_admission")
				var remove func()
				if fault == "candidate_admission" {
					f.sink.reserveErr = injected
					remove = func() { f.sink.reserveErr = nil }
				} else {
					remove = f.installFaultAt(t, fault, point.member)
				}
				before := f.snapshot(t)
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				out, err := f.group.Settle(f.ctx, f.members)
				if fault == "candidate_admission" {
					if !errors.Is(err, injected) {
						t.Fatalf("candidate admission cause lost: %v", err)
					}
				} else {
					requireB10NativeFault(t, backend, err)
				}
				if len(out.Results) != 0 {
					t.Fatalf("rollback minted acknowledged results: %+v", out)
				}
				receipt := collector.Snapshot()
				settlement := receipt.ByOperation[transactiontest.PipelineSettlement]
				if settlement.Begun != 1 || settlement.RollbackAttempts != 1 || settlement.CommitAttempts != 0 || receipt.Total.WriteCommits != 0 || receipt.Active != 0 {
					t.Fatalf("fault did not roll back one actual settlement: %+v", receipt)
				}
				wantFinalizers := uint64(0)
				if fault == "finalizer" {
					wantFinalizers = 1
				}
				if settlement.Revision.Finalizations != wantFinalizers {
					t.Fatalf("fault reached wrong finalizer boundary: %+v", settlement)
				}
				f.requireSnapshot(t, before)
				wantReserved := 1
				if fault == "candidate_write" || (point.member == 0 && (fault == "receipt" || fault == "route")) {
					wantReserved = 0
				}
				wantCanceled := wantReserved
				if fault == "candidate_admission" {
					wantCanceled = 0
				}
				if f.sink.reserves != wantReserved || f.sink.cancels != wantCanceled || f.sink.submits != 0 {
					t.Fatalf("rollback leaked candidate admission: %+v", f.sink)
				}
				f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
				remove()
				revision := countP16RunRevisions(t, f.db, f.seed.runID)
				out, err = f.group.Settle(f.ctx, f.members)
				requireB10Acknowledged(t, out, err, nil)
				if f.sink.submits != 1 || countP16RunRevisions(t, f.db, f.seed.runID) != revision+1 {
					t.Fatalf("known rollback retry did not commit one segment/candidate: %+v", f.sink)
				}
				f.requireObservation(t, pipelineobligation.PublicationSettlementSatisfied)
			})
		}
	}
}

func TestB12GroupSettlementLostCommitAcknowledgement(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, durable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/durable_%t", backend, durable), func(t *testing.T) {
				f := newB10GroupFaultFixture(t, backend)
				provePoisoned := func() {}
				if raw, ok := f.raw.(*PostgresStore); ok {
					state, err := raw.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.members[0].Claim)
					if err != nil {
						t.Fatal(err)
					}
					session := state.LeaseForTest().Session()
					provePoisoned = func() {
						if release, retained := session.Retain(); retained {
							_ = release()
							t.Fatal("uncertain COMMIT left its designated session reusable")
						}
					}
				}
				before := f.snapshot(t)
				revisions := countP16RunRevisions(t, f.db, f.seed.runID)
				var candidateRevision int64
				if err := f.db.QueryRow(`SELECT completion_revision FROM runs WHERE run_id=$1`, f.seed.runID).Scan(&candidateRevision); err != nil {
					t.Fatal(err)
				}
				lost := errors.New("b12_commit_acknowledgement_lost")
				f.connector.arm(func(tx driver.Tx) error {
					if durable {
						return errors.Join(lost, tx.Commit())
					}
					return errors.Join(lost, tx.Rollback())
				})
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				out, err := f.group.Settle(f.ctx, f.members)
				if !errors.Is(err, lost) || len(out.Results) != 0 {
					t.Fatalf("lost acknowledgement minted committed/handoff evidence: %+v err=%v", out, err)
				}
				provePoisoned()
				receipt := collector.Snapshot()
				counts := receipt.ByOperation[transactiontest.PipelineSettlement]
				if counts.CommitAttempts != 1 || counts.CommitFailures != 1 || counts.WriteCommits != 0 || counts.Revision.Finalizations != 1 || receipt.Active != 0 {
					t.Fatalf("physical COMMIT loss census: %+v", receipt)
				}
				if f.sink.reserves != 1 || f.sink.cancels != 1 || f.sink.submits != 0 {
					t.Fatalf("uncertainty created executable candidate handoff: %+v", f.sink)
				}
				want := pipelineobligation.PublicationSettlementPending
				if durable {
					want = pipelineobligation.PublicationSettlementSatisfied
					if got := countP16RunRevisions(t, f.db, f.seed.runID); got != revisions+1 {
						t.Fatalf("durable segment revisions=%d want=%d", got, revisions+1)
					}
					var gotRevision int64
					var due bool
					if err := f.db.QueryRow(`SELECT completion_revision,completion_due_at IS NOT NULL FROM runs WHERE run_id=$1`, f.seed.runID).Scan(&gotRevision, &due); err != nil || !due || gotRevision != candidateRevision+1 {
						t.Fatalf("uncertain durable candidate missing/coalescing changed: revision=%d want=%d due=%v err=%v", gotRevision, candidateRevision+1, due, err)
					}
				} else {
					f.requireSnapshot(t, before)
				}
				observed := f.snapshot(t)
				f.requireObservation(t, want)
				f.requireObservation(t, want)
				f.requireSnapshot(t, observed)
				if len(out.Results) != 0 || f.sink.submits != 0 || collector.Snapshot().ByOperation[transactiontest.PipelineSettlement].CommitAttempts != 1 {
					t.Fatal("readback minted acknowledgement, handoff, or retried uncertain mutation")
				}
				// Neither test knowledge of the driver's outcome nor readback is
				// permission to retry these uncertain claims.
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				owner := f.raw.(pipelineObligationParityStore).PipelineObligations()
				for _, member := range f.members {
					if raw, ok := f.raw.(*PostgresStore); ok && raw.pipelinePostgresOwner.PostgresPipelineClaimsForTest().ContainsEventForTest(member.Claim.EventID()) {
						t.Fatal("uncertain group retained a registry member after Close")
					}
					next, err := owner.ClaimEvent(f.ctx, member.Claim.EventID(), pipelineobligation.PurposeDecisionRoute)
					if durable {
						if !errors.Is(err, pipelineobligation.ErrIneligible) {
							t.Fatalf("durable acknowledgement became replayable: %v", err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("new singleton recovery cannot acquire unacknowledged member: %v", err)
					}
					beforeCleanup := f.snapshot(t)
					if err := f.group.Close(f.ctx); err != nil {
						t.Fatal(err)
					}
					if err := owner.Release(f.ctx, member.Claim); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
						t.Fatalf("old claim cleanup did not refuse consumed authority: %v", err)
					}
					f.requireSnapshot(t, beforeCleanup)
					ack, err := owner.Settle(f.ctx, next.Claim, member.Disposition)
					if err != nil || !ack.Committed() {
						t.Fatalf("uncertain predecessor cleanup stole exact successor: %+v err=%v", ack, err)
					}
				}
			})
		}
	}
}

func TestB13GroupAcknowledgedCommitSurvivesPostCommitFault(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"candidate_submit", "claim_cleanup"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				f := newB10GroupFaultFixture(t, backend)
				injected := errors.New("b13_after_acknowledged_commit")
				if fault == "candidate_submit" {
					f.sink.submitErr = injected
				} else {
					switch raw := f.raw.(type) {
					case *SQLiteRuntimeStore:
						raw.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(func() error { return injected })
						t.Cleanup(func() { raw.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(nil) })
					case *PostgresStore:
						state, err := raw.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.members[0].Claim)
						if err != nil {
							t.Fatal(err)
						}
						session := state.LeaseForTest().Session()
						session.SetEndTxErrorForTest(func() error { return injected })
						t.Cleanup(func() { session.SetEndTxErrorForTest(nil) })
					}
				}
				revisions := countP16RunRevisions(t, f.db, f.seed.runID)
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				out, err := f.group.Settle(f.ctx, f.members)
				requireB10Acknowledged(t, out, err, injected)
				if f.sink.reserves != 1 || f.sink.submits != 1 || f.sink.cancels != 0 || countP16RunRevisions(t, f.db, f.seed.runID) != revisions+1 {
					t.Fatalf("postcommit fault erased candidate/revision: %+v", f.sink)
				}
				counts := collector.Snapshot().ByOperation[transactiontest.PipelineSettlement]
				if counts.WriteCommits != 1 || counts.CommitFailures != 0 || counts.Revision.Finalizations != 1 {
					t.Fatalf("acknowledged COMMIT census: %+v", counts)
				}
				f.requireObservation(t, pipelineobligation.PublicationSettlementSatisfied)
				final := f.snapshot(t)
				if _, err := f.group.Settle(f.ctx, f.members); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("acknowledged group replay=%v", err)
				}
				f.requireSnapshot(t, final)
				if f.sink.submits != 1 {
					t.Fatal("duplicate candidate submission")
				}
			})
		}
	}
}

type b10CandidateSink struct {
	reserves, submits, cancels int
	reserveErr, submitErr      error
	runID                      string
}

func (s *b10CandidateSink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	s.reserves++
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	return s, nil
}
func (s *b10CandidateSink) Submit(c runlifecycle.Candidate) error {
	s.submits++
	if c.RunID != s.runID {
		return fmt.Errorf("foreign candidate run %s", c.RunID)
	}
	return s.submitErr
}
func (s *b10CandidateSink) Cancel() error { s.cancels++; return nil }

type b10GroupFaultFixture struct {
	ctx       context.Context
	raw       selectedFanOutLifecycleOwner
	db        *sql.DB
	connector *stopCommitConnector
	seed      fanOutOwnerFixture
	group     pipelineobligation.PublicationGroup
	members   []pipelineobligation.PublicationSettlementMember
	sink      *b10CandidateSink
	backend   string
}

func newB10GroupFaultFixture(t *testing.T, backend string) *b10GroupFaultFixture {
	t.Helper()
	raw, db, connector := newP16RaceStore(t, backend)
	seed, ctx, group, members := prepareP16CommittedGroup(t, raw, db, backend)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	t.Cleanup(cancel)
	f := &b10GroupFaultFixture{ctx: ctx, raw: raw, db: db, connector: connector, seed: seed, group: group, members: members, backend: backend, sink: &b10CandidateSink{runID: seed.runID}}
	// Seed pending route/delivery projections using existing selected-store
	// fixtures, before the tested mutation and its snapshot. Settlement itself
	// still runs through the canonical group/member kernel.
	dialect := authoractivityfixture.DialectSQLite
	if backend == "postgres" {
		dialect = authoractivityfixture.DialectPostgres
	}
	for _, member := range members {
		insertProducerIdentityDecisionObligation(t, authorActivityReceiptFixture{db: db, dialect: dialect}, ctx, member.Claim.EventID(), seed.runID, time.Now().UTC())
		var event events.Event
		if backend == "postgres" {
			event = loadPostgresDeliveryFixtureEvent(t, ctx, db, member.Claim.EventID())
		} else {
			event = loadSQLiteDeliveryFixtureEvent(t, ctx, db, member.Claim.EventID())
		}
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("b10-recipient")), Target: events.MustExistingEntityTarget(event.SourceRoute())}
		if err := commitDeliveryObligationFixture(ctx, raw.(deliveryFixtureStore), event, route); err != nil {
			t.Fatal(err)
		}
	}
	// Consume the setup candidate through the lifecycle owner. Pending work
	// leaves it awaiting mutation, so settlement must perform a real rearm.
	candidates := raw.(runlifecycle.CandidateStore)
	page, err := candidates.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: seed.bundleHash}, runlifecycle.CandidateCursor{}, 2)
	if err != nil || len(page.Candidates) != 1 || page.Candidates[0].RunID != seed.runID {
		t.Fatalf("setup completion candidate: page=%+v err=%v", page, err)
	}
	result, err := candidates.ExecuteCompletionCandidate(ctx, page.Candidates[0], runlifecycle.NewTerminalCatalog([]string{"completed"}, nil))
	if err != nil || result.Outcome != runlifecycle.OutcomeAwaitMutation {
		t.Fatalf("consume setup completion candidate: result=%+v err=%v", result, err)
	}
	var due bool
	if err := db.QueryRowContext(ctx, `SELECT completion_due_at IS NOT NULL FROM runs WHERE run_id=$1`, seed.runID).Scan(&due); err != nil || due {
		t.Fatalf("setup candidate must await real mutation: due=%v err=%v", due, err)
	}
	registration, err := raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: seed.bundleHash}, f.sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Release)
	return f
}

func (f *b10GroupFaultFixture) snapshot(t *testing.T) map[string][]string {
	t.Helper()
	return readP16PreservationSnapshot(t, f.db, "runs", "events", "event_deliveries", "fan_out_intents", "fan_out_outcomes", "decision_cards", "decision_card_route_obligations")
}
func (f *b10GroupFaultFixture) requireSnapshot(t *testing.T, before map[string][]string) {
	t.Helper()
	after := f.snapshot(t)
	if !reflect.DeepEqual(before, after) {
		for table, rows := range after {
			if !reflect.DeepEqual(rows, before[table]) {
				t.Errorf("changed durable table %s", table)
			}
		}
		t.Fatal("failed/observational settlement changed durable representations")
	}
}
func (f *b10GroupFaultFixture) requireObservation(t *testing.T, want pipelineobligation.PublicationSettlementState) {
	t.Helper()
	observed, err := f.group.ReadPublicationSettlement(f.ctx, f.members)
	if err != nil || len(observed.Rows) != 2 {
		t.Fatalf("observation=%+v err=%v", observed, err)
	}
	for i, row := range observed.Rows {
		if row.Claim != f.members[i].Claim || row.State != want {
			t.Fatalf("exact observation row=%+v want=%v", row, want)
		}
	}
}
func requireB10Acknowledged(t *testing.T, out pipelineobligation.PublicationGroupOutcome, err, wantErr error) {
	t.Helper()
	if (wantErr == nil && err != nil) || (wantErr != nil && !errors.Is(err, wantErr)) || len(out.Results) != 2 {
		t.Fatalf("acknowledged segment=%+v err=%v want=%v", out, err, wantErr)
	}
	for _, result := range out.Results {
		if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
			t.Fatalf("lost acknowledged member: %+v", result)
		}
	}
}
func requireB10NativeFault(t *testing.T, backend string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "b10_after_writes") {
		t.Fatalf("fault missed exact SQL boundary: %v", err)
	}
	if backend == "postgres" {
		var native *pq.Error
		if !errors.As(err, &native) || native.Code != "P0001" {
			t.Fatalf("lost native PG fault: %v", err)
		}
	} else {
		var native *sqlite.Error
		if !errors.As(err, &native) || native.Code() != 1811 {
			t.Fatalf("lost native SQLite fault: %v", err)
		}
	}
}

func (f *b10GroupFaultFixture) installFault(t *testing.T, fault string) func() {
	return f.installFaultAt(t, fault, 1)
}

func (f *b10GroupFaultFixture) installFaultAt(t *testing.T, fault string, member int) func() {
	t.Helper()
	if member < 0 || member >= len(f.members) {
		t.Fatalf("fault member %d outside group of %d", member, len(f.members))
	}
	name := "b10_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	first, second := f.members[0].Claim.EventID(), f.members[1].Claim.EventID()
	target := f.members[member].Claim.EventID()
	table, operation, condition := "event_receipts", "INSERT", fmt.Sprintf("NEW.event_id='%s' AND NEW.subscriber_type='platform' AND NEW.subscriber_id='pipeline'", target)
	prefix := fmt.Sprintf("EXISTS(SELECT 1 FROM event_receipts WHERE event_id='%s' AND subscriber_type='platform' AND subscriber_id='pipeline')", first)
	if member == 0 {
		// Prove failure precedes the second member instead of merely proving
		// that some statement in the settlement happened to fail.
		prefix += fmt.Sprintf(" AND NOT EXISTS(SELECT 1 FROM event_receipts WHERE event_id='%s' AND subscriber_type='platform' AND subscriber_id='pipeline')", second)
	}
	switch fault {
	case "route":
		table, operation, condition = "decision_card_route_obligations", "UPDATE", fmt.Sprintf("NEW.event_id='%s' AND NEW.status='completed'", target)
	case "delivery_handoff":
		table, operation, condition = "event_deliveries", "UPDATE", fmt.Sprintf("NEW.event_id='%s' AND NEW.continuation_handoff_at IS NOT NULL", target)
	case "candidate_write":
		table, operation, condition = "runs", "UPDATE", fmt.Sprintf("NEW.run_id='%s' AND NEW.completion_due_at IS NOT NULL", f.seed.runID)
	case "finalizer":
		table, operation, condition = "run_fork_revisions", "INSERT", fmt.Sprintf("NEW.run_id='%s'", f.seed.runID)
		prefix = fmt.Sprintf("(SELECT COUNT(*) FROM event_receipts WHERE event_id IN ('%s','%s') AND subscriber_type='platform' AND subscriber_id='pipeline')=2 AND (SELECT COUNT(*) FROM decision_card_route_obligations WHERE event_id IN ('%s','%s') AND status='completed')=2 AND (SELECT COUNT(*) FROM event_deliveries WHERE event_id IN ('%s','%s') AND continuation_handoff_at IS NOT NULL)=2", first, second, first, second, first, second)
	}
	query := fmt.Sprintf("CREATE TRIGGER %s AFTER %s ON %s WHEN %s BEGIN SELECT CASE WHEN %s THEN RAISE(ABORT,'b10_after_writes') ELSE RAISE(ABORT,'b10_missing_prefix') END; END", name, operation, table, condition, prefix)
	if f.backend == "postgres" {
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'b10_after_writes'; ELSE RAISE EXCEPTION 'b10_missing_prefix'; END IF; END $$", name, prefix)); err != nil {
			t.Fatal(err)
		}
		query = fmt.Sprintf("CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION %s()", name, operation, table, condition, name)
	}
	if _, err := f.db.ExecContext(f.ctx, query); err != nil {
		t.Fatal(err)
	}
	removed := false
	remove := func() {
		if removed {
			return
		}
		removed = true
		query := "DROP TRIGGER " + name
		if f.backend == "postgres" {
			query += " ON " + table
		}
		if _, err := f.db.Exec(query); err != nil {
			t.Error(err)
		}
		if f.backend == "postgres" {
			if _, err := f.db.Exec("DROP FUNCTION " + name + "()"); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(remove)
	return remove
}
