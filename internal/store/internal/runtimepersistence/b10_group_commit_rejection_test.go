package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

func TestB10GroupNativeCommitRejectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			statements := []string{
				`CREATE TABLE b10_commit_parent (id INTEGER PRIMARY KEY)`,
				`CREATE TABLE b10_commit_child (id INTEGER REFERENCES b10_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
			}
			// This native deferred constraint permits every domain write and the
			// finalizer, but rejects the physical COMMIT, not its acknowledgement.
			condition := fmt.Sprintf("NEW.event_id='%s' AND NEW.subscriber_type='platform' AND NEW.subscriber_id='pipeline'", f.members[1].Claim.EventID())
			if backend == "postgres" {
				statements = append(statements,
					`CREATE FUNCTION b10_commit_violation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO b10_commit_child(id) VALUES (1); RETURN NEW; END $$`,
					`CREATE TRIGGER b10_commit_violation AFTER INSERT ON event_receipts FOR EACH ROW WHEN (`+condition+`) EXECUTE FUNCTION b10_commit_violation()`)
			} else {
				statements = append(statements, `CREATE TRIGGER b10_commit_violation AFTER INSERT ON event_receipts WHEN `+condition+` BEGIN INSERT INTO b10_commit_child(id) VALUES (1); END`)
			}
			for _, statement := range statements {
				if _, err := f.db.ExecContext(f.ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			var commitErr error
			calls := 0
			f.connector.arm(func(tx driver.Tx) error {
				calls++
				commitErr = tx.Commit()
				return commitErr
			})
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			out, err := f.group.Settle(f.ctx, f.members)
			if calls != 1 || commitErr == nil || !errors.Is(err, commitErr) || len(out.Results) != 0 {
				t.Fatalf("native commit rejection: calls=%d commit=%v err=%v results=%+v", calls, commitErr, err, out)
			}
			if backend == "postgres" {
				var native *pq.Error
				if !errors.As(err, &native) || native.Code != "23503" {
					t.Fatalf("lost native deferred-constraint failure: %v", err)
				}
			} else {
				var native *sqlite.Error
				if !errors.As(err, &native) || native.Code() != 787 {
					t.Fatalf("lost native deferred-constraint failure: %v", err)
				}
			}
			counts := collector.Snapshot()
			settlement := counts.ByOperation[transactiontest.PipelineSettlement]
			if settlement.CommitAttempts != 1 || settlement.CommitFailures != 1 || settlement.Revision.Finalizations != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("failed native commit did not retire its exact transaction: %+v", counts)
			}
			if f.sink.reserves != 1 || f.sink.cancels != 1 || f.sink.submits != 0 {
				t.Fatalf("rejected commit leaked candidate execution: %+v", f.sink)
			}
			f.requireSnapshot(t, before)
			f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
			var children int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM b10_commit_child`).Scan(&children); err != nil || children != 0 {
				t.Fatalf("failed transaction remained visible: children=%d err=%v", children, err)
			}
			f.requireSnapshot(t, before)
			if calls != 1 || f.sink.submits != 0 {
				t.Fatal("observation retried a rejected commit or submitted its candidate")
			}
		})
	}
}
