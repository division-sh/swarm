package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
)

// Parent stop can beat a prepared publication, but cannot steal a durable
// member's live claim. Exercise that distinction with the actual group owner.
func TestB16ParentStopVersusPublicationGroupBothCommitOrders(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"stop", "group_publication"} {
			t.Run(backend+"/"+winner, func(t *testing.T) {
				raw, db, connector := newP16RaceStore(t, backend)
				fixture, baseCtx, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, backend, 34)
				ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
				defer cancel()
				before := readStopCommitEvidence(t, db, fixture.runID)
				stop := func() error {
					_, err := raw.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
					return err
				}
				publish := func() error {
					_, err := owner.CommitFanOutChunk(ctx, command)
					return err
				}
				first, second := stop, publish
				if winner == "group_publication" {
					first, second = publish, stop
				}
				firstErr, secondErr := raceB16AtCommit(t, ctx, backend, connector, first, second)
				if firstErr != nil {
					t.Fatalf("%s winner: %v", winner, firstErr)
				}
				wantCursor, wantEvents, wantRevisions := 0, before.events, before.revisions+1
				if winner == "stop" {
					if !errors.Is(secondErr, fanoutobligation.ErrStaleClaim) {
						t.Fatalf("parent winner did not fence group publication: %v", secondErr)
					}
					if err := group.ValidateCommitted(ctx, b16Claims(members)); err == nil {
						t.Fatal("rolled-back group acquired committed publication authority")
					}
					if _, err := group.Settle(ctx, members); err == nil {
						t.Fatal("uncommitted group acquired settlement authority")
					}
				} else {
					requireB16GroupBusy(t, secondErr, members)
					if err := group.ValidateCommitted(ctx, b16Claims(members)); err != nil {
						t.Fatal(err)
					}
					published := readStopCommitEvidence(t, db, fixture.runID)
					if published.status != before.status || published.control != before.control || published.canceled != 0 || published.revisions != before.revisions+1 {
						t.Fatalf("refused stop partly changed parent: before=%+v after=%+v", before, published)
					}
					unchanged := readB16Snapshot(t, db)
					requireB16GroupBusy(t, stop(), members)
					requireB16Snapshot(t, db, unchanged)
					settled, err := group.Settle(ctx, members)
					if err != nil || len(settled.Results) != 2 {
						t.Fatalf("settle winning group: %+v err=%v", settled, err)
					}
					for _, result := range settled.Results {
						if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
							t.Fatalf("group lost acknowledged mandatory handoff: %+v", result)
						}
					}
					if err := stop(); err != nil {
						t.Fatalf("stop after finite group handoff: %v", err)
					}
					wantCursor, wantEvents, wantRevisions = 32, before.events+2, before.revisions+3
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, wantCursor, wantCursor)
				after := readStopCommitEvidence(t, db, fixture.runID)
				if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 || after.events != wantEvents || after.revisions != wantRevisions {
					t.Fatalf("group/stop accounting: before=%+v after=%+v expected events=%d revisions=%d", before, after, wantEvents, wantRevisions)
				}
				for _, member := range members {
					if winner == "group_publication" {
						assertDirectiveReceipt(t, db, member.Claim.EventID(), "processed", nil)
					} else {
						var count int
						if err := db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1`, member.Claim.EventID()).Scan(&count); err != nil || count != 0 {
							t.Fatalf("unpublished group fabricated receipt: count=%d err=%v", count, err)
						}
					}
				}
				final := readB16Snapshot(t, db)
				if _, err := owner.LoadFanOutEvaluation(ctx, command.Claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("stopped parent retained fan-out evaluation authority: %v", err)
				}
				if err := group.Close(ctx); err != nil {
					t.Fatal(err)
				}
				// Close removes group admission before the chunk owner can inspect
				// the stale intent claim. Neither boundary may authorize a write.
				if _, err := owner.CommitFanOutChunk(ctx, command); err == nil || err.Error() != "granted fan-out publication requires its sealed publication group" {
					t.Fatalf("late publication bypassed closed group admission: %v", err)
				}
				if _, err := group.Settle(ctx, members); err == nil {
					t.Fatal("closed group retained settlement authority")
				}
				if err := stop(); !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
					t.Fatalf("duplicate stop=%v", err)
				}
				requireB16Snapshot(t, db, final)
			})
		}
	}
}

// Once successful receipts commit, stop must preserve them even if it was
// already contending for the run lock before the segment's COMMIT returned.
func TestB16ParentStopContendsWithGroupSettlementCommit(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			raw, db, connector := newP16RaceStore(t, backend)
			fixture, baseCtx, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, backend, 34)
			ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
			defer cancel()
			if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
				t.Fatal(err)
			}
			if err := group.ValidateCommitted(ctx, b16Claims(members)); err != nil {
				t.Fatal(err)
			}
			before := readStopCommitEvidence(t, db, fixture.runID)
			var outcome pipelineobligation.PublicationGroupOutcome
			settle := func() error {
				var err error
				outcome, err = group.Settle(ctx, members)
				return err
			}
			stop := func() error {
				_, err := raw.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
				return err
			}
			settleErr, stopErr := raceB16AtCommit(t, ctx, backend, connector, settle, stop)
			if settleErr != nil || stopErr != nil || len(outcome.Results) != 2 {
				t.Fatalf("settlement then parent: outcome=%+v settlement=%v stop=%v", outcome, settleErr, stopErr)
			}
			for _, result := range outcome.Results {
				if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
					t.Fatalf("parent stole acknowledged handoff: %+v", result)
				}
				assertDirectiveReceipt(t, db, result.Claim.EventID(), "processed", nil)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 32, 32)
			after := readStopCommitEvidence(t, db, fixture.runID)
			if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 || after.events != before.events || after.revisions != before.revisions+2 {
				t.Fatalf("segment and parent need exactly two revisions without rewriting prefix: before=%+v after=%+v", before, after)
			}
			final := readB16Snapshot(t, db)
			if _, err := group.Settle(ctx, members); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
				t.Fatalf("consumed group retained authority: %v", err)
			}
			if err := stop(); !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
				t.Fatalf("duplicate stop=%v", err)
			}
			requireB16Snapshot(t, db, final)
		})
	}
}

func raceB16AtCommit(t *testing.T, ctx context.Context, backend string, connector *stopCommitConnector, first, second func() error) (error, error) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	connector.arm(func(tx driver.Tx) error {
		close(entered)
		select {
		case <-release:
			return tx.Commit()
		case <-ctx.Done():
			return errors.Join(ctx.Err(), tx.Rollback())
		}
	})
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- first() }()
	select {
	case <-entered:
	case err := <-firstDone:
		t.Fatalf("winner failed before native COMMIT: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	started := make(chan struct{})
	if backend == "postgres" {
		connector.mu.Lock()
		connector.begin = started
		connector.mu.Unlock()
	}
	go func() {
		// Do not bypass SQLite's canonical writer admission before BEGIN.
		if backend == "sqlite" {
			close(started)
		}
		secondDone <- second()
	}()
	select {
	case <-started:
	case err := <-secondDone:
		t.Fatalf("contender failed before admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-secondDone:
		t.Fatalf("contender escaped uncommitted winner fence: %v", err)
	default:
	}
	unblock()
	return <-firstDone, <-secondDone
}

func b16Claims(members []pipelineobligation.PublicationSettlementMember) []pipelineobligation.Claim {
	claims := make([]pipelineobligation.Claim, len(members))
	for i := range members {
		claims[i] = members[i].Claim
	}
	return claims
}

func requireB16GroupBusy(t *testing.T, err error, members []pipelineobligation.PublicationSettlementMember) {
	t.Helper()
	failure, typed := failures.EnvelopeFromError(err)
	if !errors.Is(err, pipelineobligation.ErrBusy) || !typed || failure.Detail.Code != "pipeline_parent_claim_busy" || failure.Detail.Attributes["stage"] != "pipeline_claim" || failure.Detail.Attributes["purpose"] != string(pipelineobligation.PurposePublication) {
		t.Fatalf("stop did not preserve typed group claim fence: err=%v failure=%+v", err, failure)
	}
	for _, member := range members {
		if failure.Detail.Attributes["event_id"] == member.Claim.EventID() {
			return
		}
	}
	t.Fatalf("busy failure names an unrelated event: %+v", failure)
}

func readB16Snapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	return readP16PreservationSnapshot(t, db, "runs", "run_control_state", "fan_out_intents", "fan_out_outcomes", "events", "event_deliveries")
}

func requireB16Snapshot(t *testing.T, db *sql.DB, before map[string][]string) {
	t.Helper()
	after := readB16Snapshot(t, db)
	if !reflect.DeepEqual(before, after) {
		for table, rows := range after {
			if !reflect.DeepEqual(rows, before[table]) {
				t.Errorf("refused mutation changed table %s", table)
			}
		}
		t.Fatal("refused mutation changed parent/group/history evidence")
	}
}
