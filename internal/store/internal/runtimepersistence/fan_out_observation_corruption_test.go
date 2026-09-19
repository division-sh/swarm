package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestFanOutGrantedCorruptHeaderNeverObservesEligibleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, corruption := range []struct{ name, update string }{
			{"orphan_lease", `claim_owner=NULL,lease_expires_at=$2`},
			{"zero_generation", `claim_owner='orphan',claim_generation=0,lease_expires_at=$2`},
			{"invalid_retry_failure", `retry_ready_at=$2,retry_failure='{}'`},
			{"open_consumed_cursor", `cursor=cardinality`},
			{"invalid_capsule", `capsule='{}'`},
		} {
			t.Run(backend+"/"+corruption.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
				defer cancel()
				selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 4, time.Now().UTC())
				if err := acknowledgePipelineEventFixture(ctx, selected, fixture.eventID); err != nil {
					t.Fatal(err)
				}
				process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
				first := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
				workers := 1
				registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, first)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registration.Close)
				var turn capturedFanOutTurn
				select {
				case turn = <-first.turns:
				case err := <-first.errors:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				registration.Close()
				<-first.done
				args := []any{fixture.runID}
				if corruption.name == "orphan_lease" || corruption.name == "zero_generation" || corruption.name == "invalid_retry_failure" {
					args = append(args, time.Now().UTC().Add(-time.Minute))
				}
				update := `UPDATE fan_out_intents SET ` + corruption.update + ` WHERE run_id=$1`
				_, updateErr := db.ExecContext(ctx, update, args...)
				constrained := corruption.name == "orphan_lease" || corruption.name == "zero_generation" || corruption.name == "open_consumed_cursor"
				if constrained {
					if updateErr == nil {
						t.Fatal("DDL accepted a structurally corrupt fan-out header")
					}
					// Inject persisted corruption past the DDL only in this isolated
					// store, so readers still have to prove canonical header validity.
					if postgres {
						constraint := "fan_out_intents_check2"
						if corruption.name == "open_consumed_cursor" {
							constraint = "fan_out_intents_check1"
						}
						if _, err := db.ExecContext(ctx, `ALTER TABLE fan_out_intents DROP CONSTRAINT `+constraint); err != nil {
							t.Fatal(err)
						}
						_, updateErr = db.ExecContext(ctx, update, args...)
					} else {
						conn, err := db.Conn(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=ON`); err == nil {
							_, updateErr = conn.ExecContext(ctx, update, args...)
							_, err = conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=OFF`)
						}
						closeErr := conn.Close()
						if err != nil || closeErr != nil {
							t.Fatalf("corruption fixture connection: %v %v", err, closeErr)
						}
					}
				}
				if updateErr != nil {
					t.Fatal(updateErr)
				}
				if _, _, found, err := turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "corruption-control", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute}); err == nil || found {
					t.Errorf("canonical claim accepted corrupt header: found=%v err=%v", found, err)
				}
				obligations := selected.(interface {
					PipelineObligations() pipelineobligation.Store
				}).PipelineObligations()
				if presence, err := obligations.GlobalWorkPresence(ctx); err == nil || presence.ProcessingEligible {
					t.Errorf("corrupt global readiness: %+v err=%v", presence, err)
				}
				next := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
				nextRegistration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, next)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(nextRegistration.Close)
				page := fanoutobligation.ListPage{RunID: fixture.runID, Intents: []fanoutobligation.IntentReadback{{Key: turn.key, BundleHash: fixture.bundleHash}}}
				if read, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page); err == nil {
					t.Errorf("corrupt batch header returned a runtime projection: %+v", read.Intents[0].Runtime)
				}
				select {
				case turn := <-next.turns:
					t.Errorf("selector/D3 owner observed corrupt eligible opportunity: %+v", turn.key)
				case <-next.errors:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var outcomes int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID).Scan(&outcomes); err != nil || outcomes != 0 {
					t.Fatalf("corrupt observation published outcomes=%d err=%v", outcomes, err)
				}
			})
		}
	}
}
