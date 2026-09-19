package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestPipelineParentRefusesExactGroupAcquisitionBeforeSQLBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			before := f.snapshot(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			t.Cleanup(unblock)
			block := func() { once.Do(func() { close(entered); <-release }) }
			if f.postgres {
				registry := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimsForTest()
				// The real lease has acquired the batch's SQL parent fence, but
				// its token has not yet been installed under registry.mu.
				registry.SetHooksForTest(nil, nil, func(*postgresbackend.AdvisoryLockLease) { block() })
				t.Cleanup(func() { registry.SetHooksForTest(nil, nil, nil) })
			}
			requests := make([]pipelineobligation.PublicationClaimRequest, len(f.events))
			for i, event := range f.events {
				requests[i] = pipelineobligation.PublicationClaimRequest{Ordinal: i, Event: event}
			}
			done := make(chan error, 1)
			go func() {
				claims, err := f.group.ClaimBatch(f.ctx, requests)
				if err == nil {
					f.claims = claims
					// SQLite has no split acquiring state: mutationMu and
					// pipelineClaimMu publish the token atomically. Hold the
					// acquired batch, not a fabricated intermediate registry.
					if !f.postgres {
						block()
					}
				}
				done <- err
			}()
			awaitGroupProof(t, entered)
			eventID := f.events[0].ID()
			if f.events[1].ID() < eventID {
				eventID = f.events[1].ID()
			}
			if f.postgres {
				registry := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimsForTest()
				if !registry.AcquiringForTest(eventID) || registry.ContainsEventForTest(eventID) {
					t.Fatal("barrier missed exact reserved-but-uninstalled claim")
				}
			}
			ctx, cancel := context.WithTimeout(f.ctx, time.Second)
			defer cancel()
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var queries atomic.Int32
			f.probe.set(func(phase, _ string) error {
				if phase == "before_query" || phase == "before_exec" {
					queries.Add(1)
				}
				return nil
			})
			err = terminalizeAcquisitionEvent(ctx, f, tx, "  "+eventID+"  ")
			f.probe.set(nil)
			failure, typed := failures.EnvelopeFromError(err)
			wantOwner := "local"
			if f.postgres {
				wantOwner = "acquiring"
			}
			if !errors.Is(err, pipelineobligation.ErrBusy) || !typed || failure.Detail.Code != "pipeline_parent_claim_busy" || failure.Detail.Attributes["stage"] != "pipeline_claim" || failure.Detail.Attributes["event_id"] != eventID || failure.Detail.Attributes["claim_owner"] != wantOwner {
				t.Fatalf("parent lost exact acquisition conflict: err=%v failure=%+v", err, failure)
			}
			if queries.Load() != 0 || ctx.Err() != nil {
				t.Fatalf("parent waited on SQL instead of refusing admission: queries=%d context=%v", queries.Load(), ctx.Err())
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			// A different event in the same run is not covered by this exact
			// acquiring key. Exercise its real SQL, then roll back the probe.
			foreign, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			foreignErr := terminalizeAcquisitionEvent(ctx, f, foreign, f.seed.eventID)
			rollbackErr := foreign.Rollback()
			if foreignErr != nil || rollbackErr != nil {
				t.Fatalf("unrelated event was blocked: operation=%v rollback=%v", foreignErr, rollbackErr)
			}
			unblock()
			if err := receiveGroupProof(t, done); err != nil {
				t.Fatal(err)
			}
			if len(f.claims) != 2 {
				t.Fatalf("batch did not complete token installation: %v", f.claims)
			}
			if !reflect.DeepEqual(before, f.snapshot(t)) {
				t.Fatal("refusal or rolled-back unrelated probe changed durable rows")
			}
			if err := f.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.postgres {
				registry := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimsForTest()
				if registry.AcquiringForTest(eventID) || registry.ContainsEventForTest(eventID) {
					t.Fatal("closed batch leaked acquiring/installed token")
				}
			}
		})
	}
}

func terminalizeAcquisitionEvent(ctx context.Context, f *groupProofFixture, tx *sql.Tx, eventID string) error {
	effects := runforkrevision.NewEffects()
	disposition := pipelineobligation.DeadLetter("run_stopped", nil)
	if f.postgres {
		return f.raw.(*PostgresStore).pipelinePostgresOwner.TerminalizePipelineObligationTx(ctx, tx, effects, eventID, disposition, time.Now().UTC())
	}
	return f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.TerminalizePipelineObligationTx(ctx, tx, effects, eventID, disposition, time.Now().UTC())
}
