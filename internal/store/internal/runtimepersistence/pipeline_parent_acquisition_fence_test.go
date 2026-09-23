package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
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
			var queries atomic.Int32
			f.probe.set(func(phase, _ string) error {
				if phase == "before_query" || phase == "before_exec" {
					queries.Add(1)
				}
				return nil
			})
			err := terminalizeAcquisitionEvent(ctx, f, "  "+eventID+"  ", false)
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
			// A different event in the same run is not covered by this exact
			// acquiring key. Exercise its real SQL, then roll back the probe.
			foreignErr := terminalizeAcquisitionEvent(ctx, f, f.seed.eventID, true)
			if !errors.Is(foreignErr, errAcquisitionProbeRollback) {
				t.Fatalf("unrelated event was blocked: operation=%v", foreignErr)
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

var errAcquisitionProbeRollback = errors.New("rollback acquisition probe")

func terminalizeAcquisitionEvent(ctx context.Context, f *groupProofFixture, eventID string, rollback bool) error {
	disposition := pipelineobligation.DeadLetter("run_stopped", nil)
	write := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		var err error
		if f.postgres {
			err = f.raw.(*PostgresStore).pipelinePostgresOwner.TerminalizePipelineObligationTx(txctx, attempt, eventID, disposition, time.Now().UTC())
		} else {
			err = f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.TerminalizePipelineObligationTx(txctx, attempt, eventID, disposition, time.Now().UTC())
		}
		if err == nil && rollback {
			err = errAcquisitionProbeRollback
		}
		return struct{}{}, err
	}
	if f.postgres {
		store := f.raw.(*PostgresStore)
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, write).Err()
	}
	store := f.raw.(*SQLiteRuntimeStore)
	return mutationprotocol.RunSQLite(ctx, store.backend, "acquisition fence probe", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, write).Err()
}
