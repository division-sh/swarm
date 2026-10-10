package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestStandaloneStoragePreservesJoinedRunDeliveryAndCandidateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "standalone.storage", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{"worker"}); err != nil {
				t.Fatal(err)
			}
			candidates := fixture.store.(runtimerunlifecycle.OperationOwner)
			due := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC().Add(time.Hour))
			if _, err := candidates.RequestCompletionCandidate(ctx, runtimerunlifecycle.CandidateAtTime(event.RunID(), due)); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			run, err := ReadStandaloneRunStorageForTest(ctx, fixture.store, event.ID())
			if err != nil || run.RunID != event.RunID() || run.Status != "running" || run.TriggerEventType != string(event.Type()) {
				t.Fatalf("joined run=%+v, err=%v", run, err)
			}
			delivery, err := ReadStandaloneAgentDeliveryStorageForTest(ctx, fixture.store, event.ID(), "worker")
			if err != nil || delivery.Status != "pending" || delivery.RunStatus != "running" {
				t.Fatalf("joined delivery=%+v, err=%v", delivery, err)
			}
			candidate, err := ReadStandaloneCompletionCandidateForTest(ctx, fixture.store, event.ID())
			if err != nil || candidate.RunID != event.RunID() || candidate.BundleHash != authorActivityTestBundleHash || candidate.Revision <= 0 || !candidate.DueAt.Equal(due) {
				t.Fatalf("stored candidate=%+v, err=%v; due=%v", candidate, err, due)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("joined witnesses escaped original read transactions: %+v", counts)
			}
			if out, err := ReadStandaloneAgentDeliveryStorageForTest(ctx, fixture.store, event.ID(), "other-worker"); !errors.Is(err, sql.ErrNoRows) || out != (StandaloneAgentDeliveryStorage{}) {
				t.Fatalf("wrong subscriber borrowed delivery: %+v, %v", out, err)
			}
			missing := uuid.NewString()
			if out, err := ReadStandaloneRunStorageForTest(ctx, fixture.store, missing); !errors.Is(err, sql.ErrNoRows) || out != (StandaloneRunStorage{}) {
				t.Fatalf("missing event borrowed run: %+v, %v", out, err)
			}
			if out, err := ReadStandaloneCompletionCandidateForTest(ctx, fixture.store, missing); !errors.Is(err, sql.ErrNoRows) || out != (runtimerunlifecycle.Candidate{}) {
				t.Fatalf("missing event borrowed candidate: %+v, %v", out, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertStandaloneStorageRefused(t, cancelled, fixture.store, event.ID())
			assertStandaloneStorageRefused(t, ctx, fixture.store, "invalid")
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			assertStandaloneStorageRefused(t, ctx, fixture.store, event.ID())
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		assertStandaloneStorageRefused(t, context.Background(), invalid, uuid.NewString())
	}
}

func assertStandaloneStorageRefused(t *testing.T, ctx context.Context, selected any, eventID string) {
	t.Helper()
	if out, err := ReadStandaloneRunStorageForTest(ctx, selected, eventID); err == nil || out != (StandaloneRunStorage{}) {
		t.Fatalf("refused run returned evidence: %+v, %v", out, err)
	}
	if out, err := ReadStandaloneAgentDeliveryStorageForTest(ctx, selected, eventID, "worker"); err == nil || out != (StandaloneAgentDeliveryStorage{}) {
		t.Fatalf("refused delivery returned evidence: %+v, %v", out, err)
	}
	if out, err := ReadStandaloneCompletionCandidateForTest(ctx, selected, eventID); err == nil || out != (runtimerunlifecycle.Candidate{}) {
		t.Fatalf("refused candidate returned evidence: %+v, %v", out, err)
	}
}
