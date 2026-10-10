package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSourceArtifactHashStoragePreservesExactRunBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.storage", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if hash, err := ReadSelectedForkRunBundleHashForTest(ctx, fixture.store, event.RunID()); err != nil || hash != authorActivityTestBundleHash {
				t.Fatalf("exact stored hash=%q,err=%v", hash, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("source read escaped original owner: %+v", counts)
			}
			if hash, err := ReadSelectedForkRunBundleHashForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || hash != "" {
				t.Fatalf("foreign run borrowed source hash=%q,err=%v", hash, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if hash, err := ReadSelectedForkRunBundleHashForTest(cancelled, fixture.store, event.RunID()); !errors.Is(err, context.Canceled) || hash != "" {
				t.Fatalf("cancelled hash=%q,err=%v", hash, err)
			}
			if hash, err := ReadSelectedForkRunBundleHashForTest(ctx, fixture.store, "invalid"); err == nil || hash != "" {
				t.Fatalf("invalid hash=%q,err=%v", hash, err)
			}
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
			if hash, err := ReadSelectedForkRunBundleHashForTest(ctx, fixture.store, event.RunID()); err == nil || hash != "" {
				t.Fatalf("closed hash=%q,err=%v", hash, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if hash, err := ReadSelectedForkRunBundleHashForTest(context.Background(), invalid, uuid.NewString()); err == nil || hash != "" {
			t.Fatalf("foreign owner returned hash=%q,err=%v", hash, err)
		}
	}
}
