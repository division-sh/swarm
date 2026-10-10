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

func TestLatestNamedEventStoragePreservesGlobalOrderAndExactOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
			at := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
			for i, name := range []string{"latest.match", "latest.match", "latest.foreign"} {
				event := eventtest.RunCreatingRootIngress(ids[i], events.EventType(name), "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at.Add(time.Duration(i)*time.Second))
				if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if id, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, "latest.match", ""); err != nil || id != ids[1] {
				t.Fatalf("global latest matching id=%q,err=%v,want%s", id, err, ids[1])
			}
			if id, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, "latest.match", ids[1]); err != nil || id != ids[0] {
				t.Fatalf("explicit exclusion id=%q,err=%v,want%s", id, err, ids[0])
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("latest lookup escaped original owner: %+v", counts)
			}
			if id, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, "missing", ""); !errors.Is(err, sql.ErrNoRows) || id != "" {
				t.Fatalf("missing name borrowed identity: %q,%v", id, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if id, err := ReadLatestNamedEventIdentityStorageForTest(cancelled, fixture.store, "latest.match", ""); !errors.Is(err, context.Canceled) || id != "" {
				t.Fatalf("cancelled latest returned identity: %q,%v", id, err)
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
			if id, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, "latest.match", ""); err == nil || id != "" {
				t.Fatalf("closed latest returned identity: %q,%v", id, err)
			}
		})
	}
}
