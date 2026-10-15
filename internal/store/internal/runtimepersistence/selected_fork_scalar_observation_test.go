package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedForkScalarObservationsKeepPhysicalRunAndNameScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			sibling := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			duplicate := eventtest.ExistingRunRootIngress(uuid.NewString(), event.Type(), "fixture", "", event.Payload(), 0, event.RunID(), event.Envelope(), event.CreatedAt())
			if err := commitSemanticEventFixture(ctx, fixture.store, duplicate); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if status, err := ReadRunStatusStorageForTest(ctx, fixture.store, event.RunID()); err != nil || status != "running" {
				t.Fatalf("physical status observation changed: %q %v", status, err)
			}
			for _, tc := range []struct {
				run, name string
				want      int
			}{
				{event.RunID(), "snapshot.fixture", 2}, {sibling.RunID(), "snapshot.fixture", 1},
				{uuid.NewString(), "snapshot.fixture", 0}, {event.RunID(), "", 0},
				{event.RunID(), "snapshot.fixture ", 0}, {event.RunID(), "SNAPSHOT.FIXTURE", 0},
			} {
				if got, err := CountRunNamedEventStorageForTest(ctx, fixture.store, tc.run, tc.name); err != nil || got != tc.want {
					t.Fatalf("physical event count %s/%q=%d/%v, want %d", tc.run, tc.name, got, err, tc.want)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 7 || counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("scalar observations bypassed original selected read owner: %+v", counts)
			}
			if status, err := ReadRunStatusStorageForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || status != "" {
				t.Fatalf("absent run manufactured status evidence: %q %v", status, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if status, err := ReadRunStatusStorageForTest(cancelled, fixture.store, event.RunID()); !errors.Is(err, context.Canceled) || status != "" {
				t.Fatalf("cancelled status read yielded evidence: %q %v", status, err)
			}
			if count, err := CountRunNamedEventStorageForTest(cancelled, fixture.store, event.RunID(), "snapshot.fixture"); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("cancelled count read yielded evidence: %d %v", count, err)
			}
			if status, err := ReadRunStatusStorageForTest(ctx, struct{}{}, event.RunID()); err == nil || status != "" {
				t.Fatalf("foreign owner yielded status evidence: %q %v", status, err)
			}
			if count, err := CountRunNamedEventStorageForTest(ctx, fixture.store, "invalid", "snapshot.fixture"); err == nil || count != 0 {
				t.Fatalf("invalid run yielded cardinality evidence: %d %v", count, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if status, err := ReadRunStatusStorageForTest(ctx, fixture.store, event.RunID()); err == nil || status != "" {
				t.Fatalf("closed owner yielded status evidence: %q %v", status, err)
			}
			if count, err := CountRunNamedEventStorageForTest(ctx, fixture.store, event.RunID(), "snapshot.fixture"); err == nil || count != 0 {
				t.Fatalf("closed owner yielded cardinality evidence: %d %v", count, err)
			}
		})
	}
}
