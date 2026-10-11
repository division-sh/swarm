package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReplayEventTargetRouteFaultKeepsExactNativeCutsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := CorruptReplayEventTargetRouteForTest(ctx, fixture.store, event.ID()); err != nil {
				t.Fatal(err)
			}
			corrupt, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			for table, original := range before {
				if table != "events" && !reflect.DeepEqual(original, corrupt[table]) {
					t.Fatalf("route fault changed unrelated table %s", table)
				}
			}
			originalRows, err := DecodeSelectedForkSnapshotRowsForTest(before["events"])
			if err != nil || len(originalRows) != 1 {
				t.Fatalf("original event rows=%d: %v", len(originalRows), err)
			}
			corruptRows, err := DecodeSelectedForkSnapshotRowsForTest(corrupt["events"])
			if err != nil || len(corruptRows) != 1 {
				t.Fatalf("corrupt event rows=%d: %v", len(corruptRows), err)
			}
			for column, original := range originalRows[0] {
				if column == "target_route" {
					if text, err := corruptRows[0][column].Text(); err != nil || text != `"bad"` {
						t.Fatalf("malformed route payload changed: %q/%v", text, err)
					}
				} else if !reflect.DeepEqual(original, corruptRows[0][column]) {
					t.Fatalf("route fault changed event column %s", column)
				}
			}
			if _, _, err := ReadCanonicalEventRecordForTest(ctx, fixture.store, event.ID()); err == nil || !strings.Contains(err.Error(), "target_route") {
				t.Fatalf("canonical reader accepted malformed route: %v", err)
			}
			if err := RestoreReplayEventTargetRouteForTest(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("restoration changed other storage or clocks: %v", err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 2 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("route fault escaped original unrevisioned writer: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := CorruptReplayEventTargetRouteForTest(cancelled, fixture.store, event.ID()); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled route fault did not refuse: %v", err)
			}
			for _, selected := range []any{fixture.store, struct{}{}} {
				if err := CorruptReplayEventTargetRouteForTest(ctx, selected, uuid.NewString()); err == nil {
					t.Fatal("absent/foreign fault accepted")
				}
			}
			if err := CorruptReplayEventTargetRouteForTest(ctx, fixture.store, "invalid"); err == nil {
				t.Fatal("invalid event fault accepted")
			}
			after, err = ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused route fault changed storage: %v", err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := RestoreReplayEventTargetRouteForTest(ctx, fixture.store, event); err == nil {
				t.Fatal("closed route restoration accepted")
			}
		})
	}
}
