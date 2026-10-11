package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReplayScopeHistoryPreservesExactClocksAndOriginalCutsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			at := time.Unix(1700002315, 0).UTC()
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := StampHistoricalReplayScopeForTest(ctx, selected, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, at); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("marker clock refresh escaped the original unrevisioned cut: %+v", counts)
			}
			assertHistoricalReplayScope(t, ctx, selected, root.RunID(), root.ID(), at)
			if err := DeleteCommittedReplayScopeForTest(ctx, selected, root.ID()); err != nil {
				t.Fatal(err)
			}
			if err := StampHistoricalReplayScopeForTest(ctx, selected, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, at); err != nil {
				t.Fatal(err)
			}
			assertHistoricalReplayScope(t, ctx, selected, root.RunID(), root.ID(), at)
			before := probe.Snapshot()
			eventID, postAt := uuid.NewString(), at.Add(time.Second)
			event, err := SeedPostForkSourceEventForTest(ctx, selected, root.RunID(), eventID, root.RunID(), postAt)
			if err != nil {
				t.Fatal(err)
			}
			assertPostForkSourceEvent(t, event, root.RunID(), eventID, postAt)
			if counts := probe.Snapshot(); counts.Total.WriteCommits != before.Total.WriteCommits+1 || counts.Total.Revision.Finalizations != before.Total.Revision.Finalizations+1 || counts.Active != 0 {
				t.Fatalf("post-fork event lost its original canonical event revision cut: %+v", counts)
			}
			if _, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, eventID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("event seed silently stamped a marker before the original cut: %v", err)
			}
			stored, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
			if err != nil || !found {
				t.Fatalf("post-fork event was not durably stored: %t/%v", found, err)
			}
			assertPostForkSourceEvent(t, stored, root.RunID(), eventID, postAt)
			if err := StampHistoricalReplayScopeForTest(ctx, selected, root.RunID(), eventID, pipelineobligation.ScopeDirect, postAt); err != nil {
				t.Fatal(err)
			}
			assertHistoricalReplayScope(t, ctx, selected, root.RunID(), eventID, postAt)
			before = probe.Snapshot()
			copied, err := CountCopiedSourceReplayScopeStorageForTest(ctx, selected, root.RunID(), root.ID())
			if err != nil || copied != 1 {
				t.Fatalf("exact source marker predicate=%d/%v, want one", copied, err)
			}
			local, err := CountForkLocalReplayScopeStorageForTest(ctx, selected, root.RunID(), root.ID())
			if err != nil || local != 1 {
				t.Fatalf("event inequality marker predicate=%d/%v, want one", local, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != before.Total.ReadCommits+2 || counts.Total.WriteCommits != before.Total.WriteCommits || counts.Active != 0 {
				t.Fatalf("the two original scalar observations were merged or became writes: %+v", counts)
			}
			for _, read := range []func(context.Context, any, string, string) (int, error){CountCopiedSourceReplayScopeStorageForTest, CountForkLocalReplayScopeStorageForTest} {
				if count, err := read(ctx, selected, uuid.NewString(), root.ID()); err != nil || count != 0 {
					t.Fatalf("physical marker read inferred foreign-run membership: %d/%v", count, err)
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if count, err := read(cancelled, selected, root.RunID(), root.ID()); count != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled marker read produced evidence: %d/%v", count, err)
				}
				if count, err := read(ctx, selected, "invalid", root.ID()); err == nil || count != 0 {
					t.Fatalf("invalid marker coordinate produced evidence: %d/%v", count, err)
				}
				if count, err := read(ctx, &sql.DB{}, root.RunID(), root.ID()); err == nil || count != 0 {
					t.Fatalf("raw marker read produced evidence: %d/%v", count, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range []func(context.Context, any, string, string) (int, error){CountCopiedSourceReplayScopeStorageForTest, CountForkLocalReplayScopeStorageForTest} {
				if count, err := read(ctx, selected, root.RunID(), root.ID()); err == nil || count != 0 {
					t.Fatalf("closed marker read produced evidence: %d/%v", count, err)
				}
			}
		})
	}
}

func assertPostForkSourceEvent(t testing.TB, event events.Event, runID, eventID string, at time.Time) {
	t.Helper()
	if event.ID() != eventID || event.RunID() != runID || event.Type() != "source.after" || event.ProducerType() != events.EventProducerExternal || event.Producer().ID() != "source-runtime" || event.ParentEventID() != "" || event.Envelope().EntityID != runID || event.Envelope().FlowInstance != "flow-a/1" || !event.CreatedAt().Equal(at) || !bytes.Equal(event.Payload(), []byte(`{}`)) {
		t.Fatalf("post-fork event changed original source identity/producer/envelope/bytes/clock: %+v", event)
	}
}

func assertHistoricalReplayScope(t testing.TB, ctx context.Context, selected any, runID, eventID string, at time.Time) {
	t.Helper()
	row, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, eventID)
	if err != nil || row.RunID != runID || row.Scope != string(pipelineobligation.ScopeDirect) || !row.CreatedAt.Equal(at) || !row.UpdatedAt.Equal(at) {
		t.Fatalf("historical marker changed exact scope/run/clocks: %+v/%v want %s/%s", row, err, runID, at)
	}
}

func TestReplayScopeHistoryRefusesContradictionsAndForeignOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			before, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, root.ID())
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1700002315, 0).UTC()
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			cuts := []struct {
				name         string
				ctx          context.Context
				runID, event string
				scope        pipelineobligation.CommittedScope
				at           time.Time
			}{
				{"conflicting-scope", ctx, root.RunID(), root.ID(), pipelineobligation.ScopeSubscribed, stamp},
				{"noncanonical-scope", ctx, root.RunID(), root.ID(), " direct ", stamp},
				{"invalid-scope", ctx, root.RunID(), root.ID(), "unknown", stamp},
				{"foreign-run", ctx, uuid.NewString(), root.ID(), pipelineobligation.ScopeDirect, stamp},
				{"absent-event", ctx, root.RunID(), uuid.NewString(), pipelineobligation.ScopeDirect, stamp},
				{"invalid-event", ctx, root.RunID(), "invalid", pipelineobligation.ScopeDirect, stamp},
				{"missing-time", ctx, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, time.Time{}},
				{"cancelled", cancelled, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, stamp},
			}
			for _, cut := range cuts {
				if err := StampHistoricalReplayScopeForTest(cut.ctx, selected, cut.runID, cut.event, cut.scope, cut.at); err == nil || (cut.name == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("%s accepted contradictory/absent history: %v", cut.name, err)
				}
				after, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, root.ID())
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("%s mutated scope/run/clocks: %+v/%v", cut.name, after, err)
				}
			}
			for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}} {
				if err := StampHistoricalReplayScopeForTest(ctx, owner, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, stamp); err == nil {
					t.Fatalf("foreign/uninitialized scope writer accepted: %T", owner)
				}
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("refused marker committed or retained selected writer work: %+v", counts)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := StampHistoricalReplayScopeForTest(ctx, selected, root.RunID(), root.ID(), pipelineobligation.ScopeDirect, stamp); err == nil {
				t.Fatal("closed selected scope writer accepted")
			}
		})
	}
}

func TestPostForkSourceEventRefusesInvalidAndAbsentSetupBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			before, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1700002316, 0).UTC()
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			cuts := []struct {
				name               string
				ctx                context.Context
				run, event, entity string
				at                 time.Time
			}{
				{"absent-run", ctx, uuid.NewString(), uuid.NewString(), root.RunID(), stamp},
				{"invalid-event", ctx, root.RunID(), "invalid", root.RunID(), stamp},
				{"invalid-entity", ctx, root.RunID(), uuid.NewString(), "invalid", stamp},
				{"missing-time", ctx, root.RunID(), uuid.NewString(), root.RunID(), time.Time{}},
				{"cancelled", cancelled, root.RunID(), uuid.NewString(), root.RunID(), stamp},
			}
			for _, cut := range cuts {
				if event, err := SeedPostForkSourceEventForTest(cut.ctx, selected, cut.run, cut.event, cut.entity, cut.at); err == nil || event.ID() != "" || (cut.name == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("%s produced successful/provisional post-fork event: %+v/%v", cut.name, event, err)
				}
				after, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("%s changed source storage: %v", cut.name, err)
				}
			}
		})
	}
}
