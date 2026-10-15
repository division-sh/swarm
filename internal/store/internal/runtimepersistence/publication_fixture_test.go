package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPostgresPublicationHostileSeedsKeepOriginalWriterAndPhysicalState(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	ctx := testAuthorActivityContext()
	event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
	entityID := uuid.NewString()
	probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	beforeClock := time.Now().UTC()
	if err := SeedPostgresPublicationUnroutableEntityForTest(ctx, fixture.store, event.RunID(), entityID, "orphan/inst-1"); err != nil {
		t.Fatal(err)
	}
	afterClock := time.Now().UTC()
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
		t.Fatalf("hostile seed escaped original unrevisioned writer: %+v", counts)
	}
	snapshot, err := ReadSelectedForkSourceDomainForTest(ctx, fixture.store, event.RunID())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["entity_state"])
	if err != nil || len(rows) != 1 {
		t.Fatalf("state-only seed cardinality changed: %+v %v", rows, err)
	}
	for column, want := range map[string]string{"run_id": event.RunID(), "entity_id": entityID, "flow_instance": "orphan/inst-1", "entity_type": "widget", "current_state": "pending", "gates": "{}", "fields": "{}", "accumulator": "{}"} {
		got, err := rows[0][column].Text()
		if err != nil || got != want {
			t.Fatalf("hostile physical column %s=%q/%v, want %q", column, got, err, want)
		}
	}
	var created time.Time
	for _, column := range []string{"entered_state_at", "created_at", "updated_at"} {
		var got time.Time
		cell := rows[0][column]
		if cell.Type != "time.Time" || json.Unmarshal(cell.Value, &got) != nil || got.Before(beforeClock) || got.After(afterClock) {
			t.Fatalf("database now() clock changed: %s=%+v between %s and %s", column, cell, beforeClock, afterClock)
		}
		if column == "entered_state_at" {
			created = got
		} else if !got.Equal(created) {
			t.Fatalf("database clocks no longer share the same statement cut: %s=%s, want %s", column, got, created)
		}
	}
	beforeWrite := probe.Snapshot()
	if err := SeedPostgresPublicationIdentityLookalikesForTest(ctx, fixture.store, event.RunID(), entityID); err != nil {
		t.Fatal(err)
	}
	if counts := probe.Snapshot(); counts.Total.WriteCommits != beforeWrite.Total.WriteCommits+1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
		t.Fatalf("lookalike cut bypassed original writer or manufactured revision evidence: %+v", counts)
	}
	projection, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, event.RunID(), entityID)
	var fields, bookkeeping map[string]string
	if err != nil || json.Unmarshal(projection.Fields, &fields) != nil || json.Unmarshal(projection.Bookkeeping, &bookkeeping) != nil || projection.Revision != 1 || projection.CurrentState != "pending" || !reflect.DeepEqual(fields, map[string]string{"entity_id": "authored-lookalike", "flow_instance": "authored/lookalike"}) || !reflect.DeepEqual(bookkeeping, map[string]string{"entity_id": "bookkeeping-lookalike", "flow_instance": "bookkeeping/lookalike"}) {
		t.Fatalf("hostile maps/state/revision changed: %+v %v", projection, err)
	}
	recovered, err := ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, fixture.store, event.RunID())
	if err != nil || len(recovered["flow_instances"].Rows) != 0 || len(recovered["flow_instance_runtime_readiness"].Rows) != 0 {
		t.Fatalf("hostile state gained construction or readiness: %+v %v", recovered, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := SeedPostgresPublicationIdentityLookalikesForTest(cancelled, fixture.store, event.RunID(), entityID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lookalike cut succeeded: %v", err)
	}
	if err := SeedPostgresPublicationUnroutableEntityForTest(cancelled, fixture.store, event.RunID(), uuid.NewString(), "orphan/inst-2"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled state-only seed succeeded: %v", err)
	}
	if err := SeedPostgresPublicationIdentityLookalikesForTest(ctx, struct{}{}, event.RunID(), entityID); err == nil {
		t.Fatal("foreign owner accepted a hostile mutation")
	}
	if err := SeedPostgresPublicationUnroutableEntityForTest(ctx, fixture.store, event.RunID(), "invalid", "orphan/inst-2"); err == nil {
		t.Fatal("invalid identity accepted a state-only seed")
	}
	after, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, fixture.store, event.RunID(), entityID)
	if err != nil || !reflect.DeepEqual(after, projection) {
		t.Fatalf("refusals changed exact physical state: before=%+v after=%+v err=%v", projection, after, err)
	}
}
