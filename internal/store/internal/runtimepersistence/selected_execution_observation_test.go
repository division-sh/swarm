package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedExecutionEventReadersPreservePhysicalIdentityAndPayloadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			sibling := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			stored, err := ReadSelectedExecutionEventStorageForTest(ctx, fixture.store, event.ID())
			want := SelectedExecutionEventStorage{RunID: event.RunID(), EventName: string(event.Type()), ExecutionMode: string(event.ExecutionMode())}
			if err != nil || stored != want {
				t.Fatalf("physical event identity=%+v/%v, want%+v", stored, err, want)
			}
			payloads, err := ReadSelectedExecutionPayloadsStorageForTest(ctx, fixture.store, event.RunID())
			if err != nil || len(payloads) != 1 {
				t.Fatalf("run payload inventory=%q/%v", payloads, err)
			}
			payload, err := ReadActivityForkRequestPayloadStorageForTest(ctx, fixture.store, event.RunID(), event.ID())
			if err != nil || !bytes.Equal(payload, payloads[0]) {
				t.Fatalf("stored payload bytes changed=%q/%q/%v", payloads[0], payload, err)
			}
			if rows, err := ReadSelectedExecutionPayloadsStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || rows != nil {
				t.Fatalf("foreign run borrowed payloads=%q/%v", rows, err)
			}
			if got, err := ReadSelectedExecutionEventStorageForTest(ctx, fixture.store, sibling.ID()); err != nil || got.RunID != sibling.RunID() || reflect.DeepEqual(got, stored) {
				t.Fatalf("sibling event borrowed identity=%+v/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 5 || counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("event reads bypassed original independent coordinator=%+v", counts)
			}
		})
	}
}

func TestSelectedForkUnprovenSourceTimerFixtureKeepsExactOwnerAndClockBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			entityID := uuid.NewString()
			ref := timeridentity.WorkflowTimerActivationRef{
				ActivationID: uuid.NewString(), DeclarationKey: "test-node.check_timer",
				DeclarationRevision: "sha256:test-node-check-timer", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
			}
			at := time.Unix(1700002210, 0).UTC()
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedSelectedForkSourceTimerForTest(ctx, fixture.store, event.RunID(), entityID, ref, at); err != nil {
				t.Fatal(err)
			}
			if status, err := ReadSelectedForkSourceTimerStatusForTest(ctx, fixture.store, ref.ActivationID, event.RunID()); err != nil || status != "active" {
				t.Fatalf("historical timer status=%q/%v", status, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("timer history bypassed original selected admission: %+v", counts)
			}
			restore()
			stored, err := ReadWorkflowTimerOccurrenceStorageForTest(ctx, fixture.store, event.RunID(), entityID)
			if err != nil || stored.TaskID != ref.TaskID() || stored.Status != "active" {
				t.Fatalf("timer history identity changed: %+v/%v", stored, err)
			}
			fireAt, present, err := sqliteTimeValue(stored.FireAt)
			if err != nil || !present || !fireAt.Equal(at.Add(time.Hour)) {
				t.Fatalf("timer source clock changed: %s/%t/%v", fireAt, present, err)
			}
			assertSelectedForkSourceTimerIdentityRefusals(t, ctx, fixture.store, ref.ActivationID, event.RunID())
		})
	}
}

func assertSelectedForkSourceTimerIdentityRefusals(t *testing.T, ctx context.Context, selected any, timerID, runID string) {
	t.Helper()
	for _, ids := range [][2]string{{uuid.NewString(), runID}, {timerID, uuid.NewString()}} {
		if status, err := ReadSelectedForkSourceTimerStatusForTest(ctx, selected, ids[0], ids[1]); !errors.Is(err, sql.ErrNoRows) || status != "" {
			t.Fatalf("foreign timer/run borrowed history: %q/%v", status, err)
		}
	}
}
