package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedSourceOutcomeFixturePreservesHistoricalPairAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			foreign := runForkSnapshotFixtureEvent(t, ctx, selected)
			fixture := SelectedSourceOutcomeFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: uuid.NewString(), CreatedAt: time.Unix(1700002200, 0).UTC(),
				Failure: failures.Normalize(failures.New(failures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed"),
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedSelectedSourceOutcomeForTest(ctx, selected, fixture); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("historical outcome pair escaped original unrevisioned writer: %+v", counts)
			}
			physical, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			assertSelectedSourceEntityFixtureRow(t, physical["event_receipts"], map[string]string{
				"event_id": fixture.EventID, "subscriber_type": "platform", "subscriber_id": "old-source-node",
				"entity_id": fixture.EntityID, "flow_instance": "flow-a/1", "outcome": "success",
				"reason_code": "source_outcome_must_not_suppress_fork", "side_effects": "{}",
			}, fixture.CreatedAt, "processed_at")
			assertSelectedSourceEntityFixtureRow(t, physical["dead_letters"], map[string]string{
				"original_event_id": fixture.EventID, "original_event": "item.received", "entity_id": fixture.EntityID,
				"flow_instance": "flow-a/1", "handler_node": "old-source-node",
			}, fixture.CreatedAt, "created_at")
			rows, err := DecodeSelectedForkSnapshotRowsForTest(physical["dead_letters"])
			if err != nil || len(rows) != 1 {
				t.Fatalf("native failure row changed: %+v/%v", rows, err)
			}
			raw, err := rows[0]["failure"].Text()
			if err != nil {
				t.Fatal(err)
			}
			var failure failures.Envelope
			if err := json.Unmarshal([]byte(raw), &failure); err != nil || !reflect.DeepEqual(failure, fixture.Failure) {
				t.Fatalf("historical failure was reconstructed or normalized differently: %s/%v", raw, err)
			}
			for _, cut := range []string{"duplicate", "foreign-event", "missing-source", "wrong-source", "cancelled"} {
				t.Run(cut, func(t *testing.T) {
					request, callCtx := fixture, ctx
					switch cut {
					case "foreign-event":
						request.EventID = foreign.ID()
					case "missing-source":
						callCtx = context.Background()
					case "wrong-source":
						callCtx = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact(runLifecycleCandidateParityReplacementHash))
					case "cancelled":
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						callCtx = cancelled
					}
					if err := SeedSelectedSourceOutcomeForTest(callCtx, selected, request); err == nil {
						t.Fatalf("historical pair accepted %s", cut)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
					if err != nil || !reflect.DeepEqual(after["event_receipts"], physical["event_receipts"]) || !reflect.DeepEqual(after["dead_letters"], physical["dead_letters"]) {
						t.Fatalf("%s changed the stored predecessor pair: %v", cut, err)
					}
				})
			}
		})
	}
}
