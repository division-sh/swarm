package runtimepersistence

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunDiagnosticHistoryFixturePreservesPairAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			historyBefore, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			id, at := uuid.NewString(), time.Unix(1700002214, 0).UTC()
			payload := []byte(`{"level":"info","message":"diagnostic platform row must remain lineage-only"}`)
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			event, err := SeedRunDiagnosticDeadLetterHistoryForTest(ctx, selected, root.RunID(), id, payload, at)
			if err != nil || event.ID() != id || event.RunID() != root.RunID() || event.AdmissionClass() != events.EventAdmissionDiagnosticDirect || event.ExecutionMode() != executionmode.Live || !event.CreatedAt().Equal(at) || !bytes.Equal(event.Payload(), payload) {
				t.Fatalf("diagnostic history changed exact identity/class/mode/bytes/clock: id=%s run=%s class=%s mode=%s at=%s payload=%s err=%v", event.ID(), event.RunID(), event.AdmissionClass(), event.ExecutionMode(), event.CreatedAt(), event.Payload(), err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("diagnostic pair split or escaped the original canonical event writer: %+v", counts)
			}
			historyAfter, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"run_fork_revisions", "run_fork_revision_heads", "run_fork_fact_revisions"} {
				before, hadBefore := historyBefore[table]
				after, hasAfter := historyAfter[table]
				if !hadBefore || !hasAfter || !reflect.DeepEqual(before, after) {
					t.Fatalf("lineage-only diagnostic changed physical revision history %s: before=%+v after=%+v", table, before, after)
				}
			}
			stored, found, err := ReadCanonicalEventRecordForTest(ctx, selected, id)
			if err != nil || !found || stored.ID() != id || stored.RunID() != root.RunID() || stored.AdmissionClass() != events.EventAdmissionDiagnosticDirect || stored.ParentEventID() != "" || stored.Envelope().Scope != events.EventScopeGlobal || stored.ProducerType() != events.EventProducerPlatform || stored.Producer().ID() != "runtime" || stored.ExecutionMode() != executionmode.Live || !stored.CreatedAt().Equal(at) || !bytes.Equal(stored.Payload(), payload) {
				t.Fatalf("stored diagnostic lost exact subtype/producer/lineage/bytes/clock: found=%t err=%v", found, err)
			}
			before, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
			if err != nil {
				t.Fatal(err)
			}
			assertSelectedSourceEntityFixtureRow(t, before["event_receipts"], map[string]string{
				"event_id": id, "subscriber_type": "platform", "subscriber_id": "pipeline",
				"outcome": "dead_letter", "reason_code": "runtime_log_pipeline_dead_letter", "side_effects": "{}",
			}, at, "processed_at")
			rows, err := DecodeSelectedForkSnapshotRowsForTest(before["event_receipts"])
			if err != nil || rows[0]["entity_id"].Type != "<nil>" || rows[0]["flow_instance"].Type != "<nil>" {
				t.Fatalf("lineage-only receipt fabricated target identity: %+v/%v", rows, err)
			}
			for _, cut := range []string{"duplicate-receipt", "absent-run", "invalid-event", "missing-time", "cancelled"} {
				callCtx, run, eventID, stamp := ctx, root.RunID(), id, at
				switch cut {
				case "absent-run":
					run, eventID = uuid.NewString(), uuid.NewString()
				case "invalid-event":
					eventID = "invalid"
				case "missing-time":
					stamp = time.Time{}
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					callCtx = cancelled
				}
				if out, err := SeedRunDiagnosticDeadLetterHistoryForTest(callCtx, selected, run, eventID, payload, stamp); err == nil || out.ID() != "" || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("%s yielded successful/provisional diagnostic history: %+v/%v", cut, out, err)
				}
				after, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("%s changed historical source storage: %v", cut, err)
				}
			}
			for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
				if out, err := SeedRunDiagnosticDeadLetterHistoryForTest(ctx, owner, root.RunID(), uuid.NewString(), payload, at); err == nil || out.ID() != "" {
					t.Fatalf("foreign/uninitialized writer yielded provisional diagnostic: %T/%v", owner, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if out, err := SeedRunDiagnosticDeadLetterHistoryForTest(ctx, selected, root.RunID(), uuid.NewString(), payload, at); err == nil || out.ID() != "" {
				t.Fatalf("closed writer yielded provisional diagnostic: %+v/%v", out, err)
			}
		})
	}
}

func TestRunDiagnosticHistoryFixtureRollsBackActualAppendFaultBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			before, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
			if err != nil {
				t.Fatal(err)
			}
			if err := SetRuntimeLogFixtureAppendFaultForTest(ctx, selected, root.RunID(), true); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := SetRuntimeLogFixtureAppendFaultForTest(context.WithoutCancel(ctx), selected, root.RunID(), false); err != nil {
					t.Error(err)
				}
			})
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			out, err := SeedRunDiagnosticDeadLetterHistoryForTest(ctx, selected, root.RunID(), uuid.NewString(), []byte(`{"level":"info","message":"append rollback"}`), time.Unix(1700002214, 0).UTC())
			if err == nil || !strings.Contains(err.Error(), "runtime_log_exact_append_cut") || out.ID() != "" {
				t.Fatalf("actual AFTER INSERT fault did not discard the complete pair: %+v/%v", out, err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Total.RollbackAttempts != 1 || counts.Active != 0 {
				t.Fatalf("failed diagnostic pair escaped original rollback/cleanup: %+v", counts)
			}
			after, err := ReadSelectedForkSourceDomainForTest(ctx, selected, root.RunID())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failed diagnostic pair left event, receipt or source mutation: %v", err)
			}
		})
	}
}
