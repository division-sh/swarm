package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/google/uuid"
)

type mutationProtocolHistoryFact struct {
	Revision int64
	Family   string
	Key      string
	Present  bool
	Fact     string
}

type mutationProtocolHistorySnapshot struct {
	Head            int64
	RevisionRows    int
	HistoricalFacts []mutationProtocolHistoryFact
}

func snapshotMutationProtocolHistory(t *testing.T, ctx context.Context, db *sql.DB, runID string) mutationProtocolHistorySnapshot {
	t.Helper()
	var snapshot mutationProtocolHistorySnapshot
	if err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1),0)`, runID).Scan(&snapshot.Head); err != nil {
		t.Fatalf("read revision head: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&snapshot.RevisionRows); err != nil {
		t.Fatalf("count revision rows: %v", err)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT revision,family,fact_key,present,CAST(fact AS TEXT)
		FROM run_fork_fact_revisions
		WHERE run_id=$1
		ORDER BY revision,family,fact_key
	`, runID)
	if err != nil {
		t.Fatalf("read historical revision facts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fact mutationProtocolHistoryFact
		if err := rows.Scan(&fact.Revision, &fact.Family, &fact.Key, &fact.Present, &fact.Fact); err != nil {
			t.Fatalf("scan historical revision fact: %v", err)
		}
		snapshot.HistoricalFacts = append(snapshot.HistoricalFacts, fact)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read historical revision facts: %v", err)
	}
	return snapshot
}

func requireMutationProtocolFullCaptureNoRepair(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	before := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
	// The full-family test capture is the oracle, not the writer under test.
	publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, fixture.dialect == "postgres", runID)
	after := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("full canonical capture repaired production history:\nbefore=%#v\nafter=%#v", before, after)
	}
	requireCompleteRunForkRevision(t, ctx, fixture, runID)
}

func TestMutationProtocolOrdinaryEventWriterMatchesFullHistoricalCaptureBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID, eventID := uuid.NewString(), uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, backend.name == "postgres", runID)
			baseline := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)

			event := eventtest.DiagnosticDirect(
				eventID, events.EventTypePlatformRuntimeLog, "runtime", "",
				[]byte(`{"log_level":"info","message":"history oracle","details":{"component":"mutation_protocol_history_oracle","action":"append"}}`),
				0, runID, "", events.EventEnvelope{}, time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC),
			)
			bound, err := bindSemanticEventFixturePayload(event)
			if err != nil {
				t.Fatalf("bind runtime-log payload: %v", err)
			}
			admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatalf("admit runtime-log event: %v", err)
			}
			writer, ok := fixture.store.(diagnosticRuntimeLogFixtureStore)
			if !ok {
				t.Fatalf("selected store %T has no runtime-log production writer", fixture.store)
			}
			outcome, err := writer.CommitRuntimeLogEvent(ctx, admitted)
			if err != nil || outcome != runtimebus.EventAppendInserted {
				t.Fatalf("ordinary runtime-log commit: outcome=%v err=%v", outcome, err)
			}
			inserted := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
			if inserted.Head != baseline.Head+1 || inserted.RevisionRows != baseline.RevisionRows+1 {
				t.Fatalf("ordinary writer revision = head:%d rows:%d, baseline head:%d rows:%d", inserted.Head, inserted.RevisionRows, baseline.Head, baseline.RevisionRows)
			}
			var eventFacts int
			if err := fixture.db.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM run_fork_fact_revisions
				WHERE run_id=$1 AND revision=$2 AND family='events' AND fact_key=$3 AND present=TRUE
			`, runID, inserted.Head, eventID).Scan(&eventFacts); err != nil {
				t.Fatalf("count writer event facts: %v", err)
			}
			if eventFacts != 1 {
				t.Fatalf("ordinary writer event facts = %d, want one at revision %d", eventFacts, inserted.Head)
			}
			requireMutationProtocolFullCaptureNoRepair(t, ctx, fixture, runID)

			outcome, err = writer.CommitRuntimeLogEvent(ctx, admitted)
			if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
				t.Fatalf("exact duplicate runtime-log commit: outcome=%v err=%v", outcome, err)
			}
			duplicate := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
			if !reflect.DeepEqual(duplicate, inserted) {
				t.Fatalf("exact duplicate changed history:\ninserted=%#v\nduplicate=%#v", inserted, duplicate)
			}
			requireMutationProtocolFullCaptureNoRepair(t, ctx, fixture, runID)
		})
	}
}
