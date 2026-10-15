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
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
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
			for _, diagnostic := range []bool{false, true} {
				name := "business"
				if diagnostic {
					name = "diagnostic"
				}
				t.Run(name, func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					runID, eventID := uuid.NewString(), uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, fixture.dialect == "postgres", runID)
					baseline := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)

					at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
					event := eventtest.ExistingRunRootIngress(eventID, "history.oracle", "runtime", "", []byte(`{"message":"history oracle"}`), 0, runID, events.EventEnvelope{}, at)
					wantRevisionDelta := int64(1)
					if diagnostic {
						event = eventtest.DiagnosticDirect(
							eventID, events.EventTypePlatformRuntimeLog, "runtime", "",
							[]byte(`{"log_level":"info","message":"history oracle","details":{"component":"mutation_protocol_history_oracle","action":"append"}}`),
							0, runID, "", events.EventEnvelope{}, at,
						)
						wantRevisionDelta = 0
					}
					bound, err := bindSemanticEventFixturePayload(event)
					if err != nil {
						t.Fatalf("bind event payload: %v", err)
					}
					admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
					if err != nil {
						t.Fatalf("admit event: %v", err)
					}
					writer, ok := fixture.store.(diagnosticRuntimeLogFixtureStore)
					if !ok {
						t.Fatalf("selected store %T has no runtime-log production writer", fixture.store)
					}
					commit := func() (runtimebus.EventAppendOutcome, error) {
						if diagnostic {
							return writer.CommitRuntimeLogEvent(ctx, admitted)
						}
						return commitAdmittedSemanticEventFixtureOutcome(ctx, fixture.store, admitted, nil, runtimepipelineobligation.ScopeDirect)
					}
					outcome, err := commit()
					if err != nil || outcome != runtimebus.EventAppendInserted {
						t.Fatalf("event commit: outcome=%v err=%v", outcome, err)
					}
					inserted := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
					if inserted.Head != baseline.Head+wantRevisionDelta || inserted.RevisionRows != baseline.RevisionRows+int(wantRevisionDelta) {
						t.Fatalf("ordinary writer revision = head:%d rows:%d, baseline head:%d rows:%d", inserted.Head, inserted.RevisionRows, baseline.Head, baseline.RevisionRows)
					}
					if diagnostic && !reflect.DeepEqual(inserted, baseline) {
						t.Fatal("diagnostic append changed fork history")
					}
					var eventFacts int
					if err := fixture.db.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM run_fork_fact_revisions
				WHERE run_id=$1 AND revision=$2 AND family='events' AND fact_key=$3 AND present=TRUE
			`, runID, inserted.Head, eventID).Scan(&eventFacts); err != nil {
						t.Fatalf("count writer event facts: %v", err)
					}
					if eventFacts != int(wantRevisionDelta) {
						t.Fatalf("writer event facts = %d, want %d at revision %d", eventFacts, wantRevisionDelta, inserted.Head)
					}
					requireMutationProtocolFullCaptureNoRepair(t, ctx, fixture, runID)

					outcome, err = commit()
					if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
						t.Fatalf("exact duplicate event commit: outcome=%v err=%v", outcome, err)
					}
					duplicate := snapshotMutationProtocolHistory(t, ctx, fixture.db, runID)
					if !reflect.DeepEqual(duplicate, inserted) {
						t.Fatalf("exact duplicate changed history:\ninserted=%#v\nduplicate=%#v", inserted, duplicate)
					}
					requireMutationProtocolFullCaptureNoRepair(t, ctx, fixture, runID)
				})
			}
		})
	}
}
