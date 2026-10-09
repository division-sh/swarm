package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func startSnapshotTransaction(t *testing.T) (*sql.Tx, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mock.ExpectRollback()
		_ = tx.Rollback()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return tx, mock
}

func expectOriginalStart(t *testing.T, mock sqlmock.Sqlmock, runID string, projection runforkrevision.StartProjection) {
	t.Helper()
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	origin := "deployment"
	if projection.FirstTurnEventID != "" {
		origin = "event"
	}
	mock.ExpectQuery(`SELECT h.start_revision, h.start_projection`).WithArgs(runID).
		WillReturnRows(sqlmock.NewRows([]string{"start_revision", "start_projection", "origin_kind", "origin_event_id"}).
			AddRow(int64(1), raw, origin, projection.FirstTurnEventID))
}

func TestRunForkStartSnapshotReadsOnlyExactInitialMembership(t *testing.T) {
	tx, mock := startSnapshotTransaction(t)
	runID, initial, ingress := uuid.NewString(), uuid.NewString(), uuid.NewString()
	projection := runforkrevision.StartProjection{
		Version: 1, SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FirstTurnEventID: ingress,
		Facts: []runforkrevision.StartFact{{Family: runforkrevision.FamilyEvents, Key: initial}},
	}
	expectOriginalStart(t, mock, runID, projection)
	mock.ExpectQuery(`WHERE run_id=\$1 AND revision<=\$2 AND \(\(family=\$3 AND fact_key=\$4\)\)`).
		WithArgs(runID, int64(1), "events", initial).
		WillReturnRows(sqlmock.NewRows([]string{"run_id", "family", "fact_key", "first_revision", "revision", "fact"}).
			AddRow(runID, "events", initial, 1, 1, `{"event_id":"`+initial+`","event_name":"owned.initial","payload_base64":"e30="}`))
	snapshot, err := loadRunForkPointSnapshot(context.Background(), tx, runID,
		runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1})
	if err != nil || snapshot == nil || len(snapshot.Events) != 1 || snapshot.Events[0].EventID != initial {
		t.Fatalf("exact start snapshot=%+v err=%v", snapshot, err)
	}
	if !reflect.DeepEqual(snapshot.StartProjection, &projection) {
		t.Fatal("snapshot lost original source or creating-event reference")
	}
}

func TestRunForkStartSnapshotRefusesMissingOrForeignInitialFacts(t *testing.T) {
	for _, kind := range []string{"missing", "wrong_run", "wrong_key", "earlier_revision", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			projection := runforkrevision.StartProjection{Version: 1,
				SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64),
				Facts:            []runforkrevision.StartFact{{Family: runforkrevision.FamilyEvents, Key: eventID}},
			}
			expectOriginalStart(t, mock, runID, projection)
			rows := sqlmock.NewRows([]string{"run_id", "family", "fact_key", "first_revision", "revision", "fact"})
			selectedRun, selectedKey, first := runID, eventID, int64(1)
			switch kind {
			case "wrong_run":
				selectedRun = uuid.NewString()
			case "wrong_key":
				selectedKey = uuid.NewString()
			case "earlier_revision":
				first = 0
			}
			body := `{"event_id":"` + eventID + `","event_name":"owned.initial","payload_base64":"e30="}`
			if kind != "missing" {
				rows.AddRow(selectedRun, "events", selectedKey, first, 1, body)
				if kind == "duplicate" {
					rows.AddRow(selectedRun, "events", selectedKey, first, 1, body)
				}
			}
			mock.ExpectQuery(`WITH bounded AS`).WithArgs(runID, int64(1), "events", eventID).WillReturnRows(rows)
			if snapshot, err := loadRunForkPointSnapshot(context.Background(), tx, runID,
				runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}); err == nil || snapshot != nil {
				t.Fatalf("corrupt %s admitted snapshot=%+v err=%v", kind, snapshot, err)
			}
		})
	}
}

func TestRunForkStartPointNeverResolvesLatestOrInventsAnEvent(t *testing.T) {
	tx, mock := startSnapshotTransaction(t)
	runID := uuid.NewString()
	projection := runforkrevision.StartProjection{Version: 1,
		SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("c", 64), Facts: []runforkrevision.StartFact{}}
	for _, revision := range []int64{1, 2} {
		expectOriginalStart(t, mock, runID, projection)
		cursor, err := resolveFixedRunForkRevisionPoint(context.Background(), tx, runID,
			runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: revision}, nil)
		if revision == 1 {
			if err != nil || cursor != (runForkEventCursor{Kind: runfork.RunForkPointRunStart, Revision: 1}) {
				t.Fatalf("original start cursor=%+v err=%v", cursor, err)
			}
		} else if err == nil || cursor != (runForkEventCursor{}) {
			t.Fatalf("newer start cursor admitted: %+v err=%v", cursor, err)
		}
	}
	expectOriginalStart(t, mock, runID, projection)
	snapshot, err := loadRunForkPointSnapshot(context.Background(), tx, runID,
		runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1})
	if err != nil || snapshot == nil || len(snapshot.Events) != 0 {
		t.Fatalf("empty start invented events: %+v err=%v", snapshot, err)
	}
	first, err := loadRunForkStartFirstTurn(context.Background(), tx, snapshot)
	if err != nil || first != nil {
		t.Fatalf("eventless start invented intake: %+v err=%v", first, err)
	}
}

func TestRunForkStartSourceAdvancementIncludesSameCommitIngress(t *testing.T) {
	tx, mock := startSnapshotTransaction(t)
	runID, initial, ingress := uuid.NewString(), uuid.NewString(), uuid.NewString()
	projection := runforkrevision.StartProjection{Version: 1,
		SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("d", 64), FirstTurnEventID: ingress,
		Facts: []runforkrevision.StartFact{{Family: runforkrevision.FamilyEntityMetadata, Key: initial}}}
	expectOriginalStart(t, mock, runID, projection)
	mock.ExpectQuery(`revision > \$2 OR \(revision = \$2 AND NOT`).
		WithArgs(runID, int64(1), "entity_metadata", initial).
		WillReturnRows(sqlmock.NewRows([]string{"family"}).AddRow("events").AddRow("event_deliveries"))
	facts, err := collectRunForkSourceAdvancedFacts(context.Background(), tx, runForkActivationLineage{
		SourceRunID: runID, ForkEventRevision: 1,
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1},
	})
	if err != nil || !reflect.DeepEqual(facts, []string{"source_deliveries_advanced_after_fork_point", "source_events_advanced_after_fork_point"}) {
		t.Fatalf("same-commit advancement lost: %v err=%v", facts, err)
	}
}
