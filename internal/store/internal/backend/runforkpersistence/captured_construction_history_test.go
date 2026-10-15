package runforkpersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestCapturedConstructionLineageUsesExactOriginBoundary(t *testing.T) {
	runID, eventID := eventtest.UUID("captured-source"), eventtest.UUID("captured-event")
	origin, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointEvent, 7, eventID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"exact", "run", "revision", "event", "event_run", "future_event", "missing_event"} {
		t.Run(cut, func(t *testing.T) {
			snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 7, Events: []runForkRevisionEvent{{RunID: runID, EventID: eventID, runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3}}}}
			switch cut {
			case "run":
				snapshot.RunID = eventtest.UUID("foreign-source")
			case "revision":
				snapshot.Revision++
			case "event":
				snapshot.Events[0].EventID = eventtest.UUID("foreign-event")
			case "event_run":
				snapshot.Events[0].RunID = eventtest.UUID("foreign-source")
			case "future_event":
				snapshot.Events[0].FirstRevision = 8
			case "missing_event":
				snapshot.Events = nil
			}
			if err := validateCapturedOriginBoundary(origin, snapshot); (err == nil) != (cut == "exact") {
				t.Fatalf("%s: %v", cut, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver := &capturedConstructionResolver{active: map[string]bool{runID: true}, snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
	if _, err := resolver.resolve(ctx, &runForkRevisionSnapshot{RunID: runID, Revision: 7}); !errors.Is(err, context.Canceled) {
		t.Fatalf("lineage ignored cancellation: %v", err)
	}
	if _, err := resolver.sourceCut(context.Background(), origin); err == nil {
		t.Fatal("cyclic durable lineage reached SQL")
	}
}

func TestCapturedConstructionOriginalStartUsesExclusiveProjection(t *testing.T) {
	runID := eventtest.UUID("captured-start-source")
	for _, test := range []struct {
		name      string
		firstTurn string
	}{
		{"event_origin", eventtest.UUID("captured-start-ingress")},
		{"eventless_origin", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			origin, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointRunStart, 1, "")
			if err != nil {
				t.Fatal(err)
			}
			projection := runforkrevision.StartProjection{Version: 1, SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FirstTurnEventID: test.firstTurn, Facts: []runforkrevision.StartFact{}}
			expectOriginalStart(t, mock, runID, projection)
			resolver := capturedConstructionResolver{tx: tx, active: make(map[string]bool), snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
			snapshot, err := resolver.sourceCut(context.Background(), origin)
			if err != nil || snapshot == nil || snapshot.StartProjection == nil || len(snapshot.Events) != 0 || snapshot.StartProjection.FirstTurnEventID != test.firstTurn {
				t.Fatalf("captured start expanded its exclusive projection: snapshot=%+v error=%v", snapshot, err)
			}
		})
	}
}

func TestCapturedConstructionOriginalStartRefusesMissingOrCrossedProjection(t *testing.T) {
	runID := eventtest.UUID("captured-start-source")
	origin, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointRunStart, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"missing", "wrong_revision", "foreign_run", "nil_facts", "creating_ingress_member"} {
		t.Run(kind, func(t *testing.T) {
			projection := &runforkrevision.StartProjection{Version: 1, SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FirstTurnEventID: eventtest.UUID("captured-start-ingress"), Facts: []runforkrevision.StartFact{}}
			snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 1, StartProjection: projection}
			switch kind {
			case "missing":
				snapshot.StartProjection = nil
			case "wrong_revision":
				snapshot.Revision++
			case "foreign_run":
				snapshot.RunID = eventtest.UUID("foreign-start-source")
			case "nil_facts":
				projection.Facts = nil
			case "creating_ingress_member":
				projection.Facts = []runforkrevision.StartFact{{Family: runforkrevision.FamilyEvents, Key: projection.FirstTurnEventID}}
			}
			if err := validateCapturedOriginBoundary(origin, snapshot); err == nil {
				t.Fatal("captured start accepted absent or crossed immutable projection")
			}
		})
	}
	for _, kind := range []string{"missing", "wrong_revision"} {
		t.Run("reader_"+kind, func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			if kind == "missing" {
				mock.ExpectQuery(`SELECT h.start_revision, h.start_projection`).WithArgs(runID).WillReturnRows(sqlmock.NewRows([]string{"start_revision", "start_projection", "origin_kind", "origin_event_id"}))
			} else {
				expectOriginalStart(t, mock, runID, runforkrevision.StartProjection{Version: 1, SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), Facts: []runforkrevision.StartFact{}})
				origin, err = runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointRunStart, 2, "")
				if err != nil {
					t.Fatal(err)
				}
			}
			resolver := capturedConstructionResolver{tx: tx, active: make(map[string]bool), snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
			if _, err := resolver.sourceCut(context.Background(), origin); err == nil {
				t.Fatal("captured start reader fell back outside its immutable projection")
			}
		})
	}
}
