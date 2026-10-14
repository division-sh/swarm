package pipelinepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func indexCompanionFixtureRows() *sqlmock.Rows {
	return sqlmock.NewRows(strings.Fields("path template entity parent key receipt_entity projection ready_path plan plan_hash attempt phase attempt_state emitted_at bundle run_status instance_status terminated_at"))
}

func addIndexCompanionFixtureRow(rows *sqlmock.Rows, path string) {
	rows.AddRow(path, "worker", "11111111-1111-1111-1111-111111111111", "parent", "key",
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
		"bundle-v2:sha256:"+strings.Repeat("a", 64), "running", "active", nil)
}

func TestIndexCompanionsBatchReadsAndKeepsMissingRecordsVisible(t *testing.T) {
	for _, count := range []int{0, 4, 8, 16, 32, 64, 65} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			const runID = "22222222-2222-2222-2222-222222222222"
			snapshot := flowInstanceIndexSnapshot{tx: tx, run: runlifecycle.Snapshot{RunID: runID}}
			var headers []pipeline.WorkflowInstance
			for index := 0; index < count; index++ {
				path := fmt.Sprintf("parent/worker-%03d", index)
				headers = append(headers, pipeline.WorkflowInstance{StorageRef: path})
			}
			for start := 0; start < count; start += flowInstanceCompanionBatchSize {
				args := []driver.Value{runID}
				var placeholders []string
				rows := indexCompanionFixtureRows()
				for index, header := range headers[start:min(start+flowInstanceCompanionBatchSize, count)] {
					args = append(args, header.StorageRef)
					placeholders = append(placeholders, "$"+strconv.Itoa(index+2))
					addIndexCompanionFixtureRow(rows, header.StorageRef)
				}
				mock.ExpectQuery(flowInstanceCompanionSelect + strings.Join(placeholders, ",") + ")").WithArgs(args...).WillReturnRows(rows).RowsWillBeClosed()
			}
			if err := snapshot.preloadCompanions(context.Background(), headers); err != nil {
				t.Fatal(err)
			}
			// Within this one transaction, repeated exact coordinates issue no I/O.
			if err := snapshot.preloadCompanions(context.Background(), append(headers, headers...)); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.companions) != count {
				t.Fatal("absence-preserving batch lost headers without companions")
			}
			for _, c := range snapshot.companions {
				if c.constructionPresent || c.readinessPresent || c.decodedOnce {
					t.Fatal("batch invented receipt/readiness or admitted a missing companion")
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := snapshot.preloadCompanions(cancelled, headers); !errors.Is(err, context.Canceled) {
				t.Fatalf("cached companions hid cancellation: %v", err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexCompanionsRefusePartialDuplicateAndCrossedHeaders(t *testing.T) {
	for _, fault := range []string{"missing", "duplicate", "crossed"} {
		t.Run(fault, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := flowInstanceIndexSnapshot{tx: tx, run: runlifecycle.Snapshot{RunID: "22222222-2222-2222-2222-222222222222"}}
			rows := indexCompanionFixtureRows()
			switch fault {
			case "duplicate":
				addIndexCompanionFixtureRow(rows, "parent/worker")
				addIndexCompanionFixtureRow(rows, "parent/worker")
			case "crossed":
				addIndexCompanionFixtureRow(rows, "foreign/worker")
			}
			mock.ExpectQuery(flowInstanceCompanionSelect+"$2)").WithArgs(snapshot.run.RunID, "parent/worker").WillReturnRows(rows).RowsWillBeClosed()
			err = snapshot.preloadCompanions(context.Background(), []pipeline.WorkflowInstance{{StorageRef: "parent/worker"}})
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if !errors.As(err, &corruption) || len(snapshot.companions) != 0 {
				t.Fatalf("invalid companion batch partially admitted: %v", err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexCompanionsPreserveReadFailureAndCancellation(t *testing.T) {
	for _, fault := range []string{"query", "row", "close", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := flowInstanceIndexSnapshot{tx: tx, run: runlifecycle.Snapshot{RunID: "22222222-2222-2222-2222-222222222222"}}
			want := errors.New("independent companion read failure")
			if fault == "cancelled" {
				cancel()
				want = context.Canceled
			} else {
				query := mock.ExpectQuery(flowInstanceCompanionSelect+"$2)").WithArgs(snapshot.run.RunID, "parent/worker")
				switch fault {
				case "query":
					query.WillReturnError(want)
				case "row":
					rows := indexCompanionFixtureRows()
					addIndexCompanionFixtureRow(rows, "parent/worker")
					query.WillReturnRows(rows.RowError(0, want)).RowsWillBeClosed()
				case "close":
					query.WillReturnRows(indexCompanionFixtureRows().CloseError(want)).RowsWillBeClosed()
				}
			}
			err = snapshot.preloadCompanions(ctx, []pipeline.WorkflowInstance{{StorageRef: "parent/worker"}})
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if !errors.Is(err, want) || errors.As(err, &corruption) || len(snapshot.companions) != 0 {
				t.Fatalf("read failure became absence/corruption or installed a partial view: %v", err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexCompanionsCannotTurnExactOrphanOccupancyIntoAbsence(t *testing.T) {
	const query = `SELECT
		EXISTS(SELECT 1 FROM entity_state WHERE run_id=$1 AND flow_instance=$2) OR
		EXISTS(SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2) OR
		EXISTS(SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2)`
	for _, occupied := range []bool{false, true} {
		t.Run(strconv.FormatBool(occupied), func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := flowInstanceIndexSnapshot{tx: tx, run: runlifecycle.Snapshot{RunID: "22222222-2222-2222-2222-222222222222"}}
			mock.ExpectQuery(query).WithArgs(snapshot.run.RunID, "parent/missing-header").WillReturnRows(sqlmock.NewRows([]string{"occupied"}).AddRow(occupied))
			err = snapshot.requireAbsentCoordinate(context.Background(), "parent/missing-header")
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if occupied != errors.As(err, &corruption) || (!occupied && err != nil) {
				t.Fatalf("exact orphan coordinate was hidden by header-driven joins: occupied=%t err=%v", occupied, err)
			}
			if corruption != nil && (corruption.RunID != snapshot.run.RunID || corruption.InstancePath != "parent/missing-header") {
				t.Fatal("orphan refusal lost its exact coordinate")
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func indexCompanionReadinessFixture(t *testing.T) flowInstanceCompanions {
	t.Helper()
	const runID = "22222222-2222-2222-2222-222222222222"
	plan, err := (pipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: flowidentity.Instance{TemplateID: ".", ScopeKey: ".", InstancePath: runID, InstanceID: runID, EntityID: runID, HasStoredPath: true},
		RunID:    runID, BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), WorkflowVersion: "fixture", ExecutionMode: executionmode.Mock,
	}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := plan.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return flowInstanceCompanions{readinessPresent: true, readiness: pipeline.DynamicFlowRuntimeReadinessPersistenceRecord{
		RunID: runID, InstancePath: runID, Plan: raw, PlanHash: hash, AttemptOrdinal: 1,
		OwningRunBundleHash: plan.BundleHash, RunStatus: "running", InstanceStatus: "active", Phase: pipeline.FlowAttachmentReady, AttemptState: "accepted",
	}}
}

func TestIndexCompanionsDecodeReadinessOnceInTheirOwnedSnapshot(t *testing.T) {
	c := indexCompanionReadinessFixture(t)
	want, err := pipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(c.readiness)
	if err != nil {
		t.Fatal(err)
	}
	first, found, err := c.observeReadiness()
	if err != nil || !found || !reflect.DeepEqual(first, want) || !c.decodedOnce {
		t.Fatalf("first canonical readiness decode: %v", err)
	}
	// Private raw records cannot change mid-snapshot; prove a second call uses
	// the first admitted decode rather than interpreting those bytes again.
	c.readiness.Plan = nil
	second, found, err := c.observeReadiness()
	if err != nil || !found || !reflect.DeepEqual(second, want) {
		t.Fatalf("readiness decoded again in the same read snapshot: %v", err)
	}
	fresh := indexCompanionReadinessFixture(t)
	fresh.readiness.PlanHash = strings.Repeat("0", 64)
	if _, found, err := fresh.observeReadiness(); err == nil || found {
		t.Fatal("fresh snapshot accepted a corrupt plan/hash")
	}
}

func TestConstructionReadinessCorrespondenceRetainsEveryExactCoordinate(t *testing.T) {
	c := indexCompanionReadinessFixture(t)
	ready, found, err := c.observeReadiness()
	if err != nil || !found {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: ready.Plan.RunID, Route: ready.Plan.Identity.Route()}
	evidence := pipeline.FlowConstructionPublicationEvidence{Identity: ready.Plan.Identity}
	record := flowConstructionPublicationRecord{templateID: ".", entityID: ready.Plan.Identity.EntityID}
	if _, err := record.admitReadiness(owner, evidence, ready.Plan); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"template", "entity", "parent", "key", "plan_identity", "plan_run"} {
		t.Run(fault, func(t *testing.T) {
			broken, plan := record, ready.Plan
			switch fault {
			case "template":
				broken.templateID = "foreign"
			case "entity":
				broken.entityID = "11111111-1111-1111-1111-111111111111"
			case "parent":
				broken.parentInstance = "foreign"
			case "key":
				broken.instanceKey = "foreign"
			case "plan_identity":
				plan.Identity.EntityID = "11111111-1111-1111-1111-111111111111"
			case "plan_run":
				plan.RunID = "11111111-1111-1111-1111-111111111111"
			}
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if _, err := broken.admitReadiness(owner, evidence, plan); !errors.As(err, &corruption) {
				t.Fatalf("crossed constructor/readiness %s accepted: %v", fault, err)
			}
		})
	}
}
