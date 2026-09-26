package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const postgresStatementRunID = "00000000-0000-4000-8000-000000000001"

func postgresStatementRouteSets(owners int) []runtimebus.FlowInstanceRouteRecordSet {
	sets := make([]runtimebus.FlowInstanceRouteRecordSet, owners)
	for i := range sets {
		id := flowidentity.RunScopedFlowInstance{RunID: postgresStatementRunID, Route: flowidentity.DeriveRoute("review", fmt.Sprintf("i%03d", i))}
		sets[i].Identity = id
		for _, pattern := range []string{"work.done", "work.ready"} {
			sets[i].Routes = append(sets[i].Routes, runtimebus.FlowInstanceRouteRecord{
				Identity: id, EventPattern: pattern, SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "review",
			})
		}
	}
	return sets
}

func expectPostgresRouteAdmission(mock sqlmock.Sqlmock, active, source bool) {
	state := "running"
	if !active {
		state = "completed"
	}
	mock.ExpectQuery(`SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid FOR UPDATE`).WithArgs(postgresStatementRunID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow(state, sourceartifactfixture.BundleHash))
	if active {
		mock.ExpectQuery(`SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`).WithArgs(sourceartifactfixture.BundleHash).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(source))
	}
}

func expectEmptyPostgresRouteTopologySnapshot(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(postgresRouteTopologySnapshotSQL).WithArgs(postgresStatementRunID).
		WillReturnRows(sqlmock.NewRows([]string{"flow_instance", "event_pattern", "subscriber_type", "subscriber_id", "source_flow", "materialized_from", "status", "is_wildcard"}))
	mock.ExpectQuery(routeTopologySourcesSQL).
		WillReturnRows(sqlmock.NewRows([]string{"event_pattern", "subscriber_type", "subscriber_id", "source_flow", "rule_id", "created_at"}))
}

func TestPostgresRouteTopologyStatementsCountOrderAndLifetime(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sets := postgresStatementRouteSets(64)
	// Two invocations in the same transaction must prepare and close anew.
	for pass := 0; pass < 2; pass++ {
		expectPostgresRouteAdmission(mock, true, true)
		expectEmptyPostgresRouteTopologySnapshot(mock)
		for i, set := range sets {
			if i == 0 {
				mock.ExpectPrepare(postgresFlowInstanceRouteInactivateSQL).WillBeClosed()
			}
			mock.ExpectExec(postgresFlowInstanceRouteInactivateSQL).WithArgs(set.Identity.RunID, set.Identity.Route.InstancePath).WillReturnResult(sqlmock.NewResult(0, 2))
			for j, route := range set.Routes {
				if i == 0 && j == 0 {
					mock.ExpectPrepare(postgresFlowInstanceRouteUpsertSQL).WillBeClosed()
				}
				mock.ExpectExec(postgresFlowInstanceRouteUpsertSQL).
					WithArgs(route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath, route.SourceFlow).
					WillReturnResult(sqlmock.NewResult(0, 0))
			}
		}
		got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, true, sets)
		if err != nil || !reflect.DeepEqual(got, sets) {
			t.Fatalf("pass=%d topology=%v err=%v", pass, got, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	t.Log("per invocation: 2 prepares/2 closes; 64 changed owners + 128 upserts; exact selected-store comparison retained")
}

func TestPostgresRouteTopologyStatementsAdmissionAndErrors(t *testing.T) {
	for _, stage := range []string{"inactive_run", "missing_source", "prepare_inactivate", "execute_inactivate", "prepare_upsert", "execute_upsert", "close", "primary_before_close", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			expectPostgresRouteAdmission(mock, stage != "inactive_run", stage != "missing_source")
			fault, closeFault := errors.New("route statement refused"), errors.New("route statement close refused")
			want, label := error(nil), ""
			if stage != "inactive_run" && stage != "missing_source" {
				expectEmptyPostgresRouteTopologySnapshot(mock)
				inactive := mock.ExpectPrepare(postgresFlowInstanceRouteInactivateSQL)
				if stage == "prepare_inactivate" {
					inactive.WillReturnError(fault)
					want, label = fault, "inactivate postgres flow-instance route owner"
				} else {
					inactive.WillBeClosed()
					if stage == "close" || stage == "primary_before_close" {
						inactive.WillReturnCloseError(closeFault)
					}
					exec := mock.ExpectExec(postgresFlowInstanceRouteInactivateSQL)
					if stage == "execute_inactivate" {
						exec.WillReturnError(fault)
						want, label = fault, "inactivate postgres flow-instance route owner"
					} else {
						exec.WillReturnResult(sqlmock.NewResult(0, 0))
						upsert := mock.ExpectPrepare(postgresFlowInstanceRouteUpsertSQL)
						if stage == "prepare_upsert" {
							upsert.WillReturnError(fault)
							want, label = fault, "upsert flow instance route"
						} else {
							upsert.WillBeClosed()
							mock.ExpectExec(postgresFlowInstanceRouteUpsertSQL).WillReturnResult(sqlmock.NewResult(0, 1))
							exec := mock.ExpectExec(postgresFlowInstanceRouteUpsertSQL)
							want, label = fault, "upsert flow instance route"
							if stage == "close" {
								exec.WillReturnResult(sqlmock.NewResult(0, 1))
								want, label = closeFault, "close postgres flow-instance route statements"
							} else if stage == "cancel" {
								exec.WillReturnError(context.Canceled)
								want = context.Canceled
							} else {
								exec.WillReturnError(fault)
							}
						}
					}
				}
			}
			got, err := replaceFlowInstanceRouteTopologyTx(context.Background(), tx, true, postgresStatementRouteSets(1))
			if got != nil || err == nil || (want != nil && !errors.Is(err, want)) || !strings.Contains(err.Error(), label) {
				t.Fatalf("topology=%v err=%v want=%v label=%q", got, err, want, label)
			}
			if stage == "primary_before_close" && errors.Is(err, closeFault) {
				t.Fatalf("cleanup changed primary error: %v", err)
			}
			// Check closure before rollback can clean up forgotten handles.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresRouteTopologyStatementRejectsOtherSQL(t *testing.T) {
	var exec postgresFlowInstanceRouteExecutor
	if _, err := exec.ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("unbounded SQL slot admitted")
	}
	if err := exec.close(); err != nil {
		t.Fatal(err)
	}
}

// Reference loop from before statement reuse, retaining canonical admission,
// normalization and the shared upsert owner rather than duplicating its SQL.
func postgresRouteTopologyDirect(ctx context.Context, tx *sql.Tx, sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	normalized, err := normalizeFlowInstanceRouteTopology(sets)
	if err != nil {
		return nil, err
	}
	admitted := map[string]bool{}
	for _, set := range normalized {
		if !admitted[set.Identity.RunID] {
			if err := requirePostgresRunActive(ctx, tx, set.Identity.RunID); err != nil {
				return nil, err
			}
			admitted[set.Identity.RunID] = true
		}
		if _, err := tx.ExecContext(ctx, postgresFlowInstanceRouteInactivateSQL, set.Identity.RunID, set.Identity.Route.InstancePath); err != nil {
			return nil, fmt.Errorf("inactivate postgres flow-instance route owner %s: %w", set.Identity.Key(), err)
		}
		for _, route := range set.Routes {
			if err := upsertPostgresFlowInstanceRoute(ctx, tx, route); err != nil {
				return nil, err
			}
		}
	}
	return normalized, nil
}
