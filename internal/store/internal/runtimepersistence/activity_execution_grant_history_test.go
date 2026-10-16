package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type activityGrantHistoryConnector struct {
	driver.Connector
	fault string
	reads int
}

func (c *activityGrantHistoryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &activityGrantHistoryConn{pipelineCrashConn: &pipelineCrashConn{Conn: conn}, control: c}, nil
}

type activityGrantHistoryConn struct {
	*pipelineCrashConn
	control *activityGrantHistoryConnector
}

func (c *activityGrantHistoryConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *activityGrantHistoryConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.pipelineCrashConn.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(query, "FROM runtime_generation_grants WHERE grant_id=$1 AND state_version=$2") {
		return rows, nil
	}
	c.control.reads++
	return &activityGrantHistoryRows{Rows: rows, fault: c.control.fault}, nil
}

type activityGrantHistoryRows struct {
	driver.Rows
	fault string
}

func (r *activityGrantHistoryRows) Next(values []driver.Value) error {
	if r.fault == "missing" {
		return io.EOF
	}
	if r.fault == "read_error" {
		return errors.New("activity historical grant read unavailable")
	}
	if err := r.Rows.Next(values); err != nil {
		return err
	}
	for index, column := range r.Columns() {
		if column != r.fault {
			continue
		}
		switch column {
		case "state_version", "runtime_generation":
			values[index] = int64(999)
		case "snapshot":
			values[index] = []byte(`{"invalid":"grant"}`)
		default:
			values[index] = uuid.NewString()
		}
	}
	return nil
}

func TestActivityExecutionGrantReadUsesExactHistoricalVersionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			control := &activityGrantHistoryConnector{}
			fixture, _ := openCompletionOutcomeFixture(t, backend, func(native driver.Connector) driver.Connector {
				control.Connector = native
				return control
			})
			ctx := fixture.admissionContext
			selected := fixture.store.(agentfixture.Store)
			process, err := agentfixture.ProcessCapability(t, ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			plan, found, err := process.CurrentSourceSet(ctx)
			if err != nil || !found || len(plan.Sources) != 1 {
				t.Fatalf("exact source plan unavailable: found=%t error=%v", found, err)
			}
			grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, agenttopology.SourceCoordinate{BundleHash: plan.Sources[0].BundleHash})
			if err != nil {
				t.Fatal(err)
			}
			original, err := grant.Evidence()
			if err != nil || original.State != runtimeownership.GrantAdmitted {
				t.Fatalf("original admitted grant unavailable: state=%s error=%v", original.State, err)
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, scenario := range []string{"historical", "missing_version", "foreign_id", "missing", "read_error", "grant_id", "process_authority_id", "process_owner_id", "state_version", "state", "bundle_hash", "runtime_instance_id", "runtime_generation", "source_set_revision", "selected_binding_id", "selected_fork_run_id", "selected_execution_id", "snapshot", "empty_id", "noncanonical_id", "zero_id", "zero_version", "overflow_version"} {
				t.Run(scenario, func(t *testing.T) {
					grantID, version := original.GrantID, original.StateVersion
					control.fault = scenario
					switch scenario {
					case "missing_version":
						version += 100
					case "foreign_id":
						grantID = uuid.NewString()
					case "empty_id":
						grantID = ""
					case "noncanonical_id":
						grantID = " " + grantID
					case "zero_id":
						grantID = uuid.Nil.String()
					case "zero_version":
						version = 0
					case "overflow_version":
						version = math.MaxUint64
					}
					tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					actual, readErr := storestartup.ReadActivityExecutionGrantTx(ctx, tx, grantID, version)
					rollbackErr := tx.Rollback()
					control.fault = ""
					if rollbackErr != nil {
						t.Fatal(rollbackErr)
					}
					if scenario != "historical" {
						if readErr == nil {
							t.Fatal("inexact historical grant was accepted")
						}
						return
					}
					if readErr != nil {
						t.Fatal(readErr)
					}
					want, err := canonicaljson.Bytes(original)
					if err != nil {
						t.Fatal(err)
					}
					got, err := canonicaljson.Bytes(actual)
					if err != nil || string(want) != string(got) || actual.State != runtimeownership.GrantAdmitted {
						t.Fatal("historical read substituted the latest retired grant")
					}
				})
			}
			if control.reads != 18 {
				t.Fatalf("exact native history reads=%d, want 18", control.reads)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("historical reads changed durable rows: %v", err)
			}
		})
	}
}
