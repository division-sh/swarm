package runforkpersistence

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestForkRemovedArrivalNativeReadbackRequiresBothExactStateOwners(t *testing.T) {
	plan, _, requests, _ := arrivalJoinInventoryFixture(t, false, false)
	_, ref, ok := timeridentity.ParseJoinHandle(requests[0].Child.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("fixture lost its exact child ref")
	}
	expected, _, err := projectedRemovedArrivalArm(plan, workflowTimerProjectionChildRun, ref)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, expected); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(buckets)
	if err != nil {
		t.Fatal(err)
	}
	entry := ref.StageEntry()
	for _, postgres := range []bool{false, true} {
		for _, fault := range []string{"exact", "fieldless", "missing_header", "wrong_path", "wrong_template", "missing_type", "wrong_type", "missing_entity", "missing_arm", "read_failure"} {
			t.Run(map[bool]string{false: "sqlite/", true: "postgres/"}[postgres]+fault, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				path, template, entityType, expectedType := entry.InstancePath, entry.FlowScope, any("sample"), "sample"
				switch fault {
				case "fieldless":
					entityType, expectedType = nil, ""
				case "wrong_path":
					path = "orders/other"
				case "wrong_template":
					template = "other"
				case "missing_type":
					entityType = nil
				case "wrong_type":
					entityType = "foreign"
				}
				rows := sqlmock.NewRows([]string{"instance_path", "flow_template", "entity_type", "accumulator"})
				if fault != "missing_header" {
					rows.AddRow(path, template, entityType, string(raw))
				}
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT instance_path, flow_template, entity_type, accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$2`)).
					WithArgs(entry.RunID, entry.EntityID).WillReturnRows(rows)
				if fault == "exact" || fault == "missing_entity" || fault == "missing_arm" || fault == "read_failure" {
					query := mock.ExpectQuery(regexp.QuoteMeta(`SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3 AND entity_type=$4`)).
						WithArgs(entry.RunID, entry.EntityID, path, entityType)
					state := sqlmock.NewRows([]string{"accumulator"})
					switch fault {
					case "missing_entity":
					case "missing_arm":
						state.AddRow(`{}`)
					case "read_failure":
						query.WillReturnError(errors.New("original entity read failed"))
					default:
						state.AddRow(string(raw))
					}
					if fault != "read_failure" {
						query.WillReturnRows(state)
					}
				}
				mock.ExpectRollback()
				rollback := errors.New("require-only proof rollback")
				check := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					err := requireRunForkRemovedArrivalArm(ctx, attempt, ref, expectedType, expected)
					if (err == nil) != (fault == "exact" || fault == "fieldless") {
						t.Fatalf("%s native dependent readback: %v", fault, err)
					}
					return struct{}{}, rollback
				}
				ctx := correlation.WithRunID(context.Background(), entry.RunID)
				var result mutationprotocol.Result[struct{}]
				if postgres {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, check)
				} else {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunSQLite(ctx, backend, "removed-arrival-readback", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, check)
				}
				if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
					t.Fatalf("readback escaped original rollback: %v", result.Err())
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
