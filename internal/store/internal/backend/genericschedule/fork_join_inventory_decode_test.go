package genericschedule

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func forkJoinInventoryWire(t *testing.T, row runtimegenericschedule.Activation) []driver.Value {
	t.Helper()
	payload, err := canonicaljson.Encode(row.Command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(row.Command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	o := row.ForkJoinOrigin
	return []driver.Value{
		row.ID, row.Command.ScheduleKey, row.ImmutableHash, row.Command.RunID, row.Command.EntityID, row.Command.FlowInstance,
		string(row.Command.OwnerKind), row.Command.OwnerID, "", "", "", "", "", row.Command.EventType, payload, routing,
		string(row.Command.ExecutionMode), "", string(row.Command.Due.Kind), row.Command.Due.Absolute, "", "", row.Command.TaskID,
		row.ImmutableHash, row.AdmittedAt, row.InitialDueAt, row.CurrentDueAt, "", nil, string(row.Status), "", nil, nil, nil, nil, "", "", nil,
		o.SourceActivationID, o.SourceRunID, string(o.PointKind), o.PointRevision, o.PointEventID, o.SourceAdmittedAt, o.Owner,
	}
}

func TestForkJoinInventoryCanonicalDecoderRefusesIncompleteLineage(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, name := range []string{"exact", "source_activation", "source_run", "point_kind", "point_revision", "source_admission", "owner", "ordinary", "foreign_row", "read_error"} {
			t.Run(map[bool]string{false: "sqlite/", true: "postgres/"}[postgres]+name, func(t *testing.T) {
				request := forkJoinRequestFixture(t, ".", true, false, false)
				row, err := request.Expected(uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				wire := forkJoinInventoryWire(t, row)
				switch name {
				case "source_activation":
					wire[38] = ""
				case "source_run":
					wire[39] = ""
				case "point_kind":
					wire[40] = ""
				case "point_revision":
					wire[41] = int64(0)
				case "source_admission":
					wire[43] = nil
				case "owner":
					wire[44] = "unrelated_owner"
				case "ordinary":
					wire[38], wire[39], wire[40], wire[41], wire[42], wire[43], wire[44] = "", "", "", int64(0), "", nil, ""
				case "foreign_row":
					wire[3] = uuid.NewString()
				}
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				// This query contract catches dropping a partial-lineage predicate,
				// status filtering or per-key lookup instead of complete inventory.
				query := activationSelectColumns + ` FROM timers WHERE run_id = ?
					AND task_type IN ('timer','scheduled_task','global_recurring')
					AND (source_timer_id IS NOT NULL OR forked_from_run_id IS NOT NULL
					 OR forked_from_point_kind IS NOT NULL OR forked_from_point_revision IS NOT NULL
					 OR forked_from_event_id IS NOT NULL OR source_armed_at IS NOT NULL OR reconstruction_owner IS NOT NULL)
					ORDER BY timer_id`
				if postgres {
					query = strings.Replace(query, "run_id = ?", "run_id = $1::uuid", 1) + " FOR UPDATE"
				}
				read := mock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$").WithArgs(request.Child.RunID)
				if name == "read_error" {
					read.WillReturnError(errors.New("inventory read failed"))
				} else {
					columns := make([]string, len(wire))
					for i := range columns {
						columns[i] = fmt.Sprint(i)
					}
					read.WillReturnRows(sqlmock.NewRows(columns).AddRow(wire...))
				}
				mock.ExpectRollback()
				stop := errors.New("unit rollback")
				ctx := correlation.WithRunID(context.Background(), request.Child.RunID)
				check := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					actual, err := ReadForkJoinInventoryTx(ctx, attempt, postgres, request.Child.RunID)
					if name == "exact" {
						if err != nil || len(actual) != 1 {
							t.Fatalf("exact inventory: rows=%d err=%v", len(actual), err)
						}
						want, _ := row.EvidenceDigest()
						got, _ := actual[0].EvidenceDigest()
						if want != got {
							t.Fatal("canonical row decoder lost inherited evidence")
						}
					} else if err == nil || actual != nil {
						t.Fatalf("%s lineage acknowledged: rows=%v err=%v", name, actual, err)
					}
					return struct{}{}, stop
				}
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
					result = mutationprotocol.RunSQLite(ctx, backend, "join inventory decode unit", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, check)
				}
				if result.Acknowledged() || !errors.Is(result.Err(), stop) {
					t.Fatalf("unit frame: err=%v", result.Err())
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
