package genericschedule

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func TestPublishedOccurrenceReadbackUsesCanonicalCompleteRunCensus(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, fault := range []string{"exact", "prepared", "foreign", "missing_occurrence", "corrupt", "read_error"} {
			t.Run(fmt.Sprintf("postgres=%t/%s", postgres, fault), func(t *testing.T) {
				request := forkJoinRequestFixture(t, ".", true, false, false)
				activation, err := request.Expected(uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				wire := forkJoinInventoryWire(t, activation)
				at := activation.CurrentDueAt.Add(time.Second)
				wire[27], wire[28] = runtimegenericschedule.OccurrenceEventID(activation.ID, activation.CurrentDueAt), at
				wire[29], wire[32], wire[33] = "fired", at, at
				switch fault {
				case "prepared":
					wire[29], wire[32], wire[33] = "active", nil, nil
				case "foreign":
					wire[3] = uuid.NewString()
				case "missing_occurrence":
					wire[27], wire[28] = "", nil
				case "corrupt":
					wire[2] = "bad-hash"
				}
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				query := activationSelectColumns + ` FROM timers WHERE run_id = ? AND status = ?
					AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY timer_id`
				if postgres {
					query = activationSelectColumns + ` FROM timers WHERE run_id = $1::uuid AND status = $2
						AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY timer_id`
				}
				expect := mock.ExpectQuery("^"+regexp.QuoteMeta(query)+"$").WithArgs(request.Child.RunID, "fired")
				if fault == "read_error" {
					expect.WillReturnError(fmt.Errorf("schedule census failed"))
				} else {
					columns := make([]string, len(wire))
					for i := range columns {
						columns[i] = fmt.Sprint(i)
					}
					expect.WillReturnRows(sqlmock.NewRows(columns).AddRow(wire...))
				}
				rows, err := ReadPublishedOccurrencesTx(t.Context(), tx, postgres, request.Child.RunID)
				if fault == "exact" || fault == "missing_occurrence" {
					// The publication validator, not this row census, proves the event.
					if err != nil || len(rows) != 1 {
						t.Fatalf("canonical census: rows=%v err=%v", rows, err)
					}
				} else if err == nil || rows != nil {
					t.Fatalf("invalid census returned partial evidence: rows=%v err=%v", rows, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
