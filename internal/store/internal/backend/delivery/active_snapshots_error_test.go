package delivery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestActiveSnapshotsRefusesInterruptedEnumeration(t *testing.T) {
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		for _, cut := range []string{"query", "before_first", "after_first", "close", "scan_and_close", "empty"} {
			t.Run(string(dialect)+"/"+cut, func(t *testing.T) {
				proveActiveSnapshotsEnumerationError(t, dialect, cut)
			})
		}
	}
}

func proveActiveSnapshotsEnumerationError(t *testing.T, dialect Dialect, cut string) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mock.ExpectClose()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	query := `SELECT delivery_id FROM event_deliveries WHERE status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	if dialect == DialectPostgres {
		query = `SELECT delivery_id::text FROM event_deliveries WHERE status IN ('pending', 'in_progress', 'failed') ORDER BY delivery_id`
	}
	interrupted := errors.New("active membership enumeration interrupted")
	closeErr := errors.New("active membership cursor close failed")
	rows := sqlmock.NewRows([]string{"delivery_id"})
	want := interrupted
	switch cut {
	case "query":
		mock.ExpectQuery(query).WillReturnError(interrupted)
	case "before_first":
		rows.AddRow(uuid.NewString()).RowError(0, interrupted)
	case "after_first":
		rows.AddRow(uuid.NewString()).AddRow(uuid.NewString()).RowError(1, interrupted)
	case "close":
		rows.CloseError(closeErr)
		want = closeErr
	case "scan_and_close":
		rows = sqlmock.NewRows([]string{"delivery_id", "unexpected"}).AddRow(uuid.NewString(), "extra").CloseError(closeErr)
		want = closeErr
	case "empty":
		want = nil
	default:
		t.Fatalf("unknown enumeration cut %q", cut)
	}
	if cut != "query" {
		mock.ExpectQuery(query).WillReturnRows(rows).RowsWillBeClosed()
	}
	adapter, err := NewAdapter(dialect)
	if err != nil {
		t.Fatal(err)
	}
	// No row-decoder query is expected after an interrupted ID enumeration.
	// The after_first cut must refuse before interpreting even its first ID.
	got, err := adapter.ActiveSnapshots(context.Background(), db)
	if want == nil {
		if err != nil || len(got) != 0 {
			t.Fatalf("successful empty native enumeration: %+v err=%v", got, err)
		}
		return
	}
	if !errors.Is(err, want) || got != nil {
		t.Fatalf("enumeration lost error or returned partial membership: %+v want=%v err=%v", got, want, err)
	}
	if cut == "scan_and_close" && !strings.Contains(err.Error(), "expected 2 destination arguments") {
		t.Fatalf("scan and cleanup failure did not both survive: %v", err)
	}
}
