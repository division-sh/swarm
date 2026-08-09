package delivery

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDatabaseNowPreservesSQLiteSubsecondPrecision(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	want := time.Date(2026, 8, 9, 12, 34, 56, int(789*time.Millisecond), time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT ` + sqliteDatabaseNowExpression)).
		WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(want.Format(time.RFC3339Nano)))

	adapter, err := NewAdapter(DialectSQLite)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := adapter.databaseNow(context.Background(), db)
	if err != nil {
		t.Fatalf("databaseNow: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("databaseNow = %s, want %s", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAgentPendingRetryEligibilityUsesCanonicalDatabaseClock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		predicate  string
		expression string
	}{
		{name: "sqlite", predicate: sqliteAgentPendingEligibility, expression: sqliteDatabaseNowExpression},
		{name: "postgres", predicate: postgresAgentPendingEligibility, expression: postgresDatabaseNowExpression},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.predicate, tc.expression) {
				t.Fatalf("pending retry predicate does not use canonical database clock %q", tc.expression)
			}
		})
	}
}
