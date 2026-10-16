package runtimepersistence

import (
	"database/sql"
	"testing"
	"time"
)

func TestSelectedPoolEvidencePreservesOriginalAndWaitStatistics(t *testing.T) {
	stats := sql.DBStats{
		InUse: 3, Idle: 5, OpenConnections: 8, MaxOpenConnections: 13,
		WaitCount: 21, WaitDuration: 34*time.Second + 55*time.Nanosecond,
	}
	want := SelectedPoolEvidence{
		InUse: 3, Idle: 5, OpenConnections: 8, MaxOpenConnections: 13,
		WaitCount: 21, WaitDuration: 34*time.Second + 55*time.Nanosecond,
	}
	if got := selectedPoolEvidence(stats); got != want {
		t.Fatalf("selected pool statistics = %#v, want exact original values %#v", got, want)
	}
	if got := selectedPoolEvidence(sql.DBStats{}); got != (SelectedPoolEvidence{}) {
		t.Fatalf("empty statistics manufactured pool activity: %#v", got)
	}
	var nilPG *PostgresStore
	var nilSQLite *SQLiteRuntimeStore
	for _, foreign := range []any{nil, nilPG, nilSQLite, stats} {
		if got, err := ObserveSelectedPoolForTest(foreign); err == nil || got != (SelectedPoolEvidence{}) {
			t.Fatalf("foreign %T supplied selected pool evidence: %#v, %v", foreign, got, err)
		}
	}
}
