package postgres

import (
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestConnectionCapacitySeparatesReservationsWithoutResizing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(5)
	backend, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	assertCapacity := func(maximum, dedicated int) {
		t.Helper()
		before := db.Stats().MaxOpenConnections
		gotMaximum, gotDedicated, err := backend.ConnectionCapacity()
		if err != nil || gotMaximum != maximum || gotDedicated != dedicated {
			t.Fatalf("capacity=%d/%d err=%v, want %d/%d", gotMaximum, gotDedicated, err, maximum, dedicated)
		}
		if db.Stats().MaxOpenConnections != before {
			t.Fatal("capacity inspection resized the pool")
		}
	}
	assertCapacity(5, 0)
	releaseFirst := backend.RetainConnectionCapacity()
	defer releaseFirst()
	assertCapacity(6, 1)
	releaseSecond := backend.RetainConnectionCapacity()
	defer releaseSecond()
	assertCapacity(7, 2)
	releaseFirst()
	releaseFirst()
	assertCapacity(6, 1)
	releaseSecond()
	assertCapacity(5, 0)
	// A separately configured lower limit must be observed, not corrected by a
	// diagnostic read using stale base-capacity bookkeeping.
	db.SetMaxOpenConns(3)
	assertCapacity(3, 0)
}
