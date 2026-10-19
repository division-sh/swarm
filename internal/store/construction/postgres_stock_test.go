package construction

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/lib/pq"
)

func TestOpenPostgresUsesStockDriver(t *testing.T) {
	store, db, err := OpenPostgres("host=127.0.0.1 port=1 sslmode=disable")
	if err != nil || store == nil || db == nil {
		t.Fatalf("stock construction requires no transport policy: %v", err)
	}
	defer db.Close()
	if _, ok := db.Driver().(*pq.Driver); !ok {
		t.Fatalf("construction driver = %T, want stock pq", db.Driver())
	}
	observer := NewChannelLockOrder()
	_, driverAccess := any(observer).(interface{ Driver() driver.Driver })
	_, connectionAccess := any(observer).(interface {
		Connect(context.Context) (driver.Conn, error)
	})
	if driverAccess || connectionAccess {
		t.Fatal("fixed scheduling observer exposes connection authority")
	}
	observed, handle, err := openPostgres("host=127.0.0.1 port=1 sslmode=disable", observer)
	if err != nil || observed == nil || handle == nil {
		t.Fatalf("fixed observation must use the original construction owner: %v", err)
	}
	defer handle.Close()
	for _, reset := range []bool{false, true} {
		if reset {
			observer.Reset()
		}
		if _, _, err := openPostgres("host=127.0.0.1 port=1 sslmode=disable", observer); err == nil {
			t.Fatal("fixed observer may attach only to its original construction, including after reset")
		}
	}
	if _, err := NewPostgres("host=127.0.0.1 port=1 sslmode=disable", nil); err == nil {
		t.Fatal("an explicitly supplied nil observer must refuse")
	}
}
