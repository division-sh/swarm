package construction

import (
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
}
