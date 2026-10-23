package runtimepersistence_test

import (
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func selectedDatabase(t *testing.T, selectedStore any) *sql.DB {
	t.Helper()
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		return storetest.DatabaseForTest(selected)
	case *store.SQLiteRuntimeStore:
		return storetest.DatabaseForTest(selected)
	default:
		t.Fatalf("selected store %T has no canonical database", selectedStore)
		return nil
	}
}
