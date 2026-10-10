package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestExternalAttemptStoragePreservesPhysicalMultiplicityAndState(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "attempts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE runtime_external_effect_attempts(attempt_id TEXT,operation_id TEXT,state TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][3]string{{"1", "operation-a", "settled"}, {"2", "operation-a", "settled"}, {"3", "operation-b", "authorized"}} {
		if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts VALUES(?,?,?)`, row[0], row[1], row[2]); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner := &EffectSQLiteOwner{backend: backend, requireCurrent: func() error { return nil }}
	got, err := owner.ReadExternalAttemptStorageForTest(context.Background())
	want := []ExternalAttemptStorage{{"operation-a", "settled"}, {"operation-a", "settled"}, {"operation-b", "authorized"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("physical attempts were filtered, deduplicated or normalized: %+v/%v", got, err)
	}
	stale := errors.New("schema is stale")
	owner.requireCurrent = func() error { return stale }
	if got, err := owner.ReadExternalAttemptStorageForTest(context.Background()); !errors.Is(err, stale) || got != nil {
		t.Fatalf("stale schema supplied evidence: %+v/%v", got, err)
	}
}
