package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/google/uuid"
)

func TestToolEntityReadbackProjectsLoopsWithoutReservedState(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC().Round(time.Microsecond)
	accumulated := operatorLoopAccumulatedJSON(t, uuid.NewString(), uuid.NewString(), now)
	rows, err := db.Query(`
		SELECT 'entity-1', 'run-1', 'review/primary', 'mvp_spec', 'Primary', 'collecting',
		       '{}', '{}', ?, 1, ?, ?, ?
	`, accumulated, now, now, now)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got, err := scanToolEntityRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("tool rows = %#v", got)
	}
	accumulator, _ := got[0]["accumulator"].(map[string]any)
	if _, leaked := accumulator[loopruntime.BucketKey]; leaked {
		t.Fatalf("reserved loop state leaked through tool readback: %#v", accumulator)
	}
	loops, ok := got[0]["loops"].([]loopruntime.PublicActivation)
	if !ok || len(loops) != 1 || loops[0].ID != "revision" || loops[0].Status != loopruntime.StatusOpen {
		t.Fatalf("tool loop projection = %#v", got[0]["loops"])
	}
}
