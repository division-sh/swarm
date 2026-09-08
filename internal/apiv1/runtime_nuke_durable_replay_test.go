package apiv1

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	cap := acquireAdministrativeResetCapability(t, selected)
	lifecycle := &countingAdministrativeResetLifecycle{}
	external := newBlockingAdministrativeExternalWork()
	external.release()
	coordinator := &destructivereset.Coordinator{
		Operations: cap, RuntimeContexts: lifecycle, Planner: destructivereset.InventoryPlanner{Reader: selected},
		Locks: selected, Quiescer: destructivereset.Quiescer{Store: selected},
		Cleaner: destructivereset.Cleaner{Store: administrativeResetCleanup{cap}}, Containers: external,
	}
	ctx := context.Background()
	if _, err := db.Exec(`CREATE FUNCTION lose_reset_api_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'API completion lost'; END $$;
	CREATE TRIGGER lose_reset_api_completion BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION lose_reset_api_completion()`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	req := Request{Method: "runtime.nuke", ActorTokenID: "operator", RequestHash: "reset", Params: map[string]any{"idempotency_key": "permanent-reset", "include_source_artifacts": false}}
	opts := RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}
	if _, err := executeRuntimeNuke(ctx, req, opts, now); err == nil {
		t.Fatal("API completion fault was not exercised")
	}
	if _, err := db.Exec(`DROP TRIGGER lose_reset_api_completion ON api_idempotency; DROP FUNCTION lose_reset_api_completion()`); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db.QueryRow(`SELECT record FROM runtime_reset_operations`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var admitted destructivereset.Operation
	if err := json.Unmarshal(raw, &admitted); err != nil {
		t.Fatal(err)
	}
	if admitted.Phase != destructivereset.PhaseCompleted {
		t.Fatalf("domain completion was lost: %s", admitted.Phase)
	}
	laterRun := uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{RunID: laterRun, Origin: runlifecyclefixture.ScenarioSetupOrigin()})
	first, err := executeRuntimeNuke(ctx, req, opts, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second, err := executeRuntimeNuke(ctx, req, opts, now.Add(48*time.Hour))
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("expired cache replay = %+v, %v", second, err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = $1`, laterRun).Scan(&count); err != nil || count != 1 {
		t.Fatalf("historical replay affected later run: %d, %v", count, err)
	}
	if lifecycle.begins != 1 {
		t.Fatalf("replay withdrew execution %d times", lifecycle.begins)
	}
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_reset_operations`).Scan(&operations); err != nil || operations != 1 {
		t.Fatalf("operation identities = %d, %v", operations, err)
	}
}

type countingAdministrativeResetLifecycle struct{ begins int }

func (l *countingAdministrativeResetLifecycle) BeginDestructiveReset(context.Context) (destructivereset.RuntimeReset, error) {
	l.begins++
	return emptyAdministrativeResetLifecycle{}, nil
}
