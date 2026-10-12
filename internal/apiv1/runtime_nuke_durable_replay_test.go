package apiv1

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func (*countingAdministrativeResetLifecycle) ResetSourceProjections(context.Context) ([]destructivereset.SourceProjection, error) {
	return nil, nil
}

func TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration(t *testing.T) {
	selected := storetest.StartPostgresRuntimeStore(t)
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
	if err := storetest.InstallPostgresResetAPICompletionLoss(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	restoreCompletion := sync.OnceValue(func() error {
		return storetest.RemovePostgresResetAPICompletionLoss(context.Background(), selected)
	})
	defer func() {
		if err := restoreCompletion(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Now().UTC()
	req := Request{Method: "runtime.nuke", ActorTokenID: "operator", RequestHash: "reset", Params: map[string]any{"idempotency_key": "permanent-reset", "include_source_artifacts": false}}
	opts := RuntimeNukeHandlerOptions{Coordinator: coordinator, Idempotency: selected}
	if _, err := executeRuntimeNuke(ctx, req, opts, now); err == nil {
		t.Fatal("API completion fault was not exercised")
	} else if !strings.Contains(err.Error(), "API completion lost") {
		t.Fatalf("reset failed before the actual transport completion cut: %v", err)
	}
	if err := restoreCompletion(); err != nil {
		t.Fatal(err)
	}
	raw, err := storetest.ReadResetCompletionStorageRecord(context.Background(), selected)
	if err != nil {
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
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: laterRun, Origin: storetest.ScenarioSetupOrigin()})
	first, err := executeRuntimeNuke(ctx, req, opts, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second, err := executeRuntimeNuke(ctx, req, opts, now.Add(48*time.Hour))
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("expired cache replay = %+v, %v", second, err)
	}
	if count, err := storetest.CountPhysicalRunIdentity(context.Background(), selected, laterRun); err != nil || count != 1 {
		t.Fatalf("historical replay affected later run: %d, %v", count, err)
	}
	if lifecycle.begins != 1 {
		t.Fatalf("replay withdrew execution %d times", lifecycle.begins)
	}
	if operations, err := storetest.CountResetCompletionStorageOperations(context.Background(), selected); err != nil || operations != 1 {
		t.Fatalf("operation identities = %d, %v", operations, err)
	}
}

type countingAdministrativeResetLifecycle struct{ begins int }

func (l *countingAdministrativeResetLifecycle) BeginDestructiveReset(context.Context, string) (destructivereset.RuntimeReset, error) {
	l.begins++
	return emptyAdministrativeResetLifecycle{}, nil
}
