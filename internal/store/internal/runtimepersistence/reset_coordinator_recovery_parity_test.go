package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestResetCoordinatorContinuesDurableReceiptsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"planned", "quiesced", "cleanup_committed", "completed"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				var selected interface {
					startupownership.Store
					destructivereset.InventoryReader
					destructivereset.LockManager
					destructivereset.QuiescenceStore
				}
				var db *sql.DB
				if backend == "sqlite" {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected, db = s, s.backend.ConstructionHandle()
				} else {
					_, database, _ := testutil.StartPostgres(t)
					selected, db = newTestPostgresStore(t, database), database
				}
				ctx := testAuthorActivityContext()
				cap, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-continuation"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cap.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				seedRun := func() string {
					t.Helper()
					id := uuid.NewString()
					fixture := runlifecyclefixture.Fixture{RunID: id, Origin: runlifecyclefixture.ScenarioSetupOrigin()}
					if backend == "sqlite" {
						runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
					} else {
						runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
					}
					return id
				}
				firstRun := seedRun()
				lifecycle := &resetReceiptLifecycle{completed: map[string]bool{}}
				coordinator := &destructivereset.Coordinator{
					Operations: cap, RuntimeContexts: lifecycle, Locks: selected,
					Planner:    destructivereset.InventoryPlanner{Reader: selected},
					Quiescer:   destructivereset.Quiescer{Store: selected},
					Cleaner:    destructivereset.Cleaner{Store: resetRetainedCleanup{cap}},
					Containers: resetNoContainerTargets{},
				}
				req := destructivereset.Request{OperationID: uuid.NewString(), ActorTokenID: "operator", IdempotencyKey: "recovery", RequestHash: "hash", IncludeSourceArtifactsSet: true}
				dropFault := installResetReceiptFault(t, db, backend, phase)
				if _, err := coordinator.Execute(ctx, req); err == nil {
					t.Fatal("receipt fault did not interrupt coordinator")
				}
				before, err := cap.ReadResetOperation(ctx, req.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				dropFault()
				// Once the plan exists, a later canonical run must remain outside
				// cleanup, including when only the final receipt failed.
				laterRun := ""
				if before.Plan != nil {
					laterRun = seedRun()
				}
				if err := coordinator.RecoverPending(ctx); err != nil {
					t.Fatal(err)
				}
				after, err := cap.ReadResetOperation(ctx, req.OperationID)
				if err != nil || after.Phase != destructivereset.PhaseCompleted {
					t.Fatalf("recovery = %+v, %v", after, err)
				}
				if before.Plan != nil && !reflect.DeepEqual(before.Plan, after.Plan) {
					t.Fatal("recovery inventoried later work")
				}
				if before.Quiescence != nil && !reflect.DeepEqual(before.Quiescence, after.Quiescence) {
					t.Fatal("recovery replaced cancellation evidence")
				}
				if before.Cleanup != nil && !reflect.DeepEqual(before.Cleanup, after.Cleanup) {
					t.Fatal("recovery replaced committed cleanup evidence")
				}
				for id, want := range map[string]int{firstRun: 0, laterRun: 1} {
					if id == "" {
						continue
					}
					var count int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs WHERE run_id = $1", id).Scan(&count); err != nil || count != want {
						t.Fatalf("run %s count = %d, %v; want %d", id, count, err, want)
					}
				}
				begins, completions := lifecycle.begins, lifecycle.completions
				req.OperationID = uuid.NewString()
				replayed, err := coordinator.Execute(ctx, req)
				if err != nil || !reflect.DeepEqual(&replayed, after.Response) || lifecycle.begins != begins || lifecycle.completions != completions {
					t.Fatalf("historical replay touched current lifecycle: %v", err)
				}
				if completions != 1 {
					t.Fatalf("same operation reconstructed %d times", completions)
				}
			})
		}
	}
}

// The runtime-free fixture proves selected persistence continuation, not served
// runtime reconstruction or process-death recovery.
type resetReceiptLifecycle struct {
	id                  string
	completed           map[string]bool
	begins, completions int
}

func (l *resetReceiptLifecycle) BeginDestructiveReset(_ context.Context, id string) (destructivereset.RuntimeReset, error) {
	l.id = id
	l.begins++
	return l, nil
}
func (l *resetReceiptLifecycle) Complete(context.Context, bool) error {
	if !l.completed[l.id] {
		l.completed[l.id] = true
		l.completions++
	}
	return nil
}
func (*resetReceiptLifecycle) Release() {}

type resetRetainedCleanup struct {
	capability startupownership.ProcessCapability
}

func (s resetRetainedCleanup) ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	return s.capability.ApplyDestructiveResetCleanup(ctx, req, nil)
}

type resetNoContainerTargets struct{}

func (resetNoContainerTargets) Apply(_ context.Context, req destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error) {
	if len(req.Result.Plan.ManagedContainers) != 0 {
		return destructivereset.ContainerResetResult{}, errors.New("runtime-free fixture cannot settle container targets")
	}
	return destructivereset.ContainerResetResult{OperationName: destructivereset.DefaultOperationName}, nil
}
