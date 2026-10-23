package selected

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestAdmissionInspectionMissingSQLiteDoesNotCreateState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "store.db")
	inspection, err := OpenAdmissionInspection(context.Background(), AuthorityRequest{
		Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: path},
	})
	if err == nil {
		inspection.Close()
		t.Fatal("missing SQLite store was presented as a readable empty store")
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created state directory: %v", err)
	}
}

func TestAdmissionInspectionRealReadScopeBothStores(t *testing.T) {
	ctx := context.Background()
	rooted, _ := sourceartifactfixture.Rooted(t, "inspection", sourceartifactfixture.Artifact())
	spec, err := runtimecontracts.LoadPlatformSpecDocument(filepath.Join(selectedStoreRepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	request := store.SchemaBootstrapRequest{PlatformPlans: plans, Origin: store.RuntimeStoreOrigin{
		SwarmVersion: "admission-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, backend := range []storebackend.Backend{storebackend.BackendSQLite, storebackend.BackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			selection := AuthorityRequest{Selection: storebackend.Selection{Backend: backend}}
			var bootstrap store.SchemaBootstrapper
			var db *sql.DB
			if backend == storebackend.BackendSQLite {
				selection.Selection.SQLitePath = filepath.Join(t.TempDir(), "store.db")
				selected, handle, err := storeconstruction.OpenSQLiteRuntime(selection.Selection.SQLitePath)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = selected.Close() })
				bootstrap, db = selected, handle
			} else {
				selection.PostgresDSN, _, _ = testutil.StartEmptyPostgres(t)
				selected, handle, err := storeconstruction.OpenPostgres(selection.PostgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = selected.Close() })
				bootstrap, db = selected, handle
			}
			inspection, err := OpenAdmissionInspection(ctx, selection)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := inspection.Close(); err != nil {
					t.Error(err)
				}
			})
			fresh, err := inspection.Inspect(ctx, request, func(snapshot *AdmissionSnapshot) error {
				for _, read := range inspectionRootReads(snapshot, rooted.Admission().Reference()) {
					if got, err := read.load(ctx); err == nil || got.Validate() == nil {
						t.Fatalf("fresh inspection admitted %s source: %v", read.name, err)
					}
				}
				if _, err := snapshot.InspectRunMutationDrift(ctx, "00000000-0000-0000-0000-000000000001"); err == nil {
					t.Fatal("fresh store admitted mutation drift reads")
				}
				if _, err := snapshot.ActiveNonStandingRunBundleAvailabilities(ctx); err == nil {
					t.Fatal("fresh store admitted unprepared domain reads")
				}
				return nil
			})
			if err != nil || !fresh.Fresh {
				t.Fatalf("fresh inspection: %+v, %v", fresh, err)
			}
			if err := bootstrap.BootstrapSchema(ctx, request); err != nil {
				t.Fatal(err)
			}
			writer, ok := bootstrap.(sourceartifactfixture.RootedWriter)
			if !ok {
				t.Fatal("native bootstrap fixture does not retain its rooted source owner")
			}
			if _, err := writer.EnsureRootedSource(ctx, rooted); err != nil {
				t.Fatal(err)
			}
			var retained *AdmissionSnapshot
			result, err := inspection.Inspect(ctx, request, func(snapshot *AdmissionSnapshot) error {
				retained = snapshot
				for _, read := range inspectionRootReads(snapshot, rooted.Admission().Reference()) {
					got, err := read.load(ctx)
					if read.present {
						if err != nil || got.Admission().Reference() != rooted.Admission().Reference() || got.Validate() != nil {
							t.Fatalf("rooted inspection %s: %+v err=%v", read.name, got.Admission().Reference(), err)
						}
					} else if err == nil || got.Validate() == nil {
						t.Fatalf("missing %s responsibility disclosed source: %v", read.name, err)
					}
				}
				_, driftErr := snapshot.InspectRunMutationDrift(ctx, "00000000-0000-0000-0000-000000000001")
				var missing *runlifecycle.RunNotFoundError
				if !errors.As(driftErr, &missing) {
					t.Fatalf("missing run lost its typed identity: %v", driftErr)
				}
				if _, ok := any(snapshot).(manager.RunExecutionOwner); ok {
					t.Fatal("inspection exposed generation-grant ownership")
				}
				if _, err := snapshot.InspectAuthority(ctx); err != nil {
					return err
				}
				if _, err := snapshot.ActiveNonStandingRunBundleAvailabilities(ctx); err != nil {
					return err
				}
				if err := runbundle.AdmitPinnedSources(ctx, snapshot, "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}); err != nil {
					return err
				}
				if _, err := startuprecovery.Inspect(ctx, startuprecovery.Request{AvailabilityReader: snapshot, ArtifactReader: snapshot}); err != nil {
					return err
				}
				if _, err := snapshot.ReadTimerObligations(ctx, timerobligation.All(), time.Now().UTC()); err != nil {
					return err
				}
				if _, err := snapshot.ReadResetInventory(ctx); err != nil {
					return err
				}
				if pending, err := destructivereset.InspectPendingOperations(ctx, snapshot); err != nil || len(pending) != 0 {
					t.Fatalf("empty reset ledger inspection: %+v err=%v", pending, err)
				}
				if _, err := snapshot.ListSelectedForkRecoveryEntries(ctx); err != nil {
					return err
				}
				if _, err := snapshot.ListSelectedContractRouteRecoveryRecords(ctx); err != nil {
					return err
				}
				if inventory, err := channelonboarding.InspectRetainedActivations(ctx, snapshot); err != nil || len(inventory.Operations) != 0 || len(inventory.Activations) != 0 {
					t.Fatalf("empty connected channel read scope: %+v %v", inventory, err)
				}
				if pending, err := channelonboarding.InspectPendingTeardowns(ctx, snapshot); err != nil || len(pending) != 0 {
					t.Fatalf("empty channel teardown read scope: %+v %v", pending, err)
				}
				source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				if err != nil {
					return err
				}
				state, err := manager.InspectRecoverableStateSnapshot(ctx, source, snapshot)
				if err != nil || state.HasRecoverableWork() {
					t.Fatalf("empty manager inventory: %+v, %v", state, err)
				}
				decision, err := runtimecore.InspectStartupRecoveryAdmission(ctx, runtimecore.StartupRecoveryReadRequest{
					SourceArtifact: source, ObservedAt: time.Now().UTC(), Delivery: snapshot, Timers: snapshot, StandingRestarts: snapshot,
					ReadManagerState: func(ctx context.Context) (manager.RecoverableStateSnapshot, error) {
						return manager.InspectRecoverableStateSnapshot(ctx, source, snapshot)
					},
				})
				if err != nil || decision["recovery_inspection_complete"] != true || decision["recoverable_work_present"] != false {
					t.Fatalf("recovery inspection: %+v, %v", decision, err)
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				for _, read := range inspectionRootReads(snapshot, rooted.Admission().Reference()) {
					if got, err := read.load(cancelled); !errors.Is(err, context.Canceled) || got.Validate() == nil {
						t.Fatalf("canceled rooted inspection %s disclosed source: %v", read.name, err)
					}
				}
				if _, err := snapshot.ActiveNonStandingRunBundleAvailabilities(cancelled); !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
				if _, err := snapshot.InspectRunMutationDrift(cancelled, "00000000-0000-0000-0000-000000000001"); !errors.Is(err, context.Canceled) {
					t.Fatalf("drift read lost cancellation: %v", err)
				}
				return nil
			})
			if err != nil || result.Fresh {
				t.Fatalf("compatible inspection: %+v, %v", result, err)
			}
			for _, read := range inspectionRootReads(retained, rooted.Admission().Reference()) {
				if got, err := read.load(ctx); !errors.Is(err, context.Canceled) || got.Validate() == nil {
					t.Fatalf("expired rooted inspection %s disclosed source: %v", read.name, err)
				}
			}
			if _, err := retained.InspectAuthority(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatalf("expired snapshot escaped to authority reader: %v", err)
			}
			if _, err := retained.InspectRunMutationDrift(context.Background(), "00000000-0000-0000-0000-000000000001"); !errors.Is(err, context.Canceled) {
				t.Fatalf("expired snapshot escaped to mutation drift reader: %v", err)
			}
			if _, err := retained.PendingResetOperations(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatalf("expired snapshot escaped to reset ledger reader: %v", err)
			}
			if _, err := channelonboarding.InspectRetainedActivations(ctx, retained); !errors.Is(err, context.Canceled) {
				t.Fatalf("expired snapshot escaped to channel reader: %v", err)
			}
			if _, err := channelonboarding.InspectPendingTeardowns(ctx, retained); !errors.Is(err, context.Canceled) {
				t.Fatalf("expired snapshot escaped to teardown reader: %v", err)
			}
			stamp, err := generationauthority.ReadProcessGeneration(ctx, db, backend == storebackend.BackendSQLite)
			if err != nil {
				t.Fatal(err)
			}
			if stamp.Generation != 0 {
				t.Fatal("read inspection acquired process authority")
			}
		})
	}
}
