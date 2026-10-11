package runtimepersistence_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type flowInstanceDescriptorAuthorityStore interface {
	runtimebus.FlowInstanceActivationCommitOwner
	storetest.RunFixtureStore
	externalStoreTestDurableEventBusStore
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.ScopedActiveFlowInstanceDescriptorLister
}

type dynamicFlowSourceProjectionStore interface {
	runtimebus.FlowInstanceActivationCommitOwner
	runtimebus.FlowInstanceRouteSetPersistence
	storetest.RunFixtureStore
	sourceartifactfixture.Writer
	runtimerunlifecycle.OperationOwner
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error)
	InspectDynamicFlowRuntimeReadinessForRun(context.Context, string, runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error)
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	ReconcileDynamicFlowRuntimeReadinessPlans(context.Context, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error)
}

func TestDynamicFlowRuntimeReadinessForSourceScopesInSQLBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) dynamicFlowSourceProjectionStore
	}{
		{"postgres", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartPostgresRuntimeStore(t)
		}},
		{"sqlite", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			sourceA := mustExternalStoreTestSourceArtifactFact()
			hashA := sourceA.BundleHash()
			sourceBArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  source_b: {}\n"))
			sourceB := sourceartifactfixture.FactFor(sourceBArtifact)
			hashB := sourceB.BundleHash()
			runA, runB := uuid.NewString(), uuid.NewString()
			ctx := testAuthorActivityContext()
			storetest.RequireRun(t, ctx, store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runA, BundleHash: hashA})
			storetest.RequireRun(t, ctx, store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runB, Artifact: sourceBArtifact})
			seedExactFlowInstanceDescriptorOwner(t, store, runA, uuid.NewString(), "account/a", hashA)
			seedExactFlowInstanceDescriptorOwner(t, store, runB, uuid.NewString(), "account/b", hashB)
			seedStaticFlowInstanceRouteWithoutReadiness(t, store, runA, "standing/a")
			if _, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceA); err == nil || !strings.Contains(err.Error(), "no dynamic runtime readiness owner") {
				t.Fatalf("static route without readiness was admitted: %v", err)
			}
			standing := runtimeflowidentity.RunScopedFlowInstance{RunID: runA, Route: runtimeflowidentity.RouteForInstancePath("standing/a")}
			if err := store.ReplaceFlowInstanceRouteRecords(ctx, standing, nil); err != nil {
				t.Fatal(err)
			}
			setReadinessCoordinate(t, store, runA, "account/a", "phase", time.Now().UTC())
			projection, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceA)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.CurrentCompleted) != 1 || projection.CurrentCompleted[0].InstancePath != "account/a" || len(projection.CurrentPending) != 0 {
				t.Fatalf("source A projection = %#v", projection)
			}
			if _, err := storetest.RemoveDescriptorReadiness(ctx, store, runB, "account/b"); err != nil {
				t.Fatal(err)
			}
			account := runtimeflowidentity.RunScopedFlowInstance{RunID: runB, Route: runtimeflowidentity.RouteForInstancePath("account/b")}
			if err := store.ReplaceFlowInstanceRouteRecords(ctx, account, []runtimebus.FlowInstanceRouteRecord{{
				Identity: account, EventPattern: "account/b/event", SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "account",
			}}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceB); err == nil || !strings.Contains(err.Error(), "no dynamic runtime readiness owner") {
				t.Fatalf("invalid source-owned route error = %v", err)
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessProjectionClassifiesSourceTransitionsBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) dynamicFlowSourceProjectionStore
	}{
		{"postgres", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartPostgresRuntimeStore(t)
		}},
		{"sqlite", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			current := mustExternalStoreTestSourceArtifactFact()
			currentHash := current.BundleHash()
			predecessorArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  predecessor: {}\n"))
			predecessor := sourceartifactfixture.FactFor(predecessorArtifact)
			predecessorHash := predecessor.BundleHash()
			foreignArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  foreign: {}\n"))
			foreign := sourceartifactfixture.FactFor(foreignArtifact)
			foreignHash := foreign.BundleHash()
			ctx := testAuthorActivityContext()
			type seeded struct {
				runID, path, planHash string
				complete              bool
			}
			rows := []seeded{
				{uuid.NewString(), "account/current-complete", currentHash, true},
				{uuid.NewString(), "account/current-pending", currentHash, false},
				{uuid.NewString(), "account/transition-complete", predecessorHash, true},
				{uuid.NewString(), "account/transition-pending", predecessorHash, false},
			}
			for _, row := range rows {
				if row.planHash == predecessorHash {
					requireReadinessRun(t, ctx, selected, row.runID, predecessorHash, predecessorArtifact)
				} else {
					requireReadinessRun(t, ctx, selected, row.runID, currentHash)
				}
				seedExactFlowInstanceDescriptorOwner(t, selected, row.runID, uuid.NewString(), row.path, row.planHash)
				if row.planHash == predecessorHash {
					if _, err := selected.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: row.runID, Source: current}); err != nil {
						t.Fatal(err)
					}
				}
				if row.complete {
					setReadinessCoordinate(t, selected, row.runID, row.path, "phase", time.Now().UTC())
				}
			}
			foreignRun := uuid.NewString()
			requireReadinessRun(t, ctx, selected, foreignRun, foreignHash, foreignArtifact)
			seedExactFlowInstanceDescriptorOwner(t, selected, foreignRun, uuid.NewString(), "account/foreign-malformed", foreignHash)
			setReadinessPlanRaw(t, selected, foreignRun, "account/foreign-malformed", `{}`)

			projection, err := selected.InspectDynamicFlowRuntimeReadinessForSource(ctx, current)
			if err != nil {
				t.Fatalf("inspect current source: %v", err)
			}
			if len(projection.CurrentCompleted) != 1 || len(projection.CurrentPending) != 1 || len(projection.SourceTransitionRequired) != 2 {
				t.Fatalf("projection classes = %#v", projection)
			}
			transitionPending := 0
			for _, item := range projection.SourceTransitionRequired {
				if item.Pending() {
					transitionPending++
				}
				if !item.OwningRunSource.Matches(current) {
					t.Fatalf("transition owning source = %#v", item.OwningRunSource)
				}
			}
			if transitionPending != 1 {
				t.Fatalf("pending transition count = %d, want 1", transitionPending)
			}
			exactRun, err := selected.InspectDynamicFlowRuntimeReadinessForRun(ctx, rows[2].runID, current)
			if err != nil || len(exactRun) != 1 || exactRun[0].InstancePath != rows[2].path {
				t.Fatalf("exact run projection = %#v err=%v", exactRun, err)
			}
			wrongSource, err := selected.InspectDynamicFlowRuntimeReadinessForRun(ctx, rows[2].runID, foreign)
			if err != nil || len(wrongSource) != 0 {
				t.Fatalf("wrong-source run projection = %#v err=%v", wrongSource, err)
			}
			if _, err := selected.InspectDynamicFlowRuntimeReadinessForSource(ctx, foreign); err == nil {
				t.Fatal("foreign malformed row was not decoded by its owning source")
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) dynamicFlowSourceProjectionStore
	}{
		{"postgres", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartPostgresRuntimeStore(t)
		}},
		{"sqlite", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			otherArtifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  other: {}\n"))
			otherSource := sourceartifactfixture.FactFor(otherArtifact)
			for _, race := range []string{"plan", "source", "topology", "creation", "run_status", "instance_status", "terminated_at"} {
				t.Run(race, func(t *testing.T) {
					runID := uuid.NewString()
					path := "account/guard-" + race + "-" + uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					requireReadinessRun(t, ctx, selected, runID, bundleHash)
					seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, bundleHash)
					observed, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if err != nil || !found {
						t.Fatalf("load observation: found=%v err=%v", found, err)
					}
					desired := observed.Plan
					desired.WorkflowVersion = "2.0.0"
					callCtx := ctx
					switch race {
					case "plan":
						changed := observed.Plan
						changed.WorkflowVersion = "concurrent"
						if _, err := storetest.SetDescriptorReadinessConcurrentPlan(ctx, selected, changed); err != nil {
							t.Fatal(err)
						}
					case "source":
						setReadinessRunSource(t, selected, callCtx, runID, otherSource, otherArtifact)
						desired.BundleHash = otherSource.BundleHash()
						callCtx = runtimecorrelation.WithSourceArtifactFact(callCtx, otherSource)
					case "topology":
						setReadinessCoordinate(t, selected, runID, path, "phase", time.Now().UTC())
					case "creation":
						setReadinessCoordinate(t, selected, runID, path, "creation_event_emitted_at", time.Now().UTC())
					case "run_status":
						setReadinessRunStatus(t, selected, callCtx, runID, runtimerunlifecycle.StatePaused)
					case "instance_status":
						setReadinessInstanceStatus(t, selected, runID, path, "draining")
					case "terminated_at":
						setReadinessCoordinate(t, selected, runID, path, "instance_terminated_at", time.Now().UTC())
					}
					results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(callCtx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: desired}}, time.Now().UTC())
					if len(results) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
						t.Fatalf("stale observation results=%v err=%v", results, err)
					}
					stored, found, loadErr := selected.LoadDynamicFlowRuntimeReadiness(callCtx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if loadErr != nil || !found {
						t.Fatalf("load guarded row: found=%v err=%v", found, loadErr)
					}
					if stored.Plan.WorkflowVersion == desired.WorkflowVersion {
						t.Fatalf("stale observation overwrote desired plan: %#v", stored.Plan)
					}
				})
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessRejectsABAObservationBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) dynamicFlowSourceProjectionStore
	}{
		{"postgres", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartPostgresRuntimeStore(t)
		}},
		{"sqlite", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			runID := uuid.NewString()
			path := "account/aba-" + uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
			requireReadinessRun(t, ctx, selected, runID, source.BundleHash())
			seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, source.BundleHash())
			load := func() runtimepipeline.DynamicFlowRuntimeReadiness {
				t.Helper()
				item, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
				if err != nil || !found {
					t.Fatalf("load readiness: found=%v err=%v", found, err)
				}
				return item
			}
			original := load()
			if original.AttemptOrdinal != 1 {
				t.Fatalf("initial revision = %d, want 1", original.AttemptOrdinal)
			}
			advance := func(observed runtimepipeline.DynamicFlowRuntimeReadiness, version string) {
				t.Helper()
				expected := observed.Plan
				expected.WorkflowVersion = version
				result, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: expected}}, time.Now().UTC())
				if err != nil || len(result) != 1 || !result[0].Changed || result[0].AttemptOrdinal != observed.AttemptOrdinal+1 {
					t.Fatalf("advance to %s: result=%#v err=%v", version, result, err)
				}
			}
			advance(original, "2.0.0")
			advance(load(), original.Plan.WorkflowVersion)
			staleDesired := original.Plan
			staleDesired.WorkflowVersion = "3.0.0"
			result, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: original, Expected: staleDesired}}, time.Now().UTC())
			if len(result) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
				t.Fatalf("stale A observation after A-B-A: result=%#v err=%v", result, err)
			}
			current := load()
			if current.AttemptOrdinal != 3 || current.Plan.WorkflowVersion != original.Plan.WorkflowVersion {
				t.Fatalf("stale observation changed current row: revision=%d plan=%#v", current.AttemptOrdinal, current.Plan)
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessPlanBatchIsAtomicBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) dynamicFlowSourceProjectionStore
	}{
		{"postgres", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartPostgresRuntimeStore(t)
		}},
		{"sqlite", func(t *testing.T) dynamicFlowSourceProjectionStore {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			seedBatch := func(t *testing.T, label string) (context.Context, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation) {
				t.Helper()
				runID := uuid.NewString()
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
				requireReadinessRun(t, ctx, selected, runID, bundleHash)
				requests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, 2)
				for index := range 2 {
					path := fmt.Sprintf("account/%s-%d-%s", label, index, uuid.NewString())
					seedExactFlowInstanceDescriptorOwner(t, selected, runID, uuid.NewString(), path, bundleHash)
					observed, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(path))
					if err != nil || !found {
						t.Fatalf("load batch observation %s: found=%v err=%v", path, found, err)
					}
					expected := observed.Plan
					expected.WorkflowVersion = "2.0.0"
					requests = append(requests, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{Observed: observed, Expected: expected})
				}
				return ctx, requests
			}
			assertVersion := func(t *testing.T, ctx context.Context, requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, version string) {
				t.Helper()
				for _, request := range requests {
					stored, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, request.Expected.RunID, request.Expected.Identity.Route())
					if err != nil || !found || stored.Plan.WorkflowVersion != version {
						t.Fatalf("stored batch row %s version=%q found=%v err=%v", request.Expected.Identity.InstancePath, stored.Plan.WorkflowVersion, found, err)
					}
				}
			}

			t.Run("success", func(t *testing.T) {
				ctx, requests := seedBatch(t, "success")
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err != nil || len(results) != 2 || !results[0].Changed || !results[1].Changed {
					t.Fatalf("batch success results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "2.0.0")
			})

			t.Run("last_row_validation_failure_changes_none", func(t *testing.T) {
				ctx, requests := seedBatch(t, "validation")
				requests[1].Expected.WorkflowVersion = ""
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err == nil || len(results) != 0 {
					t.Fatalf("batch validation results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})

			t.Run("last_row_conflict_changes_none", func(t *testing.T) {
				ctx, requests := seedBatch(t, "conflict")
				last := requests[1]
				setReadinessCoordinate(t, selected, last.Expected.RunID, last.Expected.Identity.InstancePath, "phase", time.Now().UTC())
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if len(results) != 0 || !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
					t.Fatalf("batch conflict results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})

			t.Run("later_update_failure_rolls_back", func(t *testing.T) {
				ctx, requests := seedBatch(t, "rollback")
				last := requests[1].Expected
				cleanup, err := storetest.InstallDescriptorReadinessBatchUpdateFailure(ctx, selected, last.RunID, last.Identity.InstancePath)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cleanup(context.Background()); err != nil {
						t.Error(err)
					}
				})
				results, err := selected.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, time.Now().UTC())
				if err == nil || len(results) != 0 {
					t.Fatalf("injected batch failure results=%#v err=%v", results, err)
				}
				assertVersion(t, ctx, requests, "1.0.0")
			})
		})
	}
}

func seedStaticFlowInstanceRouteWithoutReadiness(t *testing.T, selected interface {
	runtimebus.FlowInstanceActivationCommitOwner
	runtimebus.FlowInstanceRouteSetPersistence
}, runID, instancePath string) {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	at := time.Now().UTC()
	command, err := flowactivationfixture.Command(ctx, runtimepipeline.WorkflowInstance{
		WorkflowName: "standing", WorkflowVersion: "1.0.0", Mode: "static",
		StorageRef: instancePath, InstanceID: runtimeflowidentity.LogicalInstanceID(instancePath),
		EntityID: uuid.NewString(), EntityType: "standing", CurrentState: "active", StageDefined: true,
		ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: runID,
		CreatedAt: at, EnteredStageAt: at, UpdatedAt: at,
	}, runtimepipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct static route component: result=%+v err=%v", committed, err)
	}
	if _, err := storetest.RemoveDescriptorReadiness(ctx, selected, runID, instancePath); err != nil {
		t.Fatal(err)
	}
	owner := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: command.Plan.Identity.Route()}
	if err := selected.ReplaceFlowInstanceRouteRecords(ctx, owner, []runtimebus.FlowInstanceRouteRecord{{
		Identity: owner, EventPattern: instancePath + "/event", SubscriberType: "node",
		SubscriberID: "receiver", SourceFlow: "standing",
	}}); err != nil {
		t.Fatal(err)
	}
}

func TestActiveFlowInstanceDescriptorAuthorityScopesCensusToExactRunBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) flowInstanceDescriptorAuthorityStore
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) flowInstanceDescriptorAuthorityStore {
				return storetest.StartPostgresRuntimeStore(t)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) flowInstanceDescriptorAuthorityStore {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.setup(t)
			runA, runB := uuid.NewString(), uuid.NewString()
			entityA, entityB := uuid.NewString(), uuid.NewString()
			source := mustExternalStoreTestSourceArtifactFact()
			bundleHash := source.BundleHash()
			ctxA := runtimecorrelation.WithRunID(testAuthorActivityContext(), runA)
			ctxB := runtimecorrelation.WithRunID(testAuthorActivityContext(), runB)
			storetest.RequireRun(t, ctxA, selected, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runA, BundleHash: bundleHash,
			})
			storetest.RequireRun(t, ctxB, selected, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runB, BundleHash: bundleHash,
			})

			seedExactFlowInstanceDescriptorOwner(t, selected, runA, entityA, "account/a", bundleHash)
			seedExactFlowInstanceDescriptorOwner(t, selected, runB, entityB, "account/b", bundleHash)

			for _, check := range []struct {
				ctx   context.Context
				runID string
				path  string
			}{
				{ctx: ctxA, runID: runA, path: "account/a"},
				{ctx: ctxB, runID: runB, path: "account/b"},
			} {
				descriptors, err := selected.ListActiveFlowInstanceDescriptors(check.ctx, check.runID)
				if err != nil {
					t.Fatalf("ListActiveFlowInstanceDescriptors(%s): %v", check.path, err)
				}
				if len(descriptors) != 1 || descriptors[0].FlowInstance != check.path {
					t.Fatalf("descriptors for %s = %#v, want exact run-owned descriptor", check.path, descriptors)
				}
			}
		})
	}
}

func seedExactFlowInstanceDescriptorOwner(
	t *testing.T,
	selected runtimebus.FlowInstanceActivationCommitOwner,
	runID, entityID, instancePath, bundleHash string,
) {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	scope, _ := runtimeauthoractivity.ScopeFromContext(ctx)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, bundleHash))
	commitDescriptorReadinessComponent(t, ctx, selected, instancePath, entityID, "1.0.0", nil)
}

// Reader/attachment components do not qualify public constructor eligibility.
// The selected construction owner, not this fixture, writes all companion rows.
func commitDescriptorReadinessComponent(t *testing.T, ctx context.Context, selected runtimebus.FlowInstanceActivationCommitOwner, path, entity, version string, fields map[string]any) runtimepipeline.FlowInstanceActivationPlan {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	at := time.Now().UTC()
	command, err := flowactivationfixture.Command(ctx, runtimepipeline.WorkflowInstance{
		WorkflowName: "account", WorkflowVersion: version, Mode: "template",
		StorageRef: path, InstanceID: runtimeflowidentity.LogicalInstanceID(path), EntityID: entity,
		EntityType: "account", CurrentState: "active", StageDefined: true, Revision: 1,
		ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: runID,
		Fields: fields, CreatedAt: at, EnteredStageAt: at, UpdatedAt: at,
	}, runtimepipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct exact descriptor component: result=%+v err=%v", committed, err)
	}
	return committed.Plan
}

func requireReadinessRun(t *testing.T, ctx context.Context, selected storetest.RunFixtureStore, runID, bundleHash string, artifacts ...*sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	fixture := storetest.RunFixture{
		Origin:     storetest.ScenarioSetupOrigin(),
		RunID:      runID,
		BundleHash: bundleHash,
	}
	if len(artifacts) > 0 {
		fixture.Artifact = artifacts[0]
	}
	storetest.RequireRun(t, ctx, selected, fixture)
}

func setReadinessPlanRaw(t *testing.T, selected any, runID, instancePath, plan string) {
	t.Helper()
	if _, err := storetest.SetDescriptorReadinessMalformedPlan(context.Background(), selected, runID, instancePath, json.RawMessage(plan)); err != nil {
		t.Fatalf("set readiness plan for %s: %v", instancePath, err)
	}
}

func setReadinessRunSource(t *testing.T, owner interface {
	runtimerunlifecycle.OperationOwner
	sourceartifactfixture.Writer
}, ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact, artifact *sourceartifact.AdmittedSourceArtifact) {
	t.Helper()
	sourceartifactfixture.RequireArtifact(t, ctx, owner, artifact)
	if _, err := owner.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: source}); err != nil {
		t.Fatalf("set readiness run source for %s: %v", runID, err)
	}
}

func setReadinessRunStatus(t *testing.T, owner runtimerunlifecycle.OperationOwner, ctx context.Context, runID string, state runtimerunlifecycle.State) {
	t.Helper()
	if _, err := owner.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: state}); err != nil {
		t.Fatalf("set readiness run status for %s: %v", runID, err)
	}
}

func setReadinessCoordinate(t *testing.T, selected interface {
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
}, runID, instancePath, coordinate string, value time.Time) {
	t.Helper()
	ctx := context.Background()
	var err error
	switch coordinate {
	case "phase":
		row, found, loadErr := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(instancePath))
		if loadErr != nil || !found {
			t.Fatalf("load exact historical readiness: found=%v err=%v", found, loadErr)
		}
		// Existing named historical cut, not a live generation or executable lease.
		storetest.RetirePlannedReadiness(t, ctx, selected, row.Plan, row.AttemptOrdinal, value)
	case "creation_event_emitted_at":
		_, err = storetest.SetDescriptorReadinessCreationClock(ctx, selected, runID, instancePath, value)
	case "instance_terminated_at":
		_, err = storetest.SetWorkflowProjectionActiveTerminatedTimestamp(ctx, selected, runID, instancePath, value)
	default:
		t.Fatalf("unsupported readiness coordinate %q", coordinate)
	}
	if err != nil {
		t.Fatalf("set %s for %s: %v", coordinate, instancePath, err)
	}
}

func setReadinessInstanceStatus(t *testing.T, selected any, runID, instancePath, status string) {
	t.Helper()
	if status != "draining" {
		t.Fatalf("unsupported readiness status fault %q", status)
	}
	if _, err := storetest.SetWorkflowProjectionDraining(context.Background(), selected, runID, instancePath); err != nil {
		t.Fatalf("set readiness instance status for %s: %v", instancePath, err)
	}
}

func TestActiveFlowInstanceDescriptorAuthorityPreservesRoutesOnInvalidProvenanceBothStores(t *testing.T) {
	type backendCase struct {
		name  string
		setup func(*testing.T) flowInstanceDescriptorAuthorityStore
	}
	backends := []backendCase{
		{
			name: "postgres",
			setup: func(t *testing.T) flowInstanceDescriptorAuthorityStore {
				return storetest.StartPostgresRuntimeStore(t)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) flowInstanceDescriptorAuthorityStore {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
	}
	cases := []struct {
		name                string
		readiness           string
		readinessOnWrongRun bool
		foreignBundle       bool
		wantListError       string
		wantError           string
	}{
		{name: "no readiness row", wantListError: "missing exact readiness plan"},
		{name: "wrong-run readiness row", readiness: "exact", readinessOnWrongRun: true, wantListError: "missing exact readiness plan"},
		{name: "incomplete readiness plan", readiness: `{}`, wantListError: "validate"},
		{name: "stale readiness version", readiness: `{"version":3}`, wantListError: "unsupported version 3"},
		{name: "malformed readiness plan", readiness: `{"workflow_version":{"unexpected":true}}`, wantListError: "decode"},
		{name: "exact readiness owner", readiness: "exact"},
		{name: "foreign run source", readiness: "exact", foreignBundle: true, wantListError: "readiness source does not match persisted run"},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					selected := backend.setup(t)
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
					bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
					source := semanticview.Wrap(bundle)
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(bundle.SourceArtifact))
					scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
					if !ok {
						t.Fatal("test author-activity scope is unavailable")
					}
					ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, bundle.SourceArtifact.BundleHash()))
					storetest.RequireBundleDataCatalog(t, ctx, selected, bundle)
					planBundleHash := bundle.SourceArtifact.BundleHash()
					bundleHash := planBundleHash
					if tc.foreignBundle {
						foreign := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  foreign: {}\n"))
						sourceartifactfixture.RequireArtifact(t, ctx, selected, foreign)
						bundleHash = foreign.BundleHash()
					}
					wrongRunID := uuid.NewString()
					seedFlowInstanceDescriptorAuthorityCase(
						t,
						ctx,
						selected,
						runID,
						wrongRunID,
						bundleHash,
						planBundleHash,
						source.WorkflowVersion(),
						tc.readiness,
						tc.readinessOnWrongRun,
					)

					descriptors, err := selected.ListActiveFlowInstanceDescriptors(ctx, runID)
					if tc.wantListError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantListError) {
							t.Fatalf("ListActiveFlowInstanceDescriptors error = %v, want %q", err, tc.wantListError)
						}
						return
					}
					if err != nil {
						t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
					}
					var testedDescriptor, stagedDescriptor *runtimebus.ActiveFlowInstanceDescriptor
					for idx := range descriptors {
						if descriptors[idx].FlowInstance == "account/existing" {
							testedDescriptor = &descriptors[idx]
						}
						if descriptors[idx].FlowInstance == "account/current" {
							stagedDescriptor = &descriptors[idx]
						}
					}
					if testedDescriptor == nil {
						t.Fatalf("active descriptors = %#v, want account/existing", descriptors)
					}
					if !testedDescriptor.HasSemanticSource() {
						t.Fatalf("descriptor semantic source = %#v, want exact source", *testedDescriptor)
					}

					flowIdentity := runtimeflowidentity.RunScopedFlowInstance{
						RunID: runID,
						Route: runtimeflowidentity.DeriveRoute(notifyallchildren.ChildFlowID, "current"),
					}
					prior := runtimebus.FlowInstanceRouteRecord{
						Identity:       flowIdentity,
						EventPattern:   flowIdentity.Route.InstancePath + "/prior.event",
						SubscriberType: "agent",
						SubscriberID:   "prior-agent",
						SourceFlow:     notifyallchildren.ChildFlowID,
					}
					if err := selected.ReplaceFlowInstanceRouteRecords(ctx, flowIdentity, []runtimebus.FlowInstanceRouteRecord{prior}); err != nil {
						t.Fatalf("seed prior exact route set: %v", err)
					}
					before, err := selected.ListFlowInstanceRouteRecords(ctx, flowIdentity)
					if err != nil {
						t.Fatalf("read prior exact route set: %v", err)
					}

					eventBus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
						ContractBundle:     source,
						SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
					})
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}
					for _, resolution := range []struct {
						name       string
						localEvent string
					}{
						{name: "select", localEvent: "account.notify.requested"},
						{name: "select-or-create", localEvent: "account.registered"},
					} {
						raw, err := json.Marshal(map[string]any{
							"account_id": "existing",
							"command":    "inspect",
						})
						if err != nil {
							t.Fatalf("marshal %s event: %v", resolution.name, err)
						}
						evt := eventtest.RunCreatingRootIngress(
							uuid.NewString(),
							events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, resolution.localEvent)),
							notifyallchildren.OwnerFlowID,
							"",
							raw,
							0,
							runID,
							"",
							events.EventEnvelope{},
							time.Now().UTC(),
						)
						_, pinErr := eventBus.CheckPublishRecipientPlan(ctx, evt)
						if tc.wantError != "" {
							if pinErr == nil || !strings.Contains(pinErr.Error(), tc.wantError) {
								t.Fatalf("%s source admission error = %v, want %q", resolution.name, pinErr, tc.wantError)
							}
						} else if pinErr != nil {
							t.Fatalf("%s exact source admission: %v", resolution.name, pinErr)
						}
						afterPin, readErr := selected.ListFlowInstanceRouteRecords(ctx, flowIdentity)
						if readErr != nil {
							t.Fatalf("read exact route set after %s pin: %v", resolution.name, readErr)
						}
						if !reflect.DeepEqual(afterPin, before) {
							t.Fatalf("%s pin mutated route state: before=%#v after=%#v", resolution.name, before, afterPin)
						}
					}
					if stagedDescriptor == nil {
						t.Fatal("route staging requires its exact constructed descriptor")
					}
					_, err = eventBus.StageFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
						Identity: flowIdentity,
						Instance: stagedDescriptor.Identity,
						ActivationVariables: map[string]string{
							"account_id": "current",
						},
					})
					if tc.wantError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantError) {
							t.Fatalf("StageFlowInstanceRouteContext error = %v, want %q", err, tc.wantError)
						}
						after, readErr := selected.ListFlowInstanceRouteRecords(ctx, flowIdentity)
						if readErr != nil {
							t.Fatalf("read exact route set after rejection: %v", readErr)
						}
						if !reflect.DeepEqual(after, before) {
							t.Fatalf("exact route set changed across provenance rejection: before=%#v after=%#v", before, after)
						}
						return
					}
					if err != nil {
						t.Fatalf("StageFlowInstanceRouteContext exact owner: %v", err)
					}
					after, err := selected.ListFlowInstanceRouteRecords(ctx, flowIdentity)
					if err != nil {
						t.Fatalf("read exact route set after replacement: %v", err)
					}
					if len(after) == 0 || reflect.DeepEqual(after, before) {
						t.Fatalf("exact readiness owner did not replace prior route set: before=%#v after=%#v", before, after)
					}
				})
			}
		})
	}
}

func seedFlowInstanceDescriptorAuthorityCase(
	t *testing.T, ctx context.Context, selected flowInstanceDescriptorAuthorityStore,
	runID, wrongRunID, bundleHash, planBundleHash, workflowVersion, readiness string,
	readinessOnWrongRun bool,
) {
	t.Helper()
	const instancePath = "account/existing"
	const stagedInstancePath = "account/current"
	for _, id := range []string{runID, wrongRunID} {
		storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
			Origin: storetest.ScenarioSetupOrigin(), RunID: id, BundleHash: planBundleHash,
		})
	}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(planBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	currentCtx := runtimecorrelation.WithRunID(ctx, runID)
	commitDescriptorReadinessComponent(t, currentCtx, selected, stagedInstancePath, uuid.NewString(), workflowVersion, map[string]any{"account_id": "current"})
	entityID := uuid.NewString()
	commitDescriptorReadinessComponent(t, currentCtx, selected, instancePath, entityID, workflowVersion, map[string]any{"account_id": "existing"})
	if readinessOnWrongRun {
		commitDescriptorReadinessComponent(t, runtimecorrelation.WithRunID(ctx, wrongRunID), selected, instancePath, entityID, workflowVersion, map[string]any{"account_id": "existing"})
	}
	if readiness == "" || readinessOnWrongRun {
		if _, err := storetest.RemoveDescriptorReadiness(ctx, selected, runID, instancePath); err != nil {
			t.Fatal(err)
		}
	} else if readiness != "exact" {
		setReadinessPlanRaw(t, selected, runID, instancePath, readiness)
	}
	if bundleHash != planBundleHash {
		source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selected.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: source}); err != nil {
			t.Fatalf("set foreign persisted run source after native construction: %v", err)
		}
	}
}

func exactFlowInstanceDescriptorReadinessJSON(
	t *testing.T,
	runID string,
	bundleHash string,
	workflowVersion string,
	templateID string,
	instancePath string,
	entityID string,
) string {
	t.Helper()
	plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: templateID, ScopeKey: templateID,
			InstanceID: runtimeflowidentity.LogicalInstanceID(instancePath), InstancePath: instancePath,
			EntityID: entityID, HasStoredPath: true,
			ParentEntityID: runtimeflowidentity.EntityID(runID),
			ParentRoute: runtimeflowidentity.ParentRoute{
				FlowID: ".", FlowInstance: runID, EntityID: runtimeflowidentity.EntityID(runID),
			},
		},
		RunID: runID, BundleHash: bundleHash, WorkflowVersion: workflowVersion, ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize exact descriptor readiness: %v", err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal exact descriptor readiness: %v", err)
	}
	return string(raw)
}
