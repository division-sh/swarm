package serveapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestPausedMixedRunsStartupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			cfg := &config.Config{Runtime: config.RuntimeConfig{RecoveryOnStartup: true}}
			var selected *selectedStoreOwner
			var db *sql.DB
			if backend == "sqlite" {
				selected = openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "paused-startup.db"), cfg)
				db = selectedStoreDatabaseForTest(t, selected)
				if _, err := initializeServePlatformStateStores(context.Background(), selected.Schema(), filepath.Join(repoRootForTest(), defaultPlatformSpecPath)); err != nil {
					t.Fatal(err)
				}
			} else {
				dsn, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = postgresDB
				selected = openSelectedPostgresOwner(t, dsn, db, cfg)
			}
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, selected) })
			module, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), writeServedTestSetupFixture(t), filepath.Join(repoRootForTest(), defaultPlatformSpecPath))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := initializeStateStores(context.Background(), selected.Schema(), bundle); err != nil {
				t.Fatal(err)
			}
			artifacts := []*sourceartifact.AdmittedSourceArtifact{
				sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# paused-context-a\n")),
				sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# paused-context-b\n")),
			}
			facts := []runtimecorrelation.SourceArtifactFact{sourceartifactfixture.FactFor(artifacts[0]), sourceartifactfixture.FactFor(artifacts[1])}
			process := worklifetime.NewProcess()
			runtimeInstanceID := uuid.NewString()
			deps := selected.RuntimeDeps()
			var runtimes []*runtimepkg.Runtime
			var coordinates []runtimeagenttopology.SourceCoordinate
			var desired []runtimeagenttopology.DesiredAgent
			for _, fact := range facts {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, selected, cfg, runtimepkg.RuntimeOptions{
					SelfCheck: false, WorkflowModule: module, LLMRuntime: servedNoopLLMRuntime{},
					ProviderTriggerCatalog: testProviderTriggerCatalog(t), ProcessWorkOwner: process,
					SourceArtifactFact: fact, RuntimeInstanceID: runtimeInstanceID,
				}))
				if err != nil {
					t.Fatal(err)
				}
				runtimes = append(runtimes, rt)
				coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
				agents, err := rt.Manager.CompileStaticTopologyDesiredAgents(module.SemanticSource(), coordinate)
				if err != nil {
					t.Fatal(err)
				}
				coordinates = append(coordinates, coordinate)
				desired = append(desired, agents...)
			}
			capability, err := selected.StartupOwnership().AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
				OwnerID: "paused-startup-context-proof", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, rt := range runtimes {
					if err := rt.Shutdown(); err != nil {
						t.Error(err)
					}
				}
				if err := closeSelectedStoreTestProcess(process, capability); err != nil {
					t.Error(err)
				}
			})
			plan, err := runtimeagenttopology.NewSourceSetPlan(coordinates, desired)
			if err != nil {
				t.Fatal(err)
			}
			if err := installServeSourceSet(context.Background(), capability, plan); err != nil {
				t.Fatal(err)
			}
			for _, rt := range runtimes {
				installSelectedStoreTestGeneration(t, capability, rt, plan, 1)
				if err := rt.PrepareAuthorActivityCatalog(); err != nil {
					t.Fatal(err)
				}
			}

			// Persist one paused recipient and a later unhanded running sibling
			// per source before the real composed startup releases any producers.
			var pausedIDs []string
			var pausedRuns []string
			var before []runtimedelivery.Snapshot
			var contexts []context.Context
			var runningEvents []string
			for index, fact := range facts {
				for _, paused := range []bool{true, false} {
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(runtimecorrelation.WithSourceArtifactFact(context.Background(), fact), runID)
					ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash()))
					fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Artifact: artifacts[index]}
					if backend == "sqlite" {
						runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
					} else {
						runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
					}
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "widget.scored", "test", "", []byte(`{"delta":1}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					var routes []events.DeliveryRoute
					if paused {
						routes = []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "scorer")), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID})}}
						controller := runtimeruncontrol.NewController(deps.RunControlStore, nil, runtimeruncontrol.Options{})
						if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "mixed-startup"}); err != nil {
							t.Fatal(err)
						}
					}
					storetest.CommitSemanticEventWithRoutes(t, ctx, deps.EventStore, event, routes, runtimepipelineobligation.ScopeSubscribed)
					if paused {
						id, err := deps.DeliveryStore.ProveHandoff(ctx, event.ID(), routes[0])
						if err != nil {
							t.Fatal(err)
						}
						snapshot, err := deps.DeliveryStore.Snapshot(ctx, id.DeliveryID())
						if err != nil {
							t.Fatal(err)
						}
						pausedIDs = append(pausedIDs, id.DeliveryID())
						pausedRuns = append(pausedRuns, runID)
						before = append(before, snapshot)
						contexts = append(contexts, ctx)
					} else {
						runningEvents = append(runningEvents, event.ID())
					}
				}
			}
			var startupContexts []serveRuntimeBundleContext
			for index, rt := range runtimes {
				startupContexts = append(startupContexts, serveRuntimeBundleContext{runtime: rt, sourceArtifactFact: facts[index]})
			}
			release, err := prepareServeRuntimeContexts(context.Background(), startupContexts, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			for index := range pausedIDs {
				after, err := deps.DeliveryStore.Snapshot(contexts[index], pausedIDs[index])
				if err != nil {
					t.Fatal(err)
				}
				expectedAuthority, err := runtimes[index].Bus.DeliveryAuthority()
				if err != nil || !after.Authority.Equal(expectedAuthority) {
					t.Fatalf("source %d restored authority = %+v, %v", index, after.Authority, err)
				}
				// Startup legitimately rebinds debt to this exact process generation;
				// that is not an attempt, handoff, retry or execution transition.
				before[index].Authority = after.Authority
				before[index].UpdatedAt = after.UpdatedAt
				if err != nil || !reflect.DeepEqual(before[index], after) {
					t.Fatalf("source %d paused delivery changed during startup: before=%+v after=%+v err=%v", index, before[index], after, err)
				}
				summary, err := deps.PipelineObligations.SummarizeRun(contexts[index], pausedRuns[index])
				if err != nil || summary.Acknowledged != 0 || summary.Replayable != 1 {
					t.Fatalf("source %d paused pipeline summary = %+v, %v", index, summary, err)
				}
				requireServedDirectivePipelineReceiptCount(t, servedControlProofRuntime{DB: db, Backend: backend}, runningEvents[index], 1)
			}
		})
	}
}
