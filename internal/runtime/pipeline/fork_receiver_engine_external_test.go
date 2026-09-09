package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimeconfig "github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type forkEngineObservation struct {
	ctx context.Context
	id  string
}

type forkEngineProbe struct {
	claimed chan forkEngineObservation
	resume  chan struct{}
	once    sync.Once
}

func (p *forkEngineProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.Status != "in_progress" || signal.EventType != "producer/work.ready" {
		return
	}
	p.once.Do(func() {
		p.claimed <- forkEngineObservation{ctx: ctx, id: signal.EventID}
		select {
		case <-p.resume:
		case <-ctx.Done():
		}
	})
}

// The declarative adapter currently has no production dispatch caller. Its arm
// uses a separate real fork and executes once before canonical terminalization
// fences its still-blocked bridge attempt; it is not a second served dispatch.
func TestSelectedForkReceiverEngineAgreementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, engine := range []string{"bridge", "declarative"} {
			for _, missing := range []bool{false, true} {
				presence := "required_present"
				if missing {
					presence = "required_missing"
				}
				t.Run(backend+"/"+engine+"/"+presence, func(t *testing.T) {
					root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
					path := filepath.Join(root, "consumer", "nodes.yaml")
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					old := "          - {target_field: processed_token, expression: payload.token}\n"
					if strings.Count(string(raw), old) != 1 {
						t.Fatal("canonical receiver fixture mutation changed")
					}
					write := "          - {target_field: marker, expression: \"'consumer-engine'\"}\n"
					if err := os.WriteFile(path, []byte(strings.Replace(string(raw), old, write, 1)), 0o600); err != nil {
						t.Fatal(err)
					}
					rt, selected, db, repo, capability := startForkEngineRuntime(t, backend, root)
					ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
					ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
					ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
					ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
					runID := uuid.NewString()
					seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "fork-engine-proof", "", []byte(`{"token":"engine-proof"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
					if err := rt.Bus.Publish(ctx, seed); err != nil {
						t.Fatal(err)
					}
					waitForkEngineCompletion(t, db, runID)
					requestEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "fork-engine-proof", "", []byte(`{"token":"engine-proof"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := rt.Bus.Publish(ctx, requestEvent); err != nil {
						t.Fatal(err)
					}
					waitForkEngineCompletion(t, db, runID)
					var frontier, sourceConsumer string
					if err := db.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, runID).Scan(&frontier); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, runID).Scan(&sourceConsumer); err != nil || forkEngineMarker(t, sourceConsumer) != "consumer-engine" {
						t.Fatalf("normal receiver did not perform independently expected write: %s err=%v", sourceConsumer, err)
					}
					bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, filepath.Join(repo, "platform-spec.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					family, ok := selected.RunFork()
					if !ok {
						t.Fatal("missing actual selected fork owner")
					}
					probe := &forkEngineProbe{claimed: make(chan forkEngineObservation, 1), resume: make(chan struct{})}
					var resumeOnce sync.Once
					resume := func() { resumeOnce.Do(func() { close(probe.resume) }) }
					forkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					finished := make(chan error, 1)
					done := make(chan struct{})
					go func() {
						defer close(done)
						_, err := family.Execute(forkCtx, runforkexecution.SelectedContractExecutionRequest{
							SourceRunID: runID, At: frontier, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
							SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: selected.SourceArtifactStore()},
							ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
							AgentRuntime:      runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture, AgentManagerOptions: runtimemanager.AgentManagerOptions{TestLifecycleProbe: probe}},
						})
						finished <- err
					}()
					t.Cleanup(func() {
						cancel()
						resume()
						select {
						case <-done:
						case <-time.After(10 * time.Second):
							t.Error("fork engine proof did not join")
						}
					})
					var observed forkEngineObservation
					select {
					case observed = <-probe.claimed:
					case err := <-finished:
						t.Fatalf("real fork did not reach engine claim: %v", err)
					case <-forkCtx.Done():
						t.Fatal(forkCtx.Err())
					}
					claim, ok := runtimedelivery.ClaimFromContext(observed.ctx)
					admission, admitted := managedexecution.FromContext(observed.ctx)
					if !ok || !admitted || admission.Kind != managedexecution.KindSelectedContractFork || admission.RunID != claim.RunID() || claim.RunID() == runID {
						t.Fatal("engine lacks actual fresh-child fork authority")
					}
					deps := selected.RuntimeDeps()
					snapshot, err := deps.DeliveryStore.Snapshot(observed.ctx, claim.DeliveryID())
					if err != nil {
						t.Fatal(err)
					}
					target := snapshot.Route.Target.Route()
					var before string
					var beforeRevision int64
					if err := db.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, claim.RunID(), target.EntityID).Scan(&before, &beforeRevision); err != nil || forkEngineMarker(t, before) != "consumer-owned" {
						t.Fatalf("fixed-revision receiver was not reconstructed independently of current source: %s err=%v", before, err)
					}
					if missing {
						result, err := db.Exec(`DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, claim.RunID(), target.EntityID)
						if err != nil {
							t.Fatal(err)
						}
						if count, err := result.RowsAffected(); err != nil || count != 1 {
							t.Fatalf("remove exact required child: %d %v", count, err)
						}
					}
					if engine == "declarative" {
						prepared, found, err := deps.EventBusDurable.PreparedEvents.LoadPreparedPublishEvent(observed.ctx, observed.id)
						if err != nil || !found {
							t.Fatalf("load actual fork publication: %t %v", found, err)
						}
						node, handlerEvent, ok := snapshot.Route.ConnectClaim.NodeHandlerOwner()
						if !ok || node.FlowPath() != "consumer" || handlerEvent != "work.ready" {
							t.Fatal("fork route lost exact compiled receiver handler")
						}
						record, ok := rt.Options.WorkflowModule.SemanticSource().ExecutableNode(node)
						if !ok {
							t.Fatal("receiver missing from loaded source")
						}
						authority, authorityOK := runtimeeffects.AuthorityFromContext(observed.ctx)
						controller, controllerOK := runtimeeffects.ControllerFromContext(observed.ctx)
						lineage, lineageOK := runtimecorrelation.RuntimeLineageFromContext(observed.ctx)
						occurrence, occurrenceOK := worklifetime.OccurrenceFromContext(observed.ctx)
						if !authorityOK || !controllerOK || !lineageOK || !occurrenceOK {
							t.Fatal("captured fork context is incomplete")
						}
						variant, err := eventreceiver.SelectedContractForkExecution(authority, admission, controller, lineage)
						if err != nil || variant.ValidateBound(observed.ctx, prepared.Event.Event().ExecutionMode()) != nil {
							t.Fatalf("captured fork context does not retain exact execution binding: %v", err)
						}
						pc := runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, runtimepipeline.PipelineCoordinatorOptions{
							Module: rt.Options.WorkflowModule, Persistence: deps.WorkflowPersistence, DeliveryStore: deps.DeliveryStore,
							SourceArtifactFact: rt.Options.SourceArtifactFact, ExecutionPosture: rt.ExecutionPosture, ReceiverExecution: variant, WorkOwner: occurrence,
							DeadLetters: deps.EventBusDurable.TargetFailureRecorder, PipelineObligations: deps.PipelineObligations,
							DecisionCards: deps.DecisionCards, ProposedEffects: deps.ProposedEffects, HumanTasks: deps.DecisionCardHumanTasks,
							DecisionCardDraftExpiry: deps.DecisionCardDraftExpiry, HumanTaskExpiry: deps.HumanTaskExpiry,
							DeliveryRuntime: rt.Bus, RunLifecycle: deps.EventBusDurable.RunLifecycle,
						})
						adapter := runtimepipeline.NewCoordinatorHandlerExecutionEngineForTest(pc, node)
						if adapter == nil {
							t.Fatal("real declarative adapter dependencies are incomplete")
						}
						heartbeat, err := runtimedelivery.StartClaimHeartbeat(observed.ctx, occurrence, deps.DeliveryStore, claim)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = heartbeat.Stop() })
						outcome, err := adapter.ExecuteHandlerSteps(heartbeat.Context(), record.Entry.EventHandlers[string(handlerEvent)], prepared.Event.Event(), string(handlerEvent))
						if missing {
							if err == nil {
								t.Fatal("declarative engine accepted missing required receiver")
							}
						} else if err != nil || outcome == nil || !outcome.Handled {
							t.Fatalf("declarative engine: outcome=%+v err=%v", outcome, err)
						}
						if err := heartbeat.Stop(); err != nil {
							t.Fatalf("join private-adapter heartbeat: %v", err)
						}
						current, err := deps.DeliveryStore.Snapshot(observed.ctx, claim.DeliveryID())
						want := runtimedelivery.StatusDelivered
						retireCount := 0
						if missing {
							want = runtimedelivery.StatusInProgress
							retireCount = 1
						}
						if err != nil || current.Status != want || current.ClaimVersion != claim.Version() {
							t.Fatalf("private-adapter exact settlement=%+v err=%v", current, err)
						}
						assertForkEngineRows(t, db, runID, claim.RunID(), target.EntityID, sourceConsumer, beforeRevision+1, missing)
						// The real store fences the original claim before its bridge can
						// run. No callback exception or execution-mode bypass is needed.
						if retired, err := deps.DeliveryStore.TerminalizeRun(observed.ctx, claim.RunID(), "fork_engine_test_cleanup"); err != nil || len(retired) != retireCount {
							t.Fatalf("retire private-adapter child claim: %+v err=%v", retired, err)
						}
						resume()
						select {
						case err := <-finished:
							if err != nil {
								t.Fatalf("fenced bridge cleanup failed: %v", err)
							}
						case <-forkCtx.Done():
							t.Fatal(forkCtx.Err())
						}
						assertForkEngineRows(t, db, runID, claim.RunID(), target.EntityID, sourceConsumer, beforeRevision+1, missing)
					} else {
						resume()
						select {
						case err := <-finished:
							if err != nil {
								t.Fatalf("bridge fork did not finish: %v", err)
							}
						case <-forkCtx.Done():
							t.Fatal(forkCtx.Err())
						}
						current, err := deps.DeliveryStore.Snapshot(ctx, claim.DeliveryID())
						want := runtimedelivery.StatusDelivered
						if missing {
							want = runtimedelivery.StatusDeadLetter
						}
						if err != nil || current.Status != want || current.ClaimVersion != claim.Version() {
							t.Fatalf("bridge exact settlement=%+v err=%v", current, err)
						}
						assertForkEngineRows(t, db, runID, claim.RunID(), target.EntityID, sourceConsumer, beforeRevision+1, missing)
					}
				})
			}
		}
	}
}

func forkEngineMarker(t *testing.T, raw string) any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	return fields["marker"]
}

func assertForkEngineRows(t *testing.T, db *sql.DB, sourceRun, childRun, receiver, sourceBefore string, wantRevision int64, missing bool) {
	t.Helper()
	var fields string
	var revision int64
	err := db.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, childRun, receiver).Scan(&fields, &revision)
	if missing {
		if err != sql.ErrNoRows {
			t.Fatalf("required-state failure recreated the receiver: %s err=%v", fields, err)
		}
	} else if err != nil || forkEngineMarker(t, fields) != "consumer-engine" || revision != wantRevision {
		t.Fatalf("engine did not write receiving entity exactly once: %s revision=%d want=%d err=%v", fields, revision, wantRevision, err)
	}
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='producer'`, childRun).Scan(&fields); err != nil || forkEngineMarker(t, fields) != "producer-owned" {
		t.Fatalf("engine borrowed producer state: %s err=%v", fields, err)
	}
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, sourceRun).Scan(&fields); err != nil || fields != sourceBefore {
		t.Fatalf("engine mutated source receiver: %s err=%v", fields, err)
	}
}

func waitForkEngineCompletion(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND (d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS (SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("source completion did not settle")
}

func startForkEngineRuntime(t *testing.T, backend, root string) (*swarmruntime.Runtime, *storeselected.Owner, *sql.DB, string, startupownership.ProcessCapability) {
	t.Helper()
	repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"SWARM_CONFIG", "SWARM_STORE_BACKEND", "SWARM_SQLITE_PATH", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "runtime.db")
	config := map[string]any{
		"runtime": map[string]any{"execution_posture": "live", "recovery_on_startup": false},
		"store":   map[string]any{"backend": backend, "sqlite": map[string]any{"path": sqlitePath}},
		"llm":     map[string]any{"backend": "anthropic", "session": map[string]any{"lock_ttl": "10s", "rotate_after_turns": 40, "rotate_on_parse_failures": 3}},
	}
	var db *sql.DB
	var dsn string
	selection := storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: sqlitePath}
	if backend == "postgres" {
		var cleanup func()
		dsn, db, cleanup = testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		connection, err := testpostgres.ParseConnection(dsn)
		if err != nil {
			t.Fatal(err)
		}
		p := connection.Parameters()
		t.Setenv("FORK_ENGINE_DB_PASSWORD", p.Password)
		config["database"] = map[string]any{"host": p.Host, "port": p.Port, "name": p.Database, "user": p.User, "password_env": "FORK_ENGINE_DB_PASSWORD", "sslmode": p.SSLMode}
		selection = storebackend.Selection{Backend: storebackend.BackendPostgres}
	}
	raw, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "swarm.yaml")
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	selected, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = selected.CloseUnactivated() })
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repo, root, filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	report := bootverify.Run(context.Background(), module.SemanticSource(), bootverify.Options{})
	if len(report.Errors()) != 0 || len(report.Warnings()) != 0 {
		t.Fatalf("fork engine fixture boot verification: errors=%+v warnings=%+v", report.Errors(), report.Warnings())
	}
	plans, err := cliapp.StateStoreSchemaPlans(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.Schema().BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans.Platform, StatePlans: plans.State,
		Origin: store.RuntimeStoreOrigin{SwarmVersion: "fork-engine-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeID, fact.BundleHash()))
	storetest.RequireBundleDataCatalog(t, ctx, selected.SourceArtifactWriter(), bundle)
	cfg, err := runtimeconfig.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(bundle.PackInventory, bundle.Platform.Platform.Version)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(dir, "provider-credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	deps := selected.RuntimeDeps()
	deps.Config = cfg
	deps.Options = swarmruntime.RuntimeOptions{WorkflowModule: module, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID, ProcessWorkOwner: process, ProviderTriggerCatalog: catalog, ProviderCredentials: credentials}
	rt, err := swarmruntime.NewRuntime(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := selected.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "fork-engine-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	family, ok := selected.RunFork()
	if !ok {
		t.Fatal("fork engine proof requires selected fork owner")
	}
	t.Cleanup(func() {
		joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := family.RetireSelectedContexts(joinCtx); err != nil {
			t.Error(err)
		}
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(joinCtx); err != nil {
			t.Error(err)
		}
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := family.BindSelectedProcess(ctx, process, capability); err != nil {
		t.Fatal(err)
	}
	if _, err := family.RecoverSelectedForkContexts(ctx, runtimeeffects.NewRecoveryRequest(time.Now().UTC(), rt.ExecutionPosture)); err != nil {
		t.Fatal(err)
	}
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	agents, err := rt.Manager.CompileStaticTopologyDesiredAgents(module.SemanticSource(), coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, agents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatal(err)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if db == nil {
		db, err = sql.Open("sqlite", sqlitePath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	return rt, selected, db, repo, capability
}
