package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Native construction/attachment/restart proof, not a provider or public-launcher
// qualification. No fixture lifecycle rows substitute for constructor execution.
func TestRuntimeConstructedActorCensusRestartBothStores(t *testing.T) {
	proveRuntimeConstructedActorCensusBothStores(t, "restart")
}

func TestRuntimeConstructedActorCensusSourceSetRebindBothStores(t *testing.T) {
	proveRuntimeConstructedActorCensusBothStores(t, "rebind")
}

func TestRuntimeConstructedActorRetainedRefreshShutdownBothStores(t *testing.T) {
	proveRuntimeConstructedActorCensusBothStores(t, "terminal_drain")
}

func TestRuntimeConstructedActorAtomicRebindRetryBothStores(t *testing.T) {
	proveRuntimeConstructedActorCensusBothStores(t, "rebind_retry")
}

func TestRuntimeConstructedActorPartialRefreshShutdownBothStores(t *testing.T) {
	proveRuntimeConstructedActorCensusBothStores(t, "rebind_partial_shutdown")
}

func proveRuntimeConstructedActorCensusBothStores(t *testing.T, journey string) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fields := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fields_%t", backend, fields), func(t *testing.T) {
				root := canonicalrouting.CopySelectedNestedConstructedAgentInput(t, fields)
				if journey == "rebind_retry" || journey == "rebind_partial_shutdown" {
					root = canonicalrouting.CopySelectedNestedConstructedAgentRebindInput(t, fields)
				}
				repo := pipeline.WorkflowRepoRoot()
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				fact, err := correlation.NewSourceArtifactFact(artifact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
				var selected startupRecoveryOrderStore
				if backend == "sqlite" {
					selected = storetest.StartSQLiteRuntimeStore(t)
				} else {
					_, db, _ := testutil.StartPostgres(t)
					selected = storetest.AdmitPostgresRuntimeStore(t, db)
				}
				runID := uuid.NewString()
				ctx := startupRecoverySourceContext(fact, runID)
				seedStartupRecoverySourceRun(t, ctx, selected, fact, artifact, runID)
				var currentCapability startupownership.ProcessCapability
				var currentContexts *runtimepkg.RuntimeContextManager
				start := func() (*runtimepkg.Runtime, func() error) {
					process := worklifetime.NewProcess()
					rt, err := runtimepkg.NewRuntime(ctx, completeExternalRuntimeTestWorkflowDeps(t, selected, runtimepkg.RuntimeDeps{
						Config:     &config.Config{Runtime: config.RuntimeConfig{RecoveryOnStartup: true}, LLM: config.LLMConfig{Backend: "anthropic"}},
						EventStore: selected, EventBusDurable: externalRuntimeTestDurableDependencies(selected),
						EventPayloadAdmissionBinder: selected, AuthorActivityRegistrars: []runtimepkg.AuthorActivityCatalogRegistrar{selected},
						RunLifecycleCandidates: selected, WorkflowPersistence: pipeline.NewWorkflowPersistence(selected),
						ManagerStore: selected, ManagerPersistenceRoles: externalRuntimeTestSelectedManagerRoles(selected),
						RuntimeLogStore: selected, ManagerLifecycleDiagnostics: selected,
						DeliveryStore: selected, PipelineObligations: selected.PipelineObligations(),
						Options: runtimepkg.RuntimeOptions{WorkflowModule: newRuntimeTestWorkflowModule(t, source), LLMRuntime: startupRecoveryOrderLLM{},
							RuntimeInstanceID: authorActivityTestRuntimeInstanceID, SourceArtifactFact: fact, ProcessWorkOwner: process},
					}))
					if err != nil {
						t.Fatal(err)
					}
					capability, _ := installExternalRuntimeTestGeneration(t, ctx, selected, rt)
					currentCapability = capability
					contexts := nativeConstructedActorRuntimeContexts(t, rt, source, fact)
					currentContexts = contexts
					closed := false
					close := func() error {
						if closed {
							return nil
						}
						var shutdownErr error
						for _, result := range contexts.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
							shutdownErr = errors.Join(shutdownErr, result.ShutdownErr)
						}
						if shutdownErr != nil {
							return shutdownErr
						}
						if err := closeExternalRuntimeTestGeneration(rt, process, capability); err != nil {
							return err
						}
						closed = true
						return nil
					}
					t.Cleanup(func() {
						if err := close(); err != nil {
							t.Error(err)
						}
					})
					if err := rt.Start(ctx); err != nil {
						t.Fatalf("start exact generation: %v", err)
					}
					return rt, close
				}
				rt, close := start()
				owned := effects.WithExecutionMode(worklifetime.WithOccurrence(ctx, rt.WorkOccurrence()), effects.ExecutionModeLive)
				parent := flowidentity.Stored(source, ".", runID, runID, runID, "")
				if err := rt.Manager.ActivateFlowInstance(owned, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: parent, OccurredAt: time.Now().UTC()}); err != nil {
					t.Fatalf("canonical root construction: %v", err)
				}
				var children []flowidentity.Instance
				for _, key := range []string{"one", "two"} {
					instance := flowidentity.Derive(source, "templ", key)
					instance.ParentRoute = flowidentity.ParentRoute{FlowID: ".", FlowInstance: runID, EntityID: runID}
					instance.ParentEntityID = runID
					trigger := eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), "task.assigned", "operator", "", []byte(fmt.Sprintf(`{"work_id":%q}`, key)), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
					if err := rt.Manager.ActivateFlowInstance(owned, pipeline.FlowInstanceActivationRequest{
						ContractBundle: source, Instance: instance, TriggerEvent: trigger, ConstructorInput: "task.assigned", ResolvedKey: key,
						Config: map[string]any{"work_id": key}, OccurredAt: trigger.CreatedAt(),
					}); err != nil {
						t.Fatalf("canonical keyed tree construction: %v", err)
					}
					for _, branch := range []string{"child", "audit"} {
						current := instance
						for _, flow := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
							current, err = flowidentity.KeylessChild(source, current, flow)
							if err != nil {
								t.Fatal(err)
							}
							children = append(children, current)
						}
					}
				}
				before := requireNativeConstructedActors(t, owned, rt, selected, source, runID, children)
				attempts := map[string]pipeline.DynamicFlowRuntimeReadiness{}
				if strings.HasPrefix(journey, "rebind") {
					proveNativeConstructedActorSourceSetRebind(t, ctx, rt, selected, currentCapability, currentContexts, runID, children, backend, journey)
					if journey != "rebind_partial_shutdown" {
						rebound := requireNativeConstructedActors(t, owned, rt, selected, source, runID, children)
						if !reflect.DeepEqual(before, rebound) {
							t.Fatal("source-set rebind changed construction or business state")
						}
					}
				}
				if journey == "terminal_drain" || journey == "rebind_partial_shutdown" {
					for _, instance := range children {
						row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
						if err != nil || !found || row.AttemptState != "accepted" || row.Phase != pipeline.FlowAttachmentReady {
							t.Fatalf("ready predecessor attempt: row=%+v found=%v err=%v", row, found, err)
						}
						attempts[instance.InstancePath] = row
					}
				}
				if journey == "terminal_drain" {
					plan, found, err := currentCapability.CurrentSourceSet(ctx)
					if err != nil || !found {
						t.Fatalf("current source set: found=%v err=%v", found, err)
					}
					transition, err := currentContexts.PrepareSourceSetTransition(ctx, plan)
					if err != nil || transition == nil {
						t.Fatalf("prepare retained refresh: err=%v", err)
					}
					if err := transition.Commit(ctx, nil); err == nil || !strings.Contains(err.Error(), "requires process capability") {
						t.Fatalf("failed refresh result=%v", err)
					}
					if err := rt.Manager.Shutdown(); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
						t.Fatalf("ordinary shutdown bypassed retained admission: %v", err)
					}
					if err := rt.Shutdown(); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
						t.Fatalf("individual runtime retirement bypassed aggregate ownership: %v", err)
					}
					if err := transition.Abort(); err != nil {
						t.Fatal(err)
					}
					if currentContexts.LookupBundleHashStatus(fact.BundleHash()).Loaded() {
						t.Fatal("post-commit Abort restored visibility")
					}
					if err := close(); err != nil {
						t.Fatalf("aggregate terminal drain and exact cleanup: %v", err)
					}
					for _, instance := range children {
						row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
						previous := attempts[instance.InstancePath]
						if err != nil || !found || row.AttemptState != "retired" || row.AttemptOrdinal != previous.AttemptOrdinal || row.PlanHash != previous.PlanHash || row.Phase != previous.Phase {
							t.Fatalf("exact predecessor retirement: row=%+v previous=%+v found=%v err=%v", row, previous, found, err)
						}
					}
					if retry, err := currentContexts.PreparePendingSourceSetTransition(ctx, plan); err == nil || retry != nil || !strings.Contains(err.Error(), "terminally draining") {
						t.Fatalf("terminal shutdown allowed refresh retry: retry=%v err=%v", retry, err)
					}
					use, lookup, err := currentContexts.AcquireBundleHash(ctx, fact.BundleHash())
					if use != nil {
						_ = use.Done()
					}
					if err != nil || use != nil || lookup.Loaded() {
						t.Fatalf("terminal shutdown restored runtime selection: use=%v lookup=%+v err=%v", use, lookup, err)
					}
				}
				if err := close(); err != nil {
					t.Fatalf("join predecessor before restart: %v", err)
				}
				if journey == "rebind_partial_shutdown" {
					for _, instance := range children {
						row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
						previous := attempts[instance.InstancePath]
						if err != nil || !found || row.AttemptState != "retired" || row.AttemptOrdinal != previous.AttemptOrdinal || row.PlanHash != previous.PlanHash || row.Phase != previous.Phase {
							t.Fatalf("partial refresh did not retire its exact attachment: row=%+v previous=%+v err=%v", row, previous, err)
						}
					}
				}
				successor, _ := start()
				after := requireNativeConstructedActors(t, ctx, successor, selected, source, runID, children)
				for route, original := range before {
					current := after[route]
					if !reflect.DeepEqual(original.Fields, current.Fields) || !reflect.DeepEqual(original.Config, current.Config) || original.CurrentState != current.CurrentState || !original.CreatedAt.Equal(current.CreatedAt) {
						t.Fatalf("restart repeated construction or changed business state: route=%s", route)
					}
				}
				if journey == "terminal_drain" || journey == "rebind_partial_shutdown" {
					for _, instance := range children {
						row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, instance.Route())
						previous := attempts[instance.InstancePath]
						if err != nil || !found || row.AttemptState != "accepted" || row.AttemptOrdinal <= previous.AttemptOrdinal || row.PlanHash != previous.PlanHash || !row.CreationEventEmittedAt.Equal(previous.CreationEventEmittedAt) {
							t.Fatalf("restart must attach a new attempt without recreating the instance: row=%+v previous=%+v found=%v err=%v", row, previous, found, err)
						}
					}
				}
			})
		}
	}
}

func nativeConstructedActorRuntimeContexts(t *testing.T, rt *runtimepkg.Runtime, source semanticview.Source, fact correlation.SourceArtifactFact) *runtimepkg.RuntimeContextManager {
	t.Helper()
	bundle, _ := semanticview.Bundle(source)
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	contexts, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		SourceArtifactFact: fact, Source: source, Runtime: rt, WorkOwner: rt.WorkOccurrence(),
		PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: projection.ProviderTriggers.Generation(), InstalledTriggerSubjects: subjects,
	})
	if err != nil {
		t.Fatal(err)
	}
	return contexts
}

func proveNativeConstructedActorSourceSetRebind(t *testing.T, ctx context.Context, rt *runtimepkg.Runtime, selected startupRecoveryOrderStore, capability startupownership.ProcessCapability, contexts *runtimepkg.RuntimeContextManager, runID string, children []flowidentity.Instance, backend, journey string) {
	t.Helper()
	reader := selected.(manager.AgentLifecycleStateReader)
	db := storetest.Database(selected)
	readinessBefore := make(map[string]pipeline.DynamicFlowRuntimeReadiness, len(children))
	stampsBefore := make(map[string][]any, len(children))
	for _, child := range children {
		row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, child.Route())
		if err != nil || !found {
			t.Fatalf("readiness before source-set refresh: found=%v err=%v", found, err)
		}
		readinessBefore[child.InstancePath] = row
		stampsBefore[child.InstancePath] = nativeReadinessStampSnapshot(t, ctx, db, backend, runID, child.InstancePath)
	}
	before := make([]manager.AgentLifecycleState, 0, len(children))
	for _, child := range children {
		cfg, err := rt.Manager.ResolveAgentConfig(runID, "worker", child.InstancePath)
		if err != nil {
			t.Fatal(err)
		}
		state, found, err := reader.LoadAgentLifecycleState(ctx, cfg.Identity)
		if err != nil || !found || state.Topology.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan {
			t.Fatalf("construction-owned lifecycle before rebind: state=%+v found=%v err=%v", state, found, err)
		}
		before = append(before, state)
	}
	if journey != "rebind" {
		states, err := selected.(manager.AgentLifecycleCellCensus).ListDurableAgentLifecycleStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before = nil
		for _, state := range states {
			if state.Identity.RunID == runID && state.Topology.Authority.Kind == agenttopology.AuthorityFlowReadinessPlan {
				before = append(before, state)
			}
		}
		if len(before) != 16 {
			t.Fatalf("native two-actor instance census=%d, want16", len(before))
		}
	}
	current, found, err := capability.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("load predecessor source set: found=%v err=%v", found, err)
	}
	spareArtifact, err := sourceartifact.AdmitDirectory(canonicalrouting.CopySelectedConstructedAgentInput(t))
	if err != nil {
		t.Fatal(err)
	}
	spareFact, err := correlation.NewSourceArtifactFact(spareArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	spareRunID := uuid.NewString()
	seedStartupRecoverySourceRun(t, startupRecoverySourceContext(spareFact, spareRunID), selected, spareFact, spareArtifact, spareRunID)
	plan, err := agenttopology.NewSourceSetPlan(append(current.Sources, agenttopology.SourceCoordinate{BundleHash: spareArtifact.BundleHash()}), current.Agents)
	if err != nil || plan.Revision == current.Revision {
		t.Fatalf("changed source-set plan: err=%v", err)
	}
	transition, err := contexts.PrepareSourceSetTransition(ctx, plan)
	if err != nil || transition == nil {
		t.Fatalf("prepare native aggregate transition: err=%v", err)
	}
	defer func() {
		if err := transition.Abort(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := capability.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan}); err != nil {
		t.Fatal(err)
	}
	if journey != "rebind" {
		sort.Slice(before, func(i, j int) bool {
			return before[i].Topology.Authority.Readiness.InstancePath+before[i].AgentID < before[j].Topology.Authority.Readiness.InstancePath+before[j].AgentID
		})
		failures := before[:2]
		if journey == "rebind_partial_shutdown" {
			failures = before[len(before)-1:]
		}
		// Both actors of the first instance independently fail a native UPDATE.
		// The shutdown variant fails the last instance after earlier commits.
		for index, failed := range failures {
			remove := failNativeActorRebind(t, db, backend, failed.AgentID)
			if err := transition.Commit(ctx, capability); err == nil || !strings.Contains(err.Error(), "injected_readiness_rebind") {
				t.Fatalf("actor%d failure=%v", index, err)
			}
			for _, old := range before {
				state, found, err := reader.LoadAgentLifecycleState(ctx, old.Identity)
				if err != nil || !found {
					t.Fatalf("failed-rebind lifecycle: %v", err)
				}
				if old.Topology.Authority.Readiness.InstancePath == failed.Topology.Authority.Readiness.InstancePath && !state.ProcessBinding.Equal(old.ProcessBinding) {
					t.Fatal("per-actor failure committed a partial instance")
				}
			}
			path := failed.Topology.Authority.Readiness.InstancePath
			if !reflect.DeepEqual(stampsBefore[path], nativeReadinessStampSnapshot(t, ctx, db, backend, runID, path)) {
				t.Fatal("actor failure committed the instance's readiness stamp")
			}
			remove()
			if journey == "rebind_partial_shutdown" {
				return
			}
			var err error
			transition, err = contexts.PreparePendingSourceSetTransition(ctx, plan)
			if err != nil || transition == nil {
				t.Fatalf("prepare exact refresh retry: %v", err)
			}
		}
	}
	if err := transition.Commit(ctx, capability); err != nil {
		t.Fatalf("commit native aggregate transition: %v", err)
	}
	committed, found, err := capability.CurrentSourceSet(ctx)
	if err != nil || !found || !reflect.DeepEqual(committed, plan) {
		t.Fatalf("successor source-set readback: plan=%+v found=%v err=%v", committed, found, err)
	}
	for _, old := range before {
		state, found, err := reader.LoadAgentLifecycleState(ctx, old.Identity)
		if err != nil || !found || !state.Topology.Equal(old.Topology) || state.Generation != old.Generation || state.ConfigRevision != old.ConfigRevision || state.ProcessBinding.RuntimeGeneration != old.ProcessBinding.RuntimeGeneration+1 || state.ProcessBinding.GenerationGrantID == old.ProcessBinding.GenerationGrantID {
			t.Fatalf("rebind lost construction ownership or exact successor binding: old=%+v new=%+v found=%v err=%v", old, state, found, err)
		}
	}
	for _, child := range children {
		row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, child.Route())
		previous := readinessBefore[child.InstancePath]
		if err != nil || !found || row.AttemptOrdinal != previous.AttemptOrdinal || row.Phase != previous.Phase || row.PlanHash != previous.PlanHash || row.AttemptState != previous.AttemptState || !row.CreationEventEmittedAt.Equal(previous.CreationEventEmittedAt) {
			t.Fatalf("successful refresh changed attachment identity/progress: previous=%+v row=%+v err=%v", previous, row, err)
		}
	}
	if pending, err := contexts.PreparePendingSourceSetTransition(ctx, plan); err != nil || pending != nil {
		t.Fatalf("completed source-set refresh retained retry work: pending=%v err=%v", pending, err)
	}
}

func nativeReadinessStampSnapshot(t *testing.T, ctx context.Context, db *sql.DB, backend, runID, path string) []any {
	t.Helper()
	query := `SELECT activation_attempt_grant_id, activation_request_id, updated_at FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	if backend == "postgres" {
		query = `SELECT activation_attempt_grant_id::text, activation_request_id::text, updated_at FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
	}
	values := make([]any, 3)
	if err := db.QueryRowContext(ctx, query, runID, path).Scan(&values[0], &values[1], &values[2]); err != nil {
		t.Fatal(err)
	}
	return values
}

func failNativeActorRebind(t *testing.T, db *sql.DB, backend, agentID string) func() {
	t.Helper()
	if backend == "sqlite" {
		if _, err := db.Exec(`CREATE TRIGGER fail_readiness_rebind BEFORE UPDATE ON agents WHEN NEW.agent_id='` + agentID + `' AND NEW.lifecycle_generation_grant_id<>OLD.lifecycle_generation_grant_id BEGIN SELECT RAISE(ABORT, 'injected_readiness_rebind'); END`); err != nil {
			t.Fatal(err)
		}
		return func() {
			if _, err := db.Exec(`DROP TRIGGER fail_readiness_rebind`); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`CREATE FUNCTION fail_readiness_rebind_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.agent_id='` + agentID + `' AND NEW.lifecycle_generation_grant_id<>OLD.lifecycle_generation_grant_id THEN RAISE EXCEPTION 'injected_readiness_rebind'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_readiness_rebind BEFORE UPDATE ON agents FOR EACH ROW EXECUTE FUNCTION fail_readiness_rebind_fn()`); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := db.Exec(`DROP TRIGGER fail_readiness_rebind ON agents`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DROP FUNCTION fail_readiness_rebind_fn()`); err != nil {
			t.Fatal(err)
		}
	}
}

func requireNativeConstructedActors(t *testing.T, ctx context.Context, rt *runtimepkg.Runtime, selected startupRecoveryOrderStore, source semanticview.Source, runID string, children []flowidentity.Instance) map[string]pipeline.WorkflowInstance {
	t.Helper()
	workflow := pipeline.NewWorkflowPersistence(selected)
	headers := map[string]pipeline.WorkflowInstance{}
	if len(children) != 8 {
		t.Fatal("fixture lost its complete nested sibling tree")
	}
	for _, expected := range children {
		cfg, err := rt.Manager.ResolveAgentConfig(runID, "worker", expected.InstancePath)
		if err != nil || expected.ValidateAgentExecution(source, cfg.Identity, cfg.FlowID, cfg.EntityID) != nil || cfg.EntityID != "" {
			t.Fatalf("exact constructed actor missing or rehomed: route=%s config=%+v err=%v", expected.InstancePath, cfg, err)
		}
		owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: expected.Route()}
		header, found, err := workflow.LoadWorkflowInstance(ctx, owner)
		if err != nil || !found {
			t.Fatalf("constructed header missing: route=%s found=%v err=%v", expected.InstancePath, found, err)
		}
		actual, err := header.ConstructionIdentity(owner)
		if err != nil || actual != expected {
			t.Fatalf("constructor parent changed: got=%+v want=%+v err=%v", actual, expected, err)
		}
		headers[expected.InstancePath] = header
	}
	for _, phantom := range []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"} {
		if _, err := rt.Manager.ResolveAgentConfig(runID, "worker", phantom); !errors.Is(err, manager.ErrAgentNotFound) {
			t.Fatalf("authored-path phantom actor survived: %s %v", phantom, err)
		}
	}
	return headers
}
