package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type selectedOperationActivationProbe struct {
	SelectedContractForkLifecycle
	before   func(context.Context)
	fail     error
	discards *int
}

func (p selectedOperationActivationProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, runID string) error {
	*p.discards++
	return p.SelectedContractForkLifecycle.DiscardMaterializedSelectedContractExecutionFork(ctx, runID)
}

func (p selectedOperationActivationProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.before(ctx)
	if p.fail != nil {
		return runfork.RunForkActivation{}, p.fail
	}
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

type selectedOperationPublicationProbe struct {
	SelectedContractReplayPersistence
	after func(context.Context)
}

func (p selectedOperationPublicationProbe) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	result, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
	if err == nil {
		p.after(ctx)
	}
	return result, err
}

func TestSelectedContractOperationReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, point := range []string{"before_execution", "after_event_commit", "before_activation", "activation_failure", "client_cancellation", "process_shutdown"} {
			t.Run(backend+"/"+point, func(t *testing.T) {
				var db *sql.DB
				var selected any
				var owner SelectedContractExecutionOwner
				var capabilityStore startupownership.Store
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, capabilityStore = s, storetest.Database(s), s
					owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, capabilityStore = s, s
					owner = selectedContractExecutionOwnerForTest(t, s)
				}
				ctx, cancel := context.WithCancel(runForkTestContext(t))
				defer cancel()
				capability := selectedContractTestProcessCapability(t, ctx, capabilityStore)
				ctx, bindErr := startupownership.WithProcessMutationAuthority(ctx, capability)
				if bindErr != nil {
					t.Fatal(bindErr)
				}
				root := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple")}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				sourceID, eventID := uuid.NewString(), uuid.NewString()
				seedSelectedOperationSource(t, ctx, selected, loaded, sourceID, eventID, true)
				ordinary, contexts := liveForkRuntimeFixture(t, ctx, selected, owner, loaded, sourceID, executionposture.MockOnly)
				process, _ := worklifetime.ProcessFromContext(ctx)
				retireRuntime := func() {
					t.Helper()
					if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if point == "before_execution" {
					retireRuntime()
				}
				var bound *worklifetime.SelectedForkOccurrence
				var interruptedSnapshot map[string]storetest.SelectedForkStorageTableSnapshot
				failure := errors.New("injected activation failure")
				var discards int
				activation := selectedOperationActivationProbe{SelectedContractForkLifecycle: owner.ports.fork, discards: &discards, before: func(ctx context.Context) {
					occurrence, ok := worklifetime.OccurrenceFromContext(ctx)
					bound, _ = occurrence.(*worklifetime.SelectedForkOccurrence)
					if !ok || bound == nil || process.ActiveCount() == 0 {
						t.Fatal("activation lost selected process ownership")
					}
					if point == "process_shutdown" || point == "activation_failure" {
						var err error
						interruptedSnapshot, err = storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
						if err != nil {
							t.Fatal(err)
						}
						if point == "process_shutdown" {
							process.Retire()
							<-ctx.Done()
						}
					}
					if point != "before_execution" && point != "after_event_commit" {
						retireRuntime()
					}
					if point == "client_cancellation" {
						cancel()
						if err := ctx.Err(); err != nil {
							t.Fatalf("committed work inherited client cancellation: %v", err)
						}
					}
				}}
				if point == "activation_failure" {
					activation.fail = failure
				}
				owner.ports.fork = activation
				if point == "after_event_commit" {
					owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(context.Context) { retireRuntime() }}
				}
				request := SelectedContractExecutionRequest{
					SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
					LiveRuntime:       ordinary, LiveContexts: contexts, LiveRuns: selected.(runlifecycle.LiveRunReader),
				}
				request.ForkOperation = &runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "operation-lifetime", IdempotencyKey: point,
					TransportHash: "operation-lifetime-" + point, SourceRunID: sourceID, ForkEventID: eventID,
					TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), ContractSelection: request.ContractSelection, AllowSourceFreeze: true,
				}
				result, err := ExecuteSelectedContractRunFork(ctx, request)
				if point == "activation_failure" || point == "process_shutdown" {
					if (point != "process_shutdown" && !errors.Is(err, failure)) || err == nil {
						t.Fatalf("activation failure: %v", err)
					}
					var sourceState, forkState, executionState string
					wantFork, wantExecution := "paused", "running"
					if err := db.QueryRow(`SELECT s.status, f.status, e.state FROM runs s JOIN runs f ON f.forked_from_run_id=s.run_id JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=f.run_id WHERE s.run_id=$1`, sourceID).Scan(&sourceState, &forkState, &executionState); err != nil || sourceState != "paused" || forkState != wantFork || executionState != wantExecution {
						t.Fatalf("failure disposition source/fork/execution = %s/%s/%s: %v", sourceState, forkState, executionState, err)
					}
					var afterRetirement map[string]storetest.SelectedForkStorageTableSnapshot
					afterRetirement, err = storetest.ReadSelectedForkApplicationStorageSnapshot(context.WithoutCancel(ctx), selected)
					if err != nil {
						t.Fatal(err)
					}
					for _, table := range []string{"events", "event_deliveries", "entity_state", "entity_mutations"} {
						if !reflect.DeepEqual(interruptedSnapshot[table], afterRetirement[table]) {
							t.Fatalf("unacknowledged handoff dispatched or discarded retained %s", table)
						}
					}
					record, found, err := selected.(liveWinnerOperationReader).LoadForkOperation(context.WithoutCancel(ctx), request.ForkOperation.Actor, point, request.ForkOperation.TransportHash)
					if err != nil || !found || record.Status != runfork.ForkOperationMaterialized || record.Result != nil || record.ForkRunID != result.Materialization.ForkRunID {
						t.Fatalf("unacknowledged operation lost recoverable materialization: %+v found=%t err=%v", record, found, err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					observed, err := observeSelectedRetainedExecution(t, context.WithoutCancel(ctx), owner, selected, result.Materialization.ForkRunID)
					if err != nil || observed.ExecutedEventCount != 1 || result.Activation.ForkRunStatus != string(runlifecycle.StateLive) || discards != 0 {
						t.Fatalf("retained execution count=%d activation=%s discards=%d: %v", observed.ExecutedEventCount, result.Activation.ForkRunStatus, discards, err)
					}
					var status, executionStatus string
					if err := db.QueryRow(`SELECT r.status, e.state FROM runs r JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=r.run_id WHERE r.run_id=$1`, result.Materialization.ForkRunID).Scan(&status, &executionStatus); err != nil || status != string(runlifecycle.StateLive) || executionStatus != "closed" {
						t.Fatalf("durable activation/execution = %s/%s: %v", status, executionStatus, err)
					}
					stage := storetest.ObserveWriterStage(t, context.WithoutCancel(ctx), selected, result.Materialization.ForkRunID, result.Materialization.ForkRunID, "done")
					if stage.State != "done" {
						t.Fatalf("live successor did not retain its ended instance: %+v", stage)
					}
					var lineage int
					if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id=$1 AND fork_run_id=$2`, sourceID, result.Materialization.ForkRunID).Scan(&lineage); err != nil || lineage != 1 {
						t.Fatalf("exact execution lineage = %d: %v", lineage, err)
					}
				}
				if bound == nil {
					t.Fatal("activation was not reached")
				}
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := bound.Begin(context.Background()); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("operation did not release selected execution: %v", err)
				}
				if err := ordinary.Shutdown(); err != nil {
					t.Fatal(err)
				}
				if process.ActiveCount() != 0 {
					t.Fatalf("operation leaked %d process leases", process.ActiveCount())
				}
			})
		}
	}
}

func seedSelectedOperationSource(t *testing.T, ctx context.Context, selected any, loaded LoadedSelectedContractSource, runID, eventID string, live bool, input ...events.Event) {
	t.Helper()
	ctx = originalSelectedFixtureContext(t, ctx, selected.(startupownership.Store))
	at := time.Unix(1700002200, 0).UTC()
	ctx = originalSelectedFixtureContext(t, ctx, any(selected).(startupownership.Store))
	fixture := storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash()), StartedAt: at.Add(-time.Minute)}
	if live {
		fixture.Origin, fixture.LiveScope = runlifecycle.DeploymentRunOrigin(), runlifecycle.LiveRunScope{FlowPath: "."}
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), fixture)
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "item.received", "source-runtime", "", []byte(`{}`), 0, runID,
		events.EventEnvelope{}, events.NoRoutingSource(), at, executionmode.Mock)
	if len(input) == 1 {
		event = input[0]
	}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("source-only-node")}, pipelineobligation.ScopeSubscribed)
	commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
}

// This is explicit component construction with no route/arm installation, not
// public activation qualification. Source history must still contain a real
// constructed header; imported fields cannot authorize selected execution.
func commitSelectedOperationRootFixture(t *testing.T, ctx context.Context, selected any, loaded LoadedSelectedContractSource, event events.Event) {
	t.Helper()
	flowID := semanticview.RootExecutionFlowID(loaded.Source)
	identity := flowidentity.Stored(loaded.Source, flowID, event.RunID(), event.RunID(), event.RunID(), "")
	command := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, identity)
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, event.RunID()), loaded.SourceArtifactFact), event.ExecutionMode())
	committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Created || !committed.Acknowledged {
		t.Fatalf("component source construction: acknowledged=%v created=%v err=%v", committed.Acknowledged, committed.Created, err)
	}
}

func selectedExecutionSourceFlowCommand(t *testing.T, ctx context.Context, loaded LoadedSelectedContractSource, event events.Event, identity flowidentity.Instance, input ...string) runtimebus.FlowInstanceActivationCommand {
	t.Helper()
	constructorInput := ""
	if len(input) > 1 {
		t.Fatal("source fixture accepts one exact constructor input")
	}
	if len(input) == 1 {
		constructorInput = input[0]
	}
	constructor, err := pipeline.CompileFlowConstructor(loaded.Source, identity.TemplateID, constructorInput)
	if err != nil {
		t.Fatal(err)
	}
	var resolvedKey any
	var payload map[string]any
	if constructorInput != "" {
		if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		resolvedKey = payload[constructor.KeyField()]
	}
	fields, err := constructor.InitialFields(payload, resolvedKey)
	if err != nil {
		t.Fatal(err)
	}
	instanceKey, err := pipeline.AdmitFlowInstanceKey(loaded.Source, identity.TemplateID, resolvedKey)
	if err != nil {
		t.Fatal(err)
	}
	graph, found := semanticview.WorkflowStageTopology(loaded.Source, identity.TemplateID)
	if !found {
		t.Fatal("component construction requires its compiled stage topology")
	}
	stage, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := entityruntime.ResolveForFlow(loaded.Source, identity.TemplateID)
	readiness := pipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: identity, RunID: event.RunID(), BundleHash: loaded.SourceArtifactFact.BundleHash(),
		WorkflowVersion: loaded.Source.WorkflowVersion(), ExecutionMode: event.ExecutionMode(),
	}
	schema, found := loaded.Source.FlowSchemaByID(identity.TemplateID)
	if !found {
		t.Fatal("source fixture lost its immutable flow owner")
	}
	instance := pipeline.WorkflowInstance{
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
		InstanceKey:  instanceKey,
		ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
		EntityType: contract.EntityType, WorkflowName: identity.TemplateID, WorkflowVersion: loaded.Source.WorkflowVersion(),
		Mode: schema.EffectiveMode(), CurrentState: stage.ID(), StageDefined: graph.StageCount() != 0,
		Fields: fields, RuntimeReadiness: &readiness, CreatedAt: event.CreatedAt(), EnteredStageAt: event.CreatedAt(),
	}
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, event.RunID()), loaded.SourceArtifactFact), event.ExecutionMode())
	command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, event.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	return command
}
