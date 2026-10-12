package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type nestedDeadlineWorkerHold struct {
	node    string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type nestedDeadlineServingContext struct {
	runlifecycle.OperationOwner
	sourceRun string
	contexts  chan context.Context
}

func (r nestedDeadlineServingContext) RequireActiveRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error) {
	fact, err := r.OperationOwner.RequireActiveRunSource(ctx, runID)
	authority, present := effects.AuthorityFromContext(ctx)
	if err == nil && runID != r.sourceRun && present && authority.SelectedFork.ForkRunID == runID {
		select {
		case r.contexts <- ctx:
		default:
		}
	}
	return fact, err
}

func (p *nestedDeadlineWorkerHold) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerStarted || signal.SubscriberID != p.node {
		return
	}
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
	case <-ctx.Done():
	}
}

// The source uses the normal runtime wiring but holds timer startup, so the
// selected child alone can fire the original, overdue deadlines.
func nestedDeadlineSourceRuntime(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource) *rootruntime.Runtime {
	t.Helper()
	nodes, err := pipeline.LoadWorkflowNodes(loaded.Source)
	if err != nil {
		t.Fatal(err)
	}
	process, _ := worklifetime.ProcessFromContext(ctx)
	p := owner.ports
	rt, err := rootruntime.NewRuntime(ctx, rootruntime.RuntimeDeps{
		Config: &config.Config{Runtime: config.RuntimeConfig{MaxConcurrentAgents: 4}, LLM: config.LLMConfig{Backend: "anthropic"}},
		Options: rootruntime.RuntimeOptions{ExecutionPosture: executionposture.MockOnly, RuntimeInstanceID: runForkTestRuntimeInstanceID,
			SourceArtifactFact: loaded.SourceArtifactFact, ProcessWorkOwner: process,
			WorkflowModule: selectedContractWorkflowModule{source: loaded.Source, nodes: nodes, guardRegistry: pipeline.NewContractGuardRegistry(loaded.Source)}},
		WorkflowPersistence: p.workflow, EventStore: p.events, EventBusDurable: p.busDurable,
		EventPayloadAdmissionBinder: selected.(rootruntime.EventPayloadAdmissionBinder), AuthorActivityRegistrars: []rootruntime.AuthorActivityCatalogRegistrar{p.fork},
		RunBundleAvailability: p.fork, RunControlStore: selected.(runcontrol.Store), RunLifecycleCandidates: p.candidates,
		RuntimeLogStore: p.logs, ManagerStore: p.manager, ManagerPersistenceRoles: p.managerRoles,
		EffectsStore: p.effects, CompletionStore: p.completion, CompletionHeartbeatStore: p.completionHeartbeat,
		ManagedCapabilitiesStore: p.managedCapabilities, DeliveryStore: p.busDurable.DeliveryLifecycle, PipelineObligations: p.pipelineObligations,
		GenericScheduleStore: p.genericSchedules, DecisionCards: p.decisionCards, ProposedEffects: p.proposedEffects,
		DecisionCardHumanTasks: p.humanTasks, DecisionCardDraftExpiry: p.decisionCardDraftExpiry, HumanTaskExpiry: p.humanTaskExpiry,
		BudgetSpendStore: p.budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: loaded.SourceArtifactFact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	capability := owner.ports.contexts.capability
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: loaded.SourceArtifactFact.BundleHash(),
		RuntimeInstanceID: runForkTestRuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.busDurable.DeliveryLifecycle.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if err := rt.Bus.SetDeliveryAuthority(authority); err != nil {
		t.Fatal(err)
	}
	if err := rt.Bus.SetDeliveryContinuationOwner(runtimebustest.NewDeliveryContinuationOwner(false)); err != nil {
		t.Fatal(err)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Scheduler.PrepareStartup(); err != nil {
		t.Fatal(err)
	}
	serving, err := startupownership.StartFanOutServing(ctx, grant, rt.WorkOccurrence(), nil, rt.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	rt.Pipeline.InstallFanOutWorkNotifier(serving)
	t.Cleanup(serving.Close)
	return rt
}

func nestedDeadlineArm(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, row replycontext.Record) joinruntime.Activation {
	t.Helper()
	instance, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: row.RunID,
		Route: flowidentity.StoredRoute(row.Origin.FlowID, row.Origin.FlowInstance, row.Origin.FlowInstance)})
	if err != nil || !found {
		t.Fatalf("read exact reply origin: found=%t error=%v", found, err)
	}
	buckets, err := joinruntime.PersistedBuckets(instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	arms, err := joinruntime.List(buckets)
	if err != nil || len(arms) != 1 || len(row.ReturnJoins) != 1 || !arms[0].JoinRef().Equal(row.ReturnJoins[0].Ref) {
		t.Fatalf("reply lost its exact retained arm: row=%+v arms=%+v error=%v", row, arms, err)
	}
	return arms[0]
}

func TestIssue642SelectedDepthThreeOriginalDeadlinesBothStores(t *testing.T) {
	proveSelectedDepthThreeDeadlines(t, false)
}

func TestIssue642SelectedDepthThreeReplyRefusalsBothStores(t *testing.T) {
	proveSelectedDepthThreeDeadlines(t, true)
}

func proveSelectedDepthThreeDeadlines(t *testing.T, refusals bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			root := canonicalrouting.CopyNestedFanOutReplyDeadlines(t)
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			servingContexts := make(chan context.Context, 1)
			owner.ports.busDurable.RunLifecycle = nestedDeadlineServingContext{OperationOwner: owner.ports.busDurable.RunLifecycle, sourceRun: runID, contexts: servingContexts}
			ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), runID)
			ctx = effects.WithExecutionMode(ctx, executionmode.Mock)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
			at := runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-2 * time.Hour))
			bundle, _ := semanticview.Bundle(loaded.Source)
			storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
				BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
			rt := nestedDeadlineSourceRuntime(t, ctx, selected, owner, loaded)
			ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
			created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
				Instance: flowidentity.Stored(loaded.Source, ".", runID, runID, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct source requester tree: created=%t error=%v", created, err)
			}
			node, err := identity.AdmitExecutableNodeDeclaration("u", "u-node")
			if err != nil {
				t.Fatal(err)
			}
			hold := &nestedDeadlineWorkerHold{node: node.Key(), started: make(chan struct{}), release: make(chan struct{})}
			rt.Pipeline.SetTestLifecycleProbe(hold)
			var release sync.Once
			resume := func() { release.Do(func() { close(hold.release) }) }
			t.Cleanup(resume)
			input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "request.started", "nested-deadline-proof", "",
				[]byte(`{"account_id":"same-account","request_id":"same-business-key"}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at, executionmode.Mock)
			publishCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			published := make(chan error, 1)
			go func() { published <- rt.Bus.PublishAndWait(publishCtx, input) }()
			select {
			case <-hold.started:
			case err := <-published:
				t.Fatalf("inner source did not remain open: %v", err)
			case <-publishCtx.Done():
				t.Fatal(publishCtx.Err())
			}
			rows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
			if err != nil || len(rows) != 3 {
				t.Fatalf("actual open source chain: %+v error=%v", rows, err)
			}
			cut := ""
			for _, row := range rows {
				arm := nestedDeadlineArm(t, ctx, owner, row)
				if row.State != replycontext.StateOpen || arm.Status != joinruntime.StatusOpen || !arm.ArmedAt.Equal(at) || !arm.DeadlineAt.Equal(at.Add(time.Hour)) {
					t.Fatalf("source lost its original open due: row=%+v arm=%+v", row, arm)
				}
				if row.ProviderFlowID == "u" {
					cut = row.RequestEventID
				}
			}
			if cut == "" {
				t.Fatal("open chain has no committed inner request cut")
			}
			// The historical cut is unclaimed. Drain the real source's later
			// claim before activation; an in-flight post-cut claim cannot be
			// reassigned to a child merely because materialization succeeded.
			resume()
			select {
			case err := <-published:
				if err != nil {
					t.Fatalf("finish original source input: %v", err)
				}
			case <-publishCtx.Done():
				t.Fatalf("original source input bound: %v", publishCtx.Err())
			}
			drain, cancelDrain := context.WithTimeout(ctx, 20*time.Second)
			defer cancelDrain()
			for {
				current, err := storetest.ReadReplyContextRecords(drain, selected, runID)
				if err != nil {
					t.Fatal(err)
				}
				innerReturned := false
				for _, row := range current {
					if row.ProviderFlowID == "u" && row.State == replycontext.StateTerminal {
						innerReturned = true
					}
				}
				summary, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(drain, runID)
				if err != nil {
					t.Fatal(err)
				}
				if innerReturned && summary.Total == summary.Delivered {
					break
				}
				select {
				case <-drain.Done():
					t.Fatalf("source has unfinished post-cut delivery: %+v cause=%v", summary, context.Cause(drain))
				case <-time.After(10 * time.Millisecond):
				}
			}
			sourceRows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
			if err != nil || len(sourceRows) != 3 {
				t.Fatalf("settled source inventory: %+v error=%v", sourceRows, err)
			}
			sourceArms := map[string]joinruntime.Activation{}
			for _, row := range sourceRows {
				sourceArms[row.ID] = nestedDeadlineArm(t, ctx, owner, row)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			options := SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability}
			childHold := &nestedDeadlineWorkerHold{node: node.Key(), started: make(chan struct{}), release: make(chan struct{})}
			var releaseChild sync.Once
			resumeChild := func() { releaseChild.Do(func() { close(childHold.release) }) }
			t.Cleanup(resumeChild)
			options.AgentManagerOptions.TestLifecycleProbe = childHold
			prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{SourceRunID: runID, At: cut, AllowSourceFreeze: true,
				SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			}()
			request, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			request.ForkOperation = &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "nested-deadline-proof", IdempotencyKey: uuid.NewString(),
				TransportHash: "nested-deadline-proof", SourceRunID: runID, ForkEventID: cut, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), ContractSelection: selection, AllowSourceFreeze: true}
			staged, err := owner.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			childRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
			if err != nil || len(childRows) != 3 {
				t.Fatalf("projected open chain: %+v error=%v", childRows, err)
			}
			for _, row := range childRows {
				arm := nestedDeadlineArm(t, ctx, owner, row)
				if row.RunID != staged.ForkRunID || row.State != replycontext.StateOpen || arm.Status != joinruntime.StatusOpen || !arm.ArmedAt.Equal(at) || !arm.DeadlineAt.Equal(at.Add(time.Hour)) {
					t.Fatalf("projection moved an original due coordinate: row=%+v arm=%+v", row, arm)
				}
			}
			activated, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: runID, At: cut,
				AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options, ForkOperation: request.ForkOperation})
			if err != nil || !activated.Activation.Activated || activated.Materialization.ForkRunID != staged.ForkRunID {
				t.Fatalf("activate exact selected child: %+v error=%v", activated, err)
			}
			wait, cancelWait := context.WithTimeout(ctx, 20*time.Second)
			defer cancelWait()
			var servingCtx context.Context
			select {
			case servingCtx = <-servingContexts:
			case <-wait.Done():
				t.Fatal("selected serving admission was not observed")
			}
			for claimed := false; !claimed; {
				select {
				case <-childHold.started:
					claimed = true
				default:
				}
				header, err := selected.(interface {
					LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				}).LoadRunHeader(wait, staged.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				if header.Failure != nil {
					t.Fatalf("selected deadline child failed before inherited claim: failure=%+v serving_cause=%v", *header.Failure, context.Cause(servingCtx))
				}
				if !claimed {
					select {
					case <-wait.Done():
						storage, err := storetest.ReadSelectedExecutionStorage(context.WithoutCancel(ctx), selected, staged.ForkRunID)
						t.Fatalf("selected child did not claim its exact inherited request: header=%+v execution=%+v storage_error=%v cause=%v", header, storage, err, context.Cause(wait))
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			for {
				closed := 0
				for _, row := range childRows {
					arm := nestedDeadlineArm(t, wait, owner, row)
					if arm.Status == joinruntime.StatusClosed && arm.CloseReason == joinruntime.CloseReasonDeadline && arm.OutcomeFired && !arm.OutcomePending && arm.Completed() == 0 {
						closed++
					}
				}
				if closed == 3 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("selected original deadlines did not settle: closed=%d cause=%v", closed, context.Cause(wait))
				case <-time.After(10 * time.Millisecond):
				}
			}
			after, err := storetest.ReadReplyContextRecords(wait, selected, runID)
			if err != nil || !reflect.DeepEqual(sourceRows, after) {
				t.Fatalf("selected deadlines changed source replies: %+v error=%v", after, err)
			}
			for _, row := range sourceRows {
				if arm := nestedDeadlineArm(t, wait, owner, row); !reflect.DeepEqual(sourceArms[row.ID], arm) {
					t.Fatalf("selected timeout changed a source arm: before=%+v after=%+v", sourceArms[row.ID], arm)
				}
			}
			for _, row := range childRows {
				arm := nestedDeadlineArm(t, wait, owner, row)
				if !arm.ArmedAt.Equal(at) || !arm.DeadlineAt.Equal(at.Add(time.Hour)) {
					t.Fatalf("selected execution reset retained time: %+v", arm)
				}
			}
			plan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: staged.ForkRunID})
			if err != nil || len(plan.JoinSchedules) != 3 {
				t.Fatalf("physical selected timeout inventory: %+v error=%v", plan.JoinSchedules, err)
			}
			for _, row := range childRows {
				arm := nestedDeadlineArm(t, wait, owner, row)
				matched := 0
				for _, schedule := range plan.JoinSchedules {
					if schedule.Command.TaskID != arm.TimerTaskID() {
						continue
					}
					matched++
					if err := genericschedule.ValidateWorkflowJoinScheduleRelation(arm, schedule); err != nil || schedule.Status != genericschedule.StatusFired ||
						!schedule.InitialDueAt.Equal(at.Add(time.Hour)) || !schedule.CurrentDueAt.Equal(at.Add(time.Hour)) ||
						schedule.ForkJoinOrigin == nil || schedule.ForkJoinOrigin.SourceRunID != runID || schedule.ForkJoinOrigin.PointEventID != cut {
						t.Fatalf("timeout changed its original schedule or cut: %+v error=%v", schedule, err)
					}
					publication, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(wait, schedule.CurrentEventID)
					if err != nil || !found || publication.Event.ID() != genericschedule.OccurrenceEventID(schedule.ID, schedule.CurrentDueAt) ||
						publication.Event.Event().RunID() != staged.ForkRunID || publication.Event.Event().Type() != "platform.join_timeout" ||
						!publication.Event.Event().CreatedAt().Equal(at.Add(time.Hour)) {
						t.Fatalf("original deadline lacks its exact physical child event: %+v found=%t error=%v", publication, found, err)
					}
				}
				if matched != 1 {
					t.Fatalf("timeout schedule duplicated or missing: count=%d arm=%+v", matched, arm)
				}
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, staged.ForkRunID, "platform.join_timeout"); err != nil || count != 3 {
				t.Fatalf("selected timeout count changed: count=%d error=%v", count, err)
			}
			resumeChild()
			if refusals {
				proveSelectedDeadlineReplyRefusals(t, servingCtx, selected, owner, loaded, childRows)
				finalRows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
				if err != nil || !reflect.DeepEqual(sourceRows, finalRows) {
					t.Fatalf("selected refusal changed source reply evidence: %+v error=%v", finalRows, err)
				}
				for _, row := range sourceRows {
					if arm := nestedDeadlineArm(t, ctx, owner, row); !reflect.DeepEqual(sourceArms[row.ID], arm) {
						t.Fatalf("selected refusal changed source arm: %+v", arm)
					}
				}
			}
		})
	}
}
