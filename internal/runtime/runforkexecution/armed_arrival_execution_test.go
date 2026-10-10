package runforkexecution

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

func armedArrivalFixture(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyExactJoinEventBusProof(t, "")
	path := filepath.Join(root, "entities.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "    initial: []\n") != 1 {
		t.Fatal("armed arrival fixture lost its exact membership initializer")
	}
	data = []byte(strings.Replace(string(data), "    initial: []\n", "    initial: [member-a]\n", 1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func seedArmedArrivalSource(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, runID string) (string, genericschedule.Activation, joinruntime.Activation) {
	t.Helper()
	ctx = effects.WithExecutionMode(correlation.WithRunID(ctx, runID), executionmode.Mock)
	at := runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-2 * time.Hour))
	bundle, found := semanticview.Bundle(loaded.Source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("armed arrival source lacks its immutable artifact")
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
		BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: at,
	})
	marker := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "item.completed", "source-runtime", "",
		[]byte(`{"member_id":"cut","result":{"value":"cut"}}`), 0, runID, events.EventEnvelope{}, events.NoRoutingSource(), at.Add(time.Second), executionmode.Mock)
	admission, err := rootruntime.NewRuntimePayloadAdmitter(nil, loaded.Source, loaded.SourceArtifactFact)(ctx, marker, ".")
	if err != nil {
		t.Fatal(err)
	}
	marker, err = events.ApplyPayloadAdmission(marker, admission)
	if err != nil {
		t.Fatal(err)
	}
	work, found := worklifetime.OccurrenceFromContext(ctx)
	if !found {
		t.Fatal("armed arrival construction requires its owned occurrence")
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventBus, err := bus.NewEventBusWithOptions(owner.ports.events, bus.EventBusOptions{
		ExecutionPosture: executionposture.MockOnly, WorkOwner: work, PipelineObligations: owner.ports.pipelineObligations,
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact, RuntimeInstanceID: runForkTestRuntimeInstanceID,
		DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(), Durable: owner.ports.busDurable,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: selectedContractWorkflowModule{source: loaded.Source}, Persistence: owner.ports.workflow,
		SourceArtifactFact: loaded.SourceArtifactFact, ExecutionPosture: executionposture.MockOnly,
		ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work, RunLifecycle: selected.(runlifecycle.OperationOwner),
		PipelineObligations: owner.ports.pipelineObligations, DeliveryStore: owner.ports.busDurable.DeliveryLifecycle,
		DeadLetters: selected.(deadletters.AcknowledgedRecorder), DeliveryRuntime: eventBus,
		DecisionCards: owner.ports.decisionCards, ProposedEffects: owner.ports.proposedEffects, HumanTasks: owner.ports.humanTasks,
		DecisionCardDraftExpiry: owner.ports.decisionCardDraftExpiry, HumanTaskExpiry: owner.ports.humanTaskExpiry,
	})
	if coordinator == nil {
		t.Fatal("armed arrival source lifecycle planner was not admitted")
	}
	identity := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	command := selectedExecutionSourceFlowCommand(t, ctx, loaded, marker, identity)
	instance, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx,
		flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()}, command.Plan.Instance, marker.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	command, err = flowactivationfixture.Command(ctx, instance, lifecycle, marker.CreatedAt())
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("canonical armed arrival construction: %+v err=%v", committed, err)
	}
	if err := committed.Validate(); err != nil {
		t.Fatal(err)
	}
	if committed.Plan.Identity != identity || committed.Plan.Readiness.Identity != identity {
		t.Fatalf("armed arrival construction lost its complete root/readiness: %+v", committed.Plan)
	}
	header, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil || !found || header.CurrentState != "awaiting" {
		t.Fatalf("constructed armed arrival source: %+v found=%v err=%v", header, found, err)
	}
	buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(buckets)
	if err != nil || len(joins) != 1 {
		t.Fatalf("constructed arrival arm: %+v err=%v", joins, err)
	}
	join := joins[0]
	if join.Status != joinruntime.StatusOpen || join.Expected() != 1 || join.Completed() != 0 ||
		!reflect.DeepEqual(join.Members, []string{"member-a"}) || join.OutcomePending || join.OutcomeFired ||
		join.TimerHandle().Kind() != timeridentity.TimerHandleJoinTimeout || !join.ArmedAt.Equal(marker.CreatedAt()) ||
		!join.DeadlineAt.Equal(join.ArmedAt.Add(time.Hour)) || !join.DeadlineAt.Before(time.Now().UTC()) {
		t.Fatalf("source is not an open, nonempty, overdue original deadline: %+v", join)
	}
	driver, dispatch := &publishedJoinSourceDriver{}, &publishedJoinSourceDispatch{}
	schedules := selected.(genericschedule.Store)
	clock, err := genericschedule.NewLifecycle(schedules, driver, eventBus, dispatch,
		rootruntime.NewGenericScheduleRuntimeLogger(rootruntime.NewRuntimeLogger(owner.ports.logs, executionposture.MockOnly, nil)), executionposture.MockOnly)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clock.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := clock.ReconcileRunWakeups(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if driver.callback == nil || driver.wakeup.ActivationID() == "" {
		t.Fatal("source deadline was not registered with its exact wakeup owner")
	}
	// Registration is not firing: the overdue source callback is never invoked.
	source, found, err := schedules.LoadGenericScheduleActivation(ctx, driver.wakeup.ActivationID())
	if err != nil || !found || source.Status != genericschedule.StatusActive || source.CurrentEventID != "" || dispatch.event.ID() != "" ||
		!source.InitialDueAt.Equal(join.DeadlineAt) || !source.CurrentDueAt.Equal(join.DeadlineAt) {
		t.Fatalf("source deadline was prepared or published before the cut: %+v found=%v err=%v", source, found, err)
	}
	if err := genericschedule.ValidateWorkflowJoinScheduleRelation(join, source); err != nil {
		t.Fatal(err)
	}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, marker, nil, pipelineobligation.ScopeSubscribed)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	return marker.ID(), source, join
}

func TestIssue642ArmedArrivalTimeoutContinuesExactDeliveryBothStores(t *testing.T) {
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
			process, _ := worklifetime.ProcessFromContext(ctx)
			baseline := process.ActiveCount()
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: armedArrivalFixture(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
			scope, err := authoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx = authoractivity.WithScope(ctx, scope)
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := owner.ports.fork.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			sourceRun := uuid.NewString()
			marker, source, originalJoin := seedArmedArrivalSource(t, ctx, selected, owner, loaded, sourceRun)
			plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].ID != source.ID ||
				plan.JoinSchedules[0].Status != genericschedule.StatusActive || plan.JoinSchedules[0].CurrentEventID != "" || len(plan.TransferredJoins) != 0 || len(plan.PendingWork) != 0 {
				t.Fatalf("fixed cut lost its sole unpublished deadline or invented delivery work: %+v err=%v", plan, err)
			}
			requireArmedArrivalAvailability(t, plan)
			sourceDigest, err := source.EvidenceDigest()
			if err != nil {
				t.Fatal(err)
			}
			cutDigest, err := plan.JoinSchedules[0].EvidenceDigest()
			if err != nil || cutDigest != sourceDigest {
				t.Fatalf("fixed cut changed the source deadline: digest=%s err=%v", cutDigest, err)
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "armed-arrival", IdempotencyKey: "fixed-cut",
				TransportHash: "armed-arrival-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			// The source callback is held; only the selected child may fire this arm.
			if err != nil || !result.Activation.Activated {
				t.Fatalf("selected armed arrival activation must succeed: %+v err=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			acknowledged, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil || acknowledged.Request.ResolvedPoint == nil {
				t.Fatalf("armed arrival lost its permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
			}
			wait, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			reader := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			for {
				header, err := reader.LoadRunHeader(wait, child)
				if err != nil {
					t.Fatal(err)
				}
				if header.Failure != nil {
					t.Fatalf("selected armed arrival failed: %+v", *header.Failure)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("armed arrival did not complete and release resources: header=%+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			workflow := flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(".", child, child)}
			header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, workflow)
			if err != nil || !found || header.CurrentState != "attention" || header.EntityID != child || header.WorkflowName != "." {
				t.Fatalf("exact timeout receiver did not reach attention: %+v found=%v err=%v", header, found, err)
			}
			buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			joins, err := joinruntime.List(buckets)
			if err != nil || len(joins) != 1 {
				t.Fatalf("child lost its retained arrival arm: %+v err=%v", joins, err)
			}
			join := joins[0]
			if join.Status != joinruntime.StatusClosed || !join.OutcomeFired || join.OutcomePending || join.Expected() != 1 || join.Completed() != 0 ||
				!reflect.DeepEqual(join.Members, originalJoin.Members) || !reflect.DeepEqual(join.Missing(), originalJoin.Missing()) ||
				!join.ArmedAt.Equal(originalJoin.ArmedAt) || !join.DeadlineAt.Equal(originalJoin.DeadlineAt) || join.TransferredPublication != nil {
				t.Fatalf("timeout changed original due/members or invented accepted-source transfer: %+v", join)
			}
			ref, originalRef := join.JoinRef(), originalJoin.JoinRef()
			entry, originalEntry := ref.StageEntry(), originalRef.StageEntry()
			if !ref.Declaration().Equal(originalRef.Declaration()) || ref.FlowPath() != "." || entry.RunID != child || entry.EntityID != child ||
				entry.InstanceID != child || entry.InstancePath != child || entry.OriginRunID != sourceRun || entry.Stage != originalEntry.Stage ||
				entry.Cause != originalEntry.Cause || entry.EventID != originalEntry.EventID || entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID {
				t.Fatalf("timeout retargeted its retained arm: %+v", ref)
			}
			childPlan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child})
			if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.WorkflowTimers) != 0 || len(childPlan.TransferredJoins) != 0 {
				t.Fatalf("child lost or duplicated its physical inherited deadline: %+v err=%v", childPlan, err)
			}
			actual := childPlan.JoinSchedules[0]
			origin := actual.ForkJoinOrigin
			if actual.Status != genericschedule.StatusFired || actual.Command.RunID != child || actual.Command.EntityID != child ||
				actual.Command.TaskID != join.TimerTaskID() || actual.Command.ExecutionMode != source.Command.ExecutionMode ||
				!actual.InitialDueAt.Equal(source.InitialDueAt) || !actual.CurrentDueAt.Equal(source.CurrentDueAt) ||
				origin == nil || origin.SourceActivationID != source.ID || origin.SourceRunID != sourceRun || !origin.SourceAdmittedAt.Equal(source.AdmittedAt) ||
				string(origin.PointKind) != string(plan.ForkPoint.Kind) || origin.PointRevision != plan.ForkPoint.Revision || origin.PointEventID != plan.ForkPoint.EventID ||
				actual.AcceptedAt.Before(actual.AdmittedAt) || !actual.CurrentDueAt.Before(actual.AdmittedAt) {
				t.Fatalf("actual child timeout lost its source/cut/original due or executed before admission: %+v", actual)
			}
			event := storetest.LoadCanonicalEventRecord(t, wait, selected, actual.CurrentEventID)
			if event.ID() != genericschedule.OccurrenceEventID(actual.ID, source.CurrentDueAt) || event.Type() != "platform.join_timeout" ||
				event.SourceAgent() != genericschedule.OccurrenceProducerID() || !event.CreatedAt().Equal(originalJoin.DeadlineAt) || event.TaskID() != join.TimerTaskID() {
				t.Fatalf("child did not publish the exact native retained timeout: %+v", event)
			}
			if _, err := actual.ValidatePublishedOccurrence(event); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_timeout"); err != nil || count != 1 {
				t.Fatalf("child timeout publication count=%d err=%v", count, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 0 {
				t.Fatalf("incomplete membership acquired a completion publication: count=%d err=%v", count, err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child, EntityID: child})}
			deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := owner.ports.busDurable.DeliveryLifecycle.Snapshot(wait, deliveryID)
			if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.EventID != event.ID() || delivery.RunID != child ||
				delivery.SubscriberClass != deliverylifecycle.SubscriberNode || delivery.SubscriberID != ref.Node().Key() ||
				delivery.Route.Recipient != route.Recipient || !delivery.Route.Target.ExistingEntity() || !events.SameRouteIdentity(delivery.Route.Target.Route(), route.Target.Route()) {
				t.Fatalf("exact inherited timeout delivery did not settle: %+v err=%v", delivery, err)
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
			if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
				t.Fatalf("child did not settle exactly one timeout obligation: %+v err=%v", settlement, err)
			}
			original, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, sourceRun), source.ID)
			if err != nil || !found {
				t.Fatalf("source deadline disappeared: found=%v err=%v", found, err)
			}
			afterDigest, err := original.EvidenceDigest()
			if err != nil || afterDigest != sourceDigest || original.Status != genericschedule.StatusActive || original.CurrentEventID != "" {
				t.Fatalf("selected timeout changed or published its source deadline: %+v err=%v", original, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, sourceRun, "platform.join_timeout"); err != nil || count != 0 {
				t.Fatalf("source deadline was published: count=%d err=%v", count, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("child timeout changed source business facts: %v", err)
			}
			retry, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
				t.Fatalf("timeout settlement changed the permanent fork acknowledgment: %+v err=%v", retry, err)
			}
		})
	}
}

func requireArmedArrivalAvailability(t *testing.T, plan runfork.RunForkPlan) {
	t.Helper()
	zero := runfork.RunForkContractFrontierAdmission{}
	ready, blockers := plan.ExecutionReady, append([]runfork.RunForkUnsupportedBlocker(nil), plan.UnsupportedBlockers...)
	if err := admitSelectedDeploymentRevisionFrontier(plan, zero); err != nil {
		t.Fatalf("exact owed arrival was rejected before native admission: %v", err)
	}
	if plan.ExecutionReady != ready || !reflect.DeepEqual(plan.UnsupportedBlockers, blockers) {
		t.Fatal("owed-schedule availability changed native admission or granted execution")
	}
	for _, fault := range []string{"missing", "canceled", "failed", "fired", "corrupt", "foreign_source", "unbound_cut"} {
		candidate := plan
		candidate.JoinSchedules = append([]genericschedule.Activation(nil), plan.JoinSchedules...)
		switch fault {
		case "missing":
			candidate.JoinSchedules = nil
		case "canceled":
			candidate.JoinSchedules[0].Status = genericschedule.StatusCancelled
		case "failed":
			candidate.JoinSchedules[0].Status = genericschedule.StatusFailed
		case "fired":
			candidate.JoinSchedules[0].Status = genericschedule.StatusFired
		case "corrupt":
			candidate.JoinSchedules[0].ImmutableHash = ""
		case "foreign_source":
			candidate.SourceRunID = uuid.NewString()
		case "unbound_cut":
			candidate.ForkPoint.Revision = 0
		}
		if err := admitSelectedDeploymentRevisionFrontier(candidate, zero); err == nil {
			t.Fatalf("%s arrival evidence invented owed work", fault)
		}
	}
}
