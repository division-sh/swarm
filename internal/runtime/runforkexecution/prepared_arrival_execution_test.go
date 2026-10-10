package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func prepareArmedArrivalSourceAtCut(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, sourceRun, initialMarker string, armed genericschedule.Activation, originalJoin joinruntime.Activation) (string, genericschedule.Activation) {
	t.Helper()
	ctx = effects.WithExecutionMode(correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), sourceRun), executionmode.Mock)
	schedules := selected.(genericschedule.Store)
	wakeup, err := genericschedule.NewWakeup(armed.ID, armed.CurrentDueAt)
	if err != nil {
		t.Fatal(err)
	}
	// Preparation is durable, but neither source callback nor publication runs.
	prepared, err := schedules.PrepareGenericScheduleOccurrence(ctx, wakeup)
	if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
		t.Fatalf("native source occurrence preparation: %+v err=%v", prepared, err)
	}
	if err := prepared.Result.Validate(); err != nil {
		t.Fatal(err)
	}
	candidate := prepared.Result.Occurrence
	source, found, err := schedules.LoadGenericScheduleActivation(ctx, armed.ID)
	wantPrepared := armed
	wantPrepared.CurrentEventID, wantPrepared.CurrentEventAdmittedAt = candidate.EventID, candidate.AdmittedAt
	if err != nil || !found || !reflect.DeepEqual(source, wantPrepared) || !reflect.DeepEqual(source, prepared.Result.Activation) ||
		source.Status != genericschedule.StatusActive || source.CurrentEventID != genericschedule.OccurrenceEventID(source.ID, source.CurrentDueAt) ||
		source.CurrentEventAdmittedAt.IsZero() || !source.CurrentDueAt.Equal(originalJoin.DeadlineAt) || !source.CurrentDueAt.Before(candidate.AdmittedAt) {
		t.Fatalf("source preparation changed original arm or lost its pending candidate: %+v found=%v err=%v", source, found, err)
	}
	if err := genericschedule.ValidateWorkflowJoinScheduleRelation(originalJoin, source); err != nil {
		t.Fatal(err)
	}
	if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, sourceRun, "platform.join_timeout"); err != nil || count != 0 {
		t.Fatalf("source preparation published a timeout: count=%d err=%v", count, err)
	}
	settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(ctx, sourceRun)
	if err != nil || settlement.Total != 0 {
		t.Fatalf("source preparation created a delivery obligation: %+v err=%v", settlement, err)
	}
	initial := storetest.LoadCanonicalEventRecord(t, ctx, selected, initialMarker)
	initialAdmission, admitted := initial.PayloadAdmission()
	flowID := originalJoin.JoinRef().FlowPath()
	if !admitted || initial.RunID() != sourceRun || initialAdmission.Binding().FlowID() != flowID ||
		initialAdmission.Binding().BundleHash() != loaded.SourceArtifactFact.BundleHash() {
		t.Fatal("prepared source marker lacks its exact original payload/artifact owner")
	}
	marker := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), initial.Type(), "source-runtime", "",
		initial.Payload(), 0, sourceRun, events.EventEnvelope{}, events.NoRoutingSource(),
		runlifecycle.CanonicalTimestamp(candidate.AdmittedAt), executionmode.Mock)
	admission, err := rootruntime.NewRuntimePayloadAdmitter(nil, loaded.Source, loaded.SourceArtifactFact)(ctx, marker, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Binding().FlowID() != flowID || admission.Binding().BundleHash() != loaded.SourceArtifactFact.BundleHash() ||
		admission.Binding().EventKey() != initialAdmission.Binding().EventKey() {
		t.Fatal("prepared source cut borrowed another payload/artifact owner")
	}
	marker, err = events.ApplyPayloadAdmission(marker, admission)
	if err != nil {
		t.Fatal(err)
	}
	// Commit order, not a guessed future timestamp, puts preparation in this cut.
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, marker, nil, pipelineobligation.ScopeSubscribed)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, sourceRun)
	return marker.ID(), source
}

func TestIssue642PreparedArrivalTimeoutContinuesExactDeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv(publishedJoinFlowEnv, "")
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
			initialMarker, armed, originalJoin := seedArmedArrivalSource(t, ctx, selected, owner, loaded, sourceRun)
			sourceCtx := effects.WithExecutionMode(correlation.WithRunID(ctx, sourceRun), executionmode.Mock)
			schedules := selected.(genericschedule.Store)
			marker, source := prepareArmedArrivalSourceAtCut(t, sourceCtx, selected, owner, loaded, sourceRun, initialMarker, armed, originalJoin)
			candidate := genericschedule.Occurrence{ActivationID: source.ID, DueAt: source.CurrentDueAt, EventID: source.CurrentEventID, AdmittedAt: source.CurrentEventAdmittedAt}
			plan, err := owner.ports.fork.PlanRunFork(sourceCtx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || plan.ForkPoint.EventID != marker || marker == initialMarker || len(plan.JoinSchedules) != 1 ||
				plan.JoinSchedules[0].ID != source.ID || len(plan.TransferredJoins) != 0 || len(plan.PendingWork) != 0 {
				t.Fatalf("new fixed cut lost its sole prepared/unpublished deadline: %+v err=%v", plan, err)
			}
			sourceDigest, err := source.EvidenceDigest()
			if err != nil {
				t.Fatal(err)
			}
			cutDigest, err := plan.JoinSchedules[0].EvidenceDigest()
			if err != nil || cutDigest != sourceDigest {
				t.Fatalf("prepared occurrence absent or changed at the new event cut: digest=%s err=%v", cutDigest, err)
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(sourceCtx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "prepared-arrival", IdempotencyKey: "fixed-cut",
				TransportHash: "prepared-arrival-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(sourceCtx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("selected prepared arrival activation must succeed: %+v err=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			acknowledged, found, err := operations.LoadForkOperation(sourceCtx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil || acknowledged.Request.ResolvedPoint == nil {
				t.Fatalf("prepared arrival lost permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
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
					t.Fatalf("selected prepared arrival failed: %+v", *header.Failure)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("prepared arrival did not complete and release resources: header=%+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			workflow := flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(".", child, child)}
			header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, workflow)
			if err != nil || !found || header.CurrentState != "attention" || header.EntityID != child || header.WorkflowName != "." {
				t.Fatalf("exact prepared timeout receiver did not reach attention: %+v found=%v err=%v", header, found, err)
			}
			buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			joins, err := joinruntime.List(buckets)
			if err != nil || len(joins) != 1 {
				t.Fatalf("child lost its prepared source arrival arm: %+v err=%v", joins, err)
			}
			join := joins[0]
			if join.Status != joinruntime.StatusClosed || !join.OutcomeFired || join.OutcomePending || join.Expected() != 1 || join.Completed() != 0 ||
				!reflect.DeepEqual(join.Members, originalJoin.Members) || !reflect.DeepEqual(join.Missing(), originalJoin.Missing()) ||
				!join.ArmedAt.Equal(originalJoin.ArmedAt) || !join.DeadlineAt.Equal(originalJoin.DeadlineAt) || join.TransferredPublication != nil {
				t.Fatalf("prepared continuation changed original due/members or invented publication transfer: %+v", join)
			}
			ref, originalRef := join.JoinRef(), originalJoin.JoinRef()
			entry, originalEntry := ref.StageEntry(), originalRef.StageEntry()
			if !ref.Declaration().Equal(originalRef.Declaration()) || ref.FlowPath() != "." || entry.RunID != child || entry.EntityID != child ||
				entry.InstanceID != child || entry.InstancePath != child || entry.FlowScope != originalEntry.FlowScope || entry.OriginRunID != sourceRun ||
				entry.Stage != originalEntry.Stage || entry.Cause != originalEntry.Cause || entry.EventID != originalEntry.EventID ||
				entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID {
				t.Fatalf("prepared continuation retargeted its original arm: %+v", ref)
			}
			childPlan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child})
			if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.WorkflowTimers) != 0 || len(childPlan.TransferredJoins) != 0 {
				t.Fatalf("child lost or duplicated its physical prepared-source deadline: %+v err=%v", childPlan, err)
			}
			actual := childPlan.JoinSchedules[0]
			origin := actual.ForkJoinOrigin
			if actual.Status != genericschedule.StatusFired || actual.ID == source.ID || actual.Command.RunID != child || actual.Command.EntityID != child || actual.Command.FlowInstance != source.Command.FlowInstance ||
				actual.Command.TaskID != join.TimerTaskID() || actual.Command.ExecutionMode != source.Command.ExecutionMode || actual.CurrentEventID == candidate.EventID ||
				!actual.InitialDueAt.Equal(source.InitialDueAt) || !actual.CurrentDueAt.Equal(source.CurrentDueAt) ||
				origin == nil || origin.SourceActivationID != source.ID || origin.SourceRunID != sourceRun || !origin.SourceAdmittedAt.Equal(source.AdmittedAt) ||
				string(origin.PointKind) != string(plan.ForkPoint.Kind) || origin.PointRevision != plan.ForkPoint.Revision || origin.PointEventID != marker ||
				actual.AcceptedAt.Before(actual.AdmittedAt) || !actual.CurrentDueAt.Before(actual.AdmittedAt) {
				t.Fatalf("child timeout lost its exact source/cut/due or reused the prepared source candidate: %+v", actual)
			}
			object, ok := actual.Command.Payload.Interface().(map[string]any)
			if !ok {
				t.Fatal("child deadline lost its typed original join handle")
			}
			handle, actualRef, valid := timeridentity.ParseJoinHandle(object)
			if !valid || handle.Kind() != timeridentity.TimerHandleJoinTimeout || !actualRef.Equal(ref) {
				t.Fatalf("child native deadline does not own its exact settled arm: %+v", actualRef)
			}
			event := storetest.LoadCanonicalEventRecord(t, wait, selected, actual.CurrentEventID)
			if event.ID() != genericschedule.OccurrenceEventID(actual.ID, source.CurrentDueAt) || event.ID() == candidate.EventID ||
				event.Type() != "platform.join_timeout" || event.RunID() != child || event.ExecutionMode() != source.Command.ExecutionMode ||
				event.SourceAgent() != genericschedule.OccurrenceProducerID() || !event.CreatedAt().Equal(originalJoin.DeadlineAt) || event.TaskID() != join.TimerTaskID() {
				t.Fatalf("child did not publish its exact native timeout independently of the source candidate: %+v", event)
			}
			if _, err := actual.ValidatePublishedOccurrence(event); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_timeout"); err != nil || count != 1 {
				t.Fatalf("child timeout publication count=%d err=%v", count, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 0 {
				t.Fatalf("prepared incomplete membership acquired completion: count=%d err=%v", count, err)
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
				t.Fatalf("exact prepared-source timeout delivery did not settle: %+v err=%v", delivery, err)
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
			if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
				t.Fatalf("child did not settle exactly one prepared-source timeout obligation: %+v err=%v", settlement, err)
			}
			original, found, err := schedules.LoadGenericScheduleActivation(sourceCtx, source.ID)
			if err != nil || !found {
				t.Fatalf("prepared source deadline disappeared: found=%v err=%v", found, err)
			}
			afterDigest, err := original.EvidenceDigest()
			if err != nil || afterDigest != sourceDigest || original.Status != genericschedule.StatusActive || original.CurrentEventID != candidate.EventID ||
				!original.CurrentEventAdmittedAt.Equal(candidate.AdmittedAt) {
				t.Fatalf("child changed or committed its prepared source candidate: %+v err=%v", original, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, sourceRun, "platform.join_timeout"); err != nil || count != 0 {
				t.Fatalf("prepared source timeout was published: count=%d err=%v", count, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("child timeout changed prepared source business facts: %v", err)
			}
			sourceSettlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, sourceRun)
			if err != nil || sourceSettlement.Total != 0 {
				t.Fatalf("child created a prepared-source delivery obligation: %+v err=%v", sourceSettlement, err)
			}
			retry, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
				t.Fatalf("prepared timeout settlement changed permanent fork acknowledgment: %+v err=%v", retry, err)
			}
		})
	}
}
