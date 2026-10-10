package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestIssue642ArmedTemplateArrivalTimeoutContinuesExactDeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv(publishedJoinFlowEnv, "orders")
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
			originalRef := originalJoin.JoinRef()
			originalEntry := originalRef.StageEntry()
			if originalRef.FlowPath() != "orders" || originalEntry.FlowScope != "orders" || originalEntry.RunID != sourceRun ||
				originalEntry.EntityID != source.Command.EntityID || originalEntry.EntityID == sourceRun ||
				originalEntry.InstancePath != source.Command.FlowInstance || !strings.HasPrefix(originalEntry.InstancePath, "orders/") {
				t.Fatalf("armed template source is not its actual constructed child: %+v", originalRef)
			}
			sourceWorkflow := flowidentity.RunScopedFlowInstance{RunID: sourceRun,
				Route: flowidentity.StoredRoute(originalEntry.FlowScope, originalEntry.InstanceID, originalEntry.InstancePath)}
			sourceHeader, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, sourceWorkflow)
			if err != nil || !found || sourceHeader.WorkflowName != "orders" || sourceHeader.EntityID != originalEntry.EntityID ||
				sourceHeader.InstanceID != originalEntry.InstanceID || sourceHeader.StorageRef != originalEntry.InstancePath || sourceHeader.CurrentState != "awaiting" ||
				sourceHeader.ParentFlowID != "." || sourceHeader.ParentFlowInstance != sourceRun || sourceHeader.ParentEntityID != sourceRun {
				t.Fatalf("armed orders receiver lost its exact existing parent: %+v found=%v err=%v", sourceHeader, found, err)
			}
			receipt, err := owner.ports.workflow.LoadFlowConstructionPublication(ctx, sourceWorkflow, originalEntry.EntityID)
			if err != nil || receipt.Identity.TemplateID != sourceHeader.WorkflowName || receipt.Identity.ScopeKey != originalEntry.FlowScope ||
				receipt.Identity.InstanceID != sourceHeader.InstanceID || receipt.Identity.InstancePath != sourceHeader.StorageRef ||
				receipt.Identity.EntityID != sourceHeader.EntityID || receipt.Identity.ParentEntityID != sourceRun ||
				receipt.Identity.ParentRoute != (flowidentity.ParentRoute{FlowID: ".", FlowInstance: sourceRun, EntityID: sourceRun}) ||
				receipt.CreatingInput.EventID == "" || receipt.CreatingInput.EventID == marker {
				t.Fatalf("armed orders source lost its actual constructor receipt: %+v err=%v", receipt, err)
			}
			declaration := semanticview.ResolveFlowEventProof(loaded.Source, "orders", receipt.CreatingInput.Input)
			if !declaration.HasSchema || !declaration.IsAuthored(loaded.Source) || declaration.EventKey() == "item.completed" {
				t.Fatalf("orders constructor borrowed a root event declaration: %+v", declaration)
			}
			creating := storetest.LoadCanonicalEventRecord(t, ctx, selected, receipt.CreatingInput.EventID)
			cut := storetest.LoadCanonicalEventRecord(t, ctx, selected, marker)
			for _, input := range []events.Event{creating, cut} {
				admission, admitted := input.PayloadAdmission()
				if !admitted || admission.Binding().FlowID() != "orders" || admission.Binding().BundleHash() != loaded.SourceArtifactFact.BundleHash() ||
					admission.Binding().EventKey() != declaration.EventKey() || string(input.Type()) != declaration.EventKey() || input.RunID() != sourceRun {
					t.Fatalf("orders history lost its canonical scoped payload/artifact owner: event=%s admission=%+v", input.ID(), admission)
				}
			}
			if !creating.CreatedAt().Equal(originalJoin.ArmedAt) || !cut.CreatedAt().After(creating.CreatedAt()) || !reflect.DeepEqual(creating.Payload(), cut.Payload()) {
				t.Fatal("fixed cut replaced or duplicated the genuine orders creating input")
			}
			plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].ID != source.ID ||
				plan.JoinSchedules[0].Status != genericschedule.StatusActive || plan.JoinSchedules[0].CurrentEventID != "" || len(plan.TransferredJoins) != 0 || len(plan.PendingWork) != 0 {
				t.Fatalf("orders fixed cut lost its sole unpublished deadline: %+v err=%v", plan, err)
			}
			requireArmedArrivalAvailability(t, plan)
			sourceDigest, err := source.EvidenceDigest()
			if err != nil {
				t.Fatal(err)
			}
			cutDigest, err := plan.JoinSchedules[0].EvidenceDigest()
			if err != nil || cutDigest != sourceDigest {
				t.Fatalf("orders fixed cut changed the original deadline: digest=%s err=%v", cutDigest, err)
			}
			// The immutability baseline includes the genuine creating input and the
			// later, distinct cut marker; neither is an owed member delivery.
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "armed-template-arrival", IdempotencyKey: "fixed-cut",
				TransportHash: "armed-template-arrival-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("selected armed template arrival activation must succeed: %+v err=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			projected, err := runfork.ProjectConstructionIdentity(sourceRun, child, receipt.Identity)
			if err != nil {
				t.Fatal(err)
			}
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			acknowledged, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil || acknowledged.Request.ResolvedPoint == nil {
				t.Fatalf("orders arrival lost its permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
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
					t.Fatalf("selected armed template arrival failed: %+v", *header.Failure)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("orders arrival did not complete and release resources: header=%+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			workflow := flowidentity.RunScopedFlowInstance{RunID: child, Route: projected.Route()}
			header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, workflow)
			if err != nil || !found || header.CurrentState != "attention" || header.EntityID != projected.EntityID || header.WorkflowName != projected.TemplateID ||
				header.InstanceID != projected.InstanceID || header.StorageRef != projected.InstancePath ||
				header.ParentFlowID != projected.ParentRoute.FlowID || header.ParentFlowInstance != projected.ParentRoute.FlowInstance || header.ParentEntityID != projected.ParentEntityID {
				t.Fatalf("exact orders timeout receiver lost its template/path/projected parent: want=%+v actual=%+v found=%v err=%v", projected, header, found, err)
			}
			if header.InstanceKind != sourceHeader.InstanceKind || header.TemplateVersion != sourceHeader.TemplateVersion || header.WorkflowVersion != sourceHeader.WorkflowVersion {
				t.Fatalf("orders descriptor changed: source kind=%q template version=%q workflow version=%q; child kind=%q template version=%q workflow version=%q",
					sourceHeader.InstanceKind, sourceHeader.TemplateVersion, sourceHeader.WorkflowVersion, header.InstanceKind, header.TemplateVersion, header.WorkflowVersion)
			}
			buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			joins, err := joinruntime.List(buckets)
			if err != nil || len(joins) != 1 {
				t.Fatalf("orders child lost its retained arrival arm: %+v err=%v", joins, err)
			}
			join := joins[0]
			if join.Status != joinruntime.StatusClosed || !join.OutcomeFired || join.OutcomePending || join.Expected() != 1 || join.Completed() != 0 ||
				!reflect.DeepEqual(join.Members, originalJoin.Members) || !reflect.DeepEqual(join.Missing(), originalJoin.Missing()) ||
				!join.ArmedAt.Equal(originalJoin.ArmedAt) || !join.DeadlineAt.Equal(originalJoin.DeadlineAt) || join.TransferredPublication != nil {
				t.Fatalf("orders timeout changed original due/members or invented accepted-source transfer: %+v", join)
			}
			ref := join.JoinRef()
			entry := ref.StageEntry()
			if !ref.Declaration().Equal(originalRef.Declaration()) || ref.FlowPath() != projected.TemplateID || entry.RunID != child || entry.EntityID != projected.EntityID ||
				entry.InstanceID != projected.InstanceID || entry.InstancePath != projected.InstancePath || entry.FlowScope != projected.ScopeKey ||
				entry.OriginRunID != sourceRun || entry.Stage != originalEntry.Stage || entry.Cause != originalEntry.Cause || entry.EventID != originalEntry.EventID ||
				entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID {
				t.Fatalf("orders timeout retargeted its retained arm: %+v", ref)
			}
			childPlan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child})
			if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.WorkflowTimers) != 0 || len(childPlan.TransferredJoins) != 0 {
				t.Fatalf("orders child lost or duplicated its physical inherited deadline: %+v err=%v", childPlan, err)
			}
			actual := childPlan.JoinSchedules[0]
			origin := actual.ForkJoinOrigin
			if actual.Status != genericschedule.StatusFired || actual.Command.RunID != child || actual.Command.EntityID != projected.EntityID ||
				actual.Command.FlowInstance != projected.InstancePath || actual.Command.TaskID != join.TimerTaskID() || actual.Command.ExecutionMode != source.Command.ExecutionMode ||
				!actual.InitialDueAt.Equal(source.InitialDueAt) || !actual.CurrentDueAt.Equal(source.CurrentDueAt) ||
				origin == nil || origin.SourceActivationID != source.ID || origin.SourceRunID != sourceRun || !origin.SourceAdmittedAt.Equal(source.AdmittedAt) ||
				string(origin.PointKind) != string(plan.ForkPoint.Kind) || origin.PointRevision != plan.ForkPoint.Revision || origin.PointEventID != plan.ForkPoint.EventID ||
				actual.AcceptedAt.Before(actual.AdmittedAt) || !actual.CurrentDueAt.Before(actual.AdmittedAt) {
				t.Fatalf("actual orders timeout lost its original owner/cut/due or executed before admission: %+v", actual)
			}
			event := storetest.LoadCanonicalEventRecord(t, wait, selected, actual.CurrentEventID)
			if event.ID() != genericschedule.OccurrenceEventID(actual.ID, source.CurrentDueAt) || event.Type() != "platform.join_timeout" || event.RunID() != child ||
				event.SourceAgent() != genericschedule.OccurrenceProducerID() || !event.CreatedAt().Equal(originalJoin.DeadlineAt) || event.TaskID() != join.TimerTaskID() {
				t.Fatalf("orders child did not publish the exact native retained timeout: %+v", event)
			}
			if _, err := actual.ValidatePublishedOccurrence(event); err != nil {
				t.Fatal(err)
			}
			object, ok := actual.Command.Payload.Interface().(map[string]any)
			if !ok {
				t.Fatal("orders child deadline lost its typed join handle")
			}
			handle, actualRef, valid := timeridentity.ParseJoinHandle(object)
			if !valid || handle.Kind() != timeridentity.TimerHandleJoinTimeout || !actualRef.Equal(ref) {
				t.Fatalf("physical orders deadline does not own the exact settled arm: %+v", actualRef)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_timeout"); err != nil || count != 1 {
				t.Fatalf("orders child timeout publication count=%d err=%v", count, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 0 {
				t.Fatalf("orders incomplete membership acquired completion: count=%d err=%v", count, err)
			}
			target := events.RouteIdentity{FlowID: projected.ScopeKey, FlowInstance: projected.InstancePath, EntityID: projected.EntityID}
			if !events.SameRouteIdentity(event.RoutingSource().Route(), target) {
				t.Fatalf("orders timeout publication has another receiver source: %+v", event.RoutingSource())
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(target)}
			deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := owner.ports.busDurable.DeliveryLifecycle.Snapshot(wait, deliveryID)
			if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.EventID != event.ID() || delivery.RunID != child ||
				delivery.SubscriberClass != deliverylifecycle.SubscriberNode || delivery.SubscriberID != ref.Node().Key() ||
				delivery.Route.Recipient != route.Recipient || !delivery.Route.Target.ExistingEntity() || !events.SameRouteIdentity(delivery.Route.Target.Route(), target) {
				t.Fatalf("exact existing orders timeout delivery did not settle: %+v err=%v", delivery, err)
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
			if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
				t.Fatalf("orders child did not settle exactly one timeout obligation: %+v err=%v", settlement, err)
			}
			original, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, sourceRun), source.ID)
			if err != nil || !found {
				t.Fatalf("source orders deadline disappeared: found=%v err=%v", found, err)
			}
			afterDigest, err := original.EvidenceDigest()
			if err != nil || afterDigest != sourceDigest || original.Status != genericschedule.StatusActive || original.CurrentEventID != "" {
				t.Fatalf("orders child changed or published its source deadline: %+v err=%v", original, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, sourceRun, "platform.join_timeout"); err != nil || count != 0 {
				t.Fatalf("source orders deadline was published: count=%d err=%v", count, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("orders child timeout changed source business facts: %v", err)
			}
			retry, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
				t.Fatalf("orders timeout settlement changed permanent fork acknowledgment: %+v err=%v", retry, err)
			}
		})
	}
}
