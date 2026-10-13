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

func TestIssue642PreparedTemplateArrivalTimeoutContinuesExactDeliveryBothStores(t *testing.T) {
	testPreparedTemplateArrivalTimeoutBothStores(t, "")
}

func TestIssue642PreparedTemplateArrivalRetainsDueAcrossSelectedDelayChangeBothStores(t *testing.T) {
	for _, delay := range []string{"1ms", "24h"} {
		t.Run(delay, func(t *testing.T) {
			testPreparedTemplateArrivalTimeoutBothStores(t, delay)
		})
	}
}

func testPreparedTemplateArrivalTimeoutBothStores(t *testing.T, selectedDelay string) {
	t.Helper()
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
			initialMarker, armed, originalJoin := seedArmedArrivalSource(t, ctx, selected, owner, loaded, sourceRun)
			marker, source := prepareArmedArrivalSourceAtCut(t, ctx, selected, owner, loaded, sourceRun, initialMarker, armed, originalJoin)
			originalRef := originalJoin.JoinRef()
			originalEntry := originalRef.StageEntry()
			if originalRef.FlowPath() != "orders" || originalEntry.FlowScope != "orders" || originalEntry.RunID != sourceRun ||
				originalEntry.EntityID != source.Command.EntityID || originalEntry.EntityID == sourceRun ||
				originalEntry.InstancePath != source.Command.FlowInstance || !strings.HasPrefix(originalEntry.InstancePath, "orders/") {
				t.Fatalf("prepared template source is not its actual constructed child: %+v", originalRef)
			}
			sourceWorkflow := flowidentity.RunScopedFlowInstance{RunID: sourceRun,
				Route: flowidentity.StoredRoute(originalEntry.FlowScope, originalEntry.InstanceID, originalEntry.InstancePath)}
			sourceHeader, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, sourceWorkflow)
			if err != nil || !found || sourceHeader.WorkflowName != "orders" || sourceHeader.WorkflowVersion != loaded.Source.WorkflowVersion() || sourceHeader.EntityID != originalEntry.EntityID ||
				sourceHeader.InstanceID != originalEntry.InstanceID || sourceHeader.StorageRef != originalEntry.InstancePath || sourceHeader.CurrentState != "awaiting" ||
				sourceHeader.ParentFlowID != "." || sourceHeader.ParentFlowInstance != sourceRun || sourceHeader.ParentEntityID != sourceRun {
				t.Fatalf("prepared orders receiver lost its exact existing parent: %+v found=%v err=%v", sourceHeader, found, err)
			}
			receipt, err := owner.ports.workflow.LoadFlowConstructionPublication(ctx, sourceWorkflow, originalEntry.EntityID)
			if err != nil || receipt.Identity.TemplateID != sourceHeader.WorkflowName || receipt.Identity.ScopeKey != originalEntry.FlowScope ||
				receipt.Identity.InstanceID != sourceHeader.InstanceID || receipt.Identity.InstancePath != sourceHeader.StorageRef ||
				receipt.Identity.EntityID != sourceHeader.EntityID || receipt.Identity.ParentEntityID != sourceRun ||
				receipt.Identity.ParentRoute != (flowidentity.ParentRoute{FlowID: ".", FlowInstance: sourceRun, EntityID: sourceRun}) ||
				receipt.CreatingInput.EventID == "" || receipt.CreatingInput.EventID == initialMarker || receipt.CreatingInput.EventID == marker || marker == initialMarker {
				t.Fatalf("prepared orders source lost its actual constructor receipt: %+v err=%v", receipt, err)
			}
			declaration := semanticview.ResolveFlowEventProof(loaded.Source, "orders", receipt.CreatingInput.Input)
			if !declaration.HasSchema || !declaration.IsAuthored(loaded.Source) || declaration.EventKey() == "item.completed" {
				t.Fatalf("orders constructor borrowed a root event declaration: %+v", declaration)
			}
			creating := storetest.LoadCanonicalEventRecord(t, ctx, selected, receipt.CreatingInput.EventID)
			initialCut := storetest.LoadCanonicalEventRecord(t, ctx, selected, initialMarker)
			cut := storetest.LoadCanonicalEventRecord(t, ctx, selected, marker)
			for _, input := range []events.Event{creating, initialCut, cut} {
				admission, admitted := input.PayloadAdmission()
				if !admitted || admission.Binding().FlowID() != "orders" || admission.Binding().BundleHash() != loaded.SourceArtifactFact.BundleHash() ||
					admission.Binding().EventKey() != declaration.EventKey() || string(input.Type()) != declaration.EventKey() || input.RunID() != sourceRun {
					t.Fatalf("prepared orders history lost its canonical scoped payload/artifact owner: event=%s admission=%+v", input.ID(), admission)
				}
			}
			if !creating.CreatedAt().Equal(originalJoin.ArmedAt) || !initialCut.CreatedAt().After(creating.CreatedAt()) ||
				!cut.CreatedAt().Equal(source.CurrentEventAdmittedAt) || !cut.CreatedAt().After(initialCut.CreatedAt()) ||
				!reflect.DeepEqual(creating.Payload(), initialCut.Payload()) || !reflect.DeepEqual(creating.Payload(), cut.Payload()) {
				t.Fatal("prepared fixed cut replaced or duplicated the genuine orders creating input/history")
			}
			plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || plan.ForkPoint.EventID != marker || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].ID != source.ID ||
				plan.JoinSchedules[0].Status != genericschedule.StatusActive || source.CurrentEventID != genericschedule.OccurrenceEventID(source.ID, source.CurrentDueAt) ||
				plan.JoinSchedules[0].CurrentEventID != source.CurrentEventID || source.CurrentEventAdmittedAt.IsZero() ||
				!plan.JoinSchedules[0].CurrentEventAdmittedAt.Equal(source.CurrentEventAdmittedAt) || len(plan.TransferredJoins) != 0 || len(plan.PendingWork) != 0 {
				t.Fatalf("orders fixed cut lost its sole prepared/unpublished deadline: %+v err=%v", plan, err)
			}
			requireArmedArrivalAvailability(t, plan)
			sourceDigest, err := source.EvidenceDigest()
			if err != nil {
				t.Fatal(err)
			}
			cutDigest, err := plan.JoinSchedules[0].EvidenceDigest()
			if err != nil || cutDigest != sourceDigest {
				t.Fatalf("orders prepared cut changed the original deadline/candidate: digest=%s err=%v", cutDigest, err)
			}
			// The baseline includes construction, the initial cut, native preparation,
			// and its later committed scoped marker, without a source publication.
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			target := loaded
			if selectedDelay != "" {
				path := filepath.Join(loader.SourceRoot, "orders", "nodes.yaml")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				const original = "deadline: {after: 1h, from: stage_entry}"
				if strings.Count(string(raw), original) != 1 {
					t.Fatal("selected delay fixture lost its exact source declaration")
				}
				raw = []byte(strings.Replace(string(raw), original, "deadline: {after: "+selectedDelay+", from: stage_entry}", 1))
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
				target, err = loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				joins := target.Source.WorkflowJoins()
				if target.SourceArtifactFact.BundleHash() == loaded.SourceArtifactFact.BundleHash() || len(joins) != 1 ||
					joins[0].Spec.Deadline == nil || joins[0].Spec.Deadline.After != selectedDelay {
					t.Fatal("selected source did not compile its genuinely changed deadline")
				}
				bundle, found := semanticview.Bundle(target.Source)
				if !found || bundle.SourceArtifact == nil {
					t.Fatal("selected delay requires its actual loader-owned source artifact")
				}
				storetest.RequireBundleDataCatalog(t, correlation.WithSourceArtifactFact(ctx, target.SourceArtifactFact),
					selected.(storetest.DurableDataCatalogStore), bundle)
			}
			selection := runforkadmission.SelectedContractSelection(target.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "prepared-template-arrival", IdempotencyKey: "fixed-cut",
				TransportHash: "prepared-template-arrival-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: target.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("selected prepared template arrival activation must succeed: %+v err=%v", result, err)
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
			if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != child || acknowledged.Result == nil ||
				acknowledged.Request.ResolvedPoint == nil || acknowledged.Request.TargetBundleHash != target.SourceArtifactFact.BundleHash() {
				t.Fatalf("prepared orders arrival lost its permanent activation acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
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
				if header.BundleHash != target.SourceArtifactFact.BundleHash() {
					t.Fatal("prepared child did not retain the exact selected contract artifact")
				}
				if header.Failure != nil {
					t.Fatalf("selected prepared template arrival failed: %+v", *header.Failure)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("prepared orders arrival did not complete and release resources: header=%+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			workflow := flowidentity.RunScopedFlowInstance{RunID: child, Route: projected.Route()}
			header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, workflow)
			if err != nil || !found || header.CurrentState != "attention" || header.EntityID != projected.EntityID || header.WorkflowName != projected.TemplateID ||
				header.InstanceID != projected.InstanceID || header.StorageRef != projected.InstancePath ||
				header.ParentFlowID != projected.ParentRoute.FlowID || header.ParentFlowInstance != projected.ParentRoute.FlowInstance || header.ParentEntityID != projected.ParentEntityID {
				t.Fatalf("exact prepared orders timeout receiver lost its template/path/projected parent: want=%+v actual=%+v found=%v err=%v", projected, header, found, err)
			}
			if header.InstanceKind != sourceHeader.InstanceKind || header.TemplateVersion != sourceHeader.TemplateVersion || header.WorkflowVersion != target.Source.WorkflowVersion() {
				t.Fatalf("orders descriptor changed: source kind=%q template version=%q; selected workflow version=%q; child kind=%q template version=%q workflow version=%q",
					sourceHeader.InstanceKind, sourceHeader.TemplateVersion, target.Source.WorkflowVersion(), header.InstanceKind, header.TemplateVersion, header.WorkflowVersion)
			}
			buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			joins, err := joinruntime.List(buckets)
			if err != nil || len(joins) != 1 {
				t.Fatalf("orders child lost its prepared-source arrival arm: %+v err=%v", joins, err)
			}
			join := joins[0]
			if join.Status != joinruntime.StatusClosed || !join.OutcomeFired || join.OutcomePending || join.Expected() != 1 || join.Completed() != 0 ||
				!reflect.DeepEqual(join.Members, originalJoin.Members) || !reflect.DeepEqual(join.Missing(), originalJoin.Missing()) ||
				!join.ArmedAt.Equal(originalJoin.ArmedAt) || !join.DeadlineAt.Equal(originalJoin.DeadlineAt) || join.TransferredPublication != nil {
				t.Fatalf("prepared orders timeout changed original due/members or invented accepted-source transfer: %+v", join)
			}
			ref := join.JoinRef()
			entry := ref.StageEntry()
			if !ref.Declaration().Equal(originalRef.Declaration()) || ref.FlowPath() != projected.TemplateID || entry.RunID != child || entry.EntityID != projected.EntityID ||
				entry.InstanceID != projected.InstanceID || entry.InstancePath != projected.InstancePath || entry.FlowScope != projected.ScopeKey ||
				entry.OriginRunID != sourceRun || entry.Stage != originalEntry.Stage || entry.Cause != originalEntry.Cause || entry.EventID != originalEntry.EventID ||
				entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID {
				t.Fatalf("prepared orders timeout retargeted its retained arm: %+v", ref)
			}
			childPlan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child})
			if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.WorkflowTimers) != 0 || len(childPlan.TransferredJoins) != 0 {
				t.Fatalf("orders child lost or duplicated its prepared-source deadline: %+v err=%v", childPlan, err)
			}
			actual := childPlan.JoinSchedules[0]
			origin := actual.ForkJoinOrigin
			if actual.Status != genericschedule.StatusFired || actual.ID == source.ID || actual.CurrentEventID == source.CurrentEventID ||
				actual.Command.RunID != child || actual.Command.EntityID != projected.EntityID ||
				actual.Command.FlowInstance != projected.InstancePath || actual.Command.TaskID != join.TimerTaskID() || actual.Command.ExecutionMode != source.Command.ExecutionMode ||
				!actual.InitialDueAt.Equal(source.InitialDueAt) || !actual.CurrentDueAt.Equal(source.CurrentDueAt) ||
				origin == nil || origin.SourceActivationID != source.ID || origin.SourceRunID != sourceRun || !origin.SourceAdmittedAt.Equal(source.AdmittedAt) ||
				string(origin.PointKind) != string(plan.ForkPoint.Kind) || origin.PointRevision != plan.ForkPoint.Revision || origin.PointEventID != plan.ForkPoint.EventID ||
				actual.AcceptedAt.Before(actual.AdmittedAt) || !actual.CurrentDueAt.Before(actual.AdmittedAt) {
				t.Fatalf("actual orders timeout lost its original owner/cut/due or reused the prepared source candidate: %+v", actual)
			}
			event := storetest.LoadCanonicalEventRecord(t, wait, selected, actual.CurrentEventID)
			if event.ID() != genericschedule.OccurrenceEventID(actual.ID, source.CurrentDueAt) || event.ID() == source.CurrentEventID ||
				event.Type() != "platform.join_timeout" || event.RunID() != child || event.ExecutionMode() != source.Command.ExecutionMode ||
				event.SourceAgent() != genericschedule.OccurrenceProducerID() || !event.CreatedAt().Equal(originalJoin.DeadlineAt) || event.TaskID() != join.TimerTaskID() {
				t.Fatalf("orders child did not publish its exact native timeout independently of the prepared source candidate: %+v", event)
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
			receiver := events.RouteIdentity{FlowID: projected.ScopeKey, FlowInstance: projected.InstancePath, EntityID: projected.EntityID}
			if !events.SameRouteIdentity(event.RoutingSource().Route(), receiver) {
				t.Fatalf("orders timeout publication has another receiver source: %+v", event.RoutingSource())
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(receiver)}
			deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := owner.ports.busDurable.DeliveryLifecycle.Snapshot(wait, deliveryID)
			if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.EventID != event.ID() || delivery.RunID != child ||
				delivery.SubscriberClass != deliverylifecycle.SubscriberNode || delivery.SubscriberID != ref.Node().Key() ||
				delivery.Route.Recipient != route.Recipient || !delivery.Route.Target.ExistingEntity() || !events.SameRouteIdentity(delivery.Route.Target.Route(), receiver) {
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
			if err != nil || afterDigest != sourceDigest || original.Status != genericschedule.StatusActive || original.CurrentEventID != source.CurrentEventID ||
				!original.CurrentEventAdmittedAt.Equal(source.CurrentEventAdmittedAt) {
				t.Fatalf("orders child changed or published its prepared source deadline: %+v err=%v", original, err)
			}
			if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, sourceRun, "platform.join_timeout"); err != nil || count != 0 {
				t.Fatalf("prepared source orders deadline was published: count=%d err=%v", count, err)
			}
			sourceSettlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, sourceRun)
			if err != nil || sourceSettlement.Total != 0 {
				t.Fatalf("prepared source orders acquired a delivery obligation: %+v err=%v", sourceSettlement, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("orders child timeout changed prepared source business facts: %v", err)
			}
			retry, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
				t.Fatalf("orders prepared timeout settlement changed permanent fork acknowledgment: %+v err=%v", retry, err)
			}
		})
	}
}
