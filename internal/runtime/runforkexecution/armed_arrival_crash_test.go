package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type armedArrivalCrashScheduleStore struct {
	genericschedule.Store
	checkpoint func(string, string)
}

func newArmedArrivalCrashScheduleStore(t *testing.T, store genericschedule.Store, checkpoint func(string, string)) genericschedule.Store {
	t.Helper()
	if _, ok := store.(interface {
		ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]genericschedule.Activation, error)
	}); !ok {
		t.Fatal("armed arrival crash store lacks the canonical run-scoped census")
	}
	return armedArrivalCrashScheduleStore{Store: store, checkpoint: checkpoint}
}

func (s armedArrivalCrashScheduleStore) ListActiveGenericScheduleActivationsForRun(ctx context.Context, runID string) ([]genericschedule.Activation, error) {
	return s.Store.(interface {
		ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]genericschedule.Activation, error)
	}).ListActiveGenericScheduleActivationsForRun(ctx, runID)
}

func (s armedArrivalCrashScheduleStore) CommitGenericScheduleOccurrence(ctx context.Context, command genericschedule.CommitCommand) (genericschedule.CommitResult, error) {
	result, err := s.Store.CommitGenericScheduleOccurrence(ctx, command)
	if result.Outcome != genericschedule.CommitCommitted {
		return result, err
	}
	if validationErr := result.Validate(); validationErr != nil {
		return result, errors.Join(err, validationErr)
	}
	if result.PublicationAlreadyCommitted || result.Next.Status != genericschedule.StatusFired ||
		result.Next.ID != command.Activation.ID || result.Next.CurrentEventID != command.Occurrence.EventID ||
		result.Next.Command.RunID != command.Activation.Command.RunID || result.Next.ForkJoinOrigin == nil {
		return result, errors.Join(err, errors.New("armed arrival crash requires its first acknowledged inherited occurrence"))
	}
	// The native transaction acknowledged; lifecycle finalization/dispatch has
	// not run. The parent kills this process while the checkpoint blocks.
	s.checkpoint(result.Next.Command.RunID, result.Next.ID)
	return result, err
}

type armedArrivalCrashActivationStore struct {
	SelectedContractForkLifecycle
	schedules  genericschedule.Store
	checkpoint func(string, string)
}

func (s armedArrivalCrashActivationStore) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	result, err := s.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
	if !result.Activated {
		return result, err
	}
	rows, readErr := s.schedules.(interface {
		ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]genericschedule.Activation, error)
	}).ListActiveGenericScheduleActivationsForRun(correlation.WithRunID(ctx, request.ForkRunID), request.ForkRunID)
	if readErr != nil || len(rows) != 1 {
		return result, errors.Join(err, readErr, errors.New("armed arrival activation requires its exact authorized native schedule"))
	}
	s.checkpoint(request.ForkRunID, rows[0].ID)
	return result, err
}

func seedArmedArrivalCrashSource(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, sourceRun string) string {
	t.Helper()
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
	marker, _, _ := seedArmedArrivalSource(t, ctx, selected, owner, loaded, sourceRun)
	return marker
}

func TestIssue642ArmedArrivalTimeoutCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_armed_join_after_activation", "retained_armed_join_occurrence_committed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				var selected startupownership.Store
				var construct func() SelectedContractExecutionOwner
				var dsn string
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, dsn = s, s.Path()
					construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					dsn = testutil.StartPostgresDSN(t)
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
					selected = s
					construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				operations := selected.(interface {
					LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
				})
				acknowledged, found, err := operations.LoadForkOperation(t.Context(), "retained-crash", "fixed-cut", "retained-crash-transport")
				if err != nil || !found || acknowledged.Status != runfork.ForkOperationActivated || acknowledged.ForkRunID != checkpoint.ForkRun || acknowledged.Result == nil || acknowledged.Request.ResolvedPoint == nil {
					t.Fatalf("armed arrival crash lost its permanent acknowledgment: %+v found=%v err=%v", acknowledged, found, err)
				}
				lifecycle := selected.(SelectedContractForkLifecycle)
				plan, err := lifecycle.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{
					SourceRunID: checkpoint.SourceRun, ResolvedPoint: acknowledged.Request.ResolvedPoint,
				})
				if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].Status != genericschedule.StatusActive || plan.JoinSchedules[0].CurrentEventID != "" || len(plan.TransferredJoins) != 0 || len(plan.PendingWork) != 0 {
					t.Fatalf("crash lost its exact unpublished source deadline: %+v err=%v", plan, err)
				}
				source := plan.JoinSchedules[0]
				var originalJoin joinruntime.Activation
				for _, entity := range plan.Entities {
					if entity.EntityID != checkpoint.SourceRun {
						continue
					}
					if entity.MaterializationMetadata == nil || entity.MaterializationMetadata.FlowInstance != checkpoint.SourceRun {
						t.Fatal("original source root lacks its exact construction metadata")
					}
					buckets, err := joinruntime.PersistedBuckets(entity.Accumulator)
					if err != nil {
						t.Fatal(err)
					}
					joins, err := joinruntime.List(buckets)
					if err != nil || len(joins) != 1 {
						t.Fatalf("original source join census: %+v err=%v", joins, err)
					}
					originalJoin = joins[0]
				}
				if originalJoin.Status != joinruntime.StatusOpen || originalJoin.Expected() != 1 || originalJoin.Completed() != 0 ||
					!reflect.DeepEqual(originalJoin.Members, []string{"member-a"}) || !originalJoin.DeadlineAt.Equal(source.CurrentDueAt) {
					t.Fatalf("crash source lost its open one-member deadline: %+v", originalJoin)
				}
				if err := genericschedule.ValidateWorkflowJoinScheduleRelation(originalJoin, source); err != nil {
					t.Fatal(err)
				}
				sourceDigest, err := source.EvidenceDigest()
				if err != nil {
					t.Fatal(err)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(t.Context(), selected, checkpoint.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				child := checkpoint.ForkRun
				published := cut == "retained_armed_join_occurrence_committed"
				// The exact-row reader grants no execution authority and does not
				// invent an event cut before the child's first publication.
				atCrash, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(t.Context(), checkpoint.ScheduleID)
				if err != nil || !found || checkpoint.ScheduleID == "" || atCrash.Command.RunID != child || atCrash.ForkJoinOrigin == nil {
					t.Fatalf("crash lost its exact native inherited deadline: %+v found=%v err=%v", atCrash, found, err)
				}
				eventID := genericschedule.OccurrenceEventID(atCrash.ID, source.CurrentDueAt)
				wantPublications := 0
				if published {
					wantPublications = 1
					if atCrash.Status != genericschedule.StatusFired || atCrash.CurrentEventID != eventID {
						t.Fatalf("publication checkpoint preceded accepted native occurrence: %+v", atCrash)
					}
					event := storetest.LoadCanonicalEventRecord(t, t.Context(), selected, eventID)
					if _, err := atCrash.ValidatePublishedOccurrence(event); err != nil {
						t.Fatal(err)
					}
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(originalJoin.JoinRef().Node()),
						Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child, EntityID: child})}
					id, err := deliverylifecycle.DeliveryID(eventID, route)
					if err != nil {
						t.Fatal(err)
					}
					delivery, err := selected.(deliverylifecycle.Store).Snapshot(t.Context(), id)
					if err != nil || delivery.Status != deliverylifecycle.StatusPending {
						t.Fatalf("publication checkpoint dispatched its native delivery: %+v err=%v", delivery, err)
					}
				} else if atCrash.Status != genericschedule.StatusActive || atCrash.CurrentEventID != "" {
					t.Fatalf("activation checkpoint published its child deadline: %+v", atCrash)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(t.Context(), selected, child, "platform.join_timeout"); err != nil || count != wantPublications {
					t.Fatalf("wrong native publication crash boundary: count=%d want=%d err=%v", count, wantPublications, err)
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				process, _ := worklifetime.ProcessFromContext(ctx)
				owner := construct()
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				t.Cleanup(func() {
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Error(err)
					}
				})
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
				})
				if err != nil || len(recovered) != 1 || recovered[0].RunID != child || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("armed arrival successor recovery: %+v err=%v", recovered, err)
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
						t.Fatalf("recovered armed arrival failed: %+v", *header.Failure)
					}
					owner.ports.contexts.mu.Lock()
					retained := len(owner.ports.contexts.entries)
					owner.ports.contexts.mu.Unlock()
					if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("armed arrival recovery did not settle/release: header=%+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
					case <-time.After(10 * time.Millisecond):
					}
				}
				header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait,
					flowidentity.RunScopedFlowInstance{RunID: child, Route: flowidentity.StoredRoute(".", child, child)})
				if err != nil || !found || header.CurrentState != "attention" || header.EntityID != child {
					t.Fatalf("recovered exact timeout receiver: %+v found=%v err=%v", header, found, err)
				}
				buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				joins, err := joinruntime.List(buckets)
				if err != nil || len(joins) != 1 {
					t.Fatalf("recovered join census: %+v err=%v", joins, err)
				}
				join := joins[0]
				if join.Status != joinruntime.StatusClosed || !join.OutcomeFired || join.OutcomePending || join.Expected() != 1 || join.Completed() != 0 ||
					!reflect.DeepEqual(join.Members, originalJoin.Members) || !reflect.DeepEqual(join.Missing(), originalJoin.Missing()) ||
					!join.ArmedAt.Equal(originalJoin.ArmedAt) || !join.DeadlineAt.Equal(originalJoin.DeadlineAt) || join.TransferredPublication != nil {
					t.Fatalf("recovery replaced retained due/membership or invented source publication: %+v", join)
				}
				ref, originalRef := join.JoinRef(), originalJoin.JoinRef()
				entry, originalEntry := ref.StageEntry(), originalRef.StageEntry()
				if !ref.Declaration().Equal(originalRef.Declaration()) || ref.FlowPath() != "." || entry.RunID != child || entry.EntityID != child ||
					entry.InstanceID != child || entry.InstancePath != child || entry.OriginRunID != checkpoint.SourceRun || entry.Stage != originalEntry.Stage ||
					entry.Cause != originalEntry.Cause || entry.EventID != originalEntry.EventID || entry.OccurrenceID != originalEntry.OccurrenceID || entry.TransitionID != originalEntry.TransitionID {
					t.Fatalf("recovery retargeted its historical deadline: %+v", ref)
				}
				childPlan, err := lifecycle.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child})
				if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.WorkflowTimers) != 0 || len(childPlan.TransferredJoins) != 0 {
					t.Fatalf("recovery duplicated or lost its inherited timer: %+v err=%v", childPlan, err)
				}
				actual := childPlan.JoinSchedules[0]
				origin := actual.ForkJoinOrigin
				if actual.ID != atCrash.ID || actual.ImmutableHash != atCrash.ImmutableHash || !actual.AdmittedAt.Equal(atCrash.AdmittedAt) ||
					actual.Status != genericschedule.StatusFired || actual.CurrentEventID != eventID || actual.Command.RunID != child || actual.Command.EntityID != child ||
					actual.Command.TaskID != join.TimerTaskID() || actual.Command.ExecutionMode != source.Command.ExecutionMode ||
					!actual.InitialDueAt.Equal(source.InitialDueAt) || !actual.CurrentDueAt.Equal(source.CurrentDueAt) ||
					origin == nil || origin.SourceActivationID != source.ID || origin.SourceRunID != checkpoint.SourceRun || !origin.SourceAdmittedAt.Equal(source.AdmittedAt) ||
					string(origin.PointKind) != string(plan.ForkPoint.Kind) || origin.PointRevision != plan.ForkPoint.Revision || origin.PointEventID != plan.ForkPoint.EventID ||
					actual.AcceptedAt.Before(actual.AdmittedAt) || !actual.CurrentDueAt.Before(actual.AdmittedAt) {
					t.Fatalf("recovery changed timer identity/due/provenance or preactivation fence: %+v", actual)
				}
				event := storetest.LoadCanonicalEventRecord(t, wait, selected, eventID)
				if event.Type() != "platform.join_timeout" || event.SourceAgent() != genericschedule.OccurrenceProducerID() ||
					!event.CreatedAt().Equal(originalJoin.DeadlineAt) || event.TaskID() != join.TimerTaskID() {
					t.Fatalf("recovery replaced its exact native timeout: %+v", event)
				}
				if _, err := actual.ValidatePublishedOccurrence(event); err != nil {
					t.Fatal(err)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_timeout"); err != nil || count != 1 {
					t.Fatalf("recovery repeated timeout publication: count=%d err=%v", count, err)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, "platform.join_complete"); err != nil || count != 0 {
					t.Fatalf("recovery invented membership completion: count=%d err=%v", count, err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()),
					Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child, EntityID: child})}
				id, err := deliverylifecycle.DeliveryID(eventID, route)
				if err != nil {
					t.Fatal(err)
				}
				delivery, err := owner.ports.busDurable.DeliveryLifecycle.Snapshot(wait, id)
				if err != nil || delivery.Status != deliverylifecycle.StatusDelivered || delivery.EventID != eventID || delivery.RunID != child ||
					delivery.SubscriberClass != deliverylifecycle.SubscriberNode || delivery.SubscriberID != ref.Node().Key() ||
					delivery.Route.Recipient != route.Recipient || !delivery.Route.Target.ExistingEntity() || !events.SameRouteIdentity(delivery.Route.Target.Route(), route.Target.Route()) {
					t.Fatalf("exact recovered timeout delivery did not settle: %+v err=%v", delivery, err)
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
				if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
					t.Fatalf("recovery did not settle exactly one timeout obligation: %+v err=%v", settlement, err)
				}
				original, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, checkpoint.SourceRun), source.ID)
				if err != nil || !found {
					t.Fatalf("source deadline disappeared: found=%v err=%v", found, err)
				}
				afterDigest, err := original.EvidenceDigest()
				if err != nil || afterDigest != sourceDigest || original.Status != genericschedule.StatusActive || original.CurrentEventID != "" {
					t.Fatalf("recovery changed or published its source deadline: %+v err=%v", original, err)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.SourceRun, "platform.join_timeout"); err != nil || count != 0 {
					t.Fatalf("source timeout was published: count=%d err=%v", count, err)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("timeout recovery changed source business facts: %v", err)
				}
				retry, found, err := operations.LoadForkOperation(wait, acknowledged.Request.Actor, acknowledged.Request.IdempotencyKey, acknowledged.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(acknowledged, retry) {
					t.Fatalf("timeout recovery changed permanent acknowledgment: %+v err=%v", retry, err)
				}
			})
		}
	}
}
