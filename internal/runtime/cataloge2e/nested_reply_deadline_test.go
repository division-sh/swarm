package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type nestedReplyDeadlineStore interface {
	storetest.RunFixtureStore
	bus.PreparedPublishEventReader
	replycontext.Store
	deliverylifecycle.Store
	genericschedule.Store
	ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]genericschedule.Activation, error)
}

func TestIssue642DepthThreeOriginalDeadlineAndLateReplyBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyNestedNodeReplyDeadlines(t), backend, false)
			if err := h.rt.Scheduler.PrepareStartup(); err != nil {
				t.Fatal(err)
			}
			var selected nestedReplyDeadlineStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			runID := uuid.NewString()
			at := runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-2 * time.Hour))
			ctx := correlation.WithRunID(worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence()), runID)
			ctx = effects.WithExecutionMode(ctx, executionmode.Live)
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
				BundleHash: h.rt.Options.SourceArtifactFact.BundleHash(), Artifact: h.bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
			source := semanticview.Wrap(h.bundle)
			created, err := h.rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source,
				Instance: flowidentity.Stored(source, ".", runID, runID, "", ""), OccurredAt: at})
			if err != nil || !created {
				t.Fatalf("construct and admit exact requester tree at its original entry: created=%t error=%v", created, err)
			}
			node, err := identity.AdmitExecutableNodeDeclaration("u", "u-node")
			if err != nil {
				t.Fatal(err)
			}
			held := &nestedReplyWorkerProbe{nodeID: node.Key(), started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
			h.rt.Pipeline.SetTestLifecycleProbe(held)
			t.Cleanup(held.resume)
			input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "request.started", "cataloge2e", "",
				[]byte(`{"account_id":"same-account","request_id":"same-business-key"}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at, executionmode.Live)
			publishCtx, cancelPublish := context.WithTimeout(ctx, 20*time.Second)
			defer cancelPublish()
			published := make(chan error, 1)
			go func() { published <- h.rt.Bus.PublishAndWait(publishCtx, input) }()
			select {
			case <-held.started:
			case err := <-published:
				t.Fatalf("source did not reach its actual open inner request: %v", err)
			case <-publishCtx.Done():
				t.Fatalf("wait for actual inner delivery within the original bound: %v", publishCtx.Err())
			}
			rows, err := storetest.ReadReplyContextRecords(ctx, selected, runID)
			if err != nil || len(rows) != 3 {
				t.Fatalf("open deadline chain: %+v error=%v", rows, err)
			}
			schedules, err := selected.ListActiveGenericScheduleActivationsForRun(ctx, runID)
			if err != nil || len(schedules) != 3 {
				t.Fatalf("original three due owners: %+v error=%v", schedules, err)
			}
			for _, row := range rows {
				if row.State != replycontext.StateOpen || len(row.ReturnJoins) != 1 {
					t.Fatalf("request did not retain its exact return arm: %+v", row)
				}
				join := readNestedReplyDeadlineArm(t, ctx, h, row)
				if join.Status != joinruntime.StatusOpen || join.Completed() != 0 || join.Expected() != 1 ||
					!join.ArmedAt.Equal(at) || !join.DeadlineAt.Equal(at.Add(time.Hour)) || !join.DeadlineAt.Before(time.Now().UTC()) {
					t.Fatalf("constructor replaced the original stage-entry deadline: %+v", join)
				}
				matched := 0
				for _, schedule := range schedules {
					if schedule.Command.TaskID != join.TimerTaskID() {
						continue
					}
					matched++
					if err := genericschedule.ValidateWorkflowJoinScheduleRelation(join, schedule); err != nil ||
						!schedule.InitialDueAt.Equal(join.DeadlineAt) || !schedule.CurrentDueAt.Equal(join.DeadlineAt) || schedule.CurrentEventID != "" {
						t.Fatalf("open reply lost its original native schedule: %+v error=%v", schedule, err)
					}
				}
				if matched != 1 {
					t.Fatalf("return arm omitted or duplicated its deadline owner: matched=%d row=%+v arm=%+v schedules=%+v", matched, row, join, schedules)
				}
			}
			if err := h.rt.Scheduler.ReleaseStartup(ctx); err != nil {
				t.Fatal(err)
			}
			wait, cancelWait := context.WithTimeout(ctx, 20*time.Second)
			defer cancelWait()
			for {
				settled := 0
				for _, row := range rows {
					join := readNestedReplyDeadlineArm(t, wait, h, row)
					if join.Status == joinruntime.StatusClosed && join.CloseReason == joinruntime.CloseReasonDeadline && join.OutcomeFired && !join.OutcomePending && join.Completed() == 0 {
						settled++
					}
				}
				if settled == 3 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("actual original deadlines did not settle: closed=%d cause=%v", settled, context.Cause(wait))
				case <-time.After(10 * time.Millisecond):
				}
			}
			for _, original := range schedules {
				actual, found, err := selected.LoadGenericScheduleActivation(wait, original.ID)
				if err != nil || !found || actual.Status != genericschedule.StatusFired || !actual.InitialDueAt.Equal(original.InitialDueAt) || !actual.CurrentDueAt.Equal(original.CurrentDueAt) {
					t.Fatalf("timeout changed its original coordinate or did not fire: %+v error=%v", actual, err)
				}
				event, found, err := selected.LoadPreparedPublishEvent(wait, actual.CurrentEventID)
				if err != nil || !found || event.Event.ID() != genericschedule.OccurrenceEventID(original.ID, original.CurrentDueAt) || !event.Event.Event().CreatedAt().Equal(original.CurrentDueAt) {
					t.Fatalf("timeout lacks its exact native publication: %+v found=%t error=%v", event, found, err)
				}
			}
			unchanged, err := storetest.ReadReplyContextRecords(wait, selected, runID)
			if err != nil || !reflect.DeepEqual(rows, unchanged) {
				t.Fatalf("timeout settled another reply level: %+v error=%v", unchanged, err)
			}
			held.resume()
			select {
			case err := <-published:
				if err != nil {
					t.Fatalf("finish actual source input after timeout: %v", err)
				}
			case <-publishCtx.Done():
				t.Fatalf("source input did not finish within its original bound: %v", publishCtx.Err())
			}
			if err := h.rt.Bus.WaitForQuiescence(wait); err != nil {
				t.Fatal(err)
			}
			var inner replycontext.Record
			for _, row := range rows {
				if row.ProviderFlowID == "u" {
					inner, err = selected.LoadReplyContext(wait, row.ID)
				}
			}
			if err != nil || inner.State != replycontext.StateTerminal || inner.AcceptedReplyEventID == "" {
				t.Fatalf("late inner worker did not retain its own exact response: %+v error=%v", inner, err)
			}
			reply, found, err := selected.LoadPreparedPublishEvent(wait, inner.AcceptedReplyEventID)
			if err != nil || !found || len(reply.DeliveryRoutes) != 1 {
				t.Fatalf("actual late inner response: %+v found=%t error=%v", reply, found, err)
			}
			id, err := deliverylifecycle.DeliveryID(reply.Event.ID(), reply.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := selected.Snapshot(wait, id)
			if err != nil || delivery.Status != deliverylifecycle.StatusDeadLetter || delivery.ClaimVersion != 1 || delivery.ReasonCode != "handler_terminal_failure" ||
				delivery.Failure == nil || delivery.Failure.Class != failures.ClassStaleArrival || delivery.Failure.Detail.Code != "join_stage_closed" ||
				delivery.Failure.Component != "runtime.pipeline" || delivery.Failure.Operation != "receiver_preparation" {
				encoded, _ := json.Marshal(delivery.Failure)
				t.Fatalf("late reply was not refused by its expired arm: %+v failure=%s error=%v", delivery, encoded, err)
			}
			for _, row := range rows {
				if row.ID == inner.ID {
					continue
				}
				actual, err := selected.LoadReplyContext(wait, row.ID)
				if err != nil || !reflect.DeepEqual(actual, row) {
					t.Fatalf("late inner response settled another level: %+v error=%v", actual, err)
				}
			}
			for _, provider := range []string{"t", "provider"} {
				for _, row := range rows {
					if row.ProviderFlowID == provider {
						t.Run("NR07_late/"+provider, func(t *testing.T) {
							publishNestedDeadlineLateReply(t, wait, h, selected, row)
						})
					}
				}
			}
			for name, expected := range map[string]int{"platform.join_timeout": 3, "platform.join_complete": 0, "request.finished": 0} {
				actual, err := storetest.ReadLifecycleEventCardinality(wait, selected, runID, name)
				if err != nil || actual != expected {
					t.Fatalf("late replies changed timeout/outcome cardinality: event=%s count=%d error=%v want=%d", name, actual, err, expected)
				}
			}
		})
	}
}

func publishNestedDeadlineLateReply(t *testing.T, ctx context.Context, h *runtimeHarness, selected nestedReplyDeadlineStore, row replycontext.Record) {
	t.Helper()
	beforeRows, err := storetest.ReadReplyContextRecords(ctx, selected, row.RunID)
	if err != nil {
		t.Fatal(err)
	}
	request, found, err := selected.LoadPreparedPublishEvent(ctx, row.RequestEventID)
	if err != nil || !found || len(request.DeliveryRoutes) != 1 {
		t.Fatalf("late response lacks its real request/recipient: %+v found=%t error=%v", request, found, err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(row.ProviderFlowID, row.ProviderFlowID+"-node")
	if err != nil {
		t.Fatal(err)
	}
	source, err := events.NewStaticFlowRoutingSource(request.DeliveryRoutes[0].Target.Route())
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(request.Event.Event().Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	payload["result"] = "approved"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	response := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), events.EventType(h.bundle.ResolveFlowEventReference(row.ProviderFlowID, row.ProviderOutputPin)),
		eventtest.Producer(events.EventProducerNode, node.Key()), "", raw, request.Event.Event().ChainDepth()+1, events.LineageFromEvent(request.Event.Event()),
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, request.DeliveryRoutes[0].Target.Route()), source, time.Now().UTC())
	ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: row.ID}})
	if err := h.rt.Bus.PublishAndWait(ctx, response); err != nil {
		t.Fatalf("execute exact late response: %v", err)
	}
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, response.ID())
	if err != nil || !found || len(publication.DeliveryRoutes) != 1 || publication.DeliveryRoutes[0].Target.Route() != row.Origin ||
		publication.DeliveryRoutes[0].Context.ReplyContextID() != row.EnclosingReplyContextID || !reflect.DeepEqual(publication.DeliveryRoutes[0].Context.Joins, row.ReturnJoins) {
		t.Fatalf("late response changed its original return binding: %+v found=%t error=%v", publication, found, err)
	}
	id, err := deliverylifecycle.DeliveryID(response.ID(), publication.DeliveryRoutes[0])
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := selected.Snapshot(ctx, id)
	count, countErr := storetest.CountDeadLettersForOriginalEvent(ctx, selected, response.ID())
	if err != nil || countErr != nil || count != 1 || delivery.Status != deliverylifecycle.StatusDeadLetter || delivery.ClaimVersion != 1 ||
		delivery.ReasonCode != "handler_terminal_failure" || delivery.Failure == nil || delivery.Failure.Class != failures.ClassStaleArrival ||
		delivery.Failure.Detail.Code != "join_stage_closed" || delivery.Failure.Component != "runtime.pipeline" || delivery.Failure.Operation != "receiver_preparation" {
		t.Fatalf("late response missed its exact once-only refusal: delivery=%+v failure=%+v dead_letters=%d errors=%v/%v", delivery, delivery.Failure, count, err, countErr)
	}
	for _, original := range beforeRows {
		actual, err := selected.LoadReplyContext(ctx, original.ID)
		if err != nil {
			t.Fatal(err)
		}
		if original.ID == row.ID {
			if actual.State != replycontext.StateTerminal || actual.AcceptedReplyEventID != response.ID() || !actual.SameIdentity(original) {
				t.Fatalf("late response did not retain only its own evidence: %+v", actual)
			}
		} else if !reflect.DeepEqual(actual, original) {
			t.Fatalf("late response settled a different level: %+v", actual)
		}
		arm := readNestedReplyDeadlineArm(t, ctx, h, original)
		if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonDeadline || !arm.OutcomeFired || arm.OutcomePending || arm.Completed() != 0 {
			t.Fatalf("late response reopened or contributed to a timed-out arm: %+v", arm)
		}
	}
}

func readNestedReplyDeadlineArm(t *testing.T, ctx context.Context, h *runtimeHarness, row replycontext.Record) joinruntime.Activation {
	t.Helper()
	instance, found, err := h.workflow.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: row.RunID,
		Route: flowidentity.StoredRoute(row.Origin.FlowID, row.Origin.FlowInstance, row.Origin.FlowInstance)})
	if err != nil || !found {
		t.Fatalf("read actual requester deadline owner: found=%t error=%v", found, err)
	}
	buckets, err := joinruntime.PersistedBuckets(instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(buckets)
	if err != nil || len(joins) != 1 || !joins[0].JoinRef().Equal(row.ReturnJoins[0].Ref) {
		t.Fatalf("request did not keep its original arm: %+v error=%v", joins, err)
	}
	return joins[0]
}
