package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// These test the publication owner, not delivery/store execution. M43's actual
// both-store journeys remain separate proof obligations.
func TestA2JoinAdmissionFirstPublicationAndRetainedEvidence(t *testing.T) {
	bundle := workflowJoinLifecycleBundle(t)
	plan := exactCompiledJoinPlanForTest(bundle, "orders")
	source := exactWorkflowJoinSource{Source: workflowJoinLifecycleRootAndFlowSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan}}
	run := eventtest.UUID("join-admission-run-a")
	target := events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/one", EntityID: FlowInstanceEntityID("orders/one")}
	owner, err := WorkflowJoinAdmissionOwner(source, run, target)
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{RunID: run, FlowScope: owner.Route.ScopeKey, InstanceID: owner.Route.InstanceID,
		InstancePath: owner.Route.InstancePath, EntityID: target.EntityID, Stage: "awaiting", Cause: "construction"}
	declaration, err := timeridentity.NewJoinRef(plan.Node, plan.HandlerEvent, plan.Spec.Stage, plan.Spec.EffectiveID())
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(plan.Node), Target: events.MustExistingEntityTarget(target)}
	instance := materializedWorkflowInstanceForSource(t, source, correlation.WithRunID(context.Background(), run), WorkflowInstance{
		InstanceID: owner.Route.InstanceID, StorageRef: owner.Route.InstancePath, EntityID: target.EntityID,
		WorkflowName: "orders", EntityType: "test_entity", CurrentState: "awaiting", Revision: 1,
	})
	arm := func(entry timeridentity.StageEntryRef) {
		t.Helper()
		instance.Bookkeeping = map[string]any{}
		if err := workflowlifecycle.StoreStageEntry(instance.Bookkeeping, entry); err != nil {
			t.Fatal(err)
		}
		ref, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
		if err != nil {
			t.Fatal(err)
		}
		at := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
		activation, err := joinruntime.NewActivation(ref, []string{"member"}, nil, at, at.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		buckets := map[string]map[string]any{}
		if err := joinruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		instance.StateBuckets = map[string]any{}
		for key, value := range buckets {
			instance.StateBuckets[key] = value
		}
	}
	arm(entry)
	first, fence, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, &instance)
	if err != nil || len(first) != 1 || first[0].Ref.StageEntry() != entry || fence == nil || fence.Entry != entry || len(fence.Arms) != 1 || fence.Arms[0].Receipt != first[0] || fence.Arms[0].Status != joinruntime.StatusOpen {
		t.Fatalf("first admission = %#v fence=%#v err=%v", first, fence, err)
	}
	instance.Revision++
	if matched, err := fence.MatchesCurrent(instance.CurrentState, instance.Bookkeeping, instance.StateBuckets); err != nil || !matched {
		t.Fatalf("field-only revision changed admission: matched=%v err=%v", matched, err)
	}
	concrete, _, err := PrepareWorkflowJoinAdmission(source, run, target.FlowInstance+"/"+plan.HandlerEvent, route, &instance)
	if err != nil || !reflect.DeepEqual(concrete, first) {
		t.Fatalf("exact scoped publication bypassed join admission: %#v err=%v", concrete, err)
	}
	retainedRoute := route
	retainedRoute.Context.Joins = first
	if _, _, err := PrepareWorkflowJoinAdmission(source, run, "orders/foreign/"+plan.HandlerEvent, retainedRoute, nil); err == nil {
		t.Fatal("a foreign instance event borrowed the exact receipt")
	}
	second := entry
	second.Cause, second.EventID, second.OccurrenceID, second.TransitionID = "delivery", "event-2", "delivery-2", "transition-2"
	instance.Revision = 2
	arm(second)
	newOutput, _, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, &instance)
	if err != nil || len(newOutput) != 1 || newOutput[0].Ref.StageEntry() != second || newOutput[0].Ref.Equal(first[0].Ref) {
		t.Fatalf("new output must bind actual E2: %#v err=%v", newOutput, err)
	}
	corrupt := instance
	corrupt.Bookkeeping = nil
	if _, _, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, &corrupt); err == nil {
		t.Fatal("missing canonical entry was reinterpreted as early arrival")
	}
	corrupt = instance
	corrupt.StateBuckets = nil
	if _, _, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, &corrupt); err == nil {
		t.Fatal("existing stage entry without its required arm was admitted")
	}
	route.Context.Joins = first
	retained, fence, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, nil)
	if err != nil || !reflect.DeepEqual(retained, first) || fence != nil {
		t.Fatalf("retained E1 was replanned: %#v fence=%#v err=%v", retained, fence, err)
	}
	route.Context.Joins = nil
	early, fence, err := PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, nil)
	if err != nil || len(early) != 1 || early[0].Disposition != events.JoinAdmissionEarly || fence == nil || !fence.Entry.Empty() || len(fence.Arms) != 1 || fence.Arms[0].Receipt != early[0] || fence.Arms[0].Status != "" {
		t.Fatalf("unarmed publication = %#v fence=%#v err=%v", early, fence, err)
	}
	route.Context.Joins = early
	retained, fence, err = PrepareWorkflowJoinAdmission(source, run, plan.HandlerEvent, route, &instance)
	if err != nil || !reflect.DeepEqual(retained, early) || fence != nil {
		t.Fatalf("early refusal rebound after arming: %#v fence=%#v err=%v", retained, fence, err)
	}
	terminal := instance
	terminal.CurrentState, terminal.Status = "ready", "active"
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(plan.HandlerEvent), "operator", "", []byte(`{}`), 0,
		run, events.EventEnvelope{}, exactJoinRoutingSource("orders", target.FlowInstance, target.EntityID), time.Now().UTC())
	for _, test := range []struct {
		name     string
		receipts []events.JoinAdmissionReceipt
		class    failures.Class
	}{
		{name: "retained early stays early", receipts: early, class: failures.ClassEarlyArrival},
		{name: "retained bound becomes late", receipts: first, class: failures.ClassStaleArrival},
		{name: "fresh terminal remains unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			route.Context.Joins = test.receipts
			err := validateAdmittedReceiverAvailability(withWorkflowNodeDeliveryRoute(context.Background(), route), source, "orders", event, terminal)
			if test.class == "" {
				var refusal *TerminalReceiverError
				if !errors.As(err, &refusal) {
					t.Fatalf("fresh terminal publication was admitted or reclassified: %v", err)
				}
				return
			}
			envelope, typed := failures.EnvelopeFromError(err)
			if !typed || envelope.Class != test.class {
				t.Fatalf("retained disposition changed: expected=%s error=%v", test.class, err)
			}
		})
	}
	route.Context.Joins = first
	if _, _, err := PrepareWorkflowJoinAdmission(source, "foreign-run", plan.HandlerEvent, route, nil); err == nil {
		t.Fatal("foreign retained entry was admitted")
	}
	if _, _, err := PrepareWorkflowJoinAdmission(source, run, "unrelated.event", route, nil); err == nil {
		t.Fatal("retained receipt borrowed by an unrelated message")
	}
	other := plan.Clone()
	other.HandlerEvent = "another.completed"
	plan.UntilEvent, other.UntilEvent = "halt.requested", "halt.requested"
	source.plans = []runtimecontracts.WorkflowJoinPlan{plan, other}
	if _, _, err := PrepareWorkflowJoinAdmission(source, run, "halt.requested", route, nil); err == nil {
		t.Fatal("partial multi-join binding was admitted")
	}
}

func TestA2JoinAdmissionOwnerUsesCanonicalRootScope(t *testing.T) {
	bundle := workflowJoinLifecycleBundle(t)
	source := workflowJoinLifecycleRootAndFlowSource(bundle)
	run := uuid.NewString()
	owner, err := WorkflowJoinAdmissionOwner(source, run, events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: run, EntityID: run,
	})
	if err != nil || owner.Route != flowidentity.StoredRoute(".", run, run) {
		t.Fatalf("root owner = %#v err=%v", owner, err)
	}
}
