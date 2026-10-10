package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type nativeLifecycleClaimAdmissionProbeForTest struct {
	WorkflowEngineMutationOwner
	t        *testing.T
	pc       *PipelineCoordinator
	fixture  *PipelineDeliveryNativeFixtureForTest
	observed int
	event    events.Event
}

func (p *nativeLifecycleClaimAdmissionProbeForTest) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	if command.State.ExpectedState == "queued" && command.State.CurrentState == "done" {
		p.observed++
		transition, err := compiledLifecycleTransitionForTest(p.pc, ".", "queued", "done", string(p.event.Type()))
		if err != nil {
			p.t.Fatal(err)
		}
		effect, err := workflowlifecycle.NewAcceptedEvent(command.State.Identity.Route, identity.NormalizeEntityID(command.State.EntityID), p.event.ID(), string(p.event.Type()), p.event.ExecutionMode(), p.event.CreatedAt(), transition)
		if err != nil {
			p.t.Fatal(err)
		}
		claim, found := deliverylifecycle.ClaimFromContext(ctx)
		if !found {
			p.t.Fatal("original native mutation lacks its acquired claim")
		}
		effect, err = effect.WithExecutionOccurrence("delivery", claim.DeliveryID())
		if err != nil {
			p.t.Fatal(err)
		}
		before := p.fixture.Transactions()
		route, found := workflowNodeDeliveryRoute(ctx)
		if !found {
			p.t.Fatal("native mutation lacks its exact retained route")
		}
		foreign := route
		foreign.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: effect.Route().InstancePath, EntityID: "11111111-1111-1111-1111-111111111111"})
		for _, hostile := range []context.Context{deliverylifecycle.WithoutClaim(ctx), withWorkflowNodeDeliveryRoute(ctx, foreign), correlation.WithInboundEvent(ctx, nativeWorkflowJoinEventForTest(ctx, ".", effect.Route().InstancePath, effect.EntityID().String(), effect.EventType(), []byte("{}"), time.Now().UTC()))} {
			if _, err := admitTestLifecycleDeliveryOccurrence(hostile, p.pc, effect); err == nil {
				p.t.Fatal("unowned lifecycle occurrence was admitted")
			}
		}
		accepted, err := admitTestLifecycleDeliveryOccurrence(ctx, p.pc, effect)
		if err != nil || !reflect.DeepEqual(accepted, effect) {
			p.t.Fatalf("original live claim did not preserve its exact lifecycle effect: %v", err)
		}
		if after := p.fixture.Transactions(); after != before {
			p.t.Fatalf("claim validation performed persistence: before=%+v after=%+v", before, after)
		}
	}
	return p.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
}

func VerifyNativeDirectLifecycleComponentRejectsMissingOrForeignClaimForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
			probe := &nativeLifecycleClaimAdmissionProbeForTest{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, t: t, pc: pc, fixture: fixture}
			pc.workflowStore.engineMutations = probe
			run := correlation.RunIDFromContext(ctx)
			event := nativeWorkflowJoinEventForTest(ctx, ".", run, run, "flow.transitioned", []byte("{}"), time.Now().UTC())
			probe.event = event
			node := pipelineNode(t, ".", "lifecycle-owner")
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)["flow.transitioned"]
			if _, err := executeNativePublishedWorkflowJoinForTest(t, fixture, mutations, pc, correlation.WithInboundEvent(ctx, event), node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(run), run), HandlerEventKey: "flow.transitioned"}); err != nil {
				t.Fatal(err)
			}
			if probe.observed != 1 {
				t.Fatalf("live claim validation probes=%d, want one", probe.observed)
			}
		})
	}
}

func nativeLifecycleComponentForTest(t *testing.T, backend string, bundle *contracts.WorkflowContractBundle, initial string, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativePipelineLifecycleMutationObservationForTest) {
	t.Helper()
	fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, nil, open)
	pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
	if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pc.workflowTimers.stop(join); err != nil {
			t.Error(err)
		}
	})
	run := correlation.RunIDFromContext(ctx)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: run, StorageRef: run, EntityID: run, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(), CurrentState: initial, EnteredStageAt: time.Now().UTC(), Fields: map[string]any{}, EntityType: "test_entity"})); err != nil {
		t.Fatal(err)
	}
	return fixture, pc, ctx, mutations
}

func executeNativeLifecycleTransitionForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, mutations *nativePipelineLifecycleMutationObservationForTest, pc *PipelineCoordinator, ctx context.Context, name string) (events.Event, error) {
	t.Helper()
	run := correlation.RunIDFromContext(ctx)
	event := nativeWorkflowJoinEventForTest(ctx, ".", run, run, name, []byte("{}"), time.Now().UTC())
	from := string(mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(run), run).Stage)
	transition, err := compiledLifecycleTransitionForTest(pc, ".", from, nativeLifecycleTargetForTest(t, pc.SemanticSource(), from, name), name)
	if err != nil {
		return event, err
	}
	node, handlerName, found := transition.HandlerOrigin()
	if !found {
		return event, fmt.Errorf("native lifecycle event has no compiled handler owner")
	}
	handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)[handlerName]
	_, err = executeNativePublishedWorkflowJoinForTest(t, fixture, mutations, pc, ctx, node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(run), run), HandlerEventKey: handlerName})
	return event, err
}

func nativeLifecycleTargetForTest(t *testing.T, source semanticview.Source, from, name string) string {
	t.Helper()
	graph, found := semanticview.WorkflowStageTopology(source, ".")
	if !found {
		t.Fatal("native lifecycle fixture has no compiled root topology")
	}
	var target string
	for _, edge := range graph.Edges {
		if edge.EventType == name && edge.From == from && edge.Node.Valid() && edge.HandlerEvent != "" {
			if target != "" {
				t.Fatal("ambiguous lifecycle handler")
			}
			target = edge.To
		}
	}
	if target == "" {
		t.Fatal("native lifecycle fixture has no matching handler")
	}
	return target
}

func nativeGateCardForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context) decisioncard.Card {
	t.Helper()
	items, cursor, err := fixture.Cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: correlation.RunIDFromContext(ctx), AnchorKind: string(decisioncard.AnchorKindStageGate), Limit: 10})
	if err != nil || cursor != "" || len(items) != 1 {
		t.Fatalf("exact native gate cards: count=%d cursor=%s error=%v", len(items), cursor, err)
	}
	card, err := fixture.Cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	return card
}
