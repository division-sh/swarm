package tools

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

// Isolate conversion at the authoritative planner-result boundary. The separate
// reply-planning test below must produce its failures without this injection.
type emitRefusalBoundaryBus struct {
	*bus.EventBus
	code string
}

func (b emitRefusalBoundaryBus) CheckPublishRecipientPlan(ctx context.Context, event events.Event) (bus.PublishRecipientPlan, error) {
	plan, err := b.EventBus.CheckPublishRecipientPlan(ctx, event)
	if err != nil {
		return plan, err
	}
	if !plan.UsesCanonicalRouteAuthority() {
		return plan, fmt.Errorf("fixture lacks canonical route authority")
	}
	plan.TargetFailure = b.code
	return plan, nil
}

func TestIssue642ToolEmitRefusalFailureTaxonomy(t *testing.T) {
	connectFailure := pinrouting.TargetFailureFromConnect(pinrouting.ConnectFailureTargetUnresolved).Code()
	for _, test := range []struct {
		code   string
		class  failures.Class
		detail string
	}{
		{"target_required_missing", failures.ClassTargetUnreachable, "target_required_missing"},
		{"platform.reply_already_terminal", failures.ClassReplyAlreadyTerminal, "reply_already_terminal"},
		{"platform.stale_arrival", failures.ClassStaleArrival, "stale_arrival"},
		{connectFailure, failures.ClassTargetUnreachable, connectFailure},
	} {
		t.Run(test.code, func(t *testing.T) {
			source := emitRoutePlanStaticSource(t, contracts.FlowConnect{Event: "deploy.done", From: "producer", To: "consumer", Rename: "deploy.completed"})
			store := newEmitRoutePlanStore()
			store.targetOwners = []bus.ActiveTargetDescriptor{{ID: "consumer", EntityID: flowidentity.EntityID("consumer"), FlowInstance: "consumer",
				Availability: pipeline.NewDeliveryTargetAvailability("", "active", false)}}
			eventBus := newEmitRoutePlanEventBus(t, store, source)
			actor := actors.AgentConfig{ExecutionMode: "live", ID: "producer-agent", Identity: toolTestAgentIdentity(t, "producer-agent", "producer", "producer"),
				Role: "producer", FlowID: "producer", FlowPath: "producer", EntityID: flowidentity.EntityID("producer-entity"), EmitEvents: []string{"deploy.done"}}
			executor := NewExecutorWithOptions(emitRefusalBoundaryBus{eventBus, test.code}, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})
			actor = toolTestConstructedActor(t, executor, actor)
			ctx := bus.WithInboundEvent(correlation.WithRunID(unmanagedToolTestContext(), actor.Identity.RunID), eventtest.RunCreatingRootIngress(
				eventtest.UUID("issue642-refusal-parent"), "producer/deploy.requested", "runtime", "", nil, 0, actor.Identity.RunID, "", events.EventEnvelope{}, time.Now().UTC()))
			_, err := executor.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
			if err == nil {
				t.Fatal("expected refusal before publication")
			}
			failure := failures.Normalize(err, "test", "emit")
			if failure.Class != test.class || failure.Detail.Code != test.detail || failure.Component != "tool-executor" ||
				failure.Operation != "handle_emit_tool.route_plan_preflight" || failure.Detail.Attributes["tool"] != "emit_deploy_done" ||
				failure.Detail.Attributes["event"] != "producer/deploy.done" {
				t.Fatalf("tool reinterpreted refusal or lost caller diagnostics: got=%+v want=%s/%s", failure, test.class, test.detail)
			}
			if len(store.events) != 0 || len(store.routes) != 0 || len(store.active) != 0 {
				t.Fatal("refusal published, persisted recipients or opened mutation work")
			}
		})
	}
}

type emitReplyReadOwner struct {
	record                 replycontext.Record
	loads, creates, claims int
}

func (s *emitReplyReadOwner) LoadReplyContext(_ context.Context, id string) (replycontext.Record, error) {
	s.loads++
	if id != s.record.ID {
		return replycontext.Record{}, replycontext.ErrNotFound
	}
	return s.record.Normalized(), nil
}

func (s *emitReplyReadOwner) CreateReplyContext(context.Context, replycontext.Record) error {
	s.creates++
	return fmt.Errorf("emit preflight must not create a reply")
}

func (s *emitReplyReadOwner) ClaimReplyContext(context.Context, string, string) (replycontext.Record, replycontext.ClaimOutcome, error) {
	s.claims++
	return replycontext.Record{}, "", fmt.Errorf("emit preflight must not claim a reply")
}

type emitCanonicalReplyPlanBus struct {
	*bus.EventBus
	plan  bus.PublishRecipientPlan
	calls int
}

func (b *emitCanonicalReplyPlanBus) CheckPublishRecipientPlan(ctx context.Context, event events.Event) (bus.PublishRecipientPlan, error) {
	b.calls++
	plan, err := b.EventBus.CheckPublishRecipientPlan(ctx, event)
	b.plan = plan
	return plan, err
}

// Real compiled request/response pairing and the real reply resolver produce
// the refusal. Only row readback is in-memory; no planner failure is injected.
func TestIssue642ToolEmitConsumesActualReplyPlanRefusals(t *testing.T) {
	for _, missingReference := range []bool{false, true} {
		name, class, detail := "terminal", failures.ClassReplyAlreadyTerminal, "reply_already_terminal"
		if missingReference {
			name, class, detail = "missing_reference", failures.ClassStaleArrival, "stale_arrival"
		}
		t.Run(name, func(t *testing.T) {
			bundle := emitRoutePlanTestBundle(t, []emitRoutePlanTestFlow{
				{id: "producer", mode: "static", inputs: []contracts.FlowInputEventPin{{Event: "deploy.requested"}},
					outputs: []contracts.FlowOutputEventPin{{Event: "deploy.done"}}, nodes: map[string]contracts.SystemNodeContract{
						"request-observer": {EventHandlers: map[string]contracts.SystemNodeEventHandler{"deploy.requested": {}}},
					}},
				{id: "consumer", mode: "static", inputs: []contracts.FlowInputEventPin{{Event: "deploy.completed"}},
					outputs: []contracts.FlowOutputEventPin{{Event: "deploy.requested"}}, nodes: map[string]contracts.SystemNodeContract{
						"consumer-node": {EventHandlers: map[string]contracts.SystemNodeEventHandler{"deploy.completed": {}}},
					}},
			}, []contracts.FlowConnect{
				{Event: "deploy.requested", From: "consumer", To: "producer"},
				{Event: "deploy.done", From: "producer", To: "consumer", Rename: "deploy.completed", RepliesTo: "deploy.requested"},
			})
			source := toolTestSourceWithDeclaredAgent(t, bundle, "producer-agent", "producer", "deploy.done")
			graph := pinrouting.CompileConnectGraph(source)
			if len(graph.Issues()) != 0 {
				t.Fatalf("real reply fixture did not compile: %+v", graph.Issues())
			}
			var response pinrouting.ConnectRoutePlan
			for _, plan := range graph.Plans() {
				if plan.ReplyRole() == pinrouting.ConnectReplyRoleResponse {
					if response.ReplyResolution() != nil {
						t.Fatal("reply fixture has ambiguous response plans")
					}
					response = plan
				}
			}
			if response.ReplyResolution() == nil {
				t.Fatal("reply fixture lacks its actual paired response")
			}
			binding := response.ReplyResolution().Readback()
			requestID, now := uuid.NewString(), time.Now().UTC()
			terminalAt := now.Add(time.Second)
			record := replycontext.Record{
				RunID: toolTestRunID, RequestEventID: requestID, SourceRunID: toolTestRunID, SourceRequestEventID: requestID,
				RequesterFlowID: binding.RequesterFlowID, RequestOutputPin: binding.RequestOutputPin, ReplyInputPin: binding.ReplyInputPin,
				ProviderFlowID: binding.ProviderFlowID, ProviderInputPin: binding.ProviderInputPin, ProviderOutputPin: binding.ProviderOutputPin,
				Origin:               events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: flowidentity.EntityID("consumer")},
				RequestCorrelationID: requestID, State: replycontext.StateTerminal, AcceptedReplyEventID: uuid.NewString(),
				CreatedAt: now, UpdatedAt: terminalAt, TerminalAt: &terminalAt,
			}
			record.ID = replycontext.DeterministicID(requestID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
			if err := record.Validate(); err != nil || !response.MatchesReplyRecord(record) {
				t.Fatalf("captured component row does not match the compiled pairing: %+v error=%v", record, err)
			}
			replies := &emitReplyReadOwner{record: record}
			store := newEmitRoutePlanStore()
			store.targetOwners = []bus.ActiveTargetDescriptor{{ID: "consumer", EntityID: record.Origin.EntityID, FlowInstance: "consumer",
				Availability: pipeline.NewDeliveryTargetAvailability("", "active", false)}}
			probe := &emitCanonicalReplyPlanBus{EventBus: newEmitRoutePlanEventBusWithReplyOwner(t, store, source, replies)}
			actor := actors.AgentConfig{ExecutionMode: "live", ID: "producer-agent", Identity: toolTestAgentIdentity(t, "producer-agent", "producer", "producer"),
				Role: "producer", FlowID: "producer", FlowPath: "producer", EntityID: flowidentity.EntityID("producer-entity"), EmitEvents: []string{"deploy.done"}}
			executor := NewExecutorWithOptions(probe, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})
			actor = toolTestConstructedActor(t, executor, actor)
			ctx := bus.WithInboundEvent(correlation.WithRunID(unmanagedToolTestContext(), actor.Identity.RunID), eventtest.RunCreatingRootIngress(
				requestID, "producer/deploy.requested", "runtime", "", nil, 0, actor.Identity.RunID, "", events.EventEnvelope{}, now))
			if !missingReference {
				ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}})
			}
			_, err := executor.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
			if err == nil {
				t.Fatal("actual reply refusal did not stop tool publication")
			}
			failure := failures.Normalize(err, "test", "emit")
			if probe.calls != 1 || !probe.plan.UsesCanonicalRouteAuthority() || probe.plan.TargetFailure != string(class) || len(probe.plan.DeliveryRoutes) != 0 ||
				failure.Class != class || failure.Detail.Code != detail || failure.Component != "tool-executor" || failure.Operation != "handle_emit_tool.route_plan_preflight" {
				t.Fatalf("actual reply-plan refusal was not propagated: plan=%+v calls=%d failure=%+v", probe.plan, probe.calls, failure)
			}
			wantLoads := 1
			if missingReference {
				wantLoads = 0
			}
			if replies.loads != wantLoads || replies.creates != 0 || replies.claims != 0 || !reflect.DeepEqual(replies.record, record) ||
				len(store.events) != 0 || len(store.routes) != 0 || len(store.active) != 0 {
				t.Fatalf("preflight changed reply/publication state: loads=%d creates=%d claims=%d events=%d", replies.loads, replies.creates, replies.claims, len(store.events))
			}
		})
	}
}
