package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Static route headers are compiler-owned component-fixture inputs. This does
// not prove C/E boot materialization of persisted lifecycle headers. No entity
// or lifecycle rows are inserted to make these payload-only handlers execute.
func TestA2FieldlessPairedReplyPreservesEntitylessExecutionOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID, token := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, canonicalrouting.ArrivalJoinRoutingFiles(t, canonicalrouting.ArrivalJoinFieldlessReply)))
			if issues := pinrouting.CompileConnectGraph(source).Issues(); len(issues) != 0 || len(source.WorkflowJoins()) != 0 {
				t.Fatalf("fieldless reply requires admitted paired Connect and no joins: issues=%#v joins=%#v", issues, source.WorkflowJoins())
			}
			sender := externalPipelineSourceNode(t, source, "requester", "sender")
			receiver := externalPipelineSourceNode(t, source, "requester", "receiver")
			provider := externalPipelineSourceNode(t, source, "provider", "provider")
			for _, declaration := range []struct {
				flow, event string
				node        identity.ExecutableNode
			}{
				{"requester", "request.send", sender},
				{"requester", "provider.replied", receiver},
				{"provider", "provider.requested", provider},
			} {
				handler, found := source.ExecutableNodeEventHandlers(declaration.node)[declaration.event]
				policy, err := pipeline.CompileDeliveryTargetCompatibilityPolicy(source, declaration.node, declaration.flow, events.EventType(declaration.event), handler)
				if !found || err != nil || policy.Dependency != pipeline.DeliveryTargetEntityOptional {
					t.Fatalf("payload-only handler requires invented state: handler=%s found=%v policy=%#v err=%v", declaration.node.Key(), found, policy, err)
				}
			}
			probe := &a2HeldWorkerProbe{Probe: lifecycleprobe.New(), nodeID: provider.Key(), started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
			t.Cleanup(probe.resume)
			logger := &exactJoinRuntimeLogger{}
			module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
				{Node: sender, Subscriptions: []events.EventType{"requester/request.send"}, ExecutionType: contracts.SystemNodeExecutionType},
				{Node: receiver, Subscriptions: []events.EventType{"requester/provider.replied"}, ExecutionType: contracts.SystemNodeExecutionType},
				{Node: provider, Subscriptions: []events.EventType{"provider/provider.requested"}, ExecutionType: contracts.SystemNodeExecutionType},
			}}
			newBus := func() *runtimebus.EventBus {
				t.Helper()
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger})
				if err != nil {
					t.Fatal(err)
				}
				return bus
			}
			bus := newBus()
			options := pipeline.PipelineCoordinatorOptions{Module: module, TestLifecycleProbe: probe}
			pc := newGateRecoveryCoordinator(bus, selected, options)
			bus.SetInterceptors(pc)
			count := func(query string) int {
				t.Helper()
				var n int
				if err := selected.db.QueryRowContext(ctx, query, runID).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			assertNoState := func() {
				t.Helper()
				for _, table := range []string{"entity_state", "entity_mutations", "workflow_instance_initial_materializations", "timers"} {
					if n := count("SELECT COUNT(*) FROM " + table + " WHERE run_id=$1"); n != 0 {
						t.Fatalf("payload-only reply invented %s rows: %d", table, n)
					}
				}
			}
			assertNoState()
			payload, err := json.Marshal(map[string]any{"token": token})
			if err != nil {
				t.Fatal(err)
			}
			origin := events.RouteIdentity{FlowID: "requester", FlowInstance: "requester"}
			providerRoute := events.RouteIdentity{FlowID: "provider", FlowInstance: "provider"}
			trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "requester/request.send", "operator", "", payload, 0, runID,
				events.EventEnvelope{}, eventtest.StaticFlowRoutingSource(origin.FlowID, origin.FlowInstance, ""), time.Now().UTC())
			if err := bus.PublishAcknowledged(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			var requestID string
			select {
			case signal := <-probe.started:
				requestID = signal.EventID
			case <-time.After(10 * time.Second):
				t.Fatalf("fieldless provider did not receive real request: logs=%s", logger.String())
			}
			request, found, err := selected.events.LoadPreparedPublishEvent(ctx, requestID)
			if err != nil || !found || len(request.DeliveryRoutes) != 1 {
				t.Fatalf("read actual paired request: found=%v routes=%#v err=%v", found, request.DeliveryRoutes, err)
			}
			requestEvent, requestRoute := request.Event.Event(), request.DeliveryRoutes[0]
			if requestEvent.ParentEventID() != trigger.ID() || requestEvent.SourceRoute() != origin || requestEvent.RoutingSource().Route() != origin ||
				requestEvent.Producer().ID() != sender.Key() || requestRoute.Recipient.ID() != provider.Key() || !requestRoute.Target.EntitylessReceiver() ||
				requestRoute.Target.Route() != providerRoute || len(requestRoute.Context.Joins) != 0 {
				t.Fatalf("request lost canonical entityless source/target headers: event=%#v route=%#v", requestEvent, requestRoute)
			}
			record, err := selected.events.LoadReplyContext(ctx, requestRoute.Context.ReplyContextID())
			if err != nil || record.Validate() != nil || record.State != replycontext.StateOpen || record.RunID != runID || record.RequestEventID != requestID ||
				record.RequesterFlowID != "requester" || record.ProviderFlowID != "provider" || record.Origin != origin || len(record.ReturnJoins) != 0 ||
				record.AcceptedReplyEventID != "" || record.TerminalAt != nil || record.ID != replycontext.DeterministicID(requestID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, origin) {
				t.Fatalf("request invented join/entity return authority: record=%#v err=%v", record, err)
			}
			assertNoState()
			probe.resume()
			replyID := exactJoinOccurrenceEventID(t, selected, ctx, runID, source.ResolveFlowEventReference("provider", "provider.replied"))
			reply, found, err := selected.events.LoadPreparedPublishEvent(ctx, replyID)
			if err != nil || !found || len(reply.DeliveryRoutes) != 1 {
				t.Fatalf("read actual paired reply: found=%v routes=%#v err=%v", found, reply.DeliveryRoutes, err)
			}
			replyEvent, replyRoute := reply.Event.Event(), reply.DeliveryRoutes[0]
			if replyEvent.ParentEventID() != requestID || replyEvent.SourceRoute() != providerRoute || replyEvent.RoutingSource().Route() != providerRoute ||
				replyEvent.Producer().ID() != provider.Key() || replyRoute.Recipient.ID() != receiver.Key() || !replyRoute.Target.EntitylessReceiver() ||
				replyRoute.Target.Route() != origin || replyRoute.Context.Reply != nil || len(replyRoute.Context.Joins) != 0 {
				t.Fatalf("paired reply lost exact entityless origin: event=%#v route=%#v", replyEvent, replyRoute)
			}
			for _, accepted := range []struct {
				event events.Event
				node  string
			}{{trigger, sender.Key()}, {requestEvent, provider.Key()}, {replyEvent, receiver.Key()}} {
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, accepted.event, accepted.node, "completed", "delivered", logger)
				assertExactJoinDeliveryCount(t, selected, ctx, accepted.event.ID(), accepted.node, 1)
			}
			observedID := exactJoinOccurrenceEventID(t, selected, ctx, runID, source.ResolveFlowEventReference("requester", "reply.observed"))
			observed, found, err := selected.events.LoadPreparedPublishEvent(ctx, observedID)
			if err != nil || !found {
				t.Fatalf("payload-only reply effect was not durably emitted: found=%v err=%v", found, err)
			}
			var result map[string]any
			if err := json.Unmarshal(observed.Event.Event().Payload(), &result); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result, map[string]any{"token": token, "value": "provider-result"}) || observed.Event.Event().ParentEventID() != replyID ||
				observed.Event.Event().SourceRoute() != origin || observed.Event.Event().Producer().ID() != receiver.Key() {
				t.Fatalf("actual reply effect lost exact accepted event/payload/source: event=%#v payload=%#v", observed.Event.Event(), result)
			}
			terminal, err := selected.events.LoadReplyContext(ctx, record.ID)
			if err != nil || terminal.Validate() != nil || !terminal.SameIdentity(record) || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID != replyID {
				t.Fatalf("fieldless return context did not preserve exact accepted reply: record=%#v err=%v", terminal, err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(replyID, replyRoute)
			if err != nil {
				t.Fatal(err)
			}
			deliveries := selected.events.(deliverylifecycle.Store)
			deliveryBefore, err := deliveries.Snapshot(ctx, deliveryID)
			if err != nil || deliveryBefore.Status != deliverylifecycle.StatusDelivered || deliveryBefore.Failure != nil || deliveryBefore.RetryCount != 0 {
				t.Fatalf("payload-only return did not settle successfully: delivery=%#v err=%v", deliveryBefore, err)
			}
			outcomesBefore, err := deliveries.Outcomes(ctx, deliveryID)
			if err != nil || len(outcomesBefore) != 1 || outcomesBefore[0].Outcome != "delivered" || outcomesBefore[0].Failure != nil ||
				!reflect.DeepEqual(outcomesBefore[0].SideEffects, []string{"handler_completed"}) {
				t.Fatalf("payload-only return lost its once-only successful handler effect: outcomes=%#v err=%v", outcomesBefore, err)
			}
			assertNoState()
			bus = newBus()
			pc = newGateRecoveryCoordinator(bus, selected, options)
			bus.SetInterceptors(pc)
			for _, duplicate := range []events.Event{trigger, requestEvent, replyEvent} {
				if err := bus.PublishAcknowledged(ctx, duplicate); err != nil {
					t.Fatalf("exact fieldless duplicate failed after coordinator reconstruction: %v", err)
				}
			}
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{source.ResolveFlowEventReference("requester", "provider.requested"), source.ResolveFlowEventReference("provider", "provider.replied"), source.ResolveFlowEventReference("requester", "reply.observed")} {
				var publications int
				if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2", runID, name).Scan(&publications); err != nil || publications != 1 {
					t.Fatalf("fieldless replay duplicated %s: count=%d err=%v", name, publications, err)
				}
			}
			reloaded, err := selected.events.LoadReplyContext(ctx, record.ID)
			if err != nil || !reflect.DeepEqual(reloaded, terminal) || count("SELECT COUNT(*) FROM reply_contexts WHERE run_id=$1") != 1 ||
				count("SELECT COUNT(*) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1") != 0 {
				t.Fatalf("fieldless replay rewrote return context or created a refusal: record=%#v err=%v", reloaded, err)
			}
			deliveryAfter, deliveryErr := deliveries.Snapshot(ctx, deliveryID)
			outcomesAfter, outcomeErr := deliveries.Outcomes(ctx, deliveryID)
			observedAfter, found, effectErr := selected.events.LoadPreparedPublishEvent(ctx, observedID)
			if deliveryErr != nil || outcomeErr != nil || effectErr != nil || !found || !reflect.DeepEqual(deliveryAfter, deliveryBefore) ||
				!reflect.DeepEqual(outcomesAfter, outcomesBefore) || !reflect.DeepEqual(observedAfter, observed) {
				t.Fatalf("fieldless replay re-executed or rewrote accepted return/effect: delivery=%v outcomes=%v effect=%v", deliveryErr, outcomeErr, effectErr)
			}
			assertNoState()
		})
	}
}
