package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA2PortfolioPublicIndependentArrivalAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.FanInBarrier)
			requireA2PortfolioVerification(t, root)
			opts, start := lifecycleRestartHarness(t, backend, root)
			var selected serveRuntimePersistence
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p })
			process, rt := start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			setup := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "portfolio.setup", "idempotency_key": "a2-admission-setup",
				"payload": map[string]any{"portfolio_id": "portfolio-one", "period_id": "period-one", "expected_operating_ids": []string{"op-a", "op-b"}},
			})
			if !setup.NewRunCreated || setup.RunID == "" || setup.EventID == "" {
				t.Fatalf("public setup did not create a run: %+v", setup)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
			parent := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio", "portfolio_id", "portfolio-one")
			period := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio/period", "period_id", "period-one")
			requireA2PortfolioSetupRoutes(t, rt, setup, parent, period, "materializing_entity")
			initialArm := requireA2PortfolioJoin(t, rt, period, 0, false, nil)
			workers := map[string]bool{}
			requireSettled := func(deliveryID string) {
				t.Helper()
				var claimed, settled, open int
				if err := rt.DB.QueryRow(`SELECT
 (SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND closure_kind='settled' AND open_marker=FALSE),
 (SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND open_marker=TRUE)`, deliveryID).Scan(&claimed, &settled, &open); err != nil || claimed != 1 || settled != 1 || open != 0 {
					t.Fatalf("actual arrival claim accounting: claimed=%d settled=%d open=%d err=%v", claimed, settled, open, err)
				}
			}
			publish := func(key, member string, revenue int, failureClass failures.Class, failureCode string) (map[string]any, servedEventPublishRPCResult, operatorread.OperatorEventFull) {
				t.Helper()
				payload := map[string]any{"portfolio_id": "portfolio-one", "period_id": "period-one", "operating_id": member, "revenue": revenue}
				params := map[string]any{"run_id": setup.RunID, "event_name": "operating.report.triggered", "payload": payload, "idempotency_key": key}
				trigger := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if trigger.NewRunCreated || trigger.RunID != setup.RunID || trigger.EventID == "" {
					t.Fatalf("independent public arrival changed its run: %+v", trigger)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
				wirePayload := map[string]any{"portfolio_id": "portfolio-one", "period_id": "period-one", "operating_id": member, "revenue": float64(revenue)}
				requested, worker := requireA2PortfolioOperatingCreation(t, rt, trigger, wirePayload, "operating_instance_id")
				if workers[worker.Entity.EntityID] {
					t.Fatalf("independent trigger borrowed an earlier worker: %+v", worker)
				}
				workers[worker.Entity.EntityID] = true
				reported := requireA2PortfolioEmission(t, rt, setup.RunID, worker.Entity.FlowInstance+"/operating.reported", requested.EventID, map[string]any{
					"portfolio_id": "portfolio-one", "period_id": "period-one", "operating_id": member,
					"operating_instance_id": requested.EventID, "revenue": float64(revenue),
				})
				requireA2PortfolioDelivery(t, reported, "portfolio", "portfolio-router", parent, "existing_entity")
				var matches []operatorread.OperatorEventFull
				for _, event := range requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported") {
					if event.SourceEventID == reported.EventID {
						matches = append(matches, event)
					}
				}
				if len(matches) != 1 {
					t.Fatalf("ordinary forwarded arrival count=%d want=1", len(matches))
				}
				arrival := a2ReadJoinPublicEvent(t, rt, matches[0].EventID)
				if !reflect.DeepEqual(matches[0], arrival) || arrival.RunID != setup.RunID || !reflect.DeepEqual(arrival.Payload, wirePayload) ||
					arrival.NoDelivery != nil || len(arrival.Deliveries) != 1 {
					t.Fatalf("public forwarded event lost payload or list/get agreement: %+v", arrival)
				}
				if failureClass == "" {
					if len(arrival.DeadLetters) != 0 {
						t.Fatalf("lawful arrival dead-lettered: %+v", arrival)
					}
					requireA2PortfolioDelivery(t, arrival, "portfolio/period", "portfolio-collector", period, "existing_entity")
				} else {
					delivery := arrival.Deliveries[0]
					want := operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: "portfolio/period", FlowInstance: period.Entity.FlowInstance, EntityID: period.Entity.EntityID}
					if delivery.SubscriberID != identitytest.FlowNode(t, "portfolio/period", "portfolio-collector").Key() ||
						delivery.SubscriberType != "node" || delivery.Target != want || delivery.Status != "dead_letter" || !delivery.Terminal ||
						delivery.RetryCount != 0 || delivery.RetryScheduled || delivery.Failure == nil ||
						delivery.Failure.Class != failureClass || delivery.Failure.Detail.Code != failureCode ||
						len(delivery.DeadLetters) != 1 || len(arrival.DeadLetters) != 1 ||
						!reflect.DeepEqual(delivery.DeadLetters[0].Failure, *delivery.Failure) ||
						!reflect.DeepEqual(arrival.DeadLetters[0], delivery.DeadLetters[0]) {
						t.Fatalf("refused arrival did not settle once through the exact receiver: %+v", arrival)
					}
				}
				requireSettled(arrival.Deliveries[0].DeliveryID)
				return params, trigger, arrival
			}
			_, _, original := publish("a2-original", "op-a", 11, "", "")
			period = requireA2PortfolioEntity(t, rt, setup.RunID, period.Entity.FlowInstance)
			partial := requireA2PortfolioJoin(t, rt, period, 1, false, nil)
			if !partial.JoinRef().Equal(initialArm.JoinRef()) {
				t.Fatal("first arrival replaced its original arm")
			}
			history := func() int {
				t.Helper()
				var count int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2`, setup.RunID, period.Entity.EntityID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count
			}
			beforeHistory := history()
			requireUnchanged := func() {
				t.Helper()
				after := requireA2PortfolioEntity(t, rt, setup.RunID, period.Entity.FlowInstance)
				if !reflect.DeepEqual(partial, requireA2PortfolioJoin(t, rt, after, 1, false, nil)) ||
					!reflect.DeepEqual(period.Fields, after.Fields) || !reflect.DeepEqual(period.Gates, after.Gates) ||
					!reflect.DeepEqual(period.Accumulated, after.Accumulated) ||
					period.Entity.CurrentState != after.Entity.CurrentState || history() != beforeHistory ||
					len(requireA2PortfolioEvents(t, rt, setup.RunID, "platform.join_complete")) != 0 {
					t.Fatal("duplicate/conflict changed membership, business state, history or completion")
				}
			}
			_, _, duplicate := publish("a2-independent-duplicate", "op-a", 11, "", "")
			if duplicate.EventID == original.EventID || duplicate.Deliveries[0].DeliveryID == original.Deliveries[0].DeliveryID {
				t.Fatal("business duplicate proof reused the original publication/delivery")
			}
			requireUnchanged()
			conflictParams, conflictTrigger, conflict := publish("a2-independent-conflict", "op-a", 12, failures.ClassConflictingDuplicate, "join_member_conflicting_duplicate")
			requireUnchanged()
			unexpectedParams, unexpectedTrigger, unexpected := publish("a2-unexpected-member", "not-a-member", 33, failures.ClassUnexpectedArrival, "join_member_unexpected")
			requireUnchanged()
			beforeRequests := requireA2PortfolioEvents(t, rt, setup.RunID, "ingress/operating.report.requested")
			beforeReports := requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported")
			if len(beforeRequests) != 4 || len(beforeReports) != 4 {
				t.Fatal("independent public arrivals did not create four actual worker requests/reports")
			}
			predecessor := rt.Runtime.Options.RuntimeInstanceID
			if code := process.stop(); code != 0 {
				t.Fatalf("admission predecessor exit=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			process, rt = start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			if rt.Runtime.Options.RuntimeInstanceID == predecessor {
				t.Fatal("admission restart reused predecessor authority")
			}
			requireUnchanged()
			for _, refused := range []struct {
				params  map[string]any
				trigger servedEventPublishRPCResult
				arrival operatorread.OperatorEventFull
			}{{conflictParams, conflictTrigger, conflict}, {unexpectedParams, unexpectedTrigger, unexpected}} {
				replay := requireServedEventPublishRPCResult(t, rt.Endpoint, refused.params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
				if replay.EventID != refused.trigger.EventID || replay.RunID != setup.RunID ||
					!reflect.DeepEqual(refused.arrival, a2ReadJoinPublicEvent(t, rt, refused.arrival.EventID)) ||
					!reflect.DeepEqual(beforeRequests, requireA2PortfolioEvents(t, rt, setup.RunID, "ingress/operating.report.requested")) ||
					!reflect.DeepEqual(beforeReports, requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported")) {
					t.Fatal("committed refusal replay changed identity, settled outcomes or actual worker publications")
				}
				requireSettled(refused.arrival.Deliveries[0].DeliveryID)
				requireUnchanged()
			}
			publish("a2-final-member", "op-b", 22, "", "")
			period = waitA2PortfolioComplete(t, rt, setup.RunID, period.Entity.FlowInstance)
			closed := requireA2PortfolioJoin(t, rt, period, 2, true, []any{int64(11), int64(22)})
			if !closed.JoinRef().Equal(initialArm.JoinRef()) || len(workers) != 5 ||
				len(requireA2PortfolioEvents(t, rt, setup.RunID, "ingress/operating.report.requested")) != 5 ||
				len(requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported")) != 5 ||
				len(requireA2PortfolioEvents(t, rt, setup.RunID, "platform.join_complete")) != 1 {
				t.Fatal("valid final member did not complete the original arm exactly once")
			}
			completion := requireA2PortfolioEvents(t, rt, setup.RunID, "platform.join_complete")[0]
			requireA2PortfolioDelivery(t, a2ReadJoinPublicEvent(t, rt, completion.EventID), "portfolio/period", "portfolio-collector", period, "existing_entity")
			requireSettled(completion.Deliveries[0].DeliveryID)
			if code := process.stop(); code != 0 {
				t.Fatalf("admission successor exit=%d", code)
			}
		})
	}
}
