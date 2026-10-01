package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestA2PortfolioSupportedStreamSurfaceBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInStream)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.FanInStream)
			requireA2PortfolioVerification(t, root)
			opts, start := lifecycleRestartHarness(t, backend, root)
			var selected serveRuntimePersistence
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p })
			process, rt := start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			params := map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "operating.report.triggered",
				"payload": map[string]any{"period_id": "2026-Q1", "revenue": 100}, "idempotency_key": "a2-stream-q1",
			}
			first := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if !first.NewRunCreated || first.RunID == "" || first.EventID == "" {
				t.Fatalf("root stream ingress did not create a run: %+v", first)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, first.RunID)
			q1, firstReport := requireA2PortfolioStreamChain(t, rt, first, "2026-Q1", 100)

			second := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"run_id": first.RunID, "event_name": "operating.report.triggered",
				"payload": map[string]any{"period_id": "2026-Q2", "revenue": 300}, "idempotency_key": "a2-stream-q2",
			})
			if second.NewRunCreated || second.RunID != first.RunID || second.EventID == first.EventID {
				t.Fatalf("stream continuation lost its run: first=%+v second=%+v", first, second)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, first.RunID)
			q2, secondReport := requireA2PortfolioStreamChain(t, rt, second, "2026-Q2", 300)
			if q1.Entity.EntityID == q2.Entity.EntityID || q1.Entity.FlowInstance == q2.Entity.FlowInstance {
				t.Fatalf("stream period ownership collapsed: q1=%+v q2=%+v", q1, q2)
			}
			beforeFirst := requireA2PortfolioEvents(t, rt, first.RunID, firstReport.EventName)
			beforeSecond := requireA2PortfolioEvents(t, rt, first.RunID, secondReport.EventName)
			predecessor := rt.Runtime.Options.RuntimeInstanceID
			if code := process.stop(); code != 0 {
				t.Fatalf("stream predecessor serve exit=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			process, rt = start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			if rt.Runtime.Options.RuntimeInstanceID == predecessor || !reflect.DeepEqual(q1, requireA2PortfolioEntity(t, rt, first.RunID, q1.Entity.FlowInstance)) ||
				!reflect.DeepEqual(q2, requireA2PortfolioEntity(t, rt, first.RunID, q2.Entity.FlowInstance)) {
				t.Fatal("actual same-store stream reconstruction changed period state or reused the predecessor runtime")
			}
			t.Log("actual same-store normal serve restart: both stream periods preserved through public readback")
			replay := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if replay.EventID != first.EventID || replay.RunID != first.RunID {
				t.Fatalf("same-idempotency stream publication changed identity: first=%+v replay=%+v", first, replay)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, first.RunID)
			if !reflect.DeepEqual(q1, requireA2PortfolioEntity(t, rt, first.RunID, q1.Entity.FlowInstance)) ||
				!reflect.DeepEqual(q2, requireA2PortfolioEntity(t, rt, first.RunID, q2.Entity.FlowInstance)) ||
				!reflect.DeepEqual(beforeFirst, requireA2PortfolioEvents(t, rt, first.RunID, firstReport.EventName)) ||
				!reflect.DeepEqual(beforeSecond, requireA2PortfolioEvents(t, rt, first.RunID, secondReport.EventName)) {
				t.Fatal("sibling period or same-idempotency replay changed stream state or emitted reports")
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("stream successor serve exit=%d", code)
			}
		})
	}
}

func TestA2PortfolioSupportedFiniteJoinSurfaceBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInBarrier)
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
				"bundle_hash": rt.BundleHash, "event_name": "portfolio.setup", "idempotency_key": "a2-join-q1",
				"payload": map[string]any{"portfolio_id": "portfolio-one", "period_id": "2026-Q1", "expected_operating_ids": []string{"op-a", "op-b"}},
			})
			if !setup.NewRunCreated || setup.RunID == "" || setup.EventID == "" {
				t.Fatalf("root join setup did not create a run: %+v", setup)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
			parent := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio", "portfolio_id", "portfolio-one")
			q1 := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio/period", "period_id", "2026-Q1")
			requireA2PortfolioSetupRoutes(t, rt, setup, parent, q1, "materializing_entity")
			arm := requireA2PortfolioJoin(t, rt, q1, 0, false, nil)

			created := map[string]operatorread.OperatorEntityFull{}
			publish := func(period, member string, revenue int) {
				t.Helper()
				// Completion retires the active route; retain its exact pre-arrival key.
				child := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio/period", "period_id", period)
				arrival := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"run_id": setup.RunID, "event_name": "operating.report.triggered",
					"payload":         map[string]any{"portfolio_id": "portfolio-one", "period_id": period, "operating_id": member, "revenue": revenue},
					"idempotency_key": "a2-join-" + period + "-" + member,
				})
				if arrival.RunID != setup.RunID || arrival.NewRunCreated || arrival.EventID == "" {
					t.Fatalf("root join report ingress lost its run: %+v", arrival)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
				payload := map[string]any{"portfolio_id": "portfolio-one", "period_id": period, "operating_id": member, "revenue": float64(revenue)}
				requested, operating := requireA2PortfolioOperatingCreation(t, rt, arrival, payload, "operating_instance_id")
				if _, duplicate := created[operating.Entity.EntityID]; duplicate {
					t.Fatalf("distinct root reports reused an operating instance: %+v", operating)
				}
				created[operating.Entity.EntityID] = operating
				reportPayload := map[string]any{"portfolio_id": "portfolio-one", "period_id": period, "operating_id": member, "operating_instance_id": requested.EventID, "revenue": float64(revenue)}
				reported := requireA2PortfolioEmission(t, rt, setup.RunID, operating.Entity.FlowInstance+"/operating.reported", requested.EventID, reportPayload)
				requireA2PortfolioDelivery(t, reported, "portfolio", "portfolio-router", parent, "existing_entity")
				forwarded := requireA2PortfolioEmission(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported", reported.EventID, payload)
				child = requireA2PortfolioEntity(t, rt, setup.RunID, child.Entity.FlowInstance)
				requireA2PortfolioDelivery(t, forwarded, "portfolio/period", "portfolio-collector", child, "existing_entity")
			}
			publish("2026-Q1", "op-b", 22)
			partial := requireA2PortfolioJoin(t, rt, requireA2PortfolioEntity(t, rt, setup.RunID, q1.Entity.FlowInstance), 1, false, nil)
			partialResults, err := partial.Results()
			if !partial.JoinRef().Equal(arm.JoinRef()) || err != nil || !reflect.DeepEqual(partialResults, []any{int64(22)}) || len(partial.Missing()) != 1 || partial.Missing()[0] != "op-a" {
				t.Fatalf("ordinary partial arrival lost its arm or typed output: %+v", partial)
			}

			secondSetup := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"run_id": setup.RunID, "event_name": "portfolio.setup", "idempotency_key": "a2-join-q2",
				"payload": map[string]any{"portfolio_id": "portfolio-one", "period_id": "2026-Q2", "expected_operating_ids": []string{"op-a", "op-b"}},
			})
			if secondSetup.RunID != setup.RunID || secondSetup.NewRunCreated {
				t.Fatalf("sibling setup lost its run: %+v", secondSetup)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, setup.RunID)
			q2 := requireA2PortfolioKeyedEntity(t, rt, setup.RunID, "portfolio/period", "period_id", "2026-Q2")
			requireA2PortfolioSetupRoutes(t, rt, secondSetup, parent, q2, "existing_entity")
			secondArm := requireA2PortfolioJoin(t, rt, q2, 0, false, nil)
			if secondArm.JoinRef().Equal(arm.JoinRef()) || q2.Entity.EntityID == q1.Entity.EntityID {
				t.Fatal("sibling periods share a join arm or entity")
			}
			publish("2026-Q2", "op-a", 111)
			stillPartial := requireA2PortfolioJoin(t, rt, requireA2PortfolioEntity(t, rt, setup.RunID, q1.Entity.FlowInstance), 1, false, nil)
			if !reflect.DeepEqual(partial, stillPartial) {
				t.Fatal("same leaf business ID in another period changed the first join")
			}
			q1Partial := requireA2PortfolioEntity(t, rt, setup.RunID, q1.Entity.FlowInstance)
			q2Partial := requireA2PortfolioEntity(t, rt, setup.RunID, q2.Entity.FlowInstance)
			secondPartial := requireA2PortfolioJoin(t, rt, q2Partial, 1, false, nil)
			beforeReports := requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported")
			predecessor := rt.Runtime.Options.RuntimeInstanceID
			if code := process.stop(); code != 0 {
				t.Fatalf("finite join predecessor serve exit=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			process, rt = start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			if rt.Runtime.Options.RuntimeInstanceID == predecessor || !reflect.DeepEqual(q1Partial, requireA2PortfolioEntity(t, rt, setup.RunID, q1.Entity.FlowInstance)) ||
				!reflect.DeepEqual(q2Partial, requireA2PortfolioEntity(t, rt, setup.RunID, q2.Entity.FlowInstance)) ||
				!reflect.DeepEqual(beforeReports, requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported")) {
				t.Fatal("actual same-store finite join reconstruction changed partial periods, reports, or runtime identity")
			}
			for _, operating := range created {
				if !reflect.DeepEqual(operating, requireA2PortfolioEntity(t, rt, setup.RunID, operating.Entity.FlowInstance)) {
					t.Fatal("restart changed an actually created operating instance")
				}
			}
			if !reflect.DeepEqual(partial, requireA2PortfolioJoin(t, rt, q1Partial, 1, false, nil)) ||
				!reflect.DeepEqual(secondPartial, requireA2PortfolioJoin(t, rt, q2Partial, 1, false, nil)) {
				t.Fatal("restart replaced a retained period arm or its typed outputs")
			}
			t.Log("actual same-store normal serve restart: both partial joins and operating instances preserved through public readback")
			publish("2026-Q1", "op-a", 11)
			q1 = waitA2PortfolioComplete(t, rt, setup.RunID, q1.Entity.FlowInstance)
			closed := requireA2PortfolioJoin(t, rt, q1, 2, true, []any{int64(11), int64(22)})
			if !closed.JoinRef().Equal(arm.JoinRef()) {
				t.Fatal("completion replaced the first period's arm")
			}
			requireA2PortfolioJoin(t, rt, requireA2PortfolioEntity(t, rt, setup.RunID, q2.Entity.FlowInstance), 1, false, nil)
			publish("2026-Q2", "op-b", 222)
			q2 = waitA2PortfolioComplete(t, rt, setup.RunID, q2.Entity.FlowInstance)
			secondClosed := requireA2PortfolioJoin(t, rt, q2, 2, true, []any{int64(111), int64(222)})
			if !secondClosed.JoinRef().Equal(secondArm.JoinRef()) || !reflect.DeepEqual(q1, requireA2PortfolioEntity(t, rt, setup.RunID, q1.Entity.FlowInstance)) {
				t.Fatal("second completion replaced its arm or changed the completed sibling")
			}
			if arrivals := requireA2PortfolioEvents(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.reported"); len(arrivals) != 4 {
				t.Fatalf("ordinary nested join arrival count=%d, want 4", len(arrivals))
			}
			if len(created) != 4 {
				t.Fatalf("ordinary operating creation count=%d, want 4 independent instances", len(created))
			}
			t.Log("root-triggered operating creations=4; complete typed ordered results: 2026-Q1=[int64(11), int64(22)], 2026-Q2=[int64(111), int64(222)]")
			if code := process.stop(); code != 0 {
				t.Fatalf("finite join successor serve exit=%d", code)
			}
		})
	}
}

func a2PortfolioReadbackRuntime(t *testing.T, process *serveRuntimeTestProcess, rt servedControlProofRuntime, selected serveRuntimePersistence) servedControlProofRuntime {
	t.Helper()
	rt.DB, rt.Postgres, rt.SQLite = selectedRuntimeStoreForTest(t, selected)
	process.mu.Lock()
	rt.Runtime = process.runtime
	process.mu.Unlock()
	if rt.Runtime == nil {
		t.Fatal("normal serve restart harness did not retain the actual runtime")
	}
	return rt
}

func requireA2PortfolioVerification(t *testing.T, root string) {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, root)
	if findings := bootverify.Run(t.Context(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("canonical portfolio hard invalidities: %#v", findings)
	}
}

func requireA2PortfolioKeyedEntity(t *testing.T, rt servedControlProofRuntime, runID, flow, field, key string) operatorread.OperatorEntityFull {
	t.Helper()
	ctx := servedControlProofAuthorActivityContext(t, rt)
	var descriptors []bus.ActiveFlowInstanceDescriptor
	var err error
	if rt.Backend == "postgres" {
		descriptors, err = rt.Postgres.ListActiveFlowInstanceDescriptorsForKey(ctx, runID, flow, "entity."+field, key)
	} else {
		descriptors, err = rt.SQLite.ListActiveFlowInstanceDescriptorsForKey(ctx, runID, flow, "entity."+field, key)
	}
	if err != nil || len(descriptors) != 1 {
		t.Fatalf("exact ordinary instance key %s/%s=%s: descriptors=%+v err=%v", flow, field, key, descriptors, err)
	}
	descriptor := descriptors[0]
	if descriptor.RunID != runID || descriptor.FlowTemplate != flow || descriptor.AddressFields["entity."+field] != key {
		t.Fatalf("ordinary routing descriptor lost its exact key: %+v", descriptor)
	}
	full := requireA2PortfolioEntity(t, rt, runID, descriptor.FlowInstance)
	if full.Entity.EntityID != descriptor.EntityID || full.Fields[field] != key {
		t.Fatalf("public keyed entity disagrees with ordinary route: entity=%+v descriptor=%+v", full, descriptor)
	}
	return full
}

func requireA2PortfolioEntity(t *testing.T, rt servedControlProofRuntime, runID, path string) operatorread.OperatorEntityFull {
	t.Helper()
	var list operatorread.OperatorEntityListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.list", map[string]any{"run_id": runID, "limit": 100}, &list)
	var matches []operatorread.OperatorEntitySummary
	for _, entity := range list.Entities {
		if entity.FlowInstance == path {
			matches = append(matches, entity)
		}
	}
	if list.NextCursor != "" || len(matches) != 1 || matches[0].RunID != runID || matches[0].EntityID != flowidentity.EntityID(path) {
		t.Fatalf("exact public portfolio entity %s: matches=%+v list=%+v", path, matches, list)
	}
	var full operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": matches[0].EntityID}, &full)
	if !reflect.DeepEqual(matches[0], full.Entity) {
		t.Fatalf("portfolio entity list/get disagree: summary=%+v full=%+v", matches[0], full)
	}
	return full
}

func requireA2PortfolioEvents(t *testing.T, rt servedControlProofRuntime, runID, name string) []operatorread.OperatorEventFull {
	t.Helper()
	var list operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": name}, "limit": 100}, &list)
	if list.NextCursor != "" {
		t.Fatalf("small portfolio event proof unexpectedly paginated: %+v", list)
	}
	for _, event := range list.Events {
		if event.RunID != runID || event.EventName != name || event.EventID == "" {
			t.Fatalf("portfolio event list lost exact identity: %+v", event)
		}
	}
	return list.Events
}

func requireA2PortfolioEvent(t *testing.T, rt servedControlProofRuntime, eventID, runID, name string, payload map[string]any) operatorread.OperatorEventFull {
	t.Helper()
	var event operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
	if event.EventID != eventID || event.RunID != runID || event.EventName != name || !reflect.DeepEqual(event.Payload, payload) ||
		len(event.DeadLetters) != 0 || event.NoDelivery != nil || len(event.Deliveries) != 1 {
		t.Fatalf("exact public portfolio event: %+v, want %s payload=%#v", event, name, payload)
	}
	return event
}

func requireA2PortfolioEmission(t *testing.T, rt servedControlProofRuntime, runID, name, sourceID string, payload map[string]any) operatorread.OperatorEventFull {
	t.Helper()
	var matches []operatorread.OperatorEventFull
	for _, event := range requireA2PortfolioEvents(t, rt, runID, name) {
		if event.SourceEventID == sourceID {
			matches = append(matches, event)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("ordinary emission %s from %s: matches=%+v", name, sourceID, matches)
	}
	full := requireA2PortfolioEvent(t, rt, matches[0].EventID, runID, name, payload)
	if !reflect.DeepEqual(matches[0], full) {
		t.Fatalf("portfolio event list/get disagree: list=%+v full=%+v", matches[0], full)
	}
	return full
}

func requireA2PortfolioDelivery(t *testing.T, event operatorread.OperatorEventFull, flow, node string, entity operatorread.OperatorEntityFull, kind string) {
	t.Helper()
	if len(event.Deliveries) != 1 {
		t.Fatalf("portfolio event deliveries: %+v", event)
	}
	delivery := event.Deliveries[0]
	want := operatorread.OperatorDeliveryTarget{Kind: kind, FlowID: flow, FlowInstance: entity.Entity.FlowInstance, EntityID: entity.Entity.EntityID}
	if delivery.SubscriberType != "node" || delivery.SubscriberID != identitytest.FlowNode(t, flow, node).Key() || delivery.Status != "delivered" ||
		!delivery.Terminal || delivery.RetryCount != 0 || delivery.Failure != nil || len(delivery.DeadLetters) != 0 || delivery.Target != want {
		t.Fatalf("ordinary portfolio route/settlement: %+v, want target=%+v node=%s/%s", delivery, want, flow, node)
	}
}

func requireA2PortfolioOperatingCreation(t *testing.T, rt servedControlProofRuntime, trigger servedEventPublishRPCResult, payload map[string]any, instanceField string) (operatorread.OperatorEventFull, operatorread.OperatorEntityFull) {
	t.Helper()
	input := requireA2PortfolioEvent(t, rt, trigger.EventID, trigger.RunID, "operating.report.triggered", payload)
	// Stateless ingress has delivery ownership, but no entity-state row.
	ingress := operatorread.OperatorEntityFull{Entity: operatorread.OperatorEntitySummary{FlowInstance: "ingress"}}
	requireA2PortfolioDelivery(t, input, "ingress", "ingress-node", ingress, "entityless_receiver")
	requested := requireA2PortfolioEmission(t, rt, trigger.RunID, "ingress/operating.report.requested", trigger.EventID, payload)
	operating := requireA2PortfolioKeyedEntity(t, rt, trigger.RunID, "operating", instanceField, requested.EventID)
	requireA2PortfolioDelivery(t, requested, "operating", "operating-node", operating, "materializing_entity")
	var projectionRaw []byte
	if err := rt.DB.QueryRow(`SELECT delivery_payload_projection FROM event_deliveries WHERE delivery_id=$1`, requested.Deliveries[0].DeliveryID).Scan(&projectionRaw); err != nil {
		t.Fatal(err)
	}
	var projection events.DeliveryPayloadProjection
	if err := json.Unmarshal(projectionRaw, &projection); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projection.Fields(), map[string]string{instanceField: requested.EventID}) {
		t.Fatalf("declared event.id delivery projection: %s", projectionRaw)
	}
	return requested, operating
}

func requireA2PortfolioStreamChain(t *testing.T, rt servedControlProofRuntime, trigger servedEventPublishRPCResult, period string, revenue int) (operatorread.OperatorEntityFull, operatorread.OperatorEventFull) {
	t.Helper()
	payload := map[string]any{"period_id": period, "revenue": float64(revenue)}
	requested, operating := requireA2PortfolioOperatingCreation(t, rt, trigger, payload, "operating_id")
	// The source payload has no operating_id; the declared event.id create
	// projection must become the exact business ID in the emitted report.
	payload["operating_id"] = requested.EventID
	reported := requireA2PortfolioEmission(t, rt, trigger.RunID, operating.Entity.FlowInstance+"/operating.reported", requested.EventID, payload)
	portfolio := requireA2PortfolioKeyedEntity(t, rt, trigger.RunID, "portfolio", "period_id", period)
	requireA2PortfolioDelivery(t, reported, "portfolio", "portfolio-collector", portfolio, "materializing_entity")
	wantFields := map[string]any{"period_id": period, "last_revenue": float64(revenue), "reports": map[string]any{requested.EventID: payload}}
	if !reflect.DeepEqual(portfolio.Fields, wantFields) {
		t.Fatalf("exact period-keyed stream fields: %#v, want %#v", portfolio.Fields, wantFields)
	}
	node := identitytest.FlowNode(t, "portfolio", "portfolio-collector")
	carrier, err := engine.StateCarrierFromPersisted(portfolio.Fields, portfolio.Bookkeeping, portfolio.Gates, portfolio.Accumulated)
	if err != nil {
		t.Fatal(err)
	}
	buckets, _ := carrier.StateBuckets[node.Key()]["handler_accumulators"].(map[string]any)
	key := timeridentity.NewAccumulatorBucketRef(node, "operating.reported").Key()
	raw, ok := buckets[key].(map[string]any)
	if !ok || len(buckets) != 1 {
		t.Fatalf("exact public stream accumulator identity: %#v, want %s", buckets, key)
	}
	state := accumulator.Load(raw)
	if state.Err() != nil || len(state.Received) != 1 || state.Received[requested.EventID] == "" || len(state.Deliveries) != 0 || !reflect.DeepEqual(state.Items, []map[string]any{payload}) {
		t.Fatalf("ordinary public stream accumulation: %+v err=%v", state, state.Err())
	}
	return portfolio, reported
}

func requireA2PortfolioSetupRoutes(t *testing.T, rt servedControlProofRuntime, setup servedEventPublishRPCResult, parent, period operatorread.OperatorEntityFull, parentKind string) {
	t.Helper()
	fields := map[string]any{"portfolio_id": "portfolio-one", "period_id": period.Fields["period_id"], "expected_operating_ids": []any{"op-a", "op-b"}}
	input := requireA2PortfolioEvent(t, rt, setup.EventID, setup.RunID, "portfolio.setup", fields)
	requireA2PortfolioDelivery(t, input, "portfolio", "portfolio-router", parent, parentKind)
	forwarded := requireA2PortfolioEmission(t, rt, setup.RunID, parent.Entity.FlowInstance+"/period.setup", setup.EventID, fields)
	requireA2PortfolioDelivery(t, forwarded, "portfolio/period", "portfolio-collector", period, "materializing_entity")
	if !reflect.DeepEqual(period.Fields, fields) {
		t.Fatalf("public join membership snapshot fields: %#v, want %#v", period.Fields, fields)
	}
}

func requireA2PortfolioJoin(t *testing.T, rt servedControlProofRuntime, period operatorread.OperatorEntityFull, completed int, closed bool, results []any) joinruntime.Activation {
	t.Helper()
	// Retain persisted integer tokens for typed results, and separately bind
	// that evidence to the authenticated public accumulated-state projection.
	var raw []byte
	if err := rt.DB.QueryRow(`SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$2`, period.Entity.RunID, period.Entity.EntityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var public, persisted map[string]any
	if err := json.Unmarshal(raw, &public); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(public, period.Accumulated) {
		t.Fatalf("public period join differs from persisted state: public=%#v persisted=%s", period.Accumulated, raw)
	}
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(period.Fields, period.Bookkeeping, period.Gates, persisted)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil || len(joins) != 1 {
		t.Fatalf("public period join activation: joins=%+v err=%v", joins, err)
	}
	join := joins[0]
	declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "portfolio/period", "portfolio-collector"), "period.reported", "awaiting", "awaiting")
	if err != nil {
		t.Fatal(err)
	}
	ref := join.JoinRef()
	if err := ref.StageEntry().RequireOwner(period.Entity.RunID, "portfolio/period", flowidentity.LogicalInstanceID(period.Entity.FlowInstance), period.Entity.FlowInstance, period.Entity.EntityID, "awaiting"); err != nil {
		t.Fatalf("public join arm lost its exact period owner: %v", err)
	}
	if !ref.Declaration().Equal(declaration) || !reflect.DeepEqual(join.Members, []string{"op-a", "op-b"}) || join.Expected() != 2 || join.Completed() != completed || join.DeadlineAt.Sub(join.ArmedAt) != 5*time.Minute {
		t.Fatalf("ordinary join identity/membership/progress/deadline: %+v", join)
	}
	if closed {
		actual, err := join.Results()
		if period.Entity.CurrentState != "complete" || join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete ||
			!join.TimerCancelled || join.OutcomePending || !join.OutcomeFired || err != nil || !reflect.DeepEqual(actual, results) {
			t.Fatalf("real join continuation and ordered typed results: state=%s join=%+v results=%#v want=%#v err=%v", period.Entity.CurrentState, join, actual, results, err)
		}
	} else if period.Entity.CurrentState != "awaiting" || join.Status != joinruntime.StatusOpen || join.CloseReason != "" || join.TimerCancelled || join.OutcomePending || join.OutcomeFired {
		t.Fatalf("ordinary incomplete period: state=%s join=%+v", period.Entity.CurrentState, join)
	}
	return join
}

func waitA2PortfolioComplete(t *testing.T, rt servedControlProofRuntime, runID, path string) operatorread.OperatorEntityFull {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var period operatorread.OperatorEntityFull
	for time.Now().Before(deadline) {
		period = requireA2PortfolioEntity(t, rt, runID, path)
		if period.Entity.CurrentState == "complete" {
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
			return requireA2PortfolioEntity(t, rt, runID, path)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("real join continuation did not complete %s: %+v\n%s", path, period, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return period
}
