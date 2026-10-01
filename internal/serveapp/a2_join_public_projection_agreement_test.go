package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type a2JoinPublicDiagnosis struct {
	Run              operatorread.RunHeader                 `json:"run"`
	OperationalState string                                 `json:"operational_state"`
	BlockingLayer    string                                 `json:"blocking_layer"`
	BlockingReason   string                                 `json:"blocking_reason"`
	FailedDeliveries []operatorread.RunDebugFailureDelivery `json:"failed_deliveries"`
	TestQuiescence   operatorread.RunTestQuiescence         `json:"test_quiescence"`
}

func TestA2JoinPublicProjectionAgreementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyServedJoinProof(t)
			requireA2PortfolioVerification(t, root)
			opts, start := lifecycleRestartHarness(t, backend, root)
			node := identitytest.RootNode(t, "join-node")
			entered := make(chan events.Event, 1)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			// The existing claimed-handler hook holds only the real continuation,
			// after the last arrival has committed its closed-pending obligation.
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, nodeID string, event events.Event) error {
				if nodeID != node.Key() || event.Type() != "platform.join_complete" {
					return nil
				}
				select {
				case entered <- event.Clone():
				default:
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			process, rt := start()
			opened := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "order.started", "idempotency_key": "a2-m37-start",
				"payload": map[string]any{"dispatch_id": "dispatch-m37", "expected": []string{"op-a", "op-b"}},
			})
			if !opened.NewRunCreated || opened.RunID == "" || opened.EventID == "" {
				t.Fatalf("supported root ingress did not create the join run: %+v", opened)
			}
			a2WaitJoinPublicRun(t, rt, opened.RunID, "running", 0, true)
			created := requireA2PortfolioEntity(t, rt, opened.RunID, opened.RunID)
			if created.Entity.CurrentState != "dispatching" {
				t.Fatalf("actual root creation did not enter dispatching: %+v", created)
			}
			requireA2PortfolioDelivery(t, requireA2PortfolioEvent(t, rt, opened.EventID, opened.RunID, "order.started",
				map[string]any{"dispatch_id": "dispatch-m37", "expected": []any{"op-a", "op-b"}}), ".", "starter", created, "materializing_entity")
			dispatched := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"run_id": opened.RunID, "event_name": "order.dispatched", "source_event_id": opened.EventID,
				"payload": map[string]any{}, "idempotency_key": "a2-m37-dispatch",
			})
			a2WaitJoinPublicRun(t, rt, opened.RunID, "running", 0, true)
			initialEntity, initial := a2ReadJoinPublicObligation(t, rt, opened.RunID)
			entry, found, err := workflowlifecycle.LoadStageEntry(initialEntity.Bookkeeping)
			if err != nil || !found || entry != initial.JoinRef().StageEntry() || entry.EventID != dispatched.EventID || entry.Cause != "delivery" {
				t.Fatalf("public bookkeeping/arm lost the actual admitted entry: entry=%+v arm=%+v err=%v", entry, initial, err)
			}
			if initialEntity.Entity.CurrentState != "awaiting" || initial.Status != joinruntime.StatusOpen || initial.Completed() != 0 ||
				initial.Expected() != 2 || initial.OutcomePending || initial.OutcomeFired || initial.DeadlineAt.Sub(initial.ArmedAt) != time.Hour {
				t.Fatalf("public initial arrival obligation: entity=%+v arm=%+v", initialEntity, initial)
			}
			requireA2PortfolioDelivery(t, requireA2PortfolioEvent(t, rt, dispatched.EventID, opened.RunID, "order.dispatched", map[string]any{}),
				".", "dispatcher", initialEntity, "existing_entity")
			a2RequireJoinPublicGraph(t, rt, root, initialEntity, initial)
			publish := func(member, idempotency string, ok bool) servedEventPublishRPCResult {
				t.Helper()
				result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"run_id": opened.RunID, "event_name": "item.completed", "source_event_id": dispatched.EventID,
					"payload":         map[string]any{"dispatch_id": "dispatch-m37", "member_id": member, "result": map[string]any{"ok": ok}},
					"idempotency_key": idempotency,
				})
				if result.NewRunCreated || result.RunID != opened.RunID || result.EventID == "" {
					t.Fatalf("supported root arrival lost its exact run: %+v", result)
				}
				return result
			}
			first := publish("op-b", "a2-m37-b", false)
			a2WaitJoinPublicRun(t, rt, opened.RunID, "running", 0, true)
			partialEntity, partial := a2ReadJoinPublicObligation(t, rt, opened.RunID)
			a2RequireJoinPublicIdentity(t, initial, partial)
			if partial.Status != joinruntime.StatusOpen || partial.Completed() != 1 || !reflect.DeepEqual(partial.Missing(), []string{"op-a"}) {
				t.Fatalf("public partial arrival obligation: %+v", partial)
			}
			a2RequireJoinPublicResults(t, partial, []any{map[string]any{"ok": false}})
			requireA2PortfolioDelivery(t, requireA2PortfolioEvent(t, rt, first.EventID, opened.RunID, "item.completed",
				map[string]any{"dispatch_id": "dispatch-m37", "member_id": "op-b", "result": map[string]any{"ok": false}}), ".", "join-node", partialEntity, "existing_entity")
			last := publish("op-a", "a2-m37-a", true)
			var occurrence events.Event
			select {
			case occurrence = <-entered:
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatal("real platform.join_complete did not reach the existing handler-start hook")
			}
			pendingDiagnosis := a2WaitJoinPublicRun(t, rt, opened.RunID, "running", 1, false)
			pendingEntity, pending := a2ReadJoinPublicObligation(t, rt, opened.RunID)
			a2RequireJoinPublicIdentity(t, initial, pending)
			if pendingEntity.Entity.CurrentState != "awaiting" || pending.Status != joinruntime.StatusClosed || pending.CloseReason != joinruntime.CloseReasonComplete ||
				pending.Completed() != 2 || pending.TimerCancelled || !pending.OutcomePending || pending.OutcomeFired || pending.TimerHandle().Kind() != timeridentity.TimerHandleJoinComplete {
				t.Fatalf("public closed-pending obligation ran inline or lost closure: entity=%+v arm=%+v", pendingEntity, pending)
			}
			ordered := []any{map[string]any{"ok": true}, map[string]any{"ok": false}}
			a2RequireJoinPublicResults(t, pending, ordered)
			requireA2PortfolioDelivery(t, requireA2PortfolioEvent(t, rt, last.EventID, opened.RunID, "item.completed",
				map[string]any{"dispatch_id": "dispatch-m37", "member_id": "op-a", "result": map[string]any{"ok": true}}), ".", "join-node", pendingEntity, "existing_entity")
			completion := a2ReadJoinPublicEvent(t, rt, occurrence.ID())
			handle, ref, valid := timeridentity.ParseJoinHandle(completion.Payload)
			if !valid || !ref.Equal(initial.JoinRef()) || handle != pending.TimerHandle() || occurrence.TaskID() != handle.TaskID() ||
				completion.RunID != opened.RunID || completion.EventName != "platform.join_complete" || len(completion.Deliveries) != 1 ||
				completion.Deliveries[0].Status != "in_progress" || completion.Deliveries[0].Terminal || completion.Deliveries[0].Target.EntityID != pendingEntity.Entity.EntityID {
				t.Fatalf("public continuation event disagrees with exact pending obligation: event=%+v handle=%+v arm=%+v", completion, handle, pending)
			}
			a2RequireJoinPublicClients(t, rt, pendingDiagnosis, pendingEntity)
			a2RequireJoinPublicTrace(t, rt, opened.RunID, completion)

			late := publish("op-a", "a2-m37-late", false)
			lateDiagnosis := a2WaitJoinPublicRun(t, rt, opened.RunID, "running", 1, false, late.EventID)
			lateEvent := a2ReadJoinPublicEvent(t, rt, late.EventID)
			a2RequireJoinPublicLateFailure(t, lateEvent, pendingEntity, initial.JoinRef())
			unchangedEntity, unchanged := a2ReadJoinPublicObligation(t, rt, opened.RunID)
			if !reflect.DeepEqual(pendingEntity, unchangedEntity) || !reflect.DeepEqual(pending, unchanged) {
				t.Fatal("typed late refusal changed the retained closed-pending obligation or entry")
			}
			a2RequireJoinPublicFailureAgreement(t, lateDiagnosis, lateEvent)
			a2RequireJoinPublicClients(t, rt, lateDiagnosis, unchangedEntity)
			a2RequireJoinPublicTrace(t, rt, opened.RunID, lateEvent)
			human := a2JoinPublicCLI(t, rt, "event", "view", late.EventID)
			for _, want := range []string{"subscriber=node/" + node.Key(), "status=dead letter", "failure=platform.stale_arrival/join_closed"} {
				if !strings.Contains(human, want) {
					t.Fatalf("CLI late refusal omitted %q: %s", want, human)
				}
			}
			unblock()
			// Join firing is not a promise that the whole run has terminated.
			// Assert active-work agreement and compare the actual header/diagnosis.
			firedDiagnosis := a2WaitJoinPublicRun(t, rt, opened.RunID, "", 0, true, late.EventID)
			if firedDiagnosis.Run.Status != "running" && firedDiagnosis.Run.Status != "completed" || firedDiagnosis.Run.Failure != nil {
				t.Fatalf("typed late refusal incorrectly failed the containing run: %+v", firedDiagnosis)
			}
			firedEntity, fired := a2ReadJoinPublicObligation(t, rt, opened.RunID)
			a2RequireJoinPublicIdentity(t, initial, fired)
			if firedEntity.Entity.CurrentState != "ready" || fired.Status != joinruntime.StatusClosed || fired.CloseReason != joinruntime.CloseReasonComplete ||
				fired.OutcomePending || !fired.OutcomeFired || !fired.TimerCancelled || !reflect.DeepEqual(pending.Outputs, fired.Outputs) {
				t.Fatalf("public fired obligation did not consume the real continuation: entity=%+v arm=%+v", firedEntity, fired)
			}
			a2RequireJoinPublicResults(t, fired, ordered)
			completion = a2ReadJoinPublicEvent(t, rt, occurrence.ID())
			requireA2PortfolioDelivery(t, completion, ".", "join-node", firedEntity, "existing_entity")
			firedEntry, found, err := workflowlifecycle.LoadStageEntry(firedEntity.Bookkeeping)
			if err != nil || !found || firedEntry == entry || firedEntry.EventID != completion.EventID ||
				firedEntry.RequireOwner(opened.RunID, opened.RunID, opened.RunID, opened.RunID, opened.RunID, "ready") != nil {
				t.Fatalf("public fired bookkeeping lost the actual continuation's new entry: entry=%+v retained=%+v event=%s err=%v", firedEntry, entry, completion.EventID, err)
			}
			a2RequireJoinPublicFailureAgreement(t, firedDiagnosis, a2ReadJoinPublicEvent(t, rt, late.EventID))
			a2RequireJoinPublicClients(t, rt, firedDiagnosis, firedEntity)
			a2RequireJoinPublicTrace(t, rt, opened.RunID, completion)
			a2RequireJoinPublicGraph(t, rt, root, firedEntity, fired)
			t.Logf("public HTTP/CLI describe/graph/readback agreement: closed_pending(active=1, run=running) -> typed platform.stale_arrival/join_closed -> fired(active=0, run=%s, operational=%s); entry=%s arm=%s; non-loop generation absent; ordered JoinResult outputs=[{ok:true}, {ok:false}]; no restart or SQL oracle", firedDiagnosis.Run.Status, firedDiagnosis.OperationalState, entry.Key(), fired.Key())
			if code := process.stop(); code != 0 {
				t.Fatalf("normal serve exit=%d", code)
			}
		})
	}
}

func a2ReadJoinPublicObligation(t *testing.T, rt servedControlProofRuntime, runID string) (operatorread.OperatorEntityFull, joinruntime.Activation) {
	t.Helper()
	var raw json.RawMessage
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &raw)
	var entity operatorread.OperatorEntityFull
	// Decode the public DTO itself, preserving numeric tokens without a SQL oracle.
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &entity); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(entity.Fields, entity.Bookkeeping, entity.Gates, entity.Accumulated)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil || len(joins) != 1 {
		t.Fatalf("public canonical arrival-obligation catalog: joins=%+v err=%v", joins, err)
	}
	arm := joins[0]
	ref := arm.JoinRef()
	declaration, err := timeridentity.NewJoinRef(identitytest.RootNode(t, "join-node"), "item.completed", "awaiting", "awaiting")
	if err != nil {
		t.Fatal(err)
	}
	if err := ref.StageEntry().RequireOwner(runID, runID, runID, runID, runID, "awaiting"); err != nil {
		t.Fatalf("public obligation lost its exact lifecycle owner: entry=%+v err=%v", ref.StageEntry(), err)
	}
	if entity.Entity.RunID != runID || entity.Entity.EntityID != runID || entity.Entity.FlowInstance != runID || !ref.Declaration().Equal(declaration) ||
		arm.Generation() != (attemptgeneration.Generation{}) || ref.Generation() != arm.Generation() || !reflect.DeepEqual(arm.Members, []string{"op-a", "op-b"}) {
		t.Fatalf("public obligation lost exact run/instance/declaration/entry or fabricated generation: entity=%+v arm=%+v", entity, arm)
	}
	loaded, found, err := joinruntime.Load(carrier.StateBuckets, ref.Node(), arm.Key())
	if err != nil || !found || !reflect.DeepEqual(loaded, arm) || arm.Key() != joinruntime.ActivationKey(ref) {
		t.Fatalf("public catalog and exact node/activation-key readers disagree: arm=%+v loaded=%+v err=%v", arm, loaded, err)
	}
	var list operatorread.OperatorEntityListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.list", map[string]any{"run_id": runID, "limit": 10}, &list)
	if list.NextCursor != "" || len(list.Entities) != 1 || !reflect.DeepEqual(list.Entities[0], entity.Entity) {
		t.Fatalf("public entity list/get disagree on the exact join receiver: list=%+v entity=%+v", list, entity)
	}
	return entity, arm
}

func a2RequireJoinPublicIdentity(t *testing.T, original, current joinruntime.Activation) {
	t.Helper()
	if !original.JoinRef().Equal(current.JoinRef()) || original.Key() != current.Key() || original.Generation() != current.Generation() ||
		original.ArmedAt != current.ArmedAt || original.DeadlineAt != current.DeadlineAt || !reflect.DeepEqual(original.Members, current.Members) {
		t.Fatalf("public obligation replaced its captured generation/entry/key/member/deadline identity: original=%+v current=%+v", original, current)
	}
}

func a2RequireJoinPublicResults(t *testing.T, arm joinruntime.Activation, want []any) {
	t.Helper()
	results, err := arm.Results()
	if err != nil || !reflect.DeepEqual(results, want) {
		t.Fatalf("public catalog-typed membership-ordered results=%#v want=%#v err=%v", results, want, err)
	}
}

func a2RequireJoinPublicGraph(t *testing.T, rt servedControlProofRuntime, root string, entity operatorread.OperatorEntityFull, arm joinruntime.Activation) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := executeCLIFrom(context.Background(), repoRootForTest(), []string{
		"describe", root, "--graph", "--json", "--config", writeTestVerifyRuntimeConfig(t),
	}, &stdout, &stderr, nil)
	if code != 0 || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("actual CLI describe/graph: exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.SourceHash != rt.BundleHash || len(view.StageGraphs) != 1 {
		t.Fatalf("actual CLI describe/graph lost its served source: hash=%s want=%s graphs=%+v", view.SourceHash, rt.BundleHash, view.StageGraphs)
	}
	for _, diagnostic := range view.Diagnostics {
		if diagnostic.Severity != "lint_evidence" {
			t.Fatalf("actual CLI describe/graph has a non-lint diagnosis: %+v", diagnostic)
		}
	}
	graph, ref := view.StageGraphs[0], arm.JoinRef()
	want := authoringview.StageGraphJoinView{
		ID: ref.JoinID(), Stage: ref.Stage(), FlowPath: ref.Node().FlowPath(), NodeID: ref.Node().NodeID(), HandlerEvent: ref.HandlerEvent(),
		MembersFrom: "state.expected", MembersBy: "payload.member_id", Output: "payload.result", DeadlineAfter: "1h", DeadlineFrom: "stage_entry",
	}
	if graph.FlowPath != ref.Node().FlowPath() || len(graph.Joins) != 1 || graph.Joins[0] != want ||
		!reflect.DeepEqual(entity.Fields["expected"], []any{"op-a", "op-b"}) || arm.DeadlineAt.Sub(arm.ArmedAt) != time.Hour {
		t.Fatalf("CLI declaration graph/public obligation disagree: graph=%+v arm=%+v fields=%+v", graph, arm, entity.Fields)
	}
	currentFound, completeFound, deadlineFound := false, false, false
	for _, stage := range graph.Nodes {
		if stage.ID == entity.Entity.CurrentState {
			currentFound = true
			if stage.Terminal != arm.OutcomeFired {
				t.Fatalf("CLI stage graph/public lifecycle disagree: stage=%+v entity=%+v arm=%+v", stage, entity.Entity, arm)
			}
		}
	}
	for _, edge := range graph.Edges {
		if edge.NodeID != ref.Node().Key() || edge.HandlerEvent != ref.HandlerEvent() {
			continue
		}
		if !reflect.DeepEqual(edge.From, []string{ref.Stage()}) {
			t.Fatalf("CLI join outcome has a foreign entry stage: %+v", edge)
		}
		switch edge.Source {
		case "handler.join.on_complete":
			completeFound = edge.To == "ready"
		case "handler.join.on_deadline":
			deadlineFound = edge.To == "attention"
		}
	}
	if !currentFound || !completeFound || !deadlineFound {
		t.Fatalf("CLI graph omitted actual stage or exact declared outcomes: %+v", graph)
	}
}

func a2ReadJoinPublicEvent(t *testing.T, rt servedControlProofRuntime, eventID string) operatorread.OperatorEventFull {
	t.Helper()
	var event operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
	if event.EventID != eventID || event.RunID == "" || event.NoDelivery != nil || len(event.Deliveries) != 1 {
		t.Fatalf("public join event lost its exact committed identity/obligation: %+v", event)
	}
	var matches []operatorread.OperatorEventFull
	for _, listed := range requireA2PortfolioEvents(t, rt, event.RunID, event.EventName) {
		if listed.EventID == eventID {
			matches = append(matches, listed)
		}
	}
	if len(matches) != 1 || !reflect.DeepEqual(matches[0], event) {
		t.Fatalf("public join event list/get disagree: event=%+v matches=%+v", event, matches)
	}
	return event
}

func a2WaitJoinPublicRun(t *testing.T, rt servedControlProofRuntime, runID, status string, active int, ready bool, failedEventID ...string) a2JoinPublicDiagnosis {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var last, previous a2JoinPublicDiagnosis
	stable := 0
	for time.Now().Before(deadline) {
		requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &last)
		failureReady := len(failedEventID) == 0 || len(last.FailedDeliveries) == 1 && last.FailedDeliveries[0].EventID == failedEventID[0]
		if failureReady && last.Run.RunID == runID && (status == "" || last.Run.Status == status) && last.TestQuiescence.ActiveDeliveries == active && last.TestQuiescence.Ready == ready {
			if reflect.DeepEqual(previous, last) {
				stable++
			} else {
				stable = 1
			}
			if stable == 4 {
				if active > 0 && (last.OperationalState != "running" || last.BlockingLayer != "" || last.BlockingReason != "") ||
					last.Run.Status == "completed" && last.OperationalState != "completed" ||
					active == 0 && last.Run.Status == "running" && (last.OperationalState != "stalled" || last.BlockingLayer != "delivery_lifecycle" || last.BlockingReason != "no_active_deliveries") {
					t.Fatalf("public run diagnosis contradicts continuation activity/terminal state: %+v", last)
				}
				if ready && last.TestQuiescence != (operatorread.RunTestQuiescence{Ready: true}) {
					t.Fatalf("public settled run retains active work: %+v", last)
				}
				return last
			}
		} else {
			stable = 0
		}
		previous = last
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("public run did not settle to status=%s active=%d ready=%v within the existing budget: %+v", status, active, ready, last)
	return last
}

func a2RequireJoinPublicClients(t *testing.T, rt servedControlProofRuntime, diagnosis a2JoinPublicDiagnosis, entity operatorread.OperatorEntityFull) {
	t.Helper()
	runID := entity.Entity.RunID
	var header struct {
		Run operatorread.RunHeader `json:"run"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": runID}, &header)
	if !reflect.DeepEqual(header.Run, diagnosis.Run) || header.Run.EntityCount != 1 || header.Run.EventCount < 4 {
		t.Fatalf("public run header/diagnosis disagree with the actual single join receiver: header=%+v diagnosis=%+v", header, diagnosis)
	}
	var events operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100}, &events)
	if events.NextCursor != "" || header.Run.EventCount != len(events.Events) {
		t.Fatalf("public run/event readers disagree on exact event count: header=%+v events=%+v", header.Run, events)
	}
	for _, event := range events.Events {
		if event.RunID != runID || event.EventID == "" {
			t.Fatalf("public run event count includes a foreign event: %+v", event)
		}
	}
	var cliHeader struct {
		Run operatorread.RunHeader `json:"run"`
	}
	if err := json.Unmarshal([]byte(a2JoinPublicCLI(t, rt, "run", "status", runID, "--no-diagnose", "--json")), &cliHeader); err != nil {
		t.Fatal(err)
	}
	var cliDiagnosis a2JoinPublicDiagnosis
	if err := json.Unmarshal([]byte(a2JoinPublicCLI(t, rt, "run", "status", runID, "--json")), &cliDiagnosis); err != nil {
		t.Fatal(err)
	}
	var cliEntity operatorread.OperatorEntityFull
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(a2JoinPublicCLI(t, rt, "entity", "view", entity.Entity.EntityID, "--run-id", runID, "--json")), &cliEntity); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cliHeader, header) || !reflect.DeepEqual(cliDiagnosis, diagnosis) || !reflect.DeepEqual(cliEntity, entity) {
		t.Fatalf("actual CLI HTTP clients disagree with run/entity/obligation readback: header=%+v diagnosis=%+v entity=%+v", cliHeader, cliDiagnosis, cliEntity)
	}
}

func a2JoinPublicCLI(t *testing.T, rt servedControlProofRuntime, args ...string) string {
	t.Helper()
	stdout, stderr, code := runServedCLICommand(t, rt.Endpoint, args)
	if code != 0 || strings.TrimSpace(stderr) != "" {
		t.Fatalf("supported CLI %v: exit=%d stderr=%s stdout=%s", args, code, stderr, stdout)
	}
	return stdout
}

func a2RequireJoinPublicLateFailure(t *testing.T, event operatorread.OperatorEventFull, entity operatorread.OperatorEntityFull, ref timeridentity.JoinRef) {
	t.Helper()
	delivery := event.Deliveries[0]
	attrs := map[string]any{"row_id": ref.JoinID(), "stage": ref.Stage(), "node_id": ref.Node().Key(), "handler_event": ref.HandlerEvent(), "stage_entry": ref.StageEntry().Key()}
	if event.RunID != entity.Entity.RunID || event.EventName != "item.completed" || delivery.SubscriberID != ref.Node().Key() || delivery.SubscriberType != "node" ||
		!reflect.DeepEqual(event.Payload, map[string]any{"dispatch_id": "dispatch-m37", "member_id": "op-a", "result": map[string]any{"ok": false}}) ||
		delivery.Target != (operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: ".", FlowInstance: entity.Entity.FlowInstance, EntityID: entity.Entity.EntityID}) ||
		delivery.Status != "dead_letter" || !delivery.Terminal || delivery.RetryCount != 0 || delivery.RetryScheduled || delivery.Failure == nil ||
		delivery.Failure.Class != failures.ClassStaleArrival || delivery.Failure.Detail.Code != "join_closed" || delivery.Failure.Retryable || !delivery.Failure.Deterministic ||
		delivery.Failure.Component != "runtime.engine" || delivery.Failure.Operation != "join" || !reflect.DeepEqual(delivery.Failure.Detail.Attributes, attrs) ||
		len(event.DeadLetters) != 1 || len(delivery.DeadLetters) != 1 || event.DeadLetters[0].DeliveryID != delivery.DeliveryID ||
		!reflect.DeepEqual(event.DeadLetters[0].Failure, *delivery.Failure) || !reflect.DeepEqual(event.DeadLetters, delivery.DeadLetters) {
		t.Fatalf("public typed late refusal lost the captured join owner/entry or settlement: event=%+v want attributes=%+v", event, attrs)
	}
}

func a2RequireJoinPublicFailureAgreement(t *testing.T, diagnosis a2JoinPublicDiagnosis, event operatorread.OperatorEventFull) {
	t.Helper()
	if len(diagnosis.FailedDeliveries) != 1 {
		t.Fatalf("public run diagnosis must retain exactly the typed late refusal: %+v", diagnosis)
	}
	failed, delivery := diagnosis.FailedDeliveries[0], event.Deliveries[0]
	if failed.EventID != event.EventID || failed.EventName != event.EventName || failed.DeliveryID != delivery.DeliveryID || failed.EntityID != delivery.Target.EntityID ||
		failed.SubscriberType != delivery.SubscriberType || failed.SubscriberID != delivery.SubscriberID || failed.Status != delivery.Status ||
		failed.Terminal != delivery.Terminal || failed.RetryCount != delivery.RetryCount || failed.RetryScheduled != delivery.RetryScheduled ||
		!reflect.DeepEqual(failed.Failure, delivery.Failure) || !reflect.DeepEqual(failed.DeadLetters, delivery.DeadLetters) {
		t.Fatalf("public run/event readers disagree on exact typed late evidence: failed=%+v delivery=%+v", failed, delivery)
	}
}

func a2RequireJoinPublicTrace(t *testing.T, rt servedControlProofRuntime, runID string, event operatorread.OperatorEventFull) {
	t.Helper()
	var page struct {
		Trace      []operatorread.RunDebugTraceRow `json:"trace"`
		NextCursor string                          `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.trace", map[string]any{"run_id": runID, "limit": 100}, &page)
	var rows []operatorread.RunDebugTraceRow
	for _, row := range page.Trace {
		if row.EventID == event.EventID && row.DeliveryID == event.Deliveries[0].DeliveryID {
			rows = append(rows, row)
		}
	}
	if page.NextCursor != "" || len(rows) != 1 || rows[0].EventName != event.EventName || rows[0].EntityID != event.Deliveries[0].Target.EntityID ||
		rows[0].SubscriberID != event.Deliveries[0].SubscriberID || rows[0].DeliveryStatus != event.Deliveries[0].Status ||
		rows[0].DeliveryTerminal != event.Deliveries[0].Terminal || !reflect.DeepEqual(rows[0].DeliveryFailure, event.Deliveries[0].Failure) {
		t.Fatalf("public composed run trace/event readers disagree: event=%+v rows=%+v cursor=%s", event, rows, page.NextCursor)
	}
}
