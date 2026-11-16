package cataloge2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestEntitylessFanOutExecutesThroughDurableEventBusBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := entitylessScatterGatherFixture(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			items := []any{
				map[string]any{"item_id": "alpha", "value": "red"},
				map[string]any{"item_id": "beta", "value": "green"},
				map[string]any{"item_id": "gamma", "value": "blue"},
			}
			step := catalogTriggerStep{
				Event: "batch.submitted", Payload: map[string]any{"batch_id": "entityless-batch", "items": items},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e",
				inputKind: catalogReplayInputRootIngress,
			}
			if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
				t.Fatalf("publish entityless fan-out source: %v", err)
			}
			scatterGatherWait(t, h, step, time.Time{}, time.Time{})

			public, err := scatterGatherPublicEvents(h, ctx)
			if err != nil {
				t.Fatal(err)
			}
			source, ok := public[step.eventID]
			if !ok || source.EventName != "batch.submitted" || len(source.Deliveries) != 1 || source.Deliveries[0].Status != "delivered" || !source.Deliveries[0].Route.Target.MaterializingEntity() {
				t.Fatalf("source event/delivery readback: %+v found=%t", source, ok)
			}
			owner := source.Deliveries[0].Route.Target.Route()
			if owner.FlowID != "." || owner.FlowInstance != catalogRuntimeRunID || owner.EntityID != flowidentity.EntityID(catalogRuntimeRunID) {
				t.Fatalf("source delivery lost its exact constructed root: %+v", owner)
			}
			children := map[string]bool{}
			for _, event := range public {
				if event.SourceEventID != step.eventID {
					continue
				}
				key, ok := event.Payload["item_id"].(string)
				if !ok || event.EventName != "item.registered" || children[key] || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || !event.Deliveries[0].Route.Target.MaterializingEntity() {
					t.Fatalf("invalid committed child event/delivery: %+v", event)
				}
				children[key] = true
				worker := scatterGatherLoad(t, h, event.Deliveries[0].Route.Target.Route().FlowInstance, ctx)
				if worker.CurrentState != "registered" || worker.Fields["item_id"] != key {
					t.Fatalf("child %s did not execute: %+v", key, worker)
				}
			}
			if len(children) != len(items) {
				t.Fatalf("committed child executions = %v, want %d", children, len(items))
			}
			constructed, found, err := h.workflow.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: catalogRuntimeRunID, Route: flowidentity.StoredRoute(".", catalogRuntimeRunID, catalogRuntimeRunID)})
			if err != nil {
				t.Fatal(err)
			}
			if !found || constructed.EntityType != "" || len(constructed.Fields) != 0 {
				t.Fatalf("fieldless source must have only its canonical lifecycle header: found=%t instance=%+v", found, constructed)
			}
			if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
				t.Fatalf("duplicate source publication: %v", err)
			}
			if got := scatterGatherCounts(t, h, ctx); got["domain_events"] != len(items)+1 || got["entity_state"] != len(items) {
				t.Fatalf("duplicate changed committed facts: %v", got)
			}
			t.Run("fieldless_agent_three_outcomes", func(t *testing.T) {
				proveFieldlessAgentJoin(t, backend)
			})
		})
	}
}

func proveFieldlessAgentJoin(t *testing.T, backend catalogRuntimeBackend) {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":   "stages:\n  awaiting: {}\n  ready: {final: true}\n  failed: {final: true}\ninputs: [work.one, work.two, work.three]\n",
		"events.yaml":   "work.one:\nwork.two:\nwork.three:\nitem.completed:\n  member_id: text\n  value: text\n",
		"agents.yaml":   "reporter:\n  model: regular\n  intent: {inline: Report the admitted item.}\n  subscriptions: [work.one, work.two, work.three]\n  emit_events: [item.completed]\n",
		"nodes.yaml": `collector:
  item.completed:
    join:
      stage: awaiting
      members: {count: 3, by: payload.member_id}
      output: payload.value
      deadline: {after: 1h, from: stage_entry}
      on_complete: {advances_to: ready}
      on_deadline: {advances_to: failed}
`,
		"tests/fixtures.yaml": `agent_fixtures:
  reporter:
    - on: work.one
      emits: [{event: item.completed, payload: {member_id: a, value: first}}]
    - on: work.two
      emits: [{event: item.completed, payload: {member_id: b, value: second}}]
    - on: work.three
      emits: [{event: item.completed, payload: {member_id: c, value: third}}]
`,
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	h := newRuntimeHarnessForBackend(t, root, backend, true)
	ctx := catalogRunContext(h, catalogRuntimeRunID)
	for i, event := range []string{"work.one", "work.two", "work.three"} {
		if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: event, Payload: map[string]any{}}, 20*time.Second, true); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			instance, found, err := h.workflow.Load(ctx, catalogRootWorkflowRoute())
			if err != nil || !found || len(instance.Fields) != 0 || instance.EntityType != "" {
				t.Fatalf("fieldless agent owner: %+v found=%t err=%v", instance, found, err)
			}
			buckets, err := joinruntime.PersistedBuckets(instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			arms, err := joinruntime.List(buckets)
			if err != nil || len(arms) != 1 {
				t.Fatalf("fieldless join arms: %+v %v", arms, err)
			}
			arm := arms[0]
			if arm.Completed() == i+1 && (i != 2 || instance.CurrentState == "ready") {
				if arm.Expected() != 3 || i < 2 && (arm.Status != joinruntime.StatusOpen || instance.CurrentState != "awaiting") || i == 2 && (arm.Status != joinruntime.StatusClosed || !arm.OutcomeFired || arm.OutcomePending) {
					t.Fatalf("fieldless settled outcome %d: owner=%+v arm=%+v", i+1, instance, arm)
				}
				break
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("agent outcome %d did not settle: owner=%+v arm=%+v", i+1, instance, arm)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	h.llm.mu.Lock()
	calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
	h.llm.mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("fieldless agent executions=%d want=3: %+v", len(calls), calls)
	}
}

func entitylessScatterGatherFixture(t *testing.T) string {
	t.Helper()
	source := filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety")
	root := t.TempDir()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(root, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if rel == "nodes.yaml" {
			const stateful = "    data_accumulation:\n      writes:\n        - {source_field: batch_id, target_field: batch_id}\n"
			if strings.Count(string(body), stateful) != 1 {
				t.Fatal("scatter fixture no longer has one source entity mutation")
			}
			body = []byte(strings.Replace(string(body), stateful, "", 1))
		}
		if rel == "state.yaml" {
			return nil
		}
		return os.WriteFile(target, body, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}
