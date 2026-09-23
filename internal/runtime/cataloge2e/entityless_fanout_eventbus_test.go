package cataloge2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
			if !ok || source.EventName != "batch.submitted" || len(source.Deliveries) != 1 || source.Deliveries[0].Status != "delivered" || !source.Deliveries[0].Route.Target.EntitylessReceiver() {
				t.Fatalf("source event/delivery readback: %+v found=%t", source, ok)
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
			_, found, err := h.workflow.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: catalogRuntimeRunID, Route: flowidentity.RouteForInstancePath(".")})
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("entityless source materialized a root workflow entity")
			}
			if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
				t.Fatalf("duplicate source publication: %v", err)
			}
			if got := scatterGatherCounts(t, h, ctx); got["domain_events"] != len(items)+1 || got["entity_state"] != len(items) {
				t.Fatalf("duplicate changed committed facts: %v", got)
			}
		})
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
			const stateful = "      create_entity: true\n      data_accumulation:\n        writes:\n          - {source_field: batch_id, target_field: batch_id}\n"
			if strings.Count(string(body), stateful) != 1 {
				t.Fatal("scatter fixture no longer has one source entity mutation")
			}
			body = []byte(strings.Replace(string(body), stateful, "", 1))
		}
		if rel == "entities.yaml" {
			body = []byte("batch_state: {}\n")
		}
		return os.WriteFile(target, body, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}
