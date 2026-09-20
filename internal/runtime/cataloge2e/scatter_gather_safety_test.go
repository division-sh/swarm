package cataloge2e

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/replayconformance"
	"github.com/google/uuid"
)

func TestScatterGatherSafetyBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/cataloge2e/testdata/scatter-gather-safety"))
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, variant := range []struct {
			name                    string
			order                   []int
			restart, repeat, reject bool
			hundred                 bool
		}{
			{name: "ordered", order: []int{0, 1, 2}},
			{name: "reversed", order: []int{2, 1, 0}},
			{name: "duplicate_publication", order: []int{1, 0, 2}, repeat: true},
			{name: "partial_restart", order: []int{2, 0, 1}, restart: true},
			{name: "rejected_input", order: []int{0, 2, 1}, reject: true},
			{name: "hundred_reverse_completion", hundred: true},
		} {
			t.Run(string(backend)+"/"+variant.name, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
				observePublication := scatterGatherTransactionDiagnostics(t, h)
				var groups []catalogTranscriptGroup
				var phaseStart, phaseDeadline time.Time
				var phaseCtx context.Context
				var phaseCancel context.CancelFunc
				var phaseEvent string
				readContext := func() context.Context {
					if phaseCtx != nil {
						return phaseCtx
					}
					return catalogRunContext(h, catalogRuntimeRunID)
				}
				finishPhase := func() {
					t.Helper()
					if phaseCtx == nil {
						return
					}
					elapsed := time.Since(phaseStart)
					t.Logf("%s complete phase elapsed=%s original_target=20s merge_ceiling=60s; original performance obligation remains open in #2394", phaseEvent, elapsed)
					phaseCancel()
					phaseCtx = nil
					if !time.Now().Before(phaseDeadline) {
						t.Fatalf("%s exceeded its non-resetting 60s phase ceiling: %s", phaseEvent, elapsed)
					}
				}
				publish := func(event string, payload map[string]any) catalogTriggerStep {
					t.Helper()
					defer observePublication(event)()
					step := catalogTriggerStep{Event: event, Payload: payload, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
					publishTimeout := 20 * time.Second
					if backend == catalogBackendPostgres && variant.hundred && (event == "batch.submitted" || event == "batch.finished") {
						// Lead exception 5752572474: one budget, never reset on progress.
						if phaseCtx != nil {
							t.Fatal("previous phase was not fully checked")
						}
						phaseStart = time.Now()
						phaseDeadline = phaseStart.Add(time.Minute)
						phaseEvent = event
						phaseCtx, phaseCancel = context.WithDeadline(catalogRunContext(h, catalogRuntimeRunID), phaseDeadline)
						t.Cleanup(phaseCancel)
						publishTimeout = time.Until(phaseDeadline)
					}
					if err := h.publishRuntimeEventResultForStep(step, publishTimeout, false); err != nil {
						t.Fatal(err)
					}
					scatterGatherWait(t, h, step, phaseStart, phaseDeadline)
					groups = append(groups, catalogTranscriptGroup{steps: []catalogTriggerStep{step}})
					return step
				}
				items := []any{map[string]any{"item_id": "alpha", "value": "red"}, map[string]any{"item_id": "beta", "value": "green"}, map[string]any{"item_id": "gamma", "value": "blue"}}
				if variant.hundred {
					items = make([]any, 100)
					for i := range items {
						items[i] = map[string]any{"item_id": fmt.Sprintf("item-%03d", i), "value": fmt.Sprintf("value-%03d", i)}
					}
				}
				expectedIDs, expectedResults := make([]any, len(items)), make([]any, len(items))
				for i, raw := range items {
					item := raw.(map[string]any)
					expectedIDs[i], expectedResults[i] = item["item_id"], item["value"]
				}
				publish("collector/batch.opened", map[string]any{"batch_id": "batch-one", "expected_item_ids": expectedIDs})
				publish("batch.submitted", map[string]any{"batch_id": "batch-one", "items": items})
				ids := map[string]string{}
				paths := map[string]string{}
				timerIDs := map[string]string{}
				registrations, err := scatterGatherPublicEvents(h, phaseCtx)
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range registrations {
					if event.EventName != "item.registered" {
						continue
					}
					key, ok := event.Payload["item_id"].(string)
					if !ok || paths[key] != "" || len(event.Deliveries) != 1 {
						t.Fatalf("invalid registration %s: %v", event.EventID, event.Payload)
					}
					route := event.Deliveries[0].Route.Target.Route()
					if route.FlowID != "workers" || route.EntityID == "" {
						t.Fatalf("invalid worker route: %+v", route)
					}
					paths[key], ids[key] = route.FlowInstance, route.EntityID
				}
				if len(paths) != len(items) {
					t.Fatalf("registration set: %v", paths)
				}
				done := map[string]bool{}
				check := func(completed int) {
					t.Helper()
					if counts := scatterGatherCounts(t, h, readContext()); counts["entity_state"] != len(items)+2 {
						t.Fatalf("unexpected entity set: %v", counts)
					}
					for _, raw := range items {
						item := raw.(map[string]any)
						key := item["item_id"].(string)
						worker := scatterGatherLoad(t, h, paths[key], readContext())
						if old := ids[key]; old != "" && old != worker.EntityID {
							t.Fatalf("%s rematerialized: %s -> %s", key, old, worker.EntityID)
						}
						ids[key] = worker.EntityID
						state := "registered"
						active := 1
						if done[key] {
							state = "complete"
							active = 0
						}
						if worker.CurrentState != state || worker.Fields["item_id"] != key || worker.Fields["value"] != item["value"] || worker.Fields["batch_id"] != "batch-one" {
							t.Fatalf("worker %s: %+v", key, worker)
						}
						var timers int
						if err := h.db.QueryRowContext(readContext(), `SELECT COUNT(*) FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer' AND status='active'`, catalogRuntimeRunID, worker.EntityID).Scan(&timers); err != nil {
							t.Fatal(err)
						}
						if timers != active {
							t.Fatalf("worker %s active timers=%d want %d", key, timers, active)
						}
						var timerID, timerStatus string
						if err := h.db.QueryRowContext(readContext(), `SELECT timer_id,status FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer'`, catalogRuntimeRunID, worker.EntityID).Scan(&timerID, &timerStatus); err != nil {
							t.Fatal(err)
						}
						if old := timerIDs[key]; old != "" && old != timerID {
							t.Fatalf("worker %s timer recreated: %s -> %s", key, old, timerID)
						}
						timerIDs[key] = timerID
						wantTimerStatus := "active"
						if done[key] {
							wantTimerStatus = "cancelled"
						}
						if timerStatus != wantTimerStatus {
							t.Fatalf("worker %s timer=%s want %s", key, timerStatus, wantTimerStatus)
						}
					}
					collector := scatterGatherLoad(t, h, "collector", readContext())
					carrier, err := engine.StateCarrierFromPersisted(collector.Fields, collector.Bookkeeping, collector.Gates, collector.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					activation, found, err := joinruntime.Load(carrier.StateBuckets, identitytest.FlowNode(t, "collector", "gather"), joinruntime.ActivationKey("awaiting", "awaiting", "batch-one"))
					if err != nil || !found || activation.Completed() != completed || activation.Expected() != len(items) {
						t.Fatalf("gather %+v found=%v err=%v", activation, found, err)
					}
					if completed == len(items) {
						if collector.CurrentState != "complete" || activation.CloseReason != joinruntime.CloseReasonComplete || !reflect.DeepEqual(activation.Results(), expectedResults) {
							t.Fatalf("final gather: %s %+v", collector.CurrentState, activation)
						}
					} else if collector.CurrentState != "awaiting" || activation.Status != joinruntime.StatusOpen {
						t.Fatalf("premature gather completion: %+v", activation)
					}
					var reports int
					if err := h.db.QueryRowContext(readContext(), `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name LIKE 'workers/%/item.reported'`, catalogRuntimeRunID).Scan(&reports); err != nil {
						t.Fatal(err)
					}
					if reports != completed {
						t.Fatalf("reports=%d want %d", reports, completed)
					}
					public, err := scatterGatherPublicEvents(h, phaseCtx)
					if err != nil {
						t.Fatal(err)
					}
					for _, event := range public {
						if len(event.DeadLetters) != 0 {
							t.Fatalf("dead letters for %s: %+v", event.EventName, event.DeadLetters)
						}
						isReport := strings.HasSuffix(event.EventName, "/item.reported")
						if event.EventName != "item.registered" && event.EventName != "item.finished" && !isReport {
							continue
						}
						key, ok := event.Payload["item_id"].(string)
						if !ok || ids[key] == "" || len(event.Deliveries) != 1 {
							t.Fatalf("unexpected item publication: %s %+v", event.EventName, event.Payload)
						}
						delivery := event.Deliveries[0]
						route := delivery.Route.Target.Route()
						wantPath, wantID, wantNode := paths[key], ids[key], identitytest.FlowNode(t, "workers", "worker").Key()
						if isReport {
							if event.EventName != paths[key]+"/item.reported" {
								t.Fatalf("report escaped its worker: %s", event.EventName)
							}
							wantPath, wantID, wantNode = "collector", collector.EntityID, identitytest.FlowNode(t, "collector", "gather").Key()
						}
						if route.FlowInstance != wantPath || route.EntityID != wantID || delivery.SubscriberID != wantNode || delivery.Status != "delivered" || delivery.ClaimVersion != 1 {
							t.Fatalf("wrong or unsettled %s receiver for %s: %+v", event.EventName, key, delivery)
						}
					}
				}
				check(0)
				finishPhase()
				if variant.hundred {
					reversed := make([]any, len(items))
					for i, item := range items {
						reversed[len(items)-1-i] = item
					}
					publish("batch.finished", map[string]any{"batch_id": "batch-one", "items": reversed})
					for _, item := range items {
						done[item.(map[string]any)["item_id"].(string)] = true
					}
					check(len(items))
					finishPhase()
				}
				if variant.reject {
					before := scatterGatherCounts(t, h, h.ctx)
					states := scatterGatherStates(t, h, paths)
					err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "batch.submitted", Payload: map[string]any{"batch_id": "invalid", "items": []any{map[string]any{"item_id": "bad", "value": true}}}}, 20*time.Second, false)
					if err == nil {
						t.Fatal("malformed item accepted")
					}
					if !strings.Contains(err.Error(), "schema validation failed: $.items[0].value must be string") {
						t.Fatalf("wrong refusal: %v", err)
					}
					if after := scatterGatherStates(t, h, paths); !reflect.DeepEqual(states, after) {
						t.Fatal("rejected input mutated persisted workflow state")
					}
					if after := scatterGatherCounts(t, h, h.ctx); !reflect.DeepEqual(before, after) {
						t.Fatalf("rejected input mutated domain: before=%v after=%v", before, after)
					}
					check(0)
				}
				for index, member := range variant.order {
					step := publish("batch.finished", map[string]any{"batch_id": "batch-one", "items": []any{items[member]}})
					done[items[member].(map[string]any)["item_id"].(string)] = true
					check(index + 1)
					if variant.name == "ordered" {
						frontier, err := scatterGatherDeliveryProgress(h.ctx, h, step.eventID)
						if err != nil {
							t.Fatal(err)
						}
						var count int
						for _, entry := range frontier {
							if entry.status != "delivered" || entry.reason != "" {
								t.Fatalf("settled diagnostic frontier contains unfinished work: %+v", frontier)
							}
							count += entry.count
						}
						if count != 3 {
							t.Fatalf("diagnostic frontier must include ingress, worker and collector, not other publications: %+v", frontier)
						}
					}
					if variant.repeat {
						before := scatterGatherCounts(t, h, h.ctx)
						states := scatterGatherStates(t, h, paths)
						if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
							t.Fatal(err)
						}
						if after := scatterGatherCounts(t, h, h.ctx); !reflect.DeepEqual(before, after) {
							t.Fatalf("duplicate changed domain: %v -> %v", before, after)
						}
						if after := scatterGatherStates(t, h, paths); !reflect.DeepEqual(states, after) {
							t.Fatal("duplicate mutated persisted workflow state")
						}
						check(index + 1)
					}
					if variant.restart && index == 0 {
						hash, err := contracts.BundleHash(h.bundle)
						if err != nil {
							t.Fatal(err)
						}
						digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
						if err != nil {
							t.Fatal(err)
						}
						h = h.reopenFromTranscript(&catalogExecutionTranscript{version: catalogReplayTranscriptVersion, platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID, groups: groups})
						check(index + 1)
					}
				}
			})
		}
	}
}

// A committed fan-out intent can outlive PublishAndWait's process-local tree.
// Observe exact durable descendants, not elapsed time or a stable count of loops.
const scatterGatherFrontierQuery = `WITH RECURSIVE run_events AS MATERIALIZED (
	SELECT event_id,source_event_id FROM events WHERE run_id=$1 AND event_name<>'platform.runtime_log'
), descendants(event_id) AS (
	SELECT event_id FROM run_events WHERE event_id=$2
	UNION
	SELECT e.event_id FROM run_events e JOIN descendants p ON e.source_event_id=p.event_id
), delivery_counts AS (
	SELECT p.event_id, COUNT(d.delivery_id) AS total,
		COALESCE(SUM(CASE WHEN d.status<>'delivered' THEN 1 ELSE 0 END),0) AS unsettled
	FROM descendants p LEFT JOIN event_deliveries d ON d.event_id=p.event_id
	GROUP BY p.event_id
)
SELECT (SELECT COUNT(*) FROM descendants),
	(SELECT COUNT(*) FROM delivery_counts WHERE total<>1 OR unsettled<>0),
	(SELECT COUNT(*) FROM dead_letters d JOIN descendants p ON d.original_event_id=p.event_id)`

func scatterGatherWait(t testing.TB, h *runtimeHarness, step catalogTriggerStep, phaseStart, phaseDeadline time.Time) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(20 * time.Second)
	if !phaseDeadline.IsZero() {
		started, deadline = phaseStart, phaseDeadline
	}
	ctx, cancel := context.WithDeadline(h.ctx, deadline)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	want := 0
	cardinality := 0
	if items, ok := step.Payload["items"].([]any); ok {
		want = len(items)
		cardinality = len(items)
	}
	if step.Event == "batch.finished" {
		want *= 2
	}
	issued := step.Event != "batch.submitted" && step.Event != "batch.finished"
	for {
		if !issued {
			var count int
			if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2 AND status='closed' AND cardinality=$3 AND cursor=$3 AND claim_owner IS NULL`, catalogRuntimeRunID, step.eventID, cardinality).Scan(&count); err != nil {
				logScatterGatherProgress(t, h, step.eventID)
				t.Fatal(err)
			}
			if count != 1 {
				select {
				case <-ctx.Done():
					logScatterGatherProgress(t, h, step.eventID)
					t.Fatalf("%s exact issuance did not close: %v", step.Event, ctx.Err())
				case <-tick.C:
				}
				continue
			}
			issued = true
		}
		// Poll only durable identities/status. Full public hydration below remains
		// mandatory, but must not compete with issuance for every growing prefix.
		// Match the public helper's explicit ExcludeRuntimeLogs filter.
		var observed, unsettled, deadLetters int
		err := h.db.QueryRowContext(ctx, scatterGatherFrontierQuery, catalogRuntimeRunID, step.eventID).Scan(&observed, &unsettled, &deadLetters)
		if err != nil {
			logScatterGatherProgress(t, h, step.eventID)
			t.Fatal(err)
		}
		if deadLetters != 0 {
			t.Fatalf("%s durable descendants have %d dead letters", step.Event, deadLetters)
		}
		if observed != want+1 || unsettled != 0 {
			select {
			case <-ctx.Done():
				logScatterGatherProgress(t, h, step.eventID)
				t.Fatalf("%s durable descendant frontier: got %d events want %d, unsettled=%d: %v", step.Event, observed, want+1, unsettled, ctx.Err())
			case <-tick.C:
			}
			continue
		}
		if cardinality == 100 {
			t.Logf("%s exact durable frontier settled after %s", step.Event, time.Since(started))
		}
		var readCtx context.Context
		if !phaseDeadline.IsZero() {
			readCtx = ctx
		}
		public, err := scatterGatherPublicEvents(h, readCtx)
		if err != nil {
			t.Fatal(err)
		}
		if cardinality == 100 {
			t.Logf("%s full public hydration finished after %s", step.Event, time.Since(started))
		}
		tree := map[string]bool{step.eventID: true}
		for changed := true; changed; {
			changed = false
			for id, event := range public {
				if !tree[id] && tree[event.SourceEventID] {
					tree[id] = true
					changed = true
				}
			}
		}
		ready := len(tree) == want+1
		for id := range tree {
			event, found := public[id]
			if !found {
				ready = false
				continue
			}
			if len(event.DeadLetters) != 0 {
				t.Fatalf("descendant %s dead letter: %+v", event.EventName, event.DeadLetters)
			}
			if len(event.Deliveries) != 1 {
				ready = false
				continue
			}
			if event.Deliveries[0].Status != "delivered" {
				ready = false
			}
		}
		if ready {
			if step.Event == "batch.submitted" || step.Event == "batch.finished" {
				var count int
				if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2 AND status='closed' AND cardinality=$3 AND cursor=$3 AND claim_owner IS NULL`, catalogRuntimeRunID, step.eventID, cardinality).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("%s lacks exact closed %d-item issuance", step.Event, cardinality)
				}
			}
			if !phaseDeadline.IsZero() && !time.Now().Before(phaseDeadline) {
				t.Fatalf("%s exceeded its non-resetting 60s phase ceiling: %s", step.Event, time.Since(started))
			}
			return
		}
		select {
		case <-ctx.Done():
			logScatterGatherProgress(t, h, step.eventID)
			t.Fatalf("%s descendant frontier: got %d events want %d: %v", step.Event, len(tree), want+1, ctx.Err())
		case <-tick.C:
		}
	}
}

func logScatterGatherProgress(t testing.TB, h *runtimeHarness, eventID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
	defer cancel()
	rows, err := h.db.QueryContext(ctx, `SELECT status,cardinality,cursor,COALESCE(claim_owner,''),claim_generation,CAST(lease_expires_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2`, catalogRuntimeRunID, eventID)
	if err != nil {
		t.Logf("failed fan-out observation: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var status, owner string
		var cardinality, cursor, generation int
		var expiry any
		if err := rows.Scan(&status, &cardinality, &cursor, &owner, &generation, &expiry); err != nil {
			t.Logf("failed fan-out scan: %v", err)
			return
		}
		t.Logf("fan-out at failed deadline: status=%s cursor=%d/%d owner=%q generation=%d lease=%v", status, cursor, cardinality, owner, generation, expiry)
	}
	if err := rows.Err(); err != nil {
		t.Logf("failed fan-out rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Logf("failed fan-out row close: %v", err)
	}
	var events, ledgers, bytes int
	if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT CAST(route_settlement AS TEXT)),COALESCE(SUM(LENGTH(CAST(route_settlement AS TEXT))),0) FROM events WHERE run_id=$1 AND source_event_id=$2`, catalogRuntimeRunID, eventID).Scan(&events, &ledgers, &bytes); err != nil {
		t.Logf("failed fan-out event size read: %v", err)
	} else {
		t.Logf("fan-out persisted settlement inputs: events=%d distinct=%d bytes=%d", events, ledgers, bytes)
	}
	logScatterGatherDeliveryProgress(t, ctx, h, eventID)
}

func logScatterGatherDeliveryProgress(t testing.TB, ctx context.Context, h *runtimeHarness, eventID string) {
	t.Helper()
	frontier, err := scatterGatherDeliveryProgress(ctx, h, eventID)
	if err != nil {
		t.Logf("failed descendant delivery observation: %v", err)
		return
	}
	for _, entry := range frontier {
		t.Logf("descendant delivery frontier: class=%s subscriber=%s status=%s reason=%q count=%d", entry.class, entry.subscriber, entry.status, entry.reason, entry.count)
	}
}

type scatterGatherDeliveryCount struct {
	class, subscriber, status, reason string
	count                             int
}

func scatterGatherDeliveryProgress(ctx context.Context, h *runtimeHarness, eventID string) ([]scatterGatherDeliveryCount, error) {
	// Inspect the failed publication's entire causal tree, not only its issued
	// ordinals. A closed cursor does not imply that downstream receivers settled.
	rows, err := h.db.QueryContext(ctx, `
		WITH RECURSIVE descendants(event_id) AS (
			SELECT event_id FROM events WHERE run_id=$1 AND event_id=$2
			UNION
			SELECT child.event_id FROM events child
			JOIN descendants parent ON child.source_event_id=parent.event_id
			WHERE child.run_id=$1
		)
		SELECT d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,''),COUNT(*)
		FROM event_deliveries d JOIN descendants event ON event.event_id=d.event_id
		WHERE d.run_id=$1
		GROUP BY d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,'')
		ORDER BY d.subscriber_type,d.subscriber_id,d.status,COALESCE(d.reason_code,'')
	`, catalogRuntimeRunID, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []scatterGatherDeliveryCount
	for rows.Next() {
		var entry scatterGatherDeliveryCount
		if err := rows.Scan(&entry.class, &entry.subscriber, &entry.status, &entry.reason, &entry.count); err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, rows.Close()
}

func scatterGatherPublicEvents(h *runtimeHarness, ctx context.Context) (map[string]operatorread.OperatorEventFull, error) {
	if ctx == nil {
		return catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	}
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		return nil, err
	}
	return replayconformance.LoadOperatorEvents(testAuthorActivityContext(ctx), lister, catalogRuntimeRunID)
}

func scatterGatherLoad(t testing.TB, h *runtimeHarness, path string, ctx context.Context) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := h.workflow.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: catalogRuntimeRunID, Route: flowidentity.RouteForInstancePath(path)})
	if err != nil || !found {
		t.Fatalf("load %s: found=%v err=%v", path, found, err)
	}
	return instance
}

func scatterGatherCounts(t testing.TB, h *runtimeHarness, ctx context.Context) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"entity_state", "timers", "fan_out_intents"} {
		var count int
		if err := h.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=$1", table), catalogRuntimeRunID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	var domainEvents int
	if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','collector/batch.opened') OR event_name LIKE 'workers/%/item.reported')`, catalogRuntimeRunID).Scan(&domainEvents); err != nil {
		t.Fatal(err)
	}
	counts["domain_events"] = domainEvents
	return counts
}

func scatterGatherStates(t testing.TB, h *runtimeHarness, paths map[string]string) map[string]pipeline.WorkflowInstance {
	t.Helper()
	states := map[string]pipeline.WorkflowInstance{}
	for _, path := range paths {
		states[path] = scatterGatherLoad(t, h, path, catalogRunContext(h, catalogRuntimeRunID))
	}
	for _, path := range []string{catalogRuntimeRunID, "collector"} {
		states[path] = scatterGatherLoad(t, h, path, catalogRunContext(h, catalogRuntimeRunID))
	}
	return states
}
