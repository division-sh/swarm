package serveapp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// The public ACK is not a handler-completion fence. DC-6 protects the exact
// constructed instance while permitting another instance in the run to advance.
func TestPublicPublicationCreatingTurnOrderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			probe := lifecycleprobe.New()
			entered, release := make(chan string, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			node := identitytest.FlowNode(t, "hub", "hub-node").Key()
			opts, start := creatingTurnOrderHarness(t, backend)
			opts.TestLifecycleProbe = probe
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, actual string, event events.Event) error {
				if actual != node || event.Type() != "hub.start" {
					return nil
				}
				var payload struct {
					Hub string `json:"hub_id"`
				}
				if err := json.Unmarshal(event.Payload(), &payload); err != nil {
					return err
				}
				if payload.Hub != "h01" {
					return nil
				}
				entered <- event.ID()
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			process, rt := start()
			t.Cleanup(func() {
				unblock()
				if code := process.stop(); code != 0 {
					t.Errorf("held H1 diagnostic shutdown=%d", code)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			seed := issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "hub.start",
				"payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "held-h1-start",
			})
			select {
			case eventID := <-entered:
				if eventID != seed.EventID {
					t.Fatal("hold did not bind the exact acknowledged creating event")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			publish := func(event, hub, key string) servedEventPublishRPCResult {
				payload := map[string]any{"hub_id": hub}
				if event == "hub.bump" {
					payload["n"] = 1
				}
				return issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{
					"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID,
					"payload": payload, "idempotency_key": key,
				})
			}
			other := publish("hub.start", "h02", "held-h1-other-start")
			if _, err := probe.WaitForDeliveryStatus(ctx, other.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			sameBump := publish("hub.bump", "h01", "held-h1-same-bump")
			otherBump := publish("hub.bump", "h02", "held-h1-other-bump")
			if _, err := probe.WaitForDeliveryStatus(ctx, otherBump.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, sameBump.EventID); err != nil {
				t.Fatal(err)
			}
			if len(sameBump.Deliveries) != 1 {
				t.Fatalf("follower requires one exact node delivery: %+v", sameBump)
			}
			stored, err := storetest.ReadReceiverDeliveryStorage(ctx, rt.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			followerID := ""
			for _, row := range stored {
				if row.EventID == sameBump.EventID {
					if followerID != "" {
						t.Fatal("follower has multiple native delivery identities")
					}
					followerID = row.DeliveryID
				}
			}
			if followerID == "" {
				t.Fatal("ACKed follower has no durable delivery")
			}
			pending, err := rt.persistence.deps.DeliveryStore.Snapshot(ctx, followerID)
			if err != nil || pending.Status != delivery.StatusPending || pending.ClaimVersion != 0 {
				t.Fatalf("follower acquired ahead of its held creating turn: %+v err=%v", pending, err)
			}
			observed, err := rt.persistence.deps.DeliveryStore.ObserveDeliveryContinuation(ctx, pending.Authority, pending.DeliveryID)
			if err != nil || observed.Disposition != delivery.ClaimDeferred {
				t.Fatalf("exact follower did not consume the durable predecessor gate: %+v err=%v", observed, err)
			}
			prepared, found, err := rt.persistence.deps.EventBusDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, sameBump.EventID)
			if err != nil || !found || prepared.Validate() != nil {
				t.Fatalf("load exact ACKed follower: found=%t err=%v", found, err)
			}
			claim, err := rt.persistence.deps.DeliveryStore.ClaimDelivery(ctx, pending.Authority, prepared.Event.Event(), pending.Route)
			if err != nil || claim.Disposition != delivery.ClaimDeferred || claim.Snapshot.ClaimVersion != 0 {
				t.Fatalf("direct claim bypassed the creating-turn fence: %+v err=%v", claim, err)
			}
			page, err := rt.persistence.deps.DeliveryStore.ScanDeliveryContinuations(ctx, pending.Authority, delivery.ContinuationCursor{}, 500)
			if err != nil {
				t.Fatal(err)
			}
			scanned := false
			for _, item := range page.Items {
				if item.DeliveryID == pending.DeliveryID {
					scanned = true
					if item.Disposition != delivery.ClaimDeferred || item.Snapshot.ClaimVersion != 0 || item.Invariant != nil {
						t.Fatalf("continuation scan bypassed the creating-turn fence: %+v", item)
					}
				}
			}
			if !scanned {
				t.Fatal("continuation scan omitted the exact unclaimed follower")
			}
			counts := func() map[string]int {
				rows, err := storetest.ObserveH1HubFields(ctx, rt.selected, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				out := make(map[string]int, len(rows))
				for _, row := range rows {
					var fields struct {
						Hub   string `json:"hub_id"`
						Count int    `json:"count"`
					}
					if err := json.Unmarshal([]byte(row.Fields), &fields); err != nil {
						t.Fatal(err)
					}
					out[fields.Hub] = fields.Count
					t.Logf("H1_HELD_CUT run=%s hub=%s entity=%s header_revision=%d count=%d", seed.RunID, fields.Hub, row.Entity, row.Revision, fields.Count)
				}
				return out
			}
			before := counts()
			if before["h01"] != 0 || before["h02"] != 1 || len(before) != 2 {
				t.Fatalf("held creator was overtaken or independent instance did not progress: %v", before)
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err := probe.WaitForDeliveryStatus(canceled, seed.EventID, "node", node, "delivered"); err == nil {
				t.Fatal("held creator already settled")
			}
			unblock()
			if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForDeliveryStatus(ctx, sameBump.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			after := counts()
			if after["h01"] != 1 || after["h02"] != 1 || len(after) != 2 {
				t.Fatalf("creating turn and exact follower did not preserve both counters: %v", after)
			}
			bumpCount := 0
			for _, row := range storetest.ObserveEntityMutationHistory(t, ctx, rt.selected, seed.RunID) {
				if row.Domain != "authored_field" || row.Path != "count" {
					continue
				}
				t.Logf("H1_HELD_MUTATION event=%s type=%s old=%s new=%s writer=%s/%s at=%s", row.CausedByEvent, row.EventName, row.OldValue, row.NewValue, row.WriterType, row.WriterID, row.CreatedAt.Format(time.RFC3339Nano))
				if row.CausedByEvent == sameBump.EventID && string(row.OldValue) == "0" && string(row.NewValue) == "1" {
					bumpCount++
				}
			}
			if bumpCount != 1 {
				t.Fatalf("ordered follower requires exactly one zero-to-one mutation, got %d", bumpCount)
			}
			t.Logf("DC6_HELD_ORDER backend=%s seed=%s same_bump=%s independent_start=%s independent_bump=%s before=%v after=%v; creating turn precedes follower, independent instance progresses", backend, seed.EventID, sameBump.EventID, other.EventID, otherBump.EventID, before, after)
		})
	}
}

func TestPublicPublicationCreatingTurnRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := creatingTurnOrderHarness(t, backend)
			config, err := os.ReadFile(opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			configured := strings.Replace(string(config), "  recovery_on_startup: false", "  recovery_on_startup: true", 1)
			if configured == string(config) {
				t.Fatal("restart control requires the explicit existing recovery switch")
			}
			writeWorkflowValidationFixtureFile(t, opts.ConfigPath, configured)
			probe := lifecycleprobe.New()
			opts.TestLifecycleProbe = probe
			entered := make(chan string, 1)
			node := identitytest.FlowNode(t, "hub", "hub-node").Key()
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, actual string, event events.Event) error {
				if actual == node && event.Type() == "hub.start" {
					entered <- event.ID()
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("creating-turn recovery shutdown=%d", code)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			seed := issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "hub.start", "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "restart-creator"})
			select {
			case id := <-entered:
				if id != seed.EventID {
					t.Fatal("restart hold missed the exact creator")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			params := map[string]any{"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "hub.bump", "payload": map[string]any{"hub_id": "h01", "n": 1}, "idempotency_key": "restart-follower"}
			first := publicationAuditHTTP(ctx, rt.Endpoint, params)
			if first.err != nil || first.rpcError != nil {
				t.Fatalf("follower ACK before restart: %+v", first)
			}
			var follower servedEventPublishRPCResult
			if err := json.Unmarshal(first.result, &follower); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, follower.EventID); err != nil {
				t.Fatal(err)
			}
			prepared, found, err := rt.persistence.deps.EventBusDurable.PreparedEvents.LoadPreparedPublishEvent(ctx, follower.EventID)
			if err != nil || !found || prepared.Validate() != nil || len(prepared.DeliveryRoutes) != 1 {
				t.Fatalf("exact restart follower: %+v found=%t err=%v", prepared, found, err)
			}
			route := prepared.DeliveryRoutes[0]
			id, err := delivery.DeliveryID(follower.EventID, route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := rt.persistence.deps.DeliveryStore.Snapshot(ctx, id)
			if err != nil || before.Status != delivery.StatusPending || before.ClaimVersion != 0 {
				t.Fatalf("restart follower ran before creator: %+v err=%v", before, err)
			}
			target := route.Target.Route()
			owner := flowidentity.RunScopedFlowInstance{RunID: seed.RunID, Route: flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance)}
			receipt, err := storetest.ReadReceiverConstructionPublication(ctx, rt.selected, owner, target.EntityID)
			if err != nil || receipt.CreatingInput.EventID != seed.EventID {
				t.Fatalf("immutable restart predecessor: %+v err=%v", receipt, err)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("stop with durable predecessor=%d", code)
			}
			probe = lifecycleprobe.New()
			opts.TestLifecycleProbe = probe
			opts.TestWorkflowNodeHandlerStartHook = nil
			process, rt = start()
			if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForDeliveryStatus(ctx, follower.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			replay := publicationAuditHTTP(ctx, rt.Endpoint, params)
			if replay.err != nil || replay.rpcError != nil || !reflect.DeepEqual(first.result, replay.result) {
				t.Fatalf("restart changed the exact public ACK: first=%s replay=%+v", first.result, replay)
			}
			again, err := storetest.ReadReceiverConstructionPublication(ctx, rt.selected, owner, target.EntityID)
			if err != nil || !reflect.DeepEqual(receipt, again) {
				t.Fatalf("restart replaced immutable construction: before=%+v after=%+v err=%v", receipt, again, err)
			}
			after, err := rt.persistence.deps.DeliveryStore.Snapshot(ctx, id)
			if err != nil || after.Status != delivery.StatusDelivered || after.ClaimVersion != 1 {
				t.Fatalf("restart follower settlement: %+v err=%v", after, err)
			}
			outcomes, err := rt.persistence.deps.DeliveryStore.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "delivered" {
				t.Fatalf("restart duplicated or lost follower: %+v err=%v", outcomes, err)
			}
			rows, err := storetest.ObserveH1HubFields(ctx, rt.selected, seed.RunID)
			var count struct {
				Count int `json:"count"`
			}
			if err != nil || len(rows) != 1 {
				t.Fatalf("restart native hub count=%d err=%v", len(rows), err)
			}
			if err := json.Unmarshal([]byte(rows[0].Fields), &count); err != nil || count.Count != 1 {
				t.Fatalf("restart overtook initialization: count=%d err=%v", count.Count, err)
			}
		})
	}
}

func creatingTurnOrderHarness(t *testing.T, backend string) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	t.Helper()
	opts, start := issue2564ServeHarness(t, backend, issue2564H1WriteCorpus(t), false)
	provider := &issue2564H1Provider{t: t, sessions: map[string]*issue2564H1Session{}}
	server := httptest.NewServer(provider)
	t.Cleanup(server.Close)
	setDoctorProviderSecret(t, "OPENAI_COMPATIBLE_API_KEY", "issue2564-h1-proof-key")
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(config), "  backend: anthropic\n", "  backend: openai_compatible\n  openai_compatible:\n    base_url: "+server.URL+"\n", 1)
	if text == string(config) {
		t.Fatal("normal H1 provider config was not found")
	}
	writeWorkflowValidationFixtureFile(t, opts.ConfigPath, text)
	return opts, start
}
