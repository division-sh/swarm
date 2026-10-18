package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// A real standby must consume the last allowed predecessor state, while a
// locally-live predecessor cannot lend its stale generation to pooled ingress.
func TestServeProcessTakeoverRejectsLivePredecessorMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := creatingTurnOrderHarness(t, backend)
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			probe := lifecycleprobe.New()
			opts.TestLifecycleProbe = probe
			contextsReady := make(chan *runtimepkg.RuntimeContextManager, 2)
			opts.TestRuntimeContextsReadyHook = func(contexts *runtimepkg.RuntimeContextManager) { contextsReady <- contexts }
			first, fixture := start()
			contexts := <-contextsReady
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			seed := issue2564H1Publish(t, ctx, fixture.Endpoint, map[string]any{
				"bundle_hash": fixture.BundleHash, "event_name": "hub.start",
				"payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "takeover-seed",
			})
			node := identitytest.FlowNode(t, "hub", "hub-node").Key()
			if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, seed.EventID); err != nil {
				t.Fatal(err)
			}
			use, _, _, err := contexts.AcquireRun(ctx, seed.RunID)
			if err != nil || use == nil {
				t.Fatalf("acquire exact original run context: %v", err)
			}
			defer use.Done()
			oldContext, err := startupownership.FreezeMutationAuthority(use.WorkContext())
			if err != nil {
				t.Fatal(err)
			}
			original, err := startupownership.CaptureMutationAuthority(oldContext)
			if err != nil {
				t.Fatal(err)
			}
			oldEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "hub.bump", "takeover-proof", "", []byte(`{"hub_id":"h01","n":1}`), 0, seed.RunID, events.EventEnvelope{}, time.Now().UTC())
			var hold *startupownership.PossessionObservationHoldForTest
			var interruption *startupownership.OriginalSessionInterruptionForTest
			if backend == "postgres" {
				interruption, err = startupownership.PrepareOriginalSessionInterruptionForTest(ctx, original)
				if err != nil {
					t.Fatal(err)
				}
				hold, err = startupownership.HoldOriginalPossessionObservationForTest(original)
				if err != nil {
					t.Fatal(err)
				}
				defer hold.Close()
				select {
				case <-hold.Entered():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else {
				if err := use.Done(); err != nil {
					t.Fatal(err)
				}
				if code := first.stop(); code != 0 {
					t.Fatalf("sequential SQLite predecessor stop=%d", code)
				}
			}
			second := startServeRuntimeTestProcess(t, *opts)
			if backend == "postgres" {
				if err := interruption.Interrupt(ctx, original); err != nil {
					t.Fatal(err)
				}
			}
			second.waitForReadyLine()
			secondContexts := <-contextsReady
			current, _, _, err := secondContexts.AcquireRun(ctx, seed.RunID)
			if err != nil || current == nil {
				t.Fatalf("successor exact readiness preparation: %v", err)
			}
			defer current.Done()
			next, err := startupownership.CaptureMutationAuthority(current.WorkContext())
			if err != nil || next.ProcessAuthority().AuthorityGeneration != original.ProcessAuthority().AuthorityGeneration+1 {
				t.Fatalf("successor generation acknowledgment: %+v err=%v", next.ProcessAuthority(), err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, fixture.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			refusal := use.Runtime().Bus.PublishAcknowledged(oldContext, oldEvent)
			after, readErr := storetest.ReadSelectedForkSourceDomain(ctx, fixture.selected, seed.RunID)
			if refusal == nil || readErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("old ingress did not refuse conservatively: refusal=%v read=%v conserved=%t", refusal, readErr, reflect.DeepEqual(before, after))
			}
			if backend == "postgres" {
				if err := original.Validate(); err != nil || !strings.Contains(refusal.Error(), "generation grant is no longer current") {
					t.Fatalf("pooled refusal was not the durable original-generation comparison: live=%v refusal=%v", err, refusal)
				}
				if err := use.Done(); err != nil {
					t.Fatal(err)
				}
				hold.Close()
				if _, ok := first.waitForExit(serveRuntimeStopTimeout); !ok {
					t.Fatal("ordinary stop-on-loss did not resume")
				}
			}
			rows, err := storetest.ObserveH1HubFields(ctx, fixture.selected, seed.RunID)
			var fields struct {
				Count int `json:"count"`
			}
			if err != nil || len(rows) != 1 || json.Unmarshal([]byte(rows[0].Fields), &fields) != nil || fields.Count != 0 {
				t.Fatalf("successor lost the final predecessor state: rows=%+v err=%v", rows, err)
			}
			if err := current.Done(); err != nil {
				t.Fatal(err)
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("successor stop=%d", code)
			}
		})
	}
}
