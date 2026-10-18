package serveapp

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// A real standby must consume the last allowed predecessor state, while a
// locally-live predecessor cannot lend its stale generation to pooled ingress.
func TestServeProcessTakeoverRejectsLivePredecessorMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := issue2564ServeHarness(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t), false)
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			var latest serveRuntimePersistence
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { latest = p })
			probe := lifecycleprobe.New()
			opts.TestLifecycleProbe = probe
			contextsReady := make(chan *runtimepkg.RuntimeContextManager, 2)
			opts.TestRuntimeContextsReadyHook = func(contexts *runtimepkg.RuntimeContextManager) { contextsReady <- contexts }
			first, fixture := start()
			contexts := <-contextsReady
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			seed := requireServedEventPublishRPCResult(t, fixture.Endpoint, map[string]any{
				"bundle_hash": fixture.BundleHash, "event_name": "item.received",
				"payload": map[string]any{"item_id": "emit"}, "idempotency_key": "takeover-seed",
			})
			node := identitytest.RootNode(t, "item-handler").Key()
			if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, seed.EventID); err != nil {
				t.Fatal(err)
			}
			// The emitted event and its independent node delivery also settle
			// before the final state-changing predecessor command is submitted.
			fixture.waitDeliveries(t, seed.RunID)
			last := requireServedEventPublishRPCResult(t, fixture.Endpoint, map[string]any{
				"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "item.received",
				"payload": map[string]any{"item_id": "last-allowed"}, "idempotency_key": "takeover-final-predecessor",
			})
			if _, err := probe.WaitForDeliveryStatus(ctx, last.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, last.EventID); err != nil {
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
			oldEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.received", "takeover-proof", "", []byte(`{"item_id":"stale"}`), 0, seed.RunID, events.EventEnvelope{}, time.Now().UTC())
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
			selected := latest.deps.EventStore.(issue2564ServedObservationOwner)
			reader := selected.(interface {
				LoadDynamicFlowRuntimeReadiness(context.Context, string, flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error)
			})
			readiness, found, err := reader.LoadDynamicFlowRuntimeReadiness(ctx, seed.RunID, flowidentity.RouteForInstancePath(seed.RunID))
			if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady || readiness.Plan.RunID != seed.RunID || readiness.Plan.BundleHash != fixture.BundleHash || readiness.PlanHash == "" {
				t.Fatalf("successor did not reconstruct the exact persisted instance/readiness plan: %+v found=%t err=%v", readiness, found, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			refusal := use.Runtime().Bus.PublishAcknowledged(oldContext, oldEvent)
			after, readErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, seed.RunID)
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
			rows, err := storetest.ReadWorkflowStateObservationRows(ctx, selected)
			if err != nil || len(rows) != 1 || rows[0].EntityID != seed.RunID || rows[0].CurrentState != "waiting" {
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
