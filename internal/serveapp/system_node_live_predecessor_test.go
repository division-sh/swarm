package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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
			var originalCapture *storetest.OriginalProcessCapture
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
				latest = p
				if backend == "postgres" && originalCapture == nil {
					var err error
					originalCapture, err = storetest.CaptureOriginalProcess(p.deps.EventStore)
					if err != nil {
						t.Fatal(err)
					}
				}
			})
			type heldMutation struct {
				probe *storetest.WorkflowMutationCommitObservation
				err   error
			}
			mutationReady := make(chan heldMutation, 1)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if event.Type() != "item.received" {
					return nil
				}
				var payload struct {
					Item string `json:"item_id"`
				}
				if err := json.Unmarshal(event.Payload(), &payload); err != nil || payload.Item != "last-allowed" {
					return err
				}
				claim, found := deliverylifecycle.ClaimFromContext(ctx)
				if !found {
					return errors.New("held workflow handler has no original claim")
				}
				observation, err := storetest.HoldWorkflowMutationCommit(ctx, latest.deps.EventStore, claim)
				if err == nil {
					if _, duplicateErr := storetest.HoldWorkflowMutationCommit(ctx, latest.deps.EventStore, claim); duplicateErr == nil {
						err = errors.New("one original workflow owner armed a second observation")
					}
					if _, earlyErr := observation.SuccessorWaiting(ctx); earlyErr == nil {
						err = errors.New("unentered workflow observation inspected native work")
					}
				}
				mutationReady <- heldMutation{probe: observation, err: err}
				return err
			}
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
			var interruption *storetest.OriginalSessionInterruption
			if backend == "postgres" {
				interruption, err = originalCapture.PrepareForTest(ctx, original)
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
			}
			last := requireServedEventPublishRPCResult(t, fixture.Endpoint, map[string]any{
				"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "item.received",
				"payload": map[string]any{"item_id": "last-allowed"}, "idempotency_key": "takeover-final-predecessor",
			})
			var mutation *storetest.WorkflowMutationCommitObservation
			select {
			case held := <-mutationReady:
				if held.err != nil {
					t.Fatal(held.err)
				}
				mutation = held.probe
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			defer mutation.Close()
			select {
			case <-mutation.Entered():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if backend == "sqlite" {
				mutation.Close()
				if _, err := probe.WaitForDeliveryStatus(ctx, last.EventID, "node", node, "delivered"); err != nil {
					t.Fatal(err)
				}
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
				poll := time.NewTicker(5 * time.Millisecond)
				defer poll.Stop()
				for {
					waiting, err := mutation.SuccessorWaiting(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case <-poll.C:
					case <-ctx.Done():
						t.Fatal("successor never reached the exact original transaction's mutation-order wait")
					}
				}
				mutation.Close()
			}
			if _, err := probe.WaitForDeliveryStatus(ctx, last.EventID, "node", node, "delivered"); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForPostCommitDispatchCompleted(ctx, last.EventID); err != nil {
				t.Fatal(err)
			}
			if _, err := mutation.SuccessorWaiting(ctx); err == nil {
				t.Fatal("ended observation retained native wait access")
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
				if active := use.Runtime().WorkOccurrence().ActiveCount(); active != 0 {
					t.Fatalf("ordinary stop-on-loss retained local delivery leases: %d", active)
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
		t.Run(backend+"/startup_failure_cleanup", func(t *testing.T) {
			proveSystemNodeStartupFailureCleanup(t, backend)
		})
	}
}

func proveSystemNodeStartupFailureCleanup(t *testing.T, backend string) {
	t.Helper()
	opts, _ := issue2564ServeHarness(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t), false)
	setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
	prepared := make(chan *runtimepkg.Runtime, 1)
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { prepared <- rt }
	opts.TestBeforeReadinessCommit = func() error { return errors.New("controlled system-node readiness refusal") }
	failed := startServeRuntimeTestProcess(t, *opts)
	code, exited := failed.waitForExit(serveRuntimeStopTimeout)
	if !exited || code == 0 || !strings.Contains(failed.outputString(), "controlled system-node readiness refusal") {
		t.Fatalf("controlled startup failure did not finish its real cleanup: exit=%t code=%d", exited, code)
	}
	var rt *runtimepkg.Runtime
	select {
	case rt = <-prepared:
	default:
		t.Fatal("startup failure preceded actual runtime construction")
	}
	if rt.WorkOccurrence().ActiveCount() != 0 {
		t.Fatalf("startup cleanup retained local work: %d", rt.WorkOccurrence().ActiveCount())
	}
	if _, err := rt.CurrentStartupGrantEvidence(); err == nil {
		t.Fatal("startup cleanup retained an admitted runtime grant")
	}
	// Repeated retirement after the original grant was already retired must
	// refuse native work, not borrow a later grant. Local work is already joined.
	if err := rt.Bus.ResetInMemoryState(); !errors.Is(err, startupownership.ErrMutationAuthorityMissing) {
		t.Fatalf("post-retirement cleanup did not retain its missing-original refusal: %v", err)
	}
	if rt.WorkOccurrence().ActiveCount() != 0 {
		t.Fatal("post-retirement refusal reopened local delivery ownership")
	}
	opts.TestBeforeReadinessCommit = nil
	opts.TestRuntimeReadyHook = nil
	successor := startServeRuntimeTestProcess(t, *opts)
	successor.waitForReadyLine()
	if code := successor.stop(); code != 0 {
		t.Fatalf("successor after controlled startup failure stop=%d", code)
	}
}
