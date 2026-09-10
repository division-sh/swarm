package bus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type recoveryDiagnosticBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *recoveryDiagnosticBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func captureRecoveryDiagnostics(t *testing.T) *recoveryDiagnosticBuffer {
	t.Helper()
	buffer := &recoveryDiagnosticBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

func (b *recoveryDiagnosticBuffer) require(t *testing.T, fields map[string]any) map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, line := range strings.Split(b.Buffer.String(), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		matches := true
		for key, want := range fields {
			if record[key] != want {
				matches = false
				break
			}
		}
		if matches {
			return record
		}
	}
	t.Fatalf("missing diagnostic fields %v in:\n%s", fields, b.Buffer.String())
	return nil
}

type recoveryPauseDuringDispatch struct{ calls int }

type recoveryPausedIngress struct{}

func (recoveryPausedIngress) QueueableIngressPaused(context.Context) (bool, error) {
	return true, nil
}

func (g *recoveryPauseDuringDispatch) QueueableIngressPaused(context.Context) (bool, error) {
	g.calls++
	return g.calls > 1, nil
}

func TestStartupRecoveryClassifiesBlockedBranchesOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, branch := range []string{"ingress_paused_before_scan", "ingress_paused_during_dispatch", "claim_busy", "run_dispatch_blocked", "bounded_retry"} {
			t.Run(backend+"/"+branch, func(t *testing.T) {
				fixture := newCompleteEventDispatchFixture(t, backend, false)
				diagnostics := captureRecoveryDiagnostics(t)
				deliveries := fixture.subscribe(t, fixture.event.Type())
				defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)
				owner := fixture.store.PipelineObligations()
				var release func()
				switch branch {
				case "ingress_paused_before_scan":
					fixture.bus.SetRuntimeIngressDispatchGate(recoveryPausedIngress{})
					release = func() { fixture.bus.SetRuntimeIngressDispatchGate(nil) }
				case "ingress_paused_during_dispatch":
					fixture.bus.SetRuntimeIngressDispatchGate(&recoveryPauseDuringDispatch{})
					release = func() { fixture.bus.SetRuntimeIngressDispatchGate(nil) }
				case "claim_busy":
					scan, err := owner.OpenScan(fixture.ctx, runtimepipelineobligation.GlobalScanRequest().WithExecutionPosture(executionposture.Live))
					if err != nil {
						t.Fatal(err)
					}
					var once sync.Once
					release = func() {
						once.Do(func() {
							if err := owner.CloseScan(fixture.ctx, scan); err != nil {
								t.Error(err)
							}
						})
					}
					t.Cleanup(release)
					batch, err := owner.ClaimBatch(fixture.ctx, scan, 1)
					if err != nil || len(batch.Work) != 1 {
						t.Fatalf("hold competing scan claim: batch=%+v error=%v", batch, err)
					}
				case "run_dispatch_blocked":
					fixture.bus.SetRunDispatchGate(blockedRunDispatchGate{fixture.event.RunID(): true})
					release = func() { fixture.bus.SetRunDispatchGate(nil) }
				case "bounded_retry":
					fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})
					release = func() { fixture.bus.SetInterceptors() }
				}
				recovery := runtimepipeline.NewRecoveryManagerWith(fixture.bus)
				if err := recovery.RecoverToExhaustion(fixture.ctx); err == nil || err.Error() != "pipeline recovery blocked before explicit exhaustion" {
					t.Fatalf("startup admitted blocked work: %v", err)
				}
				fields := map[string]any{"reason": branch}
				if branch != "ingress_paused_before_scan" {
					fields["event_id"], fields["run_id"] = fixture.event.ID(), fixture.event.RunID()
				}
				if branch == "claim_busy" {
					fields["scan_phase"] = float64(1)
					held := diagnostics.require(t, map[string]any{"held_owner": "local_claim", "held_purpose": "recovery", "event_id": fixture.event.ID()})
					if held["held_claim"] == "" || held["held_claim"] == nil || held["held_scan"] == "" || held["held_scan"] == nil {
						t.Fatalf("missing exact competing claim/scan: %v", held)
					}
				} else if branch != "ingress_paused_before_scan" {
					fields["event_type"] = string(fixture.event.Type())
					diagnostics.require(t, map[string]any{"msg": "startup pipeline recovery claimed", "event_id": fixture.event.ID(), "run_id": fixture.event.RunID(), "scan_phase": float64(1)})
				}
				if branch == "bounded_retry" {
					fields["retry_reason"] = "activity_contract_pin_unavailable"
				}
				diagnostics.require(t, fields)
				if got := retryReleasePipelineReceiptCount(t, fixture, fixture.event.ID()); got != 0 {
					t.Fatalf("blocked event settled: receipts=%d", got)
				}
				release()
				if err := recovery.RecoverToExhaustion(fixture.ctx); err != nil {
					t.Fatalf("exact blocker release did not preserve future recovery: %v", err)
				}
				assertCompleteLocalDelivery(t, deliveries, fixture.event)
				if got := retryReleasePipelineReceiptCount(t, fixture, fixture.event.ID()); got != 1 {
					t.Fatalf("recovered event receipts=%d, want one", got)
				}
			})
		}
	}
}

func TestStartupRecoveryIdentifiesAsyncPublicationClaimOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			deliveries := fixture.subscribe(t, fixture.event.Type())
			defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)
			recovery := runtimepipeline.NewRecoveryManagerWith(fixture.bus)
			if err := recovery.RecoverToExhaustion(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			assertCompleteLocalDelivery(t, deliveries, fixture.event)
			diagnostics := captureRecoveryDiagnostics(t)
			event := newRetryReleaseTestEvent(fixture, time.Now().UTC())
			started, unblock := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(unblock) }) }
			t.Cleanup(func() {
				release()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := fixture.bus.WaitForQuiescence(ctx); err != nil {
					t.Errorf("join asynchronous publication: %v", err)
				}
			})
			fixture.bus.SetInterceptors(waitInterceptor{started: started, release: unblock})
			if err := fixture.bus.PublishAcknowledged(fixture.ctx, event); err != nil {
				t.Fatal(err)
			}
			requireSignalBefore(t, started, 5*time.Second, "publication dispatch barrier")
			if err := recovery.RecoverToExhaustion(fixture.ctx); err == nil {
				t.Fatal("recovery admitted an unsettled asynchronous publication")
			}
			diagnostics.require(t, map[string]any{"reason": "claim_busy", "event_id": event.ID(), "run_id": event.RunID(), "scan_phase": float64(1)})
			held := diagnostics.require(t, map[string]any{"held_owner": "local_claim", "held_purpose": "publication", "event_id": event.ID()})
			if held["held_claim"] == nil || held["held_claim"] == "" || held["held_scan"] != "" {
				t.Fatalf("incorrect publication claim evidence: %v", held)
			}
			release()
			assertCompleteLocalDelivery(t, deliveries, event)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := fixture.bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			if err := recovery.RecoverToExhaustion(fixture.ctx); err != nil {
				t.Fatalf("recovery after exact publication settlement: %v", err)
			}
		})
	}
}
