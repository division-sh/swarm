package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type selectedDeploymentPreReceiptGate struct {
	armed   atomic.Bool
	entered chan string
	release chan struct{}
	once    sync.Once
}

func (g *selectedDeploymentPreReceiptGate) NotifyLifecycle(_ context.Context, signal runtimelifecycleprobe.Signal) {
	if signal.Kind != runtimelifecycleprobe.EventPersisted || signal.EventType != selectedDeploymentEvent || !g.armed.CompareAndSwap(true, false) {
		return
	}
	g.entered <- signal.EventID
	<-g.release
}

func (g *selectedDeploymentPreReceiptGate) Release() {
	g.once.Do(func() { close(g.release) })
}

// R3a freezes one real publication after its event/delivery commit and before
// post-commit dispatch. R3b is a different, later cut with a settled pipeline.
func TestDeploymentSourceFixedTPublishedBeforePipelineChangedPinBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			gate := &selectedDeploymentPreReceiptGate{entered: make(chan string, 1), release: make(chan struct{})}
			defer gate.Release()
			t.Cleanup(gate.Release)
			f := selectedDeploymentResourceFixtureWithProbe(t, backend, "singleton", gate)
			server := f.operatorServer(t)
			start := func(name string, rows []byte) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, rows, 0o600); err != nil {
					t.Fatal(err)
				}
				return startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			}
			oldRows := []byte("{\"account_id\":\"old\",\"document\":{\"version\":1}}\n")
			newRows := []byte("{\"account_id\":\"new\",\"document\":{\"version\":2}}\n")
			newVersion := selectedDeploymentVersion(t, f, newRows)
			changedRunID := start("new", newRows)
			assertSelectedDeploymentRows(t, f, server, changedRunID, "singleton", string(newVersion.VersionID), newRows)
			gate.armed.Store(true)
			sourceRunID := start("old", oldRows)
			var pointEventID string
			select {
			case pointEventID = <-gate.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("R3a did not reach the committed publication before pipeline dispatch")
			}
			var deliveryID, deliveryStatus, persistedRunID string
			var handoffStamped bool
			var pipelineReceipts, attempts int
			err := f.db.QueryRowContext(f.ctx, `SELECT CAST(d.delivery_id AS TEXT),d.status,d.continuation_handoff_at IS NOT NULL,CAST(e.run_id AS TEXT),
				(SELECT COUNT(*) FROM event_receipts r WHERE r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'),
				(SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id)
				FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.event_id=$1`, pointEventID).
				Scan(&deliveryID, &deliveryStatus, &handoffStamped, &persistedRunID, &pipelineReceipts, &attempts)
			if err != nil {
				if err == sql.ErrNoRows {
					t.Fatal("R3a probe fired without committed event and delivery rows")
				}
				t.Fatal(err)
			}
			if persistedRunID != sourceRunID || deliveryID == "" || deliveryStatus != "pending" || pipelineReceipts != 0 || handoffStamped || attempts != 0 {
				t.Fatalf("R3a requires committed row before pipeline receipt or handoff at T: source=%s event_run=%s delivery=%q status=%q receipt=%d handoff=%t attempts=%d", sourceRunID, persistedRunID, deliveryID, deliveryStatus, pipelineReceipts, handoffStamped, attempts)
			}
			point := assertDeploymentForkPendingCandidate(t, f, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: pointEventID}, pointEventID, deliveryID, runfork.RunForkDeploymentPendingPublication)
			if point.Kind != runfork.RunForkPointEvent {
				t.Fatalf("R3a changed the exact event cut: %+v", point)
			}
			owner, _ := deploymentForkOwner(t, f)
			forkServer := deploymentForkServer(t, f, owner)
			ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
			defer cancel()
			result, rpcErr := deploymentForkRPC(t, ctx, forkServer, map[string]any{
				"source_run_id": sourceRunID, "fork_event_id": pointEventID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
				"data_pin_overrides": []any{map[string]any{
					"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
					"version_id":  string(newVersion.VersionID),
				}},
			})
			if len(rpcErr) != 0 {
				t.Fatalf("R3a published-before-receipt changed-pin fork: %s", rpcErr)
			}
			if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != newVersion.VersionID {
				t.Fatalf("R3a fork changed the exact pin or child: %+v", result)
			}
			if result.ForkPointKind != string(runfork.RunForkPointEvent) || result.ForkRevision != point.Revision || result.ForkEventID != pointEventID {
				t.Fatalf("R3a fork changed its fixed publication cut: %+v", result)
			}
			gate.Release()
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, result.ForkRunID, 3*time.Minute)
			page := selectedDeploymentPublicEvents(t, f.ctx, server, result.ForkRunID)
			if len(page.Events) != 2 || page.NextCursor != "" {
				t.Fatalf("R3a fork has %d old/new events, cursor=%q: %+v", len(page.Events), page.NextCursor, page.Events)
			}
			want := map[string]map[string]any{}
			for _, row := range [][]byte{oldRows, newRows} {
				var payload map[string]any
				if err := json.Unmarshal(bytes.TrimSpace(row), &payload); err != nil {
					t.Fatal(err)
				}
				want[payload["account_id"].(string)] = payload
			}
			seen := map[string]bool{}
			for _, event := range page.Events {
				key, _ := event.Payload["account_id"].(string)
				if seen[key] || !reflect.DeepEqual(event.Payload, want[key]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
					t.Fatalf("R3a fork lost exact old/new receiver settlement: %+v", event)
				}
				seen[key] = true
			}
			if !seen["old"] || !seen["new"] {
				t.Fatalf("R3a fork failed to retain old pending and new pin rows: %+v", seen)
			}
			var newFeedOutcomes int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, result.ForkRunID).Scan(&newFeedOutcomes); err != nil {
				t.Fatal(err)
			}
			if newFeedOutcomes != 1 {
				t.Fatalf("R3a fork issued %d changed-pin ordinals, want one", newFeedOutcomes)
			}
		})
	}
}
