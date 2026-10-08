package serveapp

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type nestedSnapshotHeldLog struct {
	logger   *runtimepkg.RuntimeLogger
	entered  chan string
	finished chan error
	release  <-chan struct{}
}

func (h *nestedSnapshotHeldLog) ProjectLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic) error {
	return h.logger.ProjectLifecycleDiagnostic(ctx, item)
}

func (h *nestedSnapshotHeldLog) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if component == "eventbus" && action == "published" && eventType == "left.work.requested" {
		h.entered <- eventID
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := h.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID,
		EntityID: strings.TrimSpace(entityID), SessionID: sessionID,
		Correlation: correlation, Detail: detail,
		Failure: runtimefailures.CloneEnvelope(failure), DurationUS: durationUS,
	})
	if component == "eventbus" && action == "published" && eventType == "left.work.requested" {
		h.finished <- err
	}
	return err
}

func TestNestedSnapshotLateAcceptedLogCounterexample(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, rejectSecondSeed := range []bool{false, true} {
			mode := "no_second_request"
			if rejectSecondSeed {
				mode = "rejected_second_seed"
			}
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleNestedCascade(t))
				release := make(chan struct{})
				unlock := sync.OnceFunc(func() { close(release) })
				t.Cleanup(unlock)
				hook := &nestedSnapshotHeldLog{logger: rt.Runtime.Logger, entered: make(chan string, 1), finished: make(chan error, 1), release: release}
				rt.Runtime.Bus.SetLoggerHook(hook)
				result := make(chan servedEventPublishRPCResult, 1)
				go func() {
					result <- requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
						"event_name": "left.work.requested", "bundle_hash": rt.BundleHash,
						"payload": map[string]any{"seed": true}, "idempotency_key": "tree-seed",
					})
				}()
				var seed servedEventPublishRPCResult
				select {
				case seed = <-result:
				case <-time.After(5 * time.Second):
					t.Fatal("publication acknowledgment waited for the held diagnostic")
				}
				select {
				case eventID := <-hook.entered:
					if eventID != seed.EventID {
						t.Fatalf("held another occurrence: %s vs %s", eventID, seed.EventID)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("original publication diagnostic did not enter")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				before := repeatedStaticRunSnapshot(t, rt.DB, seed.RunID)
				var priorSeedReceipts int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1`, seed.EventID).Scan(&priorSeedReceipts); err != nil {
					t.Fatal(err)
				}
				if rejectSecondSeed {
					refusal := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", map[string]any{
						"event_name": "right.work.requested", "run_id": seed.RunID,
						"payload": map[string]any{"seed": true}, "idempotency_key": "redundant-tree-seed",
					})
					details, ok := refusal.Data["details"].(map[string]any)
					if !ok || refusal.Data["code"] != "EVENT_NOT_DECLARED" || details["reason"] != "declared_event_has_no_selected_run_recipient" {
						t.Fatalf("wrong refusal: %#v", refusal)
					}
				}
				unlock()
				select {
				case err := <-hook.finished:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("held diagnostic did not finish after release")
				}
				logID := ""
				for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
					var logs operatorread.OperatorRuntimeLogListResult
					requireServedJSONRPCResult(t, rt.Endpoint, "runtime.logs", map[string]any{"run_id": seed.RunID, "component": "eventbus", "limit": 1000}, &logs)
					for _, log := range logs.Logs {
						if log.EventID == seed.EventID && log.Component == "eventbus" && log.Action == "published" {
							logID = log.LogID
						}
					}
					if logID != "" {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if logID == "" {
					t.Fatal("original publication diagnostic did not persist after release")
				}
				after := repeatedStaticRunSnapshot(t, rt.DB, seed.RunID)
				for _, table := range []string{"flow_instances", "entity_state", "deliveries", "outcomes"} {
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Fatalf("accepted-log intervention changed %s: before=%#v after=%#v", table, before[table], after[table])
					}
				}
				if len(after["events"]) != len(before["events"])+1 || reflect.DeepEqual(before, after) {
					t.Fatalf("expected exactly the held diagnostic to change the original oracle: before=%#v after=%#v", before, after)
				}
				var eventType string
				if err := rt.DB.QueryRow(`SELECT event_name FROM events WHERE event_id=$1`, logID).Scan(&eventType); err != nil || eventType != "platform.runtime_log" {
					t.Fatalf("held log %s is not its exact diagnostic event: %s %v", logID, eventType, err)
				}
				for _, table := range []string{"events", "receipts"} {
					for _, prior := range before[table] {
						found := false
						for _, current := range after[table] {
							found = found || reflect.DeepEqual(prior, current)
						}
						if !found {
							t.Fatalf("original %s row changed: %#v", table, prior)
						}
					}
				}
				var currentOriginalReceipts int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 OR event_id=$2`, logID, seed.EventID).Scan(&currentOriginalReceipts); err != nil {
					t.Fatal(err)
				}
				newReceipts := currentOriginalReceipts - priorSeedReceipts
				if len(after["receipts"])-len(before["receipts"]) != newReceipts {
					t.Fatalf("additional receipt does not belong to the original publication or held diagnostic: before=%#v after=%#v seed=%s log=%s", before["receipts"], after["receipts"], seed.EventID, logID)
				}
				t.Logf("original snapshot assertion is false; only original event %s publication log %s and %d original-publication/log receipts arrived; construction/state/delivery/outcomes unchanged; rejected_second_seed=%t", seed.EventID, logID, newReceipts, rejectSecondSeed)
			})
		}
	}
}
