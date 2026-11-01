package conformance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// R6a: a committed final ordinal cannot make a selected fork quiescent while
// its receiver is still an exact, stamped pending obligation.
func TestDeploymentSourceSelectedLastRowPendingBlocksQuiescenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			gate := &selectedDeploymentClaimGate{kind: runtimedelivery.ExecutionAuthorityNormalRuntime, entered: make(chan string, 16)}
			observed := newSelectedDeploymentSettlementObservation()
			ordinary := newSelectedDeploymentSettlementObservation()
			decorate := func(executor startupownership.FanOutExecutor) startupownership.FanOutExecutor {
				return &selectedDeploymentSettlementExecutor{FanOutExecutor: executor, observed: observed}
			}
			f := selectedDeploymentResourceFixtureWithDelivery(t, backend, "root", nil, func(owner runtimedelivery.Store) runtimedelivery.Store {
				gate.Store = owner
				return gate
			}, func(executor startupownership.FanOutExecutor) startupownership.FanOutExecutor {
				return &selectedDeploymentSettlementExecutor{FanOutExecutor: executor, observed: ordinary}
			})
			server := f.operatorServer(t)
			start := func(name string, rows []byte) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, rows, 0o600); err != nil {
					t.Fatal(err)
				}
				return startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			}
			sourceRows := []byte("{\"account_id\":\"source\",\"document\":{\"version\":1}}\n")
			changedRows := []byte("{\"account_id\":\"changed\",\"document\":{\"version\":2}}\n")
			sourceRunID := start("source", sourceRows)
			if err := f.runtime.bus.WaitForQuiescence(f.ctx); err != nil {
				t.Fatal(err)
			}
			changed := selectedDeploymentVersion(t, f, changedRows)
			pauseDeploymentFixtureRun(t, f, sourceRunID)
			_ = start("changed", changedRows)
			if err := f.runtime.bus.WaitForQuiescence(f.ctx); err != nil {
				t.Fatal(err)
			}
			observed.mu.Lock()
			observed.evaluated, observed.committed = nil, nil
			observed.mu.Unlock()
			gate.held.Store(true)
			t.Cleanup(func() { gate.held.Store(false) })
			owner, recovered := deploymentForkOwnerWithExecutor(t, f, gate, nil, nil, decorate)
			defer func() { _ = owner.FenceSelectedContexts() }()
			if len(recovered) != 0 {
				t.Fatalf("fresh R6a source has selected recovery: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			key := uuid.NewString()
			params := map[string]any{
				"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"idempotency_key": key,
				"data_pin_overrides": []any{map[string]any{
					"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
					"version_id":  string(changed.VersionID),
				}},
			}
			type forkResult struct {
				result apiv1.RunForkExecutionResult
				rpcErr []byte
				err    error
			}
			ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
			defer cancel()
			finished := make(chan forkResult, 1)
			go func() {
				result, rpcErr, err := deploymentForkRPCRequest(ctx, forkServer, params)
				finished <- forkResult{result: result, rpcErr: rpcErr, err: err}
			}()
			var finalEventID string
			var result forkResult
			select {
			case result = <-finished:
				if result.err != nil || len(result.rpcErr) != 0 || result.result.ForkRunStatus != "live" {
					t.Fatalf("R6a activation did not acknowledge its retained owner: %+v", result)
				}
			case <-ctx.Done():
				t.Fatalf("R6a activation acknowledgment not reached: %v", ctx.Err())
			}
			for finalEventID == "" {
				select {
				case eventID := <-gate.entered:
					event := storetest.LoadCanonicalEventRecord(t, ctx, f.selected, eventID)
					if event.RunID() == result.result.ForkRunID {
						finalEventID = eventID
					}
				case <-ctx.Done():
					t.Fatalf("R6a selected final receiver claim not reached: %v", ctx.Err())
				}
			}
			for {
				_, selectedCommitted := observed.snapshot()
				_, ordinaryCommitted := ordinary.snapshot()
				committed := false
				for _, eventID := range append(selectedCommitted, ordinaryCommitted...) {
					committed = committed || eventID == finalEventID
				}
				if committed {
					break
				}
				select {
				case <-observed.changed:
				case <-ordinary.changed:
				case <-ctx.Done():
					t.Fatalf("R6a final publication settlement not reached: %v", ctx.Err())
				}
			}
			var childID, operationState, executionState, feedStatus, deliveryStatus string
			var cursor, cardinality, outcomes, receipts, attempts int
			var handoff bool
			if err := f.db.QueryRowContext(ctx, `SELECT CAST(fork_run_id AS TEXT),state FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&childID, &operationState); err != nil {
				t.Fatal(err)
			}
			evaluated, _ := observed.snapshot()
			for _, runID := range evaluated {
				if runID != childID {
					t.Fatalf("selected composite evaluation used run %q, want exact fork %q", runID, childID)
				}
			}
			ordinaryEvaluated, _ := ordinary.snapshot()
			childEvaluated := len(evaluated) != 0
			for _, runID := range ordinaryEvaluated {
				childEvaluated = childEvaluated || runID == childID
			}
			if !childEvaluated {
				t.Fatal("acknowledged child publication did not pass through its exact selected or ordinary owner's BeginFanOutEvaluation")
			}
			if err := f.db.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, childID).Scan(&executionState); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(ctx, `SELECT status,cursor,cardinality FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, childID).Scan(&feedStatus, &cursor, &cardinality); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(ctx, `SELECT status,continuation_handoff_at IS NOT NULL FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, childID, finalEventID).Scan(&deliveryStatus, &handoff); err != nil {
				t.Fatal(err)
			}
			for _, check := range []struct {
				query string
				into  *int
			}{
				{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, &outcomes},
				{`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, &receipts},
				{`SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1`, &attempts},
			} {
				if err := f.db.QueryRowContext(ctx, check.query, childID).Scan(check.into); err != nil {
					t.Fatal(err)
				}
			}
			if operationState != "activated" || executionState != "closed" || feedStatus != "closed" || cursor != 1 || cardinality != 1 || outcomes != 1 || receipts != 1 || deliveryStatus != "pending" || !handoff || attempts != 0 || gate.deferred.Load() == 0 || childID != result.result.ForkRunID {
				t.Fatalf("R6a final-row pending cut: operation=%s execution=%s feed=%s cursor=%d/%d outcomes=%d receipts=%d delivery=%s handoff=%t attempts=%d deferred=%d", operationState, executionState, feedStatus, cursor, cardinality, outcomes, receipts, deliveryStatus, handoff, attempts, gate.deferred.Load())
			}
			if err := owner.FenceSelectedContexts(); err != nil {
				t.Fatal(err)
			}
			gate.held.Store(false)
			var activated int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE idempotency_key=$1 AND state='activated'`, key).Scan(&activated); err != nil {
				t.Fatal(err)
			}
			if activated != 1 {
				t.Fatal("R6a retirement rewrote the acknowledged activation")
			}
		})
	}
}
