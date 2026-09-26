package conformance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type selectedDeploymentForkSettlementProbe struct {
	runforkexecution.SelectedContractForkLifecycle
	committed chan string
}

func (p selectedDeploymentForkSettlementProbe) BindSelectedDeploymentFanOutGrant(grant startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	owner, err := p.SelectedContractForkLifecycle.BindSelectedDeploymentFanOutGrant(grant)
	if owner == nil || err != nil {
		return owner, err
	}
	return selectedDeploymentSettlementOwner{FanOutObligationOwner: owner, committed: p.committed}, nil
}

// R6a: a committed final ordinal cannot make a selected fork quiescent while
// its receiver is still an exact, stamped pending obligation.
func TestDeploymentSourceSelectedLastRowPendingBlocksQuiescenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "root")
			f.runtime.fanOutServing.Close()
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
			changed := selectedDeploymentVersion(t, f, changedRows)
			_ = start("changed", changedRows)
			gate := &selectedDeploymentClaimGate{kind: runtimedelivery.ExecutionAuthoritySelectedContractFork, entered: make(chan string, 16)}
			gate.held.Store(true)
			t.Cleanup(func() { gate.held.Store(false) })
			gate.Store = f.selected
			settled := make(chan string, 16)
			selectedFork, ok := f.selected.(runforkexecution.SelectedContractForkLifecycle)
			if !ok {
				t.Fatalf("selected store lacks selected-fork lifecycle: %T", f.selected)
			}
			forkStore := selectedDeploymentForkSettlementProbe{SelectedContractForkLifecycle: selectedFork, committed: settled}
			owner, recovered := deploymentForkOwnerWithOverrides(t, f, gate, forkStore)
			defer func() { _ = owner.FenceSelectedContexts() }()
			if len(recovered) != 0 {
				t.Fatalf("fresh R6a source has selected recovery: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			key := uuid.NewString()
			params := map[string]any{
				"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": key,
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
			select {
			case finalEventID = <-gate.entered:
			case result := <-finished:
				t.Fatalf("R6a fork finished before selected final receiver claim: %+v", result)
			case <-ctx.Done():
				t.Fatalf("R6a selected final receiver claim not reached: %v", ctx.Err())
			}
			for matched := false; !matched; {
				select {
				case eventID := <-settled:
					matched = eventID == finalEventID
				case result := <-finished:
					t.Fatalf("R6a fork finished before final publication settlement: %+v", result)
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
			if operationState != "materialized" || executionState != "running" || feedStatus != "closed" || cursor != 1 || cardinality != 1 || outcomes != 1 || receipts != 1 || deliveryStatus != "pending" || !handoff || attempts != 0 || gate.deferred.Load() == 0 {
				t.Fatalf("R6a final-row pending cut: operation=%s execution=%s feed=%s cursor=%d/%d outcomes=%d receipts=%d delivery=%s handoff=%t attempts=%d deferred=%d", operationState, executionState, feedStatus, cursor, cardinality, outcomes, receipts, deliveryStatus, handoff, attempts, gate.deferred.Load())
			}
			select {
			case result := <-finished:
				t.Fatalf("R6a returned success or failure with final receiver pending: %+v", result)
			default:
			}
			if err := owner.FenceSelectedContexts(); err != nil {
				t.Fatal(err)
			}
			gate.held.Store(false)
			var result forkResult
			select {
			case result = <-finished:
			case <-ctx.Done():
				t.Fatalf("R6a fenced pending selected receiver did not relinquish execution: %v", ctx.Err())
			}
			if result.err == nil && len(result.rpcErr) == 0 {
				t.Fatalf("R6a unfinished receiver was reported as activated: %+v", result.result)
			}
			var activated int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE idempotency_key=$1 AND state='activated'`, key).Scan(&activated); err != nil {
				t.Fatal(err)
			}
			if activated != 0 {
				t.Fatal("R6a pending receiver falsely activated selected fork")
			}
		})
	}
}
