package conformance

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type selectedDeploymentClaimHold struct {
	runtimedelivery.Store
	once    sync.Once
	entered chan runtimedelivery.Claim
	release chan struct{}
}

func (g *selectedDeploymentClaimHold) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	result, err := g.Store.ClaimDelivery(ctx, authority, event, route)
	if err != nil || authority.Kind() != runtimedelivery.ExecutionAuthoritySelectedContractFork || event.Type() != selectedDeploymentEvent {
		return result, err
	}
	if acquired, ok := result.Acquired(); ok {
		g.once.Do(func() {
			g.entered <- acquired.Claim
			<-g.release
		})
	}
	return result, nil
}

// R4: the predecessor has a real committed receiver claim, not merely a
// pending delivery. Recovery must fence that exact claim before the successor
// may settle the receiver; the predecessor token cannot write an outcome.
func TestDeploymentSourceSelectedPredecessorClaimCannotSettleAfterRecoveryBothStores(t *testing.T) {
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
			sourceRunID := start("source", []byte("{\"account_id\":\"source\",\"document\":{\"version\":1}}\n"))
			changedRows := []byte("{\"account_id\":\"changed\",\"document\":{\"version\":2}}\n")
			changed := selectedDeploymentVersion(t, f, changedRows)
			_ = start("changed", changedRows)
			gate := &selectedDeploymentClaimHold{Store: f.selected, entered: make(chan runtimedelivery.Claim, 1), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			owner, recovered := deploymentForkOwnerWithDelivery(t, f, gate)
			if len(recovered) != 0 {
				t.Fatalf("fresh R4 source has selected recovery: %+v", recovered)
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
			ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
			defer cancel()
			type forkResult struct {
				result apiv1.RunForkExecutionResult
				rpcErr []byte
				err    error
			}
			finished := make(chan forkResult, 1)
			go func() {
				result, rpcErr, err := deploymentForkRPCRequest(ctx, forkServer, params)
				finished <- forkResult{result: result, rpcErr: rpcErr, err: err}
			}()
			var predecessor runtimedelivery.Claim
			select {
			case predecessor = <-gate.entered:
			case result := <-finished:
				t.Fatalf("R4 fork finished without receiver claim: %+v", result)
			case <-ctx.Done():
				_ = owner.FenceSelectedContexts()
				t.Fatalf("R4 selected receiver claim not reached: %v", ctx.Err())
			}
			var childID, status, predecessorExecutionID string
			var claimVersion int64
			if err := f.db.QueryRowContext(ctx, `SELECT CAST(fork_run_id AS TEXT) FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&childID); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(ctx, `SELECT status,current_attempt_version,selected_execution_id FROM event_deliveries WHERE delivery_id=$1 AND run_id=$2`, predecessor.DeliveryID(), childID).Scan(&status, &claimVersion, &predecessorExecutionID); err != nil {
				t.Fatal(err)
			}
			if status != "in_progress" || claimVersion != predecessor.Version() {
				t.Fatalf("R4 claim cut lacks exact persisted predecessor: delivery=%s status=%s version=%d want=%d", predecessor.DeliveryID(), status, claimVersion, predecessor.Version())
			}
			if err := owner.FenceSelectedContexts(); err != nil {
				t.Fatal(err)
			}
			release.Do(func() { close(gate.release) })
			select {
			case first := <-finished:
				if first.err == nil && len(first.rpcErr) == 0 {
					t.Fatalf("R4 fenced predecessor returned success: %+v", first.result)
				}
			case <-ctx.Done():
				t.Fatalf("R4 fenced predecessor did not relinquish execution: %v", ctx.Err())
			}
			if err := owner.RetireSelectedContexts(context.Background()); err != nil {
				t.Fatal(err)
			}
			oldRuntime := f.runtime
			join := beginServingLifetimeJoin(oldRuntime, nil)
			assertServingJoinComplete(t, join, oldRuntime, nil)
			if err := f.topology.capability.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
			successor, recovery := deploymentForkOwner(t, f)
			var found bool
			for _, entry := range recovery {
				found = found || entry.RunID == childID
			}
			if !found {
				t.Fatalf("R4 successor lost claimed child: %+v", recovery)
			}
			var successorExecutionID, deliveryExecutionID string
			var successorGeneration, deliveryGeneration int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(execution_id AS TEXT),generation FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`, childID).Scan(&successorExecutionID, &successorGeneration); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(selected_execution_id AS TEXT),selected_execution_generation FROM event_deliveries WHERE delivery_id=$1`, predecessor.DeliveryID()).Scan(&deliveryExecutionID, &deliveryGeneration); err != nil {
				t.Fatal(err)
			}
			if successorExecutionID == predecessorExecutionID || successorGeneration <= 1 || deliveryExecutionID != successorExecutionID || deliveryGeneration != successorGeneration {
				t.Fatalf("R4 no exact successor claim transfer: predecessor=%s successor=%s/%d delivery=%s/%d", predecessorExecutionID, successorExecutionID, successorGeneration, deliveryExecutionID, deliveryGeneration)
			}
			if settled, err := f.selected.SettleSuccess(context.Background(), predecessor, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err == nil {
				t.Fatalf("R4 stale predecessor settled after successor recovery: %+v", settled)
			}
			var predecessorDelivered int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND claim_version=$2 AND outcome='delivered'`, predecessor.DeliveryID(), predecessor.Version()).Scan(&predecessorDelivered); err != nil {
				t.Fatal(err)
			}
			if predecessorDelivered != 0 {
				t.Fatal("R4 stale predecessor recorded a delivered outcome")
			}
			restartedServer := deploymentForkServer(t, f, successor)
			result, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
			if len(rpcErr) != 0 || result.ForkRunID != childID {
				t.Fatalf("R4 successor failed exact child: result=%+v error=%s", result, rpcErr)
			}
			assertSelectedDeploymentRowsWithClosedExecutions(t, f, server, childID, "root", string(changed.VersionID), changedRows, 2)
			var successorDelivered int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND claim_version>$2 AND outcome='delivered'`, predecessor.DeliveryID(), predecessor.Version()).Scan(&successorDelivered); err != nil {
				t.Fatal(err)
			}
			if successorDelivered != 1 {
				t.Fatalf("R4 exact successor settlement attempts=%d, want one", successorDelivered)
			}
		})
	}
}
