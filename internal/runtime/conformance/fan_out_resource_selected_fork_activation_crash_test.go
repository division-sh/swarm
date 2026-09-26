package conformance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/google/uuid"
)

type selectedDeploymentBeforeActivationCrash struct {
	runforkexecution.SelectedContractForkLifecycle
	entered chan string
}

func (p selectedDeploymentBeforeActivationCrash) ActivateRunForkForSelectedContractExecution(_ context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.entered <- req.ForkRunID
	panic("test-only crash after selected quiescence and before activation")
}

// R6b: interrupt after the real selected runtime has quiesced, but before its
// activation commit. The durable operation must resume without an invented
// event, a second child, or a false successful activation at the cut.
func TestDeploymentSourceQuiescedBeforeActivationCrashBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "root")
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "empty.jsonl")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			sourceRunID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			selectedFork, ok := f.selected.(runforkexecution.SelectedContractForkLifecycle)
			if !ok {
				t.Fatalf("selected store lacks fork lifecycle: %T", f.selected)
			}
			cut := selectedDeploymentBeforeActivationCrash{SelectedContractForkLifecycle: selectedFork, entered: make(chan string, 1)}
			owner, recovered := deploymentForkOwnerWithOverrides(t, f, nil, cut)
			if len(recovered) != 0 {
				t.Fatalf("fresh R6b source has selected recovery: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			key := uuid.NewString()
			params := map[string]any{
				"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": key,
			}
			forkCtx, cancelFork := context.WithTimeout(f.ctx, 15*time.Second)
			defer cancelFork()
			_, rpcErr, transportErr := deploymentForkRPCRequest(forkCtx, forkServer, params)
			if len(cut.entered) == 0 && transportErr != nil {
				_ = owner.FenceSelectedContexts()
				t.Fatalf("R6b did not reach post-quiescence activation cut: %v", transportErr)
			}
			if transportErr == nil && len(rpcErr) == 0 {
				t.Fatal("R6b interrupted activation returned success")
			}
			var childID string
			select {
			case childID = <-cut.entered:
			default:
				t.Fatal("R6b interruption happened before real selected quiescence")
			}
			var operationState, executionState, feedStatus string
			var cursor, cardinality, events, outcomes int
			if err := f.db.QueryRowContext(f.ctx, `SELECT state FROM run_fork_operations WHERE idempotency_key=$1 AND fork_run_id=$2`, key, childID).Scan(&operationState); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation DESC LIMIT 1`, childID).Scan(&executionState); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT status,cursor,cardinality FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, childID).Scan(&feedStatus, &cursor, &cardinality); err != nil {
				t.Fatal(err)
			}
			for _, check := range []struct {
				query string
				into  *int
			}{
				{`SELECT COUNT(*) FROM events WHERE run_id=$1`, &events},
				{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, &outcomes},
			} {
				if err := f.db.QueryRowContext(f.ctx, check.query, childID).Scan(check.into); err != nil {
					t.Fatal(err)
				}
			}
			if operationState != "materialized" || executionState != "quiesced" || feedStatus != "closed" || cursor != 0 || cardinality != 0 || events != 0 || outcomes != 0 {
				t.Fatalf("R6b exact pre-activation cut: operation=%s execution=%s feed=%s cursor=%d/%d events=%d outcomes=%d", operationState, executionState, feedStatus, cursor, cardinality, events, outcomes)
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
			f.boot(t)
			restarted, recovery := deploymentForkOwner(t, f)
			var found bool
			for _, entry := range recovery {
				found = found || entry.RunID == childID
			}
			if !found {
				t.Fatalf("R6b lost materialized child on recovery: %+v", recovery)
			}
			restartedServer := deploymentForkServer(t, f, restarted)
			result, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
			if len(rpcErr) != 0 || result.ForkRunID != childID {
				t.Fatalf("R6b retry did not activate exact child: result=%+v error=%s", result, rpcErr)
			}
			var children, finalEvents int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&children); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, childID).Scan(&finalEvents); err != nil {
				t.Fatal(err)
			}
			if children != 1 || finalEvents != 0 {
				t.Fatalf("R6b retry reminted child or event: operations=%d events=%d", children, finalEvents)
			}
		})
	}
}
