package conformance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// R1/R6b: a selected deployment source with no row and no event frontier must
// still have an exact durable fork result. It cannot manufacture a trigger or
// receiver merely to make the selected execution path look event-shaped.
func TestDeploymentSourceEmptyVersionForkIsQuiescentAndReplayable(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "singleton")
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "empty.jsonl")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			empty := selectedDeploymentVersion(t, f, nil)
			if len(empty.Rows) != 0 {
				t.Fatalf("empty import compiled %d rows", len(empty.Rows))
			}
			sourceRunID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			var sourcePin string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, sourceRunID).Scan(&sourcePin); err != nil {
				t.Fatal(err)
			}
			if sourcePin != string(empty.VersionID) {
				t.Fatalf("empty source pin=%s want=%s", sourcePin, empty.VersionID)
			}
			owner, initialRecovery := deploymentForkOwner(t, f)
			if len(initialRecovery) != 0 {
				t.Fatalf("fresh empty source has selected recovery: %+v", initialRecovery)
			}
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id":       sourceRunID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true,
				"idempotency_key":     uuid.NewString(),
			}
			result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 {
				t.Fatalf("empty selected source fork: %s", rpcErr)
			}
			if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != empty.VersionID {
				t.Fatalf("empty fork lost exact pin or child: %+v", result)
			}
			for _, runID := range []string{sourceRunID, result.ForkRunID} {
				for _, query := range []string{
					`SELECT COUNT(*) FROM events WHERE run_id=$1`,
					`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`,
					`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`,
				} {
					var count int
					if err := f.db.QueryRowContext(f.ctx, query, runID).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatalf("empty run %s produced %d rows for %s", runID, count, query)
					}
				}
			}
			replayServer := deploymentForkServer(t, f, owner)
			replay, rpcErr := deploymentForkRPC(t, f.ctx, replayServer, params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(replay, result) {
				t.Fatalf("quiesced empty fork replay differs: first=%+v replay=%+v error=%s", result, replay, rpcErr)
			}
			var generationsBefore int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&generationsBefore); err != nil {
				t.Fatal(err)
			}
			if generationsBefore != 1 {
				t.Fatalf("empty selected fork has %d execution generations, want one", generationsBefore)
			}
			if err := owner.RetireSelectedContexts(f.ctx); err != nil {
				t.Fatal(err)
			}
			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			assertServingJoinComplete(t, join, old, nil)
			f.boot(t)
			f.runtime.fanOutServing.Wake()
			recoveredOwner, recovery := deploymentForkOwner(t, f)
			var controlOnly bool
			for _, entry := range recovery {
				if entry.RunID == result.ForkRunID {
					controlOnly = entry.Disposition == runfork.SelectedForkRecoveryControlOnly
				}
			}
			if !controlOnly {
				t.Fatalf("empty completed selected fork recovered executable work: %+v", recovery)
			}
			restartedServer := deploymentForkServerAt(t, f, recoveredOwner, func() time.Time { return time.Now().Add(48 * time.Hour) })
			restartedReplay, rpcErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(restartedReplay, result) {
				t.Fatalf("empty restarted fork lost durable result: first=%+v replay=%+v error=%s", result, restartedReplay, rpcErr)
			}
			var generationsAfter, eventsAfter int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&generationsAfter); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, result.ForkRunID).Scan(&eventsAfter); err != nil {
				t.Fatal(err)
			}
			if generationsAfter != generationsBefore || eventsAfter != 0 {
				t.Fatalf("empty restart resurrected work: generations=%d want=%d events=%d", generationsAfter, generationsBefore, eventsAfter)
			}
		})
	}
}

// R8: a quiesced selected child may be stopped once, but a repeated stop may
// neither reactivate execution nor discard the already committed fork result.
func TestDeploymentSourceSelectedControlOnlyRepeatedStopBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "root")
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "empty.jsonl")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			sourceRunID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			owner, recovered := deploymentForkOwner(t, f)
			if len(recovered) != 0 {
				t.Fatalf("fresh source has selected recovery: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
			}
			forkCtx, cancelFork := context.WithTimeout(f.ctx, 15*time.Second)
			defer cancelFork()
			result, rpcErr, transportErr := deploymentForkRPCRequest(forkCtx, forkServer, params)
			if transportErr != nil {
				_ = owner.FenceSelectedContexts()
				t.Fatalf("R8 selected fork did not quiesce before stop: %v", transportErr)
			}
			if len(rpcErr) != 0 || result.ForkRunID == "" {
				t.Fatalf("R8 empty selected fork: result=%+v error=%s", result, rpcErr)
			}
			stop, selected, err := owner.StopSelectedFork(f.ctx, runcontrol.TransitionRequest{RunID: result.ForkRunID})
			if err != nil || !selected || stop.RunID != result.ForkRunID || stop.Status != "cancelled" {
				t.Fatalf("R8 first stop: state=%+v selected=%t error=%v", stop, selected, err)
			}
			_, selected, err = owner.StopSelectedFork(f.ctx, runcontrol.TransitionRequest{RunID: result.ForkRunID})
			if !selected || !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
				t.Fatalf("R8 repeated stop: selected=%t error=%v", selected, err)
			}
			var executionCount, eventCount int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&executionCount); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, result.ForkRunID).Scan(&eventCount); err != nil {
				t.Fatal(err)
			}
			if executionCount != 1 || eventCount != 0 {
				t.Fatalf("R8 stop fabricated child work: executions=%d events=%d", executionCount, eventCount)
			}
			if err := owner.RetireSelectedContexts(f.ctx); err != nil {
				t.Fatal(err)
			}
			restartedOwner, recovery := deploymentForkOwner(t, f)
			var terminalOnly bool
			for _, entry := range recovery {
				if entry.RunID == result.ForkRunID {
					terminalOnly = entry.Disposition == runfork.SelectedForkRecoveryTerminal
				}
			}
			if !terminalOnly {
				t.Fatalf("R8 stopped child recovered execution: %+v", recovery)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, result.ForkRunID).Scan(&executionCount); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, result.ForkRunID).Scan(&eventCount); err != nil {
				t.Fatal(err)
			}
			if executionCount != 1 || eventCount != 0 {
				t.Fatalf("R8 restart resumed terminal execution: executions=%d events=%d", executionCount, eventCount)
			}
			replayServer := deploymentForkServerAt(t, f, restartedOwner, func() time.Time { return time.Now().Add(48 * time.Hour) })
			replay, rpcErr := deploymentForkRPC(t, f.ctx, replayServer, params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(replay, result) {
				t.Fatalf("R8 stopped child lost durable fork result: first=%+v replay=%+v error=%s", result, replay, rpcErr)
			}
		})
	}
}
