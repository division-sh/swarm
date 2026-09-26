package conformance

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// R2: an eventless deployment source may be forked while its ordinary feed has
// not issued the first row. The selected fork must use its own admitted feed
// phase, not reactivate the source's ordinary serving registration.
func TestDeploymentSourceSelectedForkBeforeFirstRow(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixture(t, backend, "singleton")
			f.runtime.fanOutServing.Close()
			server := f.operatorServer(t)
			start := func(name string, row []byte) string {
				t.Helper()
				path := filepath.Join(t.TempDir(), name+".jsonl")
				if err := os.WriteFile(path, row, 0o600); err != nil {
					t.Fatal(err)
				}
				runID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
				var cardinality, cursor, outcomes int
				if err := f.db.QueryRowContext(f.ctx, `SELECT cardinality,cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&cardinality, &cursor); err != nil {
					t.Fatal(err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&outcomes); err != nil {
					t.Fatal(err)
				}
				if cardinality != 1 || cursor != 0 || outcomes != 0 {
					t.Fatalf("unissued source %s advanced: cardinality=%d cursor=%d outcomes=%d", runID, cardinality, cursor, outcomes)
				}
				return runID
			}
			originalRows := []byte("{\"account_id\":\"before\",\"document\":{\"origin\":\"source\"}}\n")
			changedRows := []byte("{\"account_id\":\"after\",\"document\":{\"origin\":\"fork\"}}\n")
			changed := selectedDeploymentVersion(t, f, changedRows)
			sourceRunID := start("source", originalRows)
			_ = start("changed", changedRows)
			owner, recovered := deploymentForkOwner(t, f)
			if len(recovered) != 0 {
				t.Fatalf("fresh selected owner inherited work: %+v", recovered)
			}
			forkServer := deploymentForkServer(t, f, owner)
			params := map[string]any{
				"source_run_id":       sourceRunID,
				"bundle_hash":         f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true,
				"idempotency_key":     uuid.NewString(),
				"data_pin_overrides": []any{map[string]any{
					"declaration": map[string]any{"flow_path": ".", "event": selectedDeploymentEvent},
					"version_id":  string(changed.VersionID),
				}},
			}
			result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 {
				t.Fatalf("selected pre-issue changed-pin fork: %s", rpcErr)
			}
			if result.ForkRunID == "" || result.ForkRunID == sourceRunID || len(result.DataPins) != 1 || result.DataPins[0].VersionID != changed.VersionID {
				t.Fatalf("pre-issue fork lost child or exact changed pin: %+v", result)
			}
			assertSelectedDeploymentRows(t, f, server, result.ForkRunID, "singleton", string(changed.VersionID), changedRows)
			replay, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 || !reflect.DeepEqual(replay, result) {
				t.Fatalf("pre-issue fork retry duplicated or changed result: first=%+v replay=%+v error=%s", result, replay, rpcErr)
			}
			var sourceCursor, childCount int
			if err := f.db.QueryRowContext(f.ctx, `SELECT cursor FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, sourceRunID).Scan(&sourceCursor); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, sourceRunID).Scan(&childCount); err != nil {
				t.Fatal(err)
			}
			if sourceCursor != 0 || childCount != 1 {
				t.Fatalf("selected pre-issue fork consumed source cursor or duplicated child: source_cursor=%d children=%d", sourceCursor, childCount)
			}
		})
	}
}
