package conformance

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestDeploymentSourceTwoPinnedFeedsSettleIndependentlyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeploymentResourceFixtureWithSource(t, backend, twoDeploymentFeedSource(t))
			server := f.operatorServer(t)
			imports := []struct {
				declaration string
				row         string
			}{
				{"portfolio/account.registered", `{"account_id":"registered"}` + "\n"},
				{"portfolio/account.notify.requested", `{"account_id":"notified","command":"ping"}` + "\n"},
			}
			versions := make(map[string]string, len(imports))
			for _, item := range imports {
				file := filepath.Join(t.TempDir(), "rows.jsonl")
				if err := os.WriteFile(file, []byte(item.row), 0o600); err != nil {
					t.Fatal(err)
				}
				runID := startDeploymentResourceRun(t, f, server, "--data", item.declaration+"="+file)
				waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)
				var version string
				if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='portfolio' AND event_name=$2`, runID, item.declaration).Scan(&version); err != nil {
					t.Fatal(err)
				}
				versions[item.declaration] = version
			}

			runID := uuid.NewString()
			args := []string{"run", "start", "--connect", server.URL, "--bundle-hash", f.runtime.sourceArtifactFact.BundleHash(), "--run-id", runID}
			for _, item := range imports {
				args = append(args, "--pin", item.declaration+"@"+versions[item.declaration])
			}
			args = append(args, "--no-follow")
			var stdout, stderr bytes.Buffer
			if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 || !strings.Contains(stdout.String(), "run_id="+runID) {
				t.Fatalf("operator two-pin run.start: code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)
			assertTwoDeploymentFeedsSettled(t, f, runID, versions)
			assertTwoDeploymentPublicEvents(t, f, server, runID)

			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			assertServingJoinComplete(t, join, old, nil)
			f.boot(t)
			assertTwoDeploymentFeedsSettled(t, f, runID, versions)
			assertTwoDeploymentPublicEvents(t, f, f.operatorServer(t), runID)
		})
	}
}

func assertTwoDeploymentPublicEvents(t *testing.T, f *deploymentResourceFixture, server *httptest.Server, runID string) {
	t.Helper()
	seenEvents := make(map[string]bool, 2)
	seenDeliveries := make(map[string]bool, 2)
	for eventName, payload := range map[string]map[string]any{
		"portfolio/account.registered":       {"account_id": "registered"},
		"portfolio/account.notify.requested": {"account_id": "notified", "command": "ping"},
	} {
		page := deploymentResourcePublicEventsByName(t, f.ctx, server, runID, eventName, "")
		if len(page.Events) != 1 || page.NextCursor != "" {
			t.Fatalf("two-pin public %s events=%d cursor=%q", eventName, len(page.Events), page.NextCursor)
		}
		event := page.Events[0]
		if event.EventID == "" || seenEvents[event.EventID] || !reflect.DeepEqual(event.Payload, payload) || len(event.Deliveries) != 1 ||
			event.Deliveries[0].Status != "delivered" || event.Deliveries[0].DeliveryID == "" || seenDeliveries[event.Deliveries[0].DeliveryID] {
			t.Fatalf("two-pin public %s lost independent payload or receiver: %+v", eventName, event)
		}
		seenEvents[event.EventID] = true
		seenDeliveries[event.Deliveries[0].DeliveryID] = true
	}
}

func twoDeploymentFeedSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyTwoDeploymentFeeds(t)
	repo := conformanceRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func assertTwoDeploymentFeedsSettled(t *testing.T, f *deploymentResourceFixture, runID string, versions map[string]string) {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT source_resource_event_name,source_resource_version_id,status,cardinality,cursor
		FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY source_resource_event_name`, runID)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(versions))
	for rows.Next() {
		var declaration, version, status string
		var cardinality, cursor int
		if err := rows.Scan(&declaration, &version, &status, &cardinality, &cursor); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if seen[declaration] || versions[declaration] != version || status != "closed" || cardinality != 1 || cursor != 1 {
			rows.Close()
			t.Fatalf("two-pin feed disagrees with its exact pin: declaration=%q version=%q status=%q cursor=%d/%d", declaration, version, status, cursor, cardinality)
		}
		seen[declaration] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(versions) {
		t.Fatalf("two-pin run has %d feeds, want %d", len(seen), len(versions))
	}
	for _, item := range []struct {
		name, query string
		want        int
	}{
		{"pins", `SELECT COUNT(*) FROM resource_version_pins WHERE run_id=$1`, 2},
		{"outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, 2},
		{"source events", `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('portfolio/account.registered','portfolio/account.notify.requested')`, 2},
		{"pipeline receipts", `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name IN ('portfolio/account.registered','portfolio/account.notify.requested') AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, 2},
		{"settled deliveries", `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name IN ('portfolio/account.registered','portfolio/account.notify.requested') AND d.status='delivered' AND d.continuation_handoff_at IS NOT NULL`, 2},
		{"child instances", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`, 2},
	} {
		var got int
		if err := f.db.QueryRowContext(f.ctx, item.query, runID).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", item.name, err)
		}
		if got != item.want {
			logTwoDeploymentFeedFailure(t, f, runID)
			t.Fatalf("two-pin run %s=%d, want %d", item.name, got, item.want)
		}
	}
}

func logTwoDeploymentFeedFailure(t *testing.T, f *deploymentResourceFixture, runID string) {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT e.event_name,COALESCE(r.outcome,''),COALESCE(r.reason_code,''),COALESCE(CAST(r.failure AS TEXT),''),COALESCE(d.status,''),
		CASE WHEN d.continuation_handoff_at IS NULL THEN 0 ELSE 1 END
		FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
		LEFT JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 ORDER BY e.event_name`, runID)
	if err != nil {
		t.Logf("two-pin diagnostic query: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var eventName, receipt, reason, failure, delivery string
		var handoff bool
		if err := rows.Scan(&eventName, &receipt, &reason, &failure, &delivery, &handoff); err != nil {
			t.Logf("two-pin diagnostic scan: %v", err)
			return
		}
		t.Logf("two-pin diagnostic event=%s receipt=%s reason=%s failure=%s delivery=%s handoff=%t", eventName, receipt, reason, failure, delivery, handoff)
	}
	if err := rows.Err(); err != nil {
		t.Logf("two-pin diagnostic rows: %v", err)
	}
}
