package conformance

import (
	"bytes"
	"context"
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
	"github.com/division-sh/swarm/internal/store/storetest"
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
				version, err := storetest.ReadPortfolioResourcePinStorage(f.ctx, f.selected, runID, item.declaration)
				if err != nil {
					t.Fatal(err)
				}
				versions[item.declaration] = version
			}

			runID := uuid.NewString()
			args := []string{"run", "start", "--connect", server.URL, "--run-id", runID}
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
	rows, err := storetest.ReadPinnedDeploymentFeedsStorage(f.ctx, f.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(versions))
	for _, row := range rows {
		declaration, version, status := row.Declaration, row.Version, row.Status
		cardinality, cursor := row.Cardinality, row.Cursor
		if seen[declaration] || versions[declaration] != version || status != "closed" || cardinality != 1 || cursor != 1 {
			t.Fatalf("two-pin feed disagrees with its exact pin: declaration=%q version=%q status=%q cursor=%d/%d", declaration, version, status, cursor, cardinality)
		}
		seen[declaration] = true
	}
	if len(seen) != len(versions) {
		t.Fatalf("two-pin run has %d feeds, want %d", len(seen), len(versions))
	}
	for _, item := range []struct {
		name string
		read func(context.Context, any, string) (int, error)
		want int
	}{
		{"pins", storetest.CountRunResourcePinsStorage, 2},
		{"outcomes", storetest.CountDeploymentCommittedOutcomesStorage, 2},
		{"source events", storetest.CountPinnedPortfolioSourceEventsStorage, 2},
		{"pipeline receipts", storetest.CountPinnedPortfolioSuccessfulPipelineReceiptsStorage, 2},
		{"settled deliveries", storetest.CountPinnedPortfolioSettledDeliveriesStorage, 2},
		{"child instances", storetest.CountAccountDeploymentReceiversStorage, 2},
	} {
		got, err := item.read(f.ctx, f.selected, runID)
		if err != nil {
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
	rows, err := storetest.ReadPinnedPortfolioDeliveryDiagnosticsStorage(f.ctx, f.selected, runID)
	if err != nil {
		t.Logf("two-pin diagnostic query: %v", err)
		return
	}
	for _, row := range rows {
		t.Logf("two-pin diagnostic event=%s receipt=%s reason=%s failure=%s delivery=%s handoff=%t", row.EventName, row.Receipt, row.Reason, row.Failure, row.Delivery, row.Handoff)
	}
}
