package serveapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestStandingDiagnosticsReadOnlyWholeChainBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.opts.SourceRoot = canonicalrouting.CopyRootIngressServedFollowUp(t)
			h.opts.AbandonActiveRuns = false
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			bundle := servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot)
			first := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "first"}, "idempotency_key": "diagnostic-first"})
			second := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "second"}, "idempotency_key": "diagnostic-second"})
			if first.RunID == second.RunID {
				t.Fatal("global diagnostic control requires distinct native runs")
			}
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			owner, err := storetest.OpenReleaseProcessReadOnlyInspection(driver, h.storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			reader := servedInspectionDeliveryReader(t, owner)
			waitServedRunDeliveryQuiescence(t, reader, first.RunID)
			waitServedRunDeliveryQuiescence(t, reader, second.RunID)
			before, err := readStandingDiagnosticStorageSnapshot(owner)
			if err != nil {
				t.Fatal(err)
			}
			text := standingMemoryStoreDiagnostics(driver, h.storeDSN)
			assertStandingThreeSectionDiagnostics(t, owner, text, first.EventID, second.EventID)
			probe := storetest.CollectTransactions(t, owner, storetest.TransactionProbeOptions{})
			if got := renderStandingStoreDiagnostics(context.Background(), owner); got != text {
				t.Fatalf("three native inspection cuts changed diagnostic output: got=%s want=%s", got, text)
			}
			if got := probe.Snapshot(); got.Total.Begun != 3 || got.Total.ReadCommits != 3 || got.Total.WriteCommits != 0 || got.Active != 0 {
				t.Fatalf("diagnostic adopted writer authority or changed three independent cuts: %+v", got)
			}
			assertStandingDiagnosticSnapshotUnchanged(t, owner, before)
			assertStandingDiagnosticLifetimeRefusal(t, owner)
			// Closing independent diagnostics must not retire the served writer.
			requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "item.received", "bundle_hash": bundle, "payload": map[string]any{"item_id": "survivor"}, "idempotency_key": "diagnostic-writer-survives"})
		})
	}
}

func assertStandingThreeSectionDiagnostics(t *testing.T, owner storetest.ReleaseProcessReadOnlyInspection, text string, first, second string) {
	t.Helper()
	for _, wanted := range []string{"item.received=2", "delivery:" + first + ":", "delivery:" + second + ":"} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("direct whole-chain diagnostic omitted %q: %s", wanted, text)
		}
	}
	logs, err := storetest.ReadStandingRuntimeLogDiagnostics(context.Background(), owner)
	if err != nil || len(logs) == 0 {
		t.Fatalf("native diagnostic requires nonempty log evidence: %v %v", logs, err)
	}
	for _, payload := range logs {
		if !strings.Contains(text, payload) {
			t.Fatalf("diagnostic changed backend payload text: %s", payload)
		}
	}
	for _, event := range []string{first, second} {
		found := false
		for _, payload := range logs {
			found = found || strings.Contains(payload, event)
		}
		if !found {
			t.Fatalf("native logs lack multi-run causal evidence for %s", event)
		}
	}
}

func assertStandingDiagnosticLifetimeRefusal(t *testing.T, owner storetest.ReleaseProcessReadOnlyInspection) {
	t.Helper()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := renderStandingStoreDiagnostics(cancelled, owner); !strings.Contains(got, context.Canceled.Error()) {
		t.Fatalf("cancelled diagnostic returned normal output: %s", got)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := renderStandingStoreDiagnostics(context.Background(), owner); got == "" || strings.Contains(got, "item.received=") {
		t.Fatalf("closed inspection returned evidence: %s", got)
	}
}

func readStandingDiagnosticStorageSnapshot(owner storetest.ReleaseProcessReadOnlyInspection) (map[string]storetest.SelectedForkStorageTableSnapshot, error) {
	var out map[string]storetest.SelectedForkStorageTableSnapshot
	err := owner.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		out, err = storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, owner)
		return err
	})
	return out, err
}

func assertStandingDiagnosticSnapshotUnchanged(t *testing.T, owner storetest.ReleaseProcessReadOnlyInspection, before map[string]storetest.SelectedForkStorageTableSnapshot) {
	if after, err := readStandingDiagnosticStorageSnapshot(owner); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("diagnostic changed durable native storage: %v", err)
	}
}

func TestStandingDiagnosticsEmptyDataBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			location := ""
			if backend == "sqlite" {
				location = storetest.StartSQLiteRuntimeStore(t).Path()
			} else {
				location = testutil.StartPostgresDSN(t)
				storetest.StartPostgresRuntimeStoreWithReopen(t, location)
			}
			if got := standingMemoryStoreDiagnostics(backend, location); got != "" {
				t.Fatalf("empty native diagnostic=%q", got)
			}
		})
	}
}

func TestStandingSQLiteDiagnosticsMissingDatabaseStaysMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if got := standingSQLiteDiagnostics(path); got == "" {
		t.Fatal("missing diagnostic database returned success-like empty output")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("diagnostic initialized a missing database: %v", err)
	}
}

func TestStandingDiagnosticsFirstReadFailureDoesNotInitializeOrRepairBothStores(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		location := testutil.StartEmptyPostgresDSN(t)
		if got := standingStoreDiagnostics("postgres", location); !strings.Contains(got, "events") || !strings.Contains(got, "does not exist") {
			t.Fatalf("uninitialized PostgreSQL diagnostic did not render its first-query failure: %s", got)
		}
	})
	for _, sample := range []struct {
		name string
		raw  []byte
	}{{"empty", nil}, {"corrupt", []byte("not a SQLite database")}} {
		t.Run("sqlite/"+sample.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "diagnostic.db")
			if err := os.WriteFile(path, sample.raw, 0600); err != nil {
				t.Fatal(err)
			}
			if got := standingStoreDiagnostics("sqlite", path); got == "" {
				t.Fatal("unreadable diagnostic database returned normal output")
			}
			if got, err := os.ReadFile(path); err != nil || !reflect.DeepEqual(got, append([]byte{}, sample.raw...)) {
				t.Fatalf("diagnostic repaired/initialized its input bytes: %q %v", got, err)
			}
		})
	}
}
