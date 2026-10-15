package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func channelBudgetStorageObservation(t *testing.T, h *channelOnboardingE2EHarness) storetest.ChannelBudgetObservation {
	t.Helper()
	channelAnchorStorageObservation(t, h)
	reader, err := storetest.ObserveChannelBudgetStorage(h.anchorObservation)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

type foreignChannelBudgetInspector struct{ storetest.ChannelObservation }

func requireChannelBudgetDiagnosticManagedEvidence(t *testing.T, reader storetest.ChannelBudgetObservation) {
	t.Helper()
	for name, read := range map[string]func(context.Context) ([]map[string]string, error){
		"spend":         reader.ReadChannelBudgetSpendDiagnosticsStorage,
		"effects":       reader.ReadChannelBudgetEffectsDiagnosticsStorage,
		"agents":        reader.ReadChannelBudgetAgentsDiagnosticsStorage,
		"deliveries":    reader.ReadChannelBudgetDeliveriesDiagnosticsStorage,
		"budget_events": reader.ReadChannelBudgetBudgetEventsDiagnosticsStorage,
		"notices":       reader.ReadChannelBudgetNoticesDiagnosticsStorage,
	} {
		if rows, err := read(context.Background()); err != nil || len(rows) == 0 {
			t.Fatalf("managed spend omitted real %s diagnostic facts: rows=%v err=%v", name, rows, err)
		}
	}
}

func TestChannelBudgetDiagnosticFacetRefusesForeignOwnership(t *testing.T) {
	for _, foreign := range []storetest.ChannelObservation{nil, foreignChannelBudgetInspector{}} {
		if reader, err := storetest.ObserveChannelBudgetStorage(foreign); reader != nil || err == nil {
			t.Fatalf("foreign inspector supplied budget evidence: %v %v", reader, err)
		}
	}
}

func TestChannelBudgetDiagnosticStorageRetainsIndependentCutsAndLifetimeBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var reader storetest.ChannelBudgetObservation
			t.Run("harness", func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				h.start(t)
				defer h.stop(t)
				reader = channelBudgetStorageObservation(t, h)
				for _, read := range []func(context.Context) ([]map[string]string, error){
					reader.ReadChannelBudgetSpendDiagnosticsStorage,
					reader.ReadChannelBudgetEffectsDiagnosticsStorage,
					reader.ReadChannelBudgetAgentsDiagnosticsStorage,
					reader.ReadChannelBudgetDeliveriesDiagnosticsStorage,
					reader.ReadChannelBudgetBudgetEventsDiagnosticsStorage,
					reader.ReadChannelBudgetNoticesDiagnosticsStorage,
				} {
					if _, err := read(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := reader.ReadChannelBudgetEmergencyNoticeStorage(context.Background()); err != sql.ErrNoRows {
					t.Fatalf("successful emergency-notice absence changed: %v", err)
				}
				if count, err := reader.CountChannelBudgetLiveCompletionsStorage(context.Background()); count != 0 || err != nil {
					t.Fatalf("empty live completion count changed: %d %v", count, err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := reader.ReadChannelBudgetEmergencyNoticeStorage(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("cancellation was hidden as absence: %v", err)
				}
			})
			if _, err := reader.ReadChannelBudgetSpendDiagnosticsStorage(context.Background()); err == nil {
				t.Fatal("borrowed diagnostic facet outlived its harness-owned inspector")
			}
		})
	}
}
