package serveapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	"github.com/division-sh/swarm/internal/servedparity"
)

type channelRecoveryUnavailableSpend struct{ budgetspend.Store }

func (channelRecoveryUnavailableSpend) SumSpendUSD(context.Context, budgetspend.SpendQuery) (float64, error) {
	return 0, fmt.Errorf("channel recovery proof: retained budget read unavailable")
}

func TestChannelDeliveryRecoveryNoticePublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			if err := os.WriteFile(filepath.Join(h.opts.SourceRoot, "policy.yaml"), []byte("budget_warning_percent: 50\nbudget_throttle_percent: 75\nbudget_emergency_percent: 90\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(h.opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.opts.ConfigPath, append(body, []byte("\nbudget:\n  system_monthly_cap: 1\n")...), 0o600); err != nil {
				t.Fatal(err)
			}
			h.start(t)
			defer h.stop(t)
			runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "connect", "recovery-token", 1001, "private", 0)
			h.stop(t)
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				persistence := previous(owner)
				persistence.deps.BudgetSpendStore = channelRecoveryUnavailableSpend{Store: persistence.deps.BudgetSpendStore}
				return persistence
			}
			t.Cleanup(func() {
				projectRuntimePersistenceForServe = previous
			})
			failed := startServeRuntimeTestProcess(t, h.opts)
			t.Cleanup(func() {
				_ = failed.stop()
			})
			code, exited := failed.waitForExit(20 * time.Second)
			if !exited || code == 0 || !strings.Contains(failed.outputString(), "retained budget read unavailable") {
				t.Fatalf("actual manager recovery did not fail closed: code=%d exited=%t\n%s", code, exited, failed.outputString())
			}
			projectRuntimePersistenceForServe = previous
			reader := channelProcessStorageObservation(t, h)
			notice, err := reader.ReadChannelRecoveryNoticeStorage(context.Background())
			if err != nil {
				t.Fatalf("actual recovery producer did not persist its notice: %v", err)
			}
			if notice.EntityID != "" || notice.Severity != "critical" || notice.Producer != "runtime" || !strings.Contains(notice.Payload, "startup_manager_recovery_failed") {
				t.Fatalf("recovery notice lost global/critical provenance: entity=%q severity=%q producer=%q payload=%s", notice.EntityID, notice.Severity, notice.Producer, notice.Payload)
			}
			h.start(t)
			messageID := waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), notice.ItemID)
			message := h.provider.Delivery(messageID - 1)
			if fmt.Sprint(message["chat_id"]) != "1001" || !strings.Contains(fmt.Sprint(message["text"]), notice.Summary) {
				t.Fatalf("retained actual recovery notice was not delivered: %v", message)
			}
			count, err := reader.CountChannelRecoveryNoticesStorage(context.Background())
			if err != nil || count != 1 {
				t.Fatalf("healthy restart fabricated another recovery notice: count=%d err=%v", count, err)
			}
		})
	}
}
