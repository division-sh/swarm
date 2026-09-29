package serveapp

import (
	"context"
	"database/sql"
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
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			// Only the retained budget read fails. The actual startup failure producer,
			// selected-store notice/plan transaction, and later delivery are unchanged.
			failed := startServeRuntimeTestProcess(t, h.opts)
			t.Cleanup(func() { _ = failed.stop() })
			code, exited := failed.waitForExit(20 * time.Second)
			if !exited || code == 0 || !strings.Contains(failed.outputString(), "retained budget read unavailable") {
				t.Fatalf("actual manager recovery did not fail closed: code=%d exited=%t\n%s", code, exited, failed.outputString())
			}
			projectRuntimePersistenceForServe = previous
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			db, err := sql.Open(driver, h.storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var noticeID, entity, severity, producer, payload, summary string
			if err := db.QueryRow(`SELECT item_id,COALESCE(CAST(entity_id AS TEXT),''),severity,from_agent,CAST(payload AS TEXT),summary
				FROM mailbox WHERE item_type='alert' AND summary LIKE 'Runtime recovery failed:%'`).Scan(&noticeID, &entity, &severity, &producer, &payload, &summary); err != nil {
				t.Fatalf("actual recovery producer did not persist its notice: %v", err)
			}
			if entity != "" || severity != "critical" || producer != "runtime" || !strings.Contains(payload, "startup_manager_recovery_failed") {
				t.Fatalf("recovery notice lost global/critical provenance: entity=%q severity=%q producer=%q payload=%s", entity, severity, producer, payload)
			}
			h.start(t)
			messageID := waitChannelAnchorReceipt(t, db, noticeID)
			message := h.provider.Delivery(messageID - 1)
			if fmt.Sprint(message["chat_id"]) != "1001" || !strings.Contains(fmt.Sprint(message["text"]), summary) {
				t.Fatalf("retained actual recovery notice was not delivered: %v", message)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM mailbox WHERE item_type='alert' AND summary LIKE 'Runtime recovery failed:%'`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("healthy restart fabricated another recovery notice: count=%d err=%v", count, err)
			}
		})
	}
}
