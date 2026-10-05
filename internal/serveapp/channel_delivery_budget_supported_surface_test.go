package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelDeliveryBudgetNoticePublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			h.opts.SourceRoot = canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			h.opts.TestLLMRuntime = nil
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
			var providerCalls atomic.Int32
			llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				var body struct {
					Messages []json.RawMessage `json:"messages"`
				}
				if request.URL.Path != "/v1/messages" || request.Header.Get("x-api-key") != "anthropic-key" || json.NewDecoder(request.Body).Decode(&body) != nil || len(body.Messages) == 0 {
					t.Error("budget proof did not use the admitted managed provider request")
					http.Error(w, "invalid managed request", http.StatusBadRequest)
					return
				}
				providerCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"model": "claude-test", "usage": map[string]int{"input_tokens": 1_000_000, "output_tokens": 1},
					"content": []any{map[string]any{"type": "text", "text": "Observed."}},
				})
			}))
			t.Cleanup(llm.Close)
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": llm.URL})
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{"telegram_bot_token": "budget-token", "ANTHROPIC_API_KEY": "anthropic-key"} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			h.start(t)
			defer h.stop(t)
			runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "connect", "budget-token", 1001, "private", 0)
			callback, signing, _ := h.provider.Registration()
			postChannelTelegramUpdate(t, callback, signing, map[string]any{
				"update_id": 839001, "message": map[string]any{
					"message_id": 839001, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": "Observe budget evidence",
				},
			})
			driver := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				driver = "postgres"
			}
			db, err := sql.Open(driver, h.storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			deadline := time.Now().Add(20 * time.Second)
			var noticeID, entity, priority, producer, summary, eventName string
			for {
				err := db.QueryRow(`SELECT m.item_id,COALESCE(CAST(m.entity_id AS TEXT),''),m.severity,m.from_agent,m.summary,e.event_name
					FROM mailbox m JOIN events e ON e.event_id=m.source_event_id
					WHERE m.item_type='alert' AND m.summary LIKE 'Budget emergency:%'`).Scan(&noticeID, &entity, &priority, &producer, &summary, &eventName)
				if err == nil {
					break
				}
				if err != sql.ErrNoRows || time.Now().After(deadline) {
					logBudgetNoticeFailure(t, db, providerCalls.Load())
					t.Fatalf("actual managed spend did not produce an emergency notice: %v", err)
				}
				time.Sleep(20 * time.Millisecond)
			}
			if entity != "" || priority != "critical" || producer != "runtime" || eventName != "platform.budget_threshold_crossed" || !strings.Contains(summary, "scope=system") {
				t.Fatalf("actual budget notice lost global/critical provenance: entity=%q priority=%q producer=%q event=%q summary=%q", entity, priority, producer, eventName, summary)
			}
			messageID := waitChannelAnchorReceipt(t, db, noticeID)
			message := h.provider.Delivery(messageID - 1)
			if fmt.Sprint(message["chat_id"]) != "1001" || !strings.Contains(fmt.Sprint(message["text"]), summary) {
				t.Fatalf("actual budget notice did not reach the selected operator: %v", message)
			}
			var completions int
			if err := db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE cost_usd >= 3 AND execution_mode='live'`).Scan(&completions); err != nil || completions != 1 {
				t.Fatalf("budget notice lacks committed live completion spend: count=%d err=%v", completions, err)
			}
		})
	}
}

// Only scalar lifecycle/accounting evidence is logged, never requests,
// responses, credentials, provider references or authority blobs.
func logBudgetNoticeFailure(t *testing.T, db *sql.DB, providerCalls int32) {
	t.Helper()
	t.Logf("budget notice diagnostic provider_calls=%d", providerCalls)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, query := range []struct{ name, sql string }{
		{"spend", `SELECT execution_mode, CAST(cost_usd AS TEXT) AS cost_usd, agent_id, transport FROM spend_ledger LIMIT 20`},
		{"effects", `SELECT effect_kind, state, COUNT(*) AS count FROM runtime_external_effect_operations GROUP BY effect_kind,state`},
		{"agents", `SELECT agent_id,lifecycle_phase,lifecycle_run_mode,turn_count FROM agents LIMIT 20`},
		{"deliveries", `SELECT subscriber_type,subscriber_id,status,reason_code,retry_count FROM event_deliveries ORDER BY created_at DESC LIMIT 20`},
		{"budget_events", `SELECT event_name,COUNT(*) AS count FROM events WHERE event_name='platform.budget_threshold_crossed' GROUP BY event_name`},
		{"notices", `SELECT item_type,status,severity,COUNT(*) AS count FROM mailbox GROUP BY item_type,status,severity`},
	} {
		rows, err := readChannelDeliveryDiagnosticRows(ctx, db, query.sql)
		t.Logf("budget notice diagnostic %s=%v err=%v", query.name, rows, err)
	}
}
