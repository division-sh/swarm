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
	"github.com/division-sh/swarm/internal/store/storetest"
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
				_ = json.NewEncoder(w).Encode(map[string]any{"model": "claude-test", "usage": map[string]int{"input_tokens": 1_000_000, "output_tokens": 1}, "content": []any{map[string]any{"type": "text", "text": "Observed."}}})
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
			postChannelTelegramUpdate(t, callback, signing, map[string]any{"update_id": 839001, "message": map[string]any{"message_id": 839001, "from": map[string]any{"id": 2001}, "chat": map[string]any{"id": 2001, "type": "private"}, "text": "Observe budget evidence"}})
			reader := channelBudgetStorageObservation(t, h)
			deadline := time.Now().Add(20 * time.Second)
			var noticeID, entity, priority, producer, summary, eventName string
			for {
				err := func() error {
					out, err := reader.ReadChannelBudgetEmergencyNoticeStorage(context.Background())
					noticeID, entity, priority, producer, summary, eventName = out.ItemID, out.EntityID, out.Severity, out.FromAgent, out.Summary, out.EventName
					return err
				}()
				if err == nil {
					break
				}
				if err != sql.ErrNoRows || time.Now().After(deadline) {
					logBudgetNoticeFailure(t, reader, providerCalls.Load())
					t.Fatalf("actual managed spend did not produce an emergency notice: %v", err)
				}
				time.Sleep(20 * time.Millisecond)
			}
			if entity != "" || priority != "critical" || producer != "runtime" || eventName != "platform.budget_threshold_crossed" || !strings.Contains(summary, "scope=system") {
				t.Fatalf("actual budget notice lost global/critical provenance: entity=%q priority=%q producer=%q event=%q summary=%q", entity, priority, producer, eventName, summary)
			}
			messageID := waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), noticeID)
			message := h.provider.Delivery(messageID - 1)
			if fmt.Sprint(message["chat_id"]) != "1001" || !strings.Contains(fmt.Sprint(message["text"]), summary) {
				t.Fatalf("actual budget notice did not reach the selected operator: %v", message)
			}
			var completions int
			if err := func() error {
				value, err := reader.CountChannelBudgetLiveCompletionsStorage(context.Background())
				completions = value
				return err
			}(); err != nil || completions != 1 {
				t.Fatalf("budget notice lacks committed live completion spend: count=%d err=%v", completions, err)
			}
			requireChannelBudgetDiagnosticManagedEvidence(t, reader)
		})
	}
}

// Only scalar lifecycle/accounting evidence is logged, never requests,
// responses, credentials, provider references or authority blobs.
func logBudgetNoticeFailure(t *testing.T, reader storetest.ChannelBudgetObservation, providerCalls int32) {
	t.Helper()
	t.Logf("budget notice diagnostic provider_calls=%d", providerCalls)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, query := range []struct {
		name string
		read func(context.Context) ([]map[string]string, error)
	}{{"spend", reader.ReadChannelBudgetSpendDiagnosticsStorage}, {"effects", reader.ReadChannelBudgetEffectsDiagnosticsStorage}, {"agents", reader.ReadChannelBudgetAgentsDiagnosticsStorage}, {"deliveries", reader.ReadChannelBudgetDeliveriesDiagnosticsStorage}, {"budget_events", reader.ReadChannelBudgetBudgetEventsDiagnosticsStorage}, {"notices", reader.ReadChannelBudgetNoticesDiagnosticsStorage}} {
		rows, err := query.read(ctx)
		t.Logf("budget notice diagnostic %s=%v err=%v", query.name, rows, err)
	}
}
