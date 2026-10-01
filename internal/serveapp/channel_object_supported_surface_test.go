package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"gopkg.in/yaml.v3"
)

func TestChannelLearnedObjectPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, scope := range []string{"direct", "shared"} {
			t.Run(string(backend)+"/"+scope, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				provider := &objectChannelProvider{commands: map[string][]any{}, calls: map[string][]map[string]any{}}
				server := httptest.NewServer(provider)
				t.Cleanup(server.Close)
				redirectExternalHosts(t, map[string]string{"mock.example.test": server.URL})
				h.opts.SourceRoot = writeObjectChannelSource(t, h.opts.ConfigPath)
				h.opts.AbandonActiveRuns = false
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				driver := "sqlite"
				if backend == servedparity.BackendExplicitPostgres {
					driver = "postgres"
				}
				db, err := sql.Open(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				var begun channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "mock", "verb": "connect", "provider_credential": "object-provider-secret",
					"save_proof": true, "client_language": "fr",
				}, &begun)
				if begun.IdentityOperation == nil {
					t.Fatalf("public connect lacks its real identity ceremony: %+v", begun)
				}
				provider.mu.Lock()
				callback, signing := provider.callback, provider.signing
				provider.mu.Unlock()
				if callback == "" || signing == "" {
					t.Fatal("public connect did not execute registration")
				}
				objectChannelIngress(t, callback, signing, "claim-1", map[string]any{
					"text": begun.IdentityOperation.Challenge, "principal": "operator-a", "room": "queue-a",
					"scope": scope, "message": map[string]any{"id": "claim-1"},
				})
				claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
				if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
					t.Fatalf("real object claim not admitted: %+v", claimed)
				}
				var confirmed map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
				}, &confirmed)
				ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				if ready.Readiness == nil || !ready.Readiness.Ready {
					t.Fatalf("learned object connect cannot complete confirmation/publication/native qualification: %+v", ready)
				}
				waitObjectNativeQualification(t, h, begun.Operation.OperationID)
				provider.mu.Lock()
				confirmationCorrect := len(provider.deliveries) > 0 && provider.deliveries[0]["queue"] == "queue-a"
				install, read := "/v2/install", "/v2/read"
				if scope == "shared" {
					install += "_shared"
					read += "_shared"
				}
				installed := provider.calls[install]
				var command string
				if len(installed) == 1 {
					commands, _ := installed[0]["commands"].([]any)
					if len(commands) == 1 {
						row, _ := commands[0].(map[string]any)
						command, _ = row["command"].(string)
					}
				}
				provider.mu.Unlock()
				if !confirmationCorrect || command == "" {
					provider.mu.Lock()
					diagnostic := fmt.Sprintf("deliveries=%#v installed=%#v", provider.deliveries, installed)
					provider.mu.Unlock()
					t.Fatalf("public learned confirmation/native install did not execute: %s", diagnostic)
				}
				assertObjectNativeReads(t, provider, read, scope)
				for _, probe := range []struct{ id, principal, address string }{
					{"wrong-member", "customer-b", "mock_bot"}, {"wrong-address", "operator-a", "other_bot"},
				} {
					objectChannelIngress(t, callback, signing, probe.id, map[string]any{
						"text": "/" + command, "principal": probe.principal, "room": "queue-a", "scope": scope,
						"message": map[string]any{"id": probe.id}, "entry": map[string]any{"reference": command, "address": probe.address},
					})
					waitObjectChannelDisposition(t, db, "operator_channel_text_intents", probe.id, "entry_rejected")
				}
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"detail": strings.Repeat("object-owned detail;", 60)}, "idempotency_key": "object-card",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				message := waitObjectCardReceipt(t, db, card)
				objectChannelIngress(t, callback, signing, "native-entry-1", map[string]any{
					"text": "/" + command, "principal": "operator-a", "room": "queue-a", "scope": scope, "message": map[string]any{"id": "entry-1"},
					"entry": map[string]any{"reference": command, "address": "mock_bot"},
				})
				postAction := func(id, reference, token string) {
					t.Helper()
					fact := map[string]any{"token": token, "cursor": id, "principal": "operator-a", "room": "queue-a", "scope": scope, "message": map[string]any{"id": reference}}
					objectChannelIngress(t, callback, signing, id, fact)
					objectChannelIngress(t, callback, signing, id, fact)
					waitObjectChannelEffect(t, provider, "/v2/ack", func(input map[string]any) bool { return input["cursor"] == id })
				}
				pages := proveObjectChannelPages(t, db, provider, card, message, postAction)
				more := waitObjectMessageControl(t, provider, message, "More choices")
				postAction("choices-1", message, more)
				waitObjectMessageControl(t, provider, message, "approve")
				more = waitObjectMessageControl(t, provider, message, "More choices")
				postAction("choices-2", message, more)
				reject := waitObjectMessageControl(t, provider, message, "reject")
				postAction("reject-1", message, reject)
				waitObjectMessageText(t, provider, message, "Input: reason (text) required")
				waitChannelDraftPromptSettlement(t, db, card)
				h.stop(t)
				h.start(t)
				restarted := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
				if restarted.Readiness == nil || !restarted.Readiness.Ready {
					t.Fatalf("retained object activation lost READY: %+v", restarted)
				}
				waitObjectNativeQualification(t, h, begun.Operation.OperationID)
				provider.mu.Lock()
				installCount, ackCount := len(provider.calls[install]), len(provider.calls["/v2/ack"])
				provider.mu.Unlock()
				if installCount != 1 || ackCount != 3+pages {
					t.Fatalf("restart/duplicate replayed effects: install=%d ack=%d", installCount, ackCount)
				}
				assertObjectNativeReads(t, provider, read, scope)
				provider.mu.Lock()
				callback, signing = provider.callback, provider.signing
				provider.mu.Unlock()
				objectChannelIngress(t, callback, signing, "answer-1", map[string]any{
					"text": "private-object-reason", "principal": "operator-a", "room": "queue-a", "scope": scope,
					"message": map[string]any{"id": "answer-1"}, "reply": map[string]any{"id": message},
				})
				waitUncertainCopyDecision(t, db, card)
				waitObjectMessageText(t, provider, message, "Decision: reject")
				for _, restart := range []bool{false, true} {
					if restart {
						h.stop(t)
						h.start(t)
					}
					provider.mu.Lock()
					callback, signing = provider.callback, provider.signing
					provider.mu.Unlock()
					id := fmt.Sprintf("stale-%t", restart)
					stale := map[string]any{"token": reject, "cursor": id, "principal": "operator-a", "room": "queue-a", "scope": scope, "message": map[string]any{"id": message}}
					objectChannelIngress(t, callback, signing, id, stale)
					objectChannelIngress(t, callback, signing, id, stale)
					waitObjectChannelDisposition(t, db, "operator_channel_action_intents", id, "stale", "rejected")
					objectChannelIngress(t, callback, signing, id+"-text", map[string]any{
						"text": "obsolete-private-answer", "principal": "operator-a", "room": "queue-a", "scope": scope,
						"message": map[string]any{"id": id + "-text"}, "reply": map[string]any{"id": message},
					})
					waitObjectChannelDisposition(t, db, "operator_channel_text_intents", id+"-text", "teaching")
				}
				var decisions int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 1 {
					t.Fatalf("object duplicate/stale controls changed decisions: %d %v", decisions, err)
				}
				proveObjectChannelRecovery(t, h, db, provider, scope, command, identity.SourceArtifacts[0].BundleHash, postAction)
				proveObjectChannelRebindRetirement(t, h, db, provider, scope, command, begun.Operation.OperationID)
				provider.mu.Lock()
				defer provider.mu.Unlock()
				for _, input := range provider.calls["/v2/edit"] {
					if strings.Contains(fmt.Sprint(input["body"]), "private-object-reason") || strings.Contains(fmt.Sprint(input["body"]), "obsolete-private-answer") {
						t.Fatal("quoted private answer leaked into provider presentation")
					}
				}
			})
		}
	}
}

func proveObjectChannelRecovery(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, p *objectChannelProvider, scope, command, bundle string, post func(string, string, string)) {
	t.Helper()
	p.mu.Lock()
	p.loseNextCard = true
	p.mu.Unlock()
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": bundle, "payload": map[string]any{"detail": strings.Repeat("uncertain object detail;", 60)}, "idempotency_key": "object-recovery-card",
	})
	card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
	waitUncertainCopyPlan(t, db, card)
	p.mu.Lock()
	callback, signing := p.callback, p.signing
	before := len(p.deliveries)
	lostReference := p.lostReference
	p.mu.Unlock()
	if lostReference == "" {
		t.Fatal("object provider did not apply the deliberately unacknowledged send")
	}
	objectChannelIngress(t, callback, signing, "recover-entry", map[string]any{
		"text": "/" + command, "principal": "operator-a", "room": "queue-a", "scope": scope, "message": map[string]any{"id": "recover-entry"}, "entry": map[string]any{"reference": command, "address": "mock_bot"},
	})
	deadline := time.Now().Add(15 * time.Second)
	var reference, token string
	for token == "" {
		p.mu.Lock()
		for ordinal, delivery := range p.deliveries[before:] {
			controls, _ := delivery["controls"].([]any)
			for _, control := range controls {
				row, _ := control.(map[string]any)
				if strings.HasPrefix(fmt.Sprint(row["name"]), "Resend card") {
					token, _ = row["value"].(string)
					reference = fmt.Sprintf("delivery-%d", before+ordinal+1)
				}
			}
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("object recovery inbox has no explicit fresh-copy control")
		}
		if token == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	post("object-resend", reference, token)
	fresh := waitObjectCardReceipt(t, db, card)
	if fresh == lostReference {
		t.Fatal("fresh copy borrowed the uncertain predecessor's reference")
	}
	more := waitObjectMessageControl(t, p, fresh, "More choices")
	post("fresh-choices", fresh, more)
	approve := waitObjectMessageControl(t, p, fresh, "approve")
	post("fresh-approve", fresh, approve)
	waitChannelAnchorDecision(t, db, card)
	waitObjectMessageText(t, p, fresh, "Decision: approve")
	var copies, uncertain, decisions int
	if err := db.QueryRow(`SELECT COUNT(*),SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END) FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1`, card).Scan(&copies, &uncertain); err != nil || copies != 2 || uncertain != 1 {
		t.Fatalf("object resend settled/replayed predecessor: %d/%d %v", copies, uncertain, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 1 {
		t.Fatalf("object fresh-copy decision cardinality: %d %v", decisions, err)
	}
}

func proveObjectChannelRebindRetirement(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, p *objectChannelProvider, scope, oldCommand, oldOperation string) {
	t.Helper()
	var begun channelonboarding.Result
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
		"provider": "mock", "verb": "rebind", "provider_credential": "object-provider-secret", "save_proof": true, "client_language": "fr",
	}, &begun)
	if begun.IdentityOperation == nil {
		t.Fatalf("object rebind has no ceremony: %+v", begun)
	}
	p.mu.Lock()
	callback, signing := p.callback, p.signing
	p.mu.Unlock()
	objectChannelIngress(t, callback, signing, "rebind-claim", map[string]any{
		"text": begun.IdentityOperation.Challenge, "principal": "operator-a", "room": "queue-b", "scope": scope, "message": map[string]any{"id": "rebind-claim"},
	})
	claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
		t.Fatalf("object rebind claim unavailable: %+v", claimed)
	}
	var result map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
		"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
	}, &result)
	ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if ready.Readiness == nil || !ready.Readiness.Ready {
		t.Fatalf("object rebind not READY: %+v", ready)
	}
	waitObjectNativeQualification(t, h, begun.Operation.OperationID)
	install := "/v2/install"
	if scope == "shared" {
		install += "_shared"
	}
	p.mu.Lock()
	var command string
	for _, call := range p.calls[install] {
		if call["queue"] == "queue-b" {
			commands, _ := call["commands"].([]any)
			if len(commands) == 1 {
				row, _ := commands[0].(map[string]any)
				command, _ = row["command"].(string)
			}
		}
	}
	before := len(p.deliveries)
	callback, signing = p.callback, p.signing
	p.mu.Unlock()
	if command == "" {
		t.Fatal("rebind did not install the new object destination")
	}
	objectChannelIngress(t, callback, signing, "old-native", map[string]any{
		"text": "/" + oldCommand, "principal": "operator-a", "room": "queue-a", "scope": scope, "message": map[string]any{"id": "old-native"}, "entry": map[string]any{"reference": oldCommand, "address": "mock_bot"},
	})
	waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "old-native", "entry_rejected")
	objectChannelIngress(t, callback, signing, "new-native", map[string]any{
		"text": "/" + command, "principal": "operator-a", "room": "queue-b", "scope": scope, "message": map[string]any{"id": "new-native"}, "entry": map[string]any{"reference": command, "address": "mock_bot"},
	})
	waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "new-native", "entry")
	p.mu.Lock()
	for _, delivery := range p.deliveries[before:] {
		if delivery["queue"] != "queue-b" {
			p.mu.Unlock()
			t.Fatalf("rebind sent authorized output to obsolete destination: %#v", delivery)
		}
	}
	p.mu.Unlock()
	var retained string
	if err := db.QueryRow(`SELECT conversation_reference FROM connected_channel_activations WHERE operation_id=$1`, oldOperation).Scan(&retained); err != nil || retained != `{"room":"queue-a"}` {
		t.Fatalf("rebind changed retained predecessor source: %s %v", retained, err)
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.unbind", map[string]any{
		"interface": ready.Operation.Interface.Selector, "expected_revision": ready.Binding.Revision, "idempotency_key": "object-unbind",
	}, &result)
	var current int
	if err := db.QueryRow(`SELECT COUNT(*) FROM connected_channel_activations WHERE status='current'`).Scan(&current); err != nil || current != 0 {
		t.Fatalf("unbind retained execution authority: %d %v", current, err)
	}
	h.stop(t)
	h.start(t)
	retired := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if retired.Readiness != nil && retired.Readiness.Ready {
		t.Fatal("retired object activation became executable on restart")
	}
	p.mu.Lock()
	count := len(p.calls[install])
	p.mu.Unlock()
	if count != 2 {
		t.Fatalf("retirement/restart reinstalled obsolete native setting: %d", count)
	}
}

func waitObjectChannelDisposition(t *testing.T, db *sql.DB, table, occurrence string, allowed ...string) {
	t.Helper()
	queries := map[string]string{
		"operator_channel_action_intents": `SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE provider_event_id=$1`,
		"operator_channel_text_intents":   `SELECT state,COALESCE(disposition,'') FROM operator_channel_text_intents WHERE provider_event_id=$1`,
	}
	query, ok := queries[table]
	if !ok {
		t.Fatal("unknown object intent table")
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var state, disposition string
		err := db.QueryRow(query, occurrence).Scan(&state, &disposition)
		if err == nil && state == "settled" {
			for _, want := range allowed {
				if disposition == want {
					return
				}
			}
			t.Fatalf("object occurrence %s settled %s, want %v", occurrence, disposition, allowed)
		}
		if err != nil && err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("object occurrence %s not settled: %s/%s %v", occurrence, state, disposition, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveObjectChannelPages(t *testing.T, db *sql.DB, p *objectChannelProvider, card, reference string, post func(string, string, string)) int {
	t.Helper()
	frozen := readChannelHistoricalRender(t, db, card)
	var input struct {
		FullText string `json:"full_text"`
	}
	if err := json.Unmarshal([]byte(frozen.input), &input); err != nil {
		t.Fatal(err)
	}
	token := waitObjectMessageControl(t, p, reference, "View full")
	var complete strings.Builder
	for index := 1; ; index++ {
		post(fmt.Sprintf("page-%d", index), reference, token)
		deadline := time.Now().Add(15 * time.Second)
		var page map[string]any
		for page == nil {
			p.mu.Lock()
			for ordinal, delivery := range p.deliveries {
				if strings.HasPrefix(fmt.Sprint(delivery["body"]), fmt.Sprintf("Page %d/", index)) {
					page = delivery
					reference = fmt.Sprintf("delivery-%d", ordinal+1)
					break
				}
			}
			p.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatalf("object full page %d missing", index)
			}
			if page == nil {
				time.Sleep(20 * time.Millisecond)
			}
		}
		parts := strings.SplitN(fmt.Sprint(page["body"]), "\n", 2)
		if len(parts) != 2 {
			t.Fatalf("page missing body: %#v", page)
		}
		complete.WriteString(parts[1])
		token = ""
		controls, _ := page["controls"].([]any)
		for _, control := range controls {
			row, _ := control.(map[string]any)
			if row["name"] == "Next page" {
				token, _ = row["value"].(string)
			}
		}
		if token == "" {
			if index < 2 || complete.String() != input.FullText {
				t.Fatalf("object pages changed immutable full text: pages=%d", index)
			}
			assertChannelHistoricalRender(t, db, card, frozen)
			return index
		}
	}
}

func waitObjectCardReceipt(t *testing.T, db *sql.DB, card string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var raw string
		err := db.QueryRow(`SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id AND p.current_receipt_operation_id=r.effect_operation_id WHERE p.source_kind='card' AND p.source_id=$1 AND p.state='sent' AND r.state='sent' ORDER BY r.settled_at DESC LIMIT 1`, card).Scan(&raw)
		if err == nil {
			var value struct {
				Reference struct {
					ID string `json:"id"`
				} `json:"delivery_reference"`
			}
			if err := json.Unmarshal([]byte(raw), &value); err != nil || value.Reference.ID == "" {
				t.Fatalf("invalid object receipt %s: %v", raw, err)
			}
			return value.Reference.ID
		}
		if err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("object card not delivered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *objectChannelProvider) message(reference string) map[string]any {
	var result map[string]any
	for index, input := range p.deliveries {
		if fmt.Sprintf("delivery-%d", index+1) == reference {
			result = input
		}
	}
	for _, input := range p.calls["/v2/edit"] {
		value, _ := input["reference"].(map[string]any)
		if value["id"] == reference {
			result = input
		}
	}
	return result
}

func waitObjectMessageControl(t *testing.T, p *objectChannelProvider, reference, label string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		p.mu.Lock()
		input := p.message(reference)
		controls, _ := input["controls"].([]any)
		for _, control := range controls {
			row, _ := control.(map[string]any)
			if row["name"] == label {
				token, _ := row["value"].(string)
				p.mu.Unlock()
				return token
			}
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("object message %s lacks %s: %#v", reference, label, input)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObjectMessageText(t *testing.T, p *objectChannelProvider, reference, text string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		p.mu.Lock()
		input := p.message(reference)
		body := fmt.Sprint(input["body"])
		p.mu.Unlock()
		if strings.Contains(body, text) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("object message %s lacks %s: %s", reference, text, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObjectNativeQualification(t *testing.T, h *channelOnboardingE2EHarness, operation string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		current := getChannelOnboardingRPC(t, h, operation)
		if current.Readiness != nil && current.Readiness.NativeInbox != nil && current.Readiness.NativeInbox.State == "qualified" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	current := getChannelOnboardingRPC(t, h, operation)
	t.Fatalf("native qualification did not complete: readiness=%+v", current.Readiness)
}

func assertObjectNativeReads(t *testing.T, provider *objectChannelProvider, operation, scope string) {
	t.Helper()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	selected, fallback := false, false
	for _, input := range provider.calls[operation] {
		if input["queue"] != "queue-a" {
			t.Fatalf("wrong object destination: %#v", input)
		}
		if scope == "shared" {
			member, ok := input["member"].(map[string]any)
			if !ok || member["principal"] != "operator-a" {
				t.Fatalf("shared read lost member object: %#v", input)
			}
		}
		selected = selected || input["language"] == "fr"
		fallback = fallback || input["language"] == ""
	}
	if !selected || !fallback {
		t.Fatalf("actual selected/fallback reads missing: %#v", provider.calls[operation])
	}
}

func waitObjectChannelEffect(t *testing.T, provider *objectChannelProvider, operation string, matches func(map[string]any) bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		provider.mu.Lock()
		for _, input := range provider.calls[operation] {
			if matches(input) {
				provider.mu.Unlock()
				return
			}
		}
		provider.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	t.Fatalf("missing real %s effect: %#v", operation, provider.calls[operation])
}

func objectChannelIngress(t *testing.T, callback, signing, id string, data map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"delivery": id, "data": data})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, callback, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Mock-Proof", signing)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		t.Fatalf("signed object ingress status=%d", response.StatusCode)
	}
}

// The provider uses workspace/queue/principal objects and its own HTTP protocol,
// not Telegram update shapes or scalarized channel references.
type objectChannelProvider struct {
	mu                sync.Mutex
	callback, signing string
	deliveries        []map[string]any
	commands          map[string][]any
	calls             map[string][]map[string]any
	loseNextCard      bool
	lostReference     string
	loseNextPrompt    bool
	loseResponseEdit  string
	businessResponse  *objectChannelResponsePause
}

func (p *objectChannelProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	input := map[string]any{}
	if r.Method != http.MethodGet {
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
	}
	p.calls[r.URL.Path] = append(p.calls[r.URL.Path], input)
	output := map[string]any{}
	switch r.URL.Path {
	case "/v2/identify":
		output["resource"] = "workspace-a"
	case "/v2/register":
		p.callback, _ = input["callback"].(string)
		p.signing, _ = input["proof"].(string)
	case "/v2/registration":
		output["callback"] = p.callback
	case "/v2/deliver":
		if !objectChannelPresentationValid(input) {
			http.Error(w, "presentation violates compiled bounds", 400)
			return
		}
		p.deliveries = append(p.deliveries, input)
		reference := fmt.Sprintf("delivery-%d", len(p.deliveries))
		output["reference"] = map[string]any{"id": reference}
		if p.loseNextCard && strings.Contains(fmt.Sprint(input["body"]), "Gate: reviews / review") {
			p.loseNextCard = false
			p.lostReference = reference
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
	case "/v2/edit":
		if !objectChannelPresentationValid(input) {
			http.Error(w, "presentation violates compiled bounds", 400)
			return
		}
		output["receipt"] = input["reference"]
		reference, _ := input["reference"].(map[string]any)
		loseResponse := p.loseResponseEdit != "" && reference["id"] == p.loseResponseEdit
		if loseResponse || p.loseNextPrompt && strings.Contains(fmt.Sprint(input["body"]), "Input: reason (text) required") {
			p.loseNextPrompt = false
			p.loseResponseEdit = ""
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
	case "/v2/ack":
	case "/v2/install", "/v2/install_shared":
		key := fmt.Sprint(input["queue"]) + ":" + fmt.Sprint(input["member"])
		p.commands[key], _ = input["commands"].([]any)
		output["accepted"] = true
	case "/v2/read", "/v2/read_shared":
		key := fmt.Sprint(input["queue"]) + ":" + fmt.Sprint(input["member"])
		commands := p.commands[key]
		if commands == nil || input["language"] != "" {
			commands = []any{}
		}
		output["commands"] = commands
	case "/v2/address":
		output["address"] = "mock_bot"
	case "/v2/launcher":
		output["mode"] = "commands"
	case "/v2/default_launcher":
		output["mode"] = "default"
	default:
		http.Error(w, "unknown mock operation", 404)
		return
	}
	if pause := p.businessResponse; pause != nil && r.URL.Path == "/v2/deliver" && input["queue"] == "queue-b" {
		p.businessResponse = nil
		close(pause.arrived)
		p.mu.Unlock()
		select {
		case <-pause.release:
		case <-r.Context().Done():
		}
		p.mu.Lock()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(output)
}

func objectChannelPresentationValid(input map[string]any) bool {
	text, ok := input["body"].(string)
	controls, array := input["controls"].([]any)
	if !ok || !array || len([]rune(text)) > 512 || len(controls) > 2 {
		return false
	}
	for _, control := range controls {
		row, ok := control.(map[string]any)
		if !ok || len([]rune(fmt.Sprint(row["name"]))) > 24 {
			return false
		}
	}
	return true
}

func writeObjectChannelSource(t *testing.T, configPath string) string {
	t.Helper()
	root := canonicalrouting.CopyChannelLearnedObjectJourney(t)
	return writeObjectChannelPacks(t, configPath, root)
}

func writeObjectChannelPacks(t *testing.T, configPath, root string) string {
	t.Helper()
	_, dirs := packfixture.DevelopmentBase(t, nil)
	// Development overrides replace the finite base inventory. Pack IDs remain
	// exact dependency coordinates; the authored provider and protocol are mock.
	for kind, body := range objectChannelPackBodies(t) {
		id := map[string]string{packmodel.TypeTrigger: "provider.telegram", packmodel.TypeConnector: "provider.telegram.connector", packmodel.TypeChannel: "provider.telegram.hitl_channel"}[kind]
		envelope := packmodel.Envelope{ID: id, Version: "0.1.0", PlatformVersion: ">=0.7.0 <0.8.0", Type: kind,
			Provenance: packmodel.Provenance{Source: "platform"}, ManifestHash: packmodel.ManifestHash(body),
			Capabilities: packmodel.Capabilities{Cannot: []string{"bypass admission"}}, Tests: []string{"channel/object_lifecycle"},
		}
		if kind == packmodel.TypeChannel {
			envelope.Implements = []string{"swarm.hitl-channel/v2"}
			envelope.Requires.Packs = map[string]string{"trigger": "provider.telegram", "connector": "provider.telegram.connector"}
		}
		if kind == packmodel.TypeTrigger {
			var manifest providertriggers.Manifest
			if err := yaml.Unmarshal(body, &manifest); err != nil {
				t.Fatal(err)
			}
			envelope.Capabilities = providertriggers.DerivedCapabilities(manifest)
			envelope.Requires = providertriggers.DerivedRequires(manifest)
		}
		if kind == packmodel.TypeConnector {
			var connector providerconnectors.ConnectorManifest
			if err := yaml.Unmarshal(body, &connector); err != nil {
				t.Fatal(err)
			}
			envelope.Capabilities = providerconnectors.DerivedCapabilities(connector)
			envelope.Requires = providerconnectors.DerivedRequires(connector)
		}
		dir := ""
		for _, candidate := range dirs {
			if filepath.Base(candidate) == id {
				dir = candidate
			}
		}
		if dir == "" {
			t.Fatalf("missing finite development override coordinate %s", id)
		}
		raw, err := yaml.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, filepath.Join(dir, "pack.yaml"), string(raw))
		writeStandingCandidateFile(t, filepath.Join(dir, packartifact.ManifestFileNameForType(kind)), string(body))
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	config["platform"] = map[string]any{"packs": map[string]any{"platform_dirs": dirs}}
	raw, err = yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func objectChannelPackBodies(t *testing.T) map[string][]byte {
	t.Helper()
	str := func(min, max int) map[string]any {
		return map[string]any{"type": "string", "minLength": min, "maxLength": max}
	}
	obj := func(properties map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "additionalProperties": false, "properties": properties, "required": required}
	}
	array := func(min, max int, item any) map[string]any {
		return map[string]any{"type": "array", "minItems": min, "maxItems": max, "items": item}
	}
	room, principal := str(1, 10), str(1, 20)
	reference := obj(map[string]any{"id": str(1, 24)}, "id")
	token := str(1, 64)
	token["pattern"] = "^[A-Za-z0-9_-]+$"
	command := obj(map[string]any{"command": str(1, 32), "description": str(1, 256)}, "command", "description")
	controls := array(0, 2, obj(map[string]any{"name": str(1, 24), "value": token}, "name", "value"))
	operations, tools := map[string]any{}, map[string]any{}
	tool := func(name, effect string, fields map[string]any, output map[string]any, body map[string]any) {
		required := make([]string, 0, len(fields))
		for name := range fields {
			required = append(required, name)
		}
		sort.Strings(required)
		tools["mock."+name] = map[string]any{"category": "provider_connector", "description": "object channel " + name, "handler_type": "http", "effect_class": effect,
			"credentials": []string{"mock_api_key"}, "input_schema": obj(fields, required...), "output_schema": output,
			"http":             map[string]any{"method": "POST", "url": "https://mock.example.test/v2/" + name, "headers": map[string]any{"X-Mock-Key": "{{credentials.mock_api_key}}"}, "body": body},
			"response_success": map[string]any{"kind": "http_status_2xx"},
		}
		if effect == "read_only" {
			tools["mock."+name].(map[string]any)["category"] = "provider_registration"
		}
	}
	operation := func(name, toolName string, input map[string]any, output map[string]any) {
		operations[name] = map[string]any{"tool": "mock." + toolName, "input": input, "output": output}
	}
	for _, name := range []string{"deliver", "edit"} {
		fields := map[string]any{"queue": room, "body": str(1, 512), "controls": controls}
		body := map[string]any{"queue": "{{input.queue}}", "body": "{{input.body}}", "controls": "{{input.controls}}"}
		input := map[string]any{"queue": "context.destination.queue", "body": "input.presentation.text", "controls": map[string]any{"each": "input.actions", "item": []any{map[string]any{"name": "item.label", "value": "item.token"}}}}
		outputKey, interfaceKey := "reference", "delivery_reference"
		if name == "edit" {
			fields["reference"] = reference
			body["reference"] = "{{input.reference}}"
			input["reference"] = "input.delivery_reference"
			outputKey, interfaceKey = "receipt", "delivery_receipt"
		}
		tool(name, "non_idempotent_write", fields, obj(map[string]any{outputKey: reference}, outputKey), body)
		operation(name, name, input, map[string]any{interfaceKey + ".id": "result." + outputKey + ".id"})
	}
	tool("ack", "non_idempotent_write", map[string]any{"cursor": str(1, 24)}, obj(map[string]any{}), map[string]any{"cursor": "{{input.cursor}}"})
	operation("acknowledge_interaction", "ack", map[string]any{"cursor": "input.interaction_reference.cursor"}, nil)
	for _, shared := range []bool{false, true} {
		for _, read := range []bool{false, true} {
			name, iface, effect := "install", "install_inbox_entry", "non_idempotent_write"
			fields, input, body := map[string]any{"queue": room}, map[string]any{"queue": "context.destination.queue"}, map[string]any{"queue": "{{input.queue}}"}
			output, projection := obj(map[string]any{"accepted": map[string]any{"type": "boolean"}}, "accepted"), map[string]any{}
			if read {
				name, iface, effect = "read", "read_inbox_entry", "read_only"
				fields["language"] = map[string]any{"type": "string", "enum": []string{"", "en", "fr"}}
				input["language"], body["language"] = "input.language_code", "{{input.language}}"
				output = obj(map[string]any{"commands": array(0, 100, command)}, "commands")
				projection["commands"] = "result.commands"
			} else {
				fields["commands"] = array(1, 1, command)
				input["commands"], body["commands"] = "input.commands", "{{input.commands}}"
			}
			if shared {
				name += "_shared"
				if read {
					iface = "read_shared_inbox_entry"
				} else {
					iface = "install_shared_inbox_entry"
				}
				fields["member"] = obj(map[string]any{"principal": principal}, "principal")
				input["member.principal"], body["member"] = "input.member_reference.principal", "{{input.member}}"
			}
			tool(name, effect, fields, output, body)
			operation(iface, name, input, projection)
		}
	}
	tool("address", "read_only", map[string]any{}, obj(map[string]any{"address": str(5, 32)}, "address"), map[string]any{})
	operation("identify_inbox_address", "address", nil, map[string]any{"address_reference": "result.address"})
	for _, name := range []string{"launcher", "default_launcher"} {
		fields, input, body := map[string]any{}, map[string]any{}, map[string]any{}
		if name == "launcher" {
			fields["queue"] = room
			input["queue"], body["queue"] = "context.destination.queue", "{{input.queue}}"
		}
		tool(name, "read_only", fields, obj(map[string]any{"mode": map[string]any{"type": "string", "enum": []string{"commands", "default", "web_app"}}}, "mode"), body)
		iface := "read_inbox_launcher"
		if name == "default_launcher" {
			iface = "read_default_inbox_launcher"
		}
		operation(iface, name, input, map[string]any{"launcher": "result.mode"})
	}
	for _, name := range []string{"identify", "register", "registration"} {
		fields, body, output := map[string]any{}, map[string]any{}, obj(map[string]any{})
		effect := "read_only"
		if name == "identify" {
			output = obj(map[string]any{"resource": str(1, 64)}, "resource")
		}
		if name == "registration" {
			output = obj(map[string]any{"callback": str(1, 512)}, "callback")
		}
		if name == "register" {
			effect = "non_idempotent_write"
			fields["callback"] = str(1, 512)
			body = map[string]any{"callback": "{{input.callback}}", "proof": "{{credentials.mock_callback_key}}"}
		}
		tool(name, effect, fields, output, body)
		tools["mock."+name].(map[string]any)["category"] = "provider_registration"
		if name == "register" {
			tools["mock."+name].(map[string]any)["credentials"] = []string{"mock_api_key", "mock_callback_key"}
		}
	}
	textFields := map[string]any{
		"text":      map[string]any{"from": "data.text", "schema": str(1, 512)},
		"principal": map[string]any{"from": "data.principal", "schema": principal},
		"room":      map[string]any{"from": "data.room", "schema": room},
		"scope":     map[string]any{"from": "data.scope", "schema": map[string]any{"type": "string", "enum": []string{"direct", "shared"}}},
		"message":   map[string]any{"from": "data.message.id", "schema": str(1, 24)},
		"reply":     map[string]any{"from": "data.reply.id", "schema": str(1, 24), "optional": true},
		"entry":     map[string]any{"from": "data.entry", "schema": obj(map[string]any{"reference": str(1, 32), "address": str(5, 32)}, "reference"), "optional": true},
	}
	actionFields := map[string]any{}
	for _, name := range []string{"principal", "room", "scope", "message"} {
		actionFields[name] = textFields[name]
	}
	actionFields["token"] = map[string]any{"from": "data.token", "schema": token}
	actionFields["cursor"] = map[string]any{"from": "data.cursor", "schema": str(1, 24)}
	trigger := map[string]any{"provider": "mock", "payload_object_required": true, "secret": map[string]any{"required": true},
		"signature": map[string]any{"type": "token_equality", "header": "X-Mock-Proof"}, "delivery_id": map[string]any{"json_path": "$.delivery", "required": true},
		"event_type": map[string]any{"literal": "message", "required": true}, "event_name": map[string]any{"literal": "inbound.mock"},
		"normalized_events": []any{map[string]any{"event": "inbound.mock.text", "fields": textFields, "when": map[string]any{"absent": []string{"data.token"}}}, map[string]any{"event": "inbound.mock.action", "fields": actionFields, "when": map[string]any{"absent": []string{"data.text"}}}},
		"ack":               map[string]any{"mode": "durable_before_dispatch"},
	}
	channel := map[string]any{"provider": "mock", "native_inbox": map[string]any{"kind": "scoped_commands_v1", "client_languages": []string{"en", "fr"}, "direct_launcher_read": "read_inbox_launcher", "default_launcher_read": "read_default_inbox_launcher", "commands_launcher": "commands", "inherited_launcher": "default"},
		"onboarding":   map[string]any{"activation": "webhook_registration", "ceremony": "authenticated_text_challenge", "provider_credential": "mock_api_key", "signing_credential": "mock_callback_key", "confirmation": "deliver", "learned_destination": map[string]any{"destination.queue": "conversation_reference.room"}},
		"registration": map[string]any{"slot": map[string]any{"namespace": "workspace_webhook", "identify": map[string]any{"tool": "mock.identify", "output": map[string]any{"resource_id": "result.resource"}}}, "credentials": map[string]any{"provider": []string{"mock_api_key"}, "signing": "mock_callback_key"}, "apply": map[string]any{"tool": "mock.register", "input": map[string]any{"callback": "context.callback_url"}}, "readback": map[string]any{"tool": "mock.registration", "output": map[string]any{"callback_url": "result.callback"}}},
		"opaque_types": map[string]any{"destination": obj(map[string]any{"queue": room}, "queue"), "conversation_reference": obj(map[string]any{"room": room}, "room"), "external_account_reference": obj(map[string]any{"principal": principal}, "principal"), "interaction_reference": obj(map[string]any{"cursor": str(1, 24)}, "cursor"), "delivery_reference": reference, "delivery_receipt": reference},
		"operations":   operations, "events": map[string]any{
			"text":   map[string]any{"event": "inbound.mock.text", "fields": map[string]any{"text": "event.text", "external_account_reference.principal": "event.principal", "conversation_reference.room": "event.room", "conversation_scope": "event.scope", "provider_message_reference.id": "event.message", "reply_to_message_reference.id": "event.reply", "entry_invocation": "event.entry"}},
			"action": map[string]any{"event": "inbound.mock.action", "fields": map[string]any{"token": "event.token", "interaction_reference.cursor": "event.cursor", "external_account_reference.principal": "event.principal", "conversation_reference.room": "event.room", "conversation_scope": "event.scope", "provider_message_reference.id": "event.message"}},
		},
	}
	bodies := map[string][]byte{}
	for kind, value := range map[string]any{packmodel.TypeTrigger: trigger, packmodel.TypeConnector: map[string]any{"provider": "mock", "tools": tools}, packmodel.TypeChannel: channel} {
		raw, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		bodies[kind] = raw
	}
	return bodies
}
