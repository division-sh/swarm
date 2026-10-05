package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestChannelSourceLifecyclePublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, operation := range []string{"disk", "new_process", "retained_reset", "source_reset", "fork"} {
			t.Run(string(backend)+"/"+operation, func(t *testing.T) {
				h, db, hash := startChannelAnchorJourney(t, backend, "source-lifecycle-token", false)
				var standingRun string
				if err := db.QueryRow(`SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&standingRun); err != nil {
					t.Fatal(err)
				}
				standingCard := waitChannelAnchorCard(t, db, standingRun, decisioncard.AnchorKindStageGate, "telegram-ingress")
				waitChannelAnchorReceipt(t, db, standingCard)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
					"idempotency_key": "source-lifecycle-seed",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				messageID := waitChannelAnchorReceipt(t, db, card)
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve")
				if !ok {
					t.Fatal("source-lifecycle card lacks frozen control")
				}
				waitChannelDeliverySendsSettled(t, db, backend)
				frozen := readChannelSourceHistory(t, db, card)
				var originalSource []byte
				if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&originalSource); err != nil {
					t.Fatal(err)
				}
				callback, signing, _ := h.provider.Registration()
				switch operation {
				case "disk", "new_process":
					path := filepath.Join(h.opts.SourceRoot, "reviews", "schema.yaml")
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					body = append(body, []byte("\n# Changed artifact bytes; semantic declaration remains identical.\n")...)
					if err := os.WriteFile(path, body, 0o600); err != nil {
						t.Fatal(err)
					}
					var identity apiv1.RuntimeIdentityResult
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
					if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash != hash {
						t.Fatalf("disk mutation changed admitted source: %+v", identity)
					}
					if operation == "disk" {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
						var detail map[string]any
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}, &detail)
						postChannelSourceControl(t, callback, signing, card, token, messageID)
						waitChannelAnchorDecision(t, db, card)
					} else {
						h.stop(t)
						h.start(t)
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
						if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash == hash {
							t.Fatalf("new process did not admit changed artifact: %+v", identity)
						}
						var retained []byte
						if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&retained); err != nil || !bytes.Equal(retained, originalSource) {
							t.Fatalf("changed process replaced retained source: %v", err)
						}
						callback, signing, _ = h.provider.Registration()
						if postChannelSourceControl(t, callback, signing, card, token, messageID) == http.StatusAccepted {
							waitChannelRejectedCallback(t, db, token)
						}
						var decisions int
						if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 0 {
							t.Fatalf("replacement admitted a predecessor control: count=%d err=%v", decisions, err)
						}
					}
				case "retained_reset", "source_reset":
					clear := operation == "source_reset"
					command := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
					postChannelTelegramUpdate(t, callback, signing, map[string]any{
						"update_id": 978001, "message": map[string]any{
							"message_id": 978001, "from": map[string]any{"id": 7000},
							"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command,
						},
					})
					waitNativeIntentCount(t, db, "entry", 1)
					waitNativeInboxDeliveries(t, h, "1001", 1)
					rejectedToken := uuid.NewString()
					postChannelTelegramUpdate(t, callback, signing, map[string]any{
						"update_id": 978002, "callback_query": map[string]any{
							"id": "reset-history-refusal", "from": map[string]any{"id": 7000},
							"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}},
							"data":    rejectedToken,
						},
					})
					waitChannelRejectedCallback(t, db, rejectedToken)
					waitChannelDeliverySendsSettled(t, db, backend)
					for _, table := range []string{"operator_channel_action_intents", "operator_channel_text_intents"} {
						var count int
						if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE state='settled'`).Scan(&count); err != nil || count == 0 {
							t.Fatalf("reset history proof has no settled %s: count=%d err=%v", table, count, err)
						}
					}
					beforeNative := readChannelNativeHistory(t, db)
					beforeHistory := readChannelPreservedHistory(t, db)
					writes := len(h.provider.CommandWrites())
					dry := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", map[string]any{
						"dry_run": true, "include_source_artifacts": clear, "idempotency_key": uuid.NewString(),
					})
					if dry.Error != nil || readChannelNativeHistory(t, db) != beforeNative || readChannelPreservedHistory(t, db) != beforeHistory {
						t.Fatalf("dry-run changed native identity or populated history: %+v", dry.Error)
					}
					params := map[string]any{
						"include_source_artifacts": clear, "idempotency_key": uuid.NewString(),
					}
					reset := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params)
					if reset.Error != nil {
						var record string
						readErr := db.QueryRow(`SELECT CAST(record AS TEXT) FROM runtime_reset_operations LIMIT 1`).Scan(&record)
						t.Fatalf("public reset failed: %+v record=%s read=%v\n%s", reset.Error, record, readErr, h.process.outputString())
					}
					var oldRun, activations, artifacts int
					for query, target := range map[string]*int{
						`SELECT COUNT(*) FROM runs WHERE run_id=$1`:                               &oldRun,
						`SELECT COUNT(*) FROM connected_channel_activations WHERE bundle_hash=$1`: &activations,
						`SELECT COUNT(*) FROM source_artifacts WHERE bundle_hash=$1`:              &artifacts,
					} {
						arg := hash
						if target == &oldRun {
							arg = seed.RunID
						}
						if err := db.QueryRow(query, arg).Scan(target); err != nil {
							t.Fatal(err)
						}
					}
					if oldRun != 0 || activations != 0 || (artifacts == 0) != clear {
						t.Fatalf("reset authority/source mismatch: run=%d activation=%d artifact=%d", oldRun, activations, artifacts)
					}
					successorPlan := ""
					if !clear {
						var freshGenerations int
						if err := db.QueryRow(`SELECT COUNT(*) FROM standing_service_generations`).Scan(&freshGenerations); err != nil || freshGenerations != 0 {
							t.Fatalf("reset created a standing generation without admitted ingress credentials: count=%d err=%v", freshGenerations, err)
						}
						stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
						restore := installChannelOnboardingCLIInput(t, "source-reset-signing-secret\n")
						t.Cleanup(restore)
						code := executeCLI(context.Background(), []string{
							"--config", h.opts.ConfigPath, "secrets", "set", "webhook_signing.telegram", "--stdin",
						}, stdout, stderr, nil)
						restore()
						if code != 0 {
							t.Fatalf("provision retained-source ingress credential: code=%d %s", code, stderr.String())
						}
						h.stop(t)
						h.start(t)
						var successorRun string
						if err := db.QueryRow(`SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&successorRun); err != nil {
							t.Fatal(err)
						}
						freshCard := waitChannelAnchorCard(t, db, successorRun, decisioncard.AnchorKindStageGate, "telegram-ingress")
						if freshCard == standingCard {
							t.Fatal("retained reset reused predecessor standing card")
						}
						deadline := time.Now().Add(15 * time.Second)
						for {
							var state, render, receipt string
							err := db.QueryRow(`SELECT delivery_id,state,COALESCE(CAST(current_render_id AS TEXT),''),COALESCE(CAST(current_receipt_operation_id AS TEXT),'') FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1 AND resend_generation=0`, freshCard).
								Scan(&successorPlan, &state, &render, &receipt)
							if err == nil {
								if state != "planned" || render != "" || receipt != "" {
									t.Fatal("successor card dispatched without a current activation")
								}
								break
							}
							if err != sql.ErrNoRows || time.Now().After(deadline) {
								t.Fatalf("successor standing card plan: %v", err)
							}
							time.Sleep(20 * time.Millisecond)
						}
					}
					afterNative := readChannelNativeHistory(t, db)
					wantNative := beforeNative
					wantNative[3], wantNative[7] = "retired", "retired"
					if afterNative != wantNative {
						t.Fatalf("reset altered native identity: before=%v after=%v", beforeNative, afterNative)
					}
					requireChannelPreservedHistory(t, db, beforeHistory, successorPlan)
					if got := len(h.provider.CommandWrites()); got != writes {
						t.Fatalf("reset wrote native settings: count %d -> %d", writes, got)
					}
					if replay := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params); replay.Error != nil || readChannelNativeHistory(t, db) != afterNative {
						t.Fatalf("reset replay changed native history: %+v", replay.Error)
					}
					postChannelSourceControl(t, callback, signing, card, token, messageID)
					for index, text := range []string{"obsolete answer", "/" + command} {
						status := postChannelSourceUpdate(t, callback, signing, map[string]any{
							"update_id": 978003 + index, "message": map[string]any{
								"message_id": 978003 + index, "from": map[string]any{"id": 7000},
								"chat": map[string]any{"id": 1001, "type": "private"}, "text": text,
								"reply_to_message": map[string]any{"message_id": messageID},
							},
						})
						if status != http.StatusNotFound {
							t.Fatalf("reset retained obsolete text/native ingress: status=%d text=%q", status, text)
						}
					}
					if response := requestServedJSONRPC(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}); response.Error == nil {
						t.Fatal("reset retained deleted card execution authority")
					}
					if clear {
						response := requestServedJSONRPC(t, h.rpcEndpoint(), "event.publish", map[string]any{
							"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
						})
						if response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
							t.Fatalf("source-inclusive reset revived execution: %+v", response.Error)
						}
					} else {
						var retained []byte
						if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&retained); err != nil || !bytes.Equal(retained, originalSource) {
							t.Fatalf("retained reset changed source bytes: %v", err)
						}
						later := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
							"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
						})
						if later.RunID == seed.RunID {
							t.Fatal("retained reset reused predecessor run identity")
						}
					}
					h.stop(t)
					h.start(t)
					if readChannelNativeHistory(t, db) != afterNative || len(h.provider.CommandWrites()) != writes {
						t.Fatal("restart revived retired native consumers or replayed installation")
					}
					if !clear {
						_, confirmationCount := h.provider.OnboardingCounts()
						journey := runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "reconnect", "source-lifecycle-token", 1001, "private", confirmationCount)
						qualified := waitNativeQualification(t, h, journey.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
						if qualified.SettingID != beforeNative[0] || len(h.provider.CommandWrites()) != writes {
							t.Fatal("reconnect did not reuse acknowledged native installation")
						}
						var state string
						if err := db.QueryRow(`SELECT state FROM channel_native_setting_consumers WHERE activation_id=$1`, beforeNative[6]).Scan(&state); err != nil || state != "retired" {
							t.Fatalf("reconnect revived predecessor consumer: %s %v", state, err)
						}
					}
				case "fork":
					storeBackend := "sqlite"
					if backend == servedparity.BackendExplicitPostgres {
						storeBackend = "postgres"
					}
					waitServedRunDeliveryQuiescence(t, db, storeBackend, seed.RunID)
					requireServedOKJSONRPC(t, h.rpcEndpoint(), "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": uuid.NewString()})
					frontier := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
						"payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
					})
					params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": uuid.NewString()}
					var fork apiv1.RunForkExecutionResult
					response := requestServedJSONRPC(t, h.rpcEndpoint(), "run.fork", params)
					if response.Error != nil {
						var failure sql.NullString
						readErr := db.QueryRow(`SELECT CAST(failure_json AS TEXT) FROM run_fork_operations WHERE idempotency_key=$1`, params["idempotency_key"]).Scan(&failure)
						t.Fatalf("public fork failed: %+v failure=%s read=%v\n%s", response.Error, failure.String, readErr, h.process.outputString())
					}
					if err := json.Unmarshal(response.Result, &fork); err != nil {
						t.Fatal(err)
					}
					var supersessionCount, exactLineageCount int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_superseded'`, fork.ForkRunID).Scan(&supersessionCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM events e
						JOIN run_fork_selected_contract_executions x ON x.fork_event_id=e.source_event_id AND x.fork_run_id=e.run_id
						WHERE e.run_id=$1 AND e.event_name='mailbox.card_superseded' AND x.source_event_id=$2`, fork.ForkRunID, frontier.EventID).Scan(&exactLineageCount); err != nil {
						t.Fatal(err)
					}
					if supersessionCount != 1 || exactLineageCount != supersessionCount {
						t.Fatalf("fork stage exit lost its selected frontier: supersessions=%d exact=%d", supersessionCount, exactLineageCount)
					}
					child := waitChannelAnchorCardInState(t, db, fork.ForkRunID, decisioncard.AnchorKindStageGate, "reviews", decisioncard.StatusPending)
					var retiredEvent []byte
					if err := db.QueryRow(`SELECT payload FROM events WHERE run_id=$1 AND event_name='mailbox.card_superseded'`, fork.ForkRunID).Scan(&retiredEvent); err != nil {
						t.Fatal(err)
					}
					var retired struct {
						CardID string `json:"card_id"`
					}
					if err := json.Unmarshal(retiredEvent, &retired); err != nil || retired.CardID == "" || retired.CardID == child {
						t.Fatalf("fork supersession has no exact predecessor: %s err=%v", retiredEvent, err)
					}
					var retiredCards int
					if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE run_id=$1 AND card_id=$2 AND status='superseded'`, fork.ForkRunID, retired.CardID).Scan(&retiredCards); err != nil || retiredCards != 1 {
						t.Fatalf("fork stage exit lost exact predecessor card retirement: count=%d err=%v", retiredCards, err)
					}
					if child == card || fork.ForkRunID == seed.RunID {
						t.Fatal("fork reused parent card/run identity")
					}
					rt := servedControlProofRuntime{Endpoint: h.rpcEndpoint(), DB: db, Backend: storeBackend, BundleHash: hash}
					decision := lifecycleDecisionParamsForCard(t, rt, child, "approve")
					refusal := requireServedJSONRPCError(t, h.rpcEndpoint(), "mailbox.decide", decision)
					if refusal.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" {
						t.Fatalf("fork acquired ordinary card mutation authority: %+v", refusal)
					}
					for method, args := range map[string]map[string]any{
						"mailbox.defer":        {"card_id": child, "until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "idempotency_key": uuid.NewString()},
						"mailbox.begin_input":  {"card_id": child, "verdict": "reject", "observed_content_hash": decision["observed_content_hash"], "idempotency_key": uuid.NewString()},
						"mailbox.cancel_input": {"card_id": child, "input_draft_id": uuid.NewString(), "idempotency_key": uuid.NewString()},
					} {
						if err := requireServedJSONRPCError(t, h.rpcEndpoint(), method, args); err.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" {
							t.Fatalf("%s did not preserve upstream fork refusal: %+v", method, err)
						}
					}
					childMessage := waitChannelAnchorReceipt(t, db, child)
					childToken, found := telegramCallbackToken(h.provider.Delivery(childMessage-1), "approve")
					if !found || childToken == token {
						t.Fatal("fork delivery reused parent control identity")
					}
					readParent := func() [3]string {
						var state [3]string
						if err := db.QueryRow(`SELECT status,COALESCE(verdict,''),CAST(updated_at AS TEXT) FROM decision_cards WHERE card_id=$1`, card).Scan(&state[0], &state[1], &state[2]); err != nil {
							t.Fatal(err)
						}
						return state
					}
					parentBefore := readParent()
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					deadline := time.Now().Add(5 * time.Second)
					for {
						var intentState, disposition string
						err := db.QueryRow(`SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE CAST(fact AS TEXT) LIKE $1`, "%"+childToken+"%").Scan(&intentState, &disposition)
						if err == nil && intentState == "settled" && disposition == "unsupported" {
							break
						}
						if time.Now().After(deadline) {
							var status, verdict string
							cardErr := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict)
							var decisions int
							eventErr := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, fork.ForkRunID).Scan(&decisions)
							t.Fatalf("selected-fork callback lacks terminal unsupported disposition: state=%s disposition=%s err=%v card_status=%s verdict=%s card_err=%v decisions=%d event_err=%v", intentState, disposition, err, status, verdict, cardErr, decisions, eventErr)
						}
						time.Sleep(20 * time.Millisecond)
					}
					var status string
					if err := db.QueryRow(`SELECT status FROM decision_cards WHERE card_id=$1`, child).Scan(&status); err != nil || status != "pending" {
						t.Fatalf("fork callback bypassed selected control refusal: status=%s err=%v", status, err)
					}
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					ordinary := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
					})
					ordinaryCard := waitChannelAnchorCard(t, db, ordinary.RunID, decisioncard.AnchorKindStageGate, "reviews")
					ordinaryMessage := waitChannelAnchorReceipt(t, db, ordinaryCard)
					ordinaryToken, ok := telegramCallbackToken(h.provider.Delivery(ordinaryMessage-1), "approve")
					if !ok {
						t.Fatal("normal run lost decision control")
					}
					postChannelSourceControl(t, callback, signing, ordinaryCard, ordinaryToken, ordinaryMessage)
					waitChannelAnchorDecision(t, db, ordinaryCard)
					waitChannelDeliverySendsSettled(t, db, backend)
					h.stop(t)
					h.start(t)
					callback, signing, _ = h.provider.Registration()
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					var verdict string
					var events, drafts int
					if err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict); err != nil || status != "pending" || verdict != "" {
						t.Fatalf("fork replay/restart changed child: %s/%s %v", status, verdict, err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, fork.ForkRunID).Scan(&events); err != nil || events != 0 {
						t.Fatalf("fork restart emitted a decision: %d %v", events, err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM decision_card_input_drafts WHERE card_id=$1`, child).Scan(&drafts); err != nil || drafts != 0 {
						t.Fatalf("upstream fork refusal created a draft: %d %v", drafts, err)
					}
					if readParent() != parentBefore {
						t.Fatal("child refusal or normal-run control mutated the frozen parent card")
					}
				}
				if after := readChannelSourceHistory(t, db, card); after != frozen {
					t.Fatal("source lifecycle changed historical receipt/render evidence")
				}
			})
		}
	}
}

func requireChannelPreservedHistory(t *testing.T, db *sql.DB, beforeHistory, successorPlan string) {
	t.Helper()
	afterHistory := readChannelPreservedHistory(t, db)
	afterHistory, err := channelHistoryWithoutSuccessorPlan(afterHistory, successorPlan)
	if err != nil {
		t.Fatal(err)
	}
	if afterHistory == beforeHistory {
		return
	}
	var before, after []json.RawMessage
	if err := json.Unmarshal([]byte(beforeHistory), &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(afterHistory), &after); err != nil {
		t.Fatal(err)
	}
	for index, row := range before {
		if !bytes.Equal(row, after[index]) {
			t.Errorf("reset altered preserved history table index %d: byte length %d -> %d", index, len(row), len(after[index]))
		}
	}
	t.FailNow()
}

// Retained reset boots the identical source and its new standing gate. Only
// that independently observed plan is new work, not pre-reset history.
func channelHistoryWithoutSuccessorPlan(history, successorPlan string) (string, error) {
	if successorPlan == "" {
		return history, nil
	}
	var tables []json.RawMessage
	if err := json.Unmarshal([]byte(history), &tables); err != nil {
		return "", err
	}
	var plans []json.RawMessage
	if err := json.Unmarshal(tables[6], &plans); err != nil {
		return "", err
	}
	removed := false
	for index, plan := range plans {
		var fields []json.RawMessage
		if err := json.Unmarshal(plan, &fields); err != nil {
			return "", err
		}
		var id string
		if err := json.Unmarshal(fields[0], &id); err != nil {
			return "", err
		}
		if id == successorPlan {
			plans = append(plans[:index], plans[index+1:]...)
			removed = true
			break
		}
	}
	if !removed {
		return "", errors.New("exact successor plan is absent from history")
	}
	body, err := json.Marshal(plans)
	if err != nil {
		return "", err
	}
	tables[6] = body
	body, err = json.Marshal(tables)
	return string(body), err
}

func TestChannelSourceLifecycleHistoryPreservation(t *testing.T) {
	before := make([]any, 15)
	before[6] = [][]string{{"old-plan", "card", "old-card", "frozen"}}
	want, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"none", "changed", "deleted", "unrelated", "duplicate", "missing_successor", "other_table"} {
		t.Run(mutation, func(t *testing.T) {
			after := append([]any(nil), before...)
			plans := [][]string{{"old-plan", "card", "old-card", "frozen"}, {"successor", "card", "fresh-card", "new"}}
			switch mutation {
			case "changed":
				plans[0][3] = "altered"
			case "deleted":
				plans = plans[1:]
			case "unrelated":
				plans = append(plans, []string{"other", "card", "other-card"})
			case "duplicate":
				plans = append(plans, plans[1])
			case "missing_successor":
				plans = plans[:1]
			case "other_table":
				after[8] = [][]string{{"altered-receipt"}}
			}
			after[6] = plans
			body, err := json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := channelHistoryWithoutSuccessorPlan(string(body), "successor")
			matches := err == nil && retained == string(want)
			if matches != (mutation == "none") {
				t.Fatalf("history exception accepted %s=%t: %v", mutation, matches, err)
			}
		})
	}
}

func readChannelNativeHistory(t *testing.T, db *sql.DB) [8]string {
	t.Helper()
	var row [8]string
	if err := db.QueryRow(`SELECT s.setting_id,CAST(s.generation AS TEXT),s.install_operation_id,s.state,
		COALESCE(s.readback_hash,''),CAST(s.desired_commands AS TEXT),c.activation_id,c.state
		FROM channel_native_settings s JOIN channel_native_setting_consumers c ON c.setting_id=s.setting_id
		ORDER BY c.updated_at LIMIT 1`).Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7]); err != nil {
		t.Fatal(err)
	}
	return row
}

// Mutable retirement and qualification metadata are deliberately not compared
// as immutable evidence. Each historical identity and frozen artifact is.
func readChannelPreservedHistory(t *testing.T, db *sql.DB) string {
	t.Helper()
	queries := []string{
		`SELECT * FROM operator_principals ORDER BY principal_id`,
		`SELECT * FROM operator_channel_operations ORDER BY operation_id`,
		`SELECT * FROM operator_channel_bindings ORDER BY interface_key`,
		`SELECT principal_id,interface_key,binding_revision,delivery_epoch,external_account_reference,conversation_reference,conversation_scope,state,first_operation_id FROM channel_delivery_defaults`,
		`SELECT setting_id,provider,resource_slot_id,conversation_reference,scope_kind,member_reference,language_code,pack_id,pack_version,pack_manifest_hash,entry_contract_hash,entry_command,desired_commands,principal_id,generation,install_operation_id,readback_hash,created_at FROM channel_native_settings ORDER BY setting_id`,
		`SELECT setting_id,activation_id,activation_revision,interface_key,binding_revision,context_publication_generation FROM channel_native_setting_consumers ORDER BY activation_id`,
		`SELECT CAST(delivery_id AS TEXT),source_kind,CAST(source_id AS TEXT),request_activation_id,summary_count,principal_id,interface_key,binding_revision,delivery_epoch,external_account_reference,conversation_reference,conversation_scope,action_capacity,text_capacity,label_capacity,resend_generation,resend_of_delivery_id,resend_action_publication_id,created_at FROM channel_delivery_plans ORDER BY delivery_id`,
		`SELECT * FROM channel_delivery_renders ORDER BY render_id`,
		`SELECT * FROM channel_delivery_receipts ORDER BY effect_operation_id`,
		`SELECT * FROM channel_delivery_actions ORDER BY action_token`,
		`SELECT * FROM operator_channel_claim_receipts ORDER BY publication_id`,
		`SELECT * FROM operator_channel_action_intents ORDER BY publication_id`,
		`SELECT * FROM operator_channel_text_intents ORDER BY publication_id`,
		`SELECT * FROM runtime_external_effect_operations WHERE effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY operation_id`,
		`SELECT a.* FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY a.attempt_id`,
	}
	var snapshots [][][]any
	for _, query := range queries {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("history query %s: %v", query, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var snapshot [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			snapshot = append(snapshot, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	body, err := json.Marshal(snapshots)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readChannelSourceHistory(t *testing.T, db *sql.DB, card string) string {
	var render, receipt string
	if err := db.QueryRow(`SELECT CAST(r.render_input AS TEXT),CAST(c.provider_reference AS TEXT)
		FROM channel_delivery_plans p JOIN channel_delivery_receipts c ON c.delivery_id=p.delivery_id
		JOIN channel_delivery_renders r ON r.render_id=c.render_id
		WHERE p.source_id=$1 AND c.state='sent' ORDER BY c.settled_at LIMIT 1`, card).Scan(&render, &receipt); err != nil {
		t.Fatal(err)
	}
	return render + "\n" + receipt
}

func postChannelSourceControl(t *testing.T, callback, signing, card, token string, messageID int) int {
	t.Helper()
	return postChannelSourceUpdate(t, callback, signing, map[string]any{"update_id": 976000 + messageID, "callback_query": map[string]any{
		"id": "source-" + card, "from": map[string]any{"id": 7000},
		"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": token,
	}})
}

func postChannelSourceUpdate(t *testing.T, callback, signing string, update map[string]any) int {
	t.Helper()
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, callback, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", signing)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNotFound {
		t.Fatalf("source-lifecycle callback returned %d", response.StatusCode)
	}
	return response.StatusCode
}
