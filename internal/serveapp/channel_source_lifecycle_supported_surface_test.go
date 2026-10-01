package serveapp

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestChannelSourceLifecyclePublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, operation := range []string{"disk", "new_process", "retained_reset", "source_reset", "fork"} {
			t.Run(string(backend)+"/"+operation, func(t *testing.T) {
				h, db, hash := startChannelAnchorJourney(t, backend, "source-lifecycle-token", false)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
					"idempotency_key": "source-lifecycle-seed",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				messageID := waitChannelAnchorReceipt(t, db, card)
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve")
				if !ok {
					t.Fatal("source-lifecycle card lacks frozen control")
				}
				waitChannelDeliverySendsSettled(t, db)
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
					reset := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", map[string]any{
						"include_source_artifacts": clear, "idempotency_key": uuid.NewString(),
					})
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
					postChannelSourceControl(t, callback, signing, card, token, messageID)
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
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.fork", params, &fork)
					child := waitChannelAnchorCard(t, db, fork.ForkRunID, decisioncard.AnchorKindStageGate)
					if child == card || fork.ForkRunID == seed.RunID {
						t.Fatal("fork reused parent card/run identity")
					}
					rt := servedControlProofRuntime{Endpoint: h.rpcEndpoint(), DB: db, Backend: storeBackend, BundleHash: hash}
					decision := lifecycleDecisionParamsForCard(t, rt, child, "approve")
					refusal := requireServedJSONRPCError(t, h.rpcEndpoint(), "mailbox.decide", decision)
					if refusal.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" {
						t.Fatalf("fork acquired ordinary card mutation authority: %+v", refusal)
					}
					childMessage := waitChannelAnchorReceipt(t, db, child)
					childToken, found := telegramCallbackToken(h.provider.Delivery(childMessage-1), "approve")
					if !found || childToken == token {
						t.Fatal("fork delivery reused parent control identity")
					}
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					deadline := time.Now().Add(5 * time.Second)
					for {
						var intentState, disposition string
						err := db.QueryRow(`SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE CAST(fact AS TEXT) LIKE $1`, "%"+childToken+"%").Scan(&intentState, &disposition)
						if err == nil && intentState == "settled" && disposition == "unsupported" {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("selected-fork callback lacks terminal unsupported disposition: state=%s disposition=%s err=%v", intentState, disposition, err)
						}
						time.Sleep(20 * time.Millisecond)
					}
					var status string
					if err := db.QueryRow(`SELECT status FROM decision_cards WHERE card_id=$1`, child).Scan(&status); err != nil || status != "pending" {
						t.Fatalf("fork callback bypassed selected control refusal: status=%s err=%v", status, err)
					}
				}
				if after := readChannelSourceHistory(t, db, card); after != frozen {
					t.Fatal("source lifecycle changed historical receipt/render evidence")
				}
			})
		}
	}
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
	body, err := json.Marshal(map[string]any{"update_id": 976000 + messageID, "callback_query": map[string]any{
		"id": "source-" + card, "from": map[string]any{"id": 7000},
		"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": token,
	}})
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
