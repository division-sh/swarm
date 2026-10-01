package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

// This is a downstream admission proof, not a public selected-fork creation
// journey. Public begin_input refuses; the retained drafts are controlled
// negatives for the otherwise unreachable partial and final input consumers.
func TestChannelSelectedForkInputBoundaryMatrix(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, mode := range []string{"plain", "quoted", "chosen", "skip"} {
			for _, partial := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/partial=%t", backend, mode, partial), func(t *testing.T) {
					root := canonicalrouting.CopyChannelLearnedObjectOptionalInputJourney(t, partial)
					h, db, p, _, hash := startObjectChannelJourney(t, backend, root, telegramPhraseBotLLMRuntime{}, "")
					card, receipt, reject := beginObjectInputCard(t, h, db, p, hash, 0)
					firstCard := card
					postObjectInputAction(t, p, "selected-input-begin", receipt, reject)
					waitChannelDraftPromptSettlement(t, db, card)
					var other string
					if mode == "chosen" {
						otherCard, otherReceipt, otherReject := beginObjectInputCard(t, h, db, p, hash, 1)
						other = otherCard
						postObjectInputAction(t, p, "selected-other-begin", otherReceipt, otherReject)
						waitChannelDraftPromptSettlement(t, db, other)
					}
					before := selectedChannelDraftState(t, db, card)
					var token, chooser string
					if mode == "chosen" {
						postObjectChannelFact(t, p, "selected-retained-answer", map[string]any{"text": "retained answer"})
						waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "selected-retained-answer", "chooser")
						_, chooser = waitObjectRequestedResponse(t, db, "selected-retained-answer")
						token = firstObjectDraftChoice(t, p, chooser)
						var raw []byte
						var renderHash string
						var position int
						if err := db.QueryRow(`SELECT r.render_input,r.render_hash,a.action_position
							FROM channel_delivery_actions a JOIN channel_delivery_renders r ON r.render_id=a.render_id
							WHERE a.action_token=$1`, token).Scan(&raw, &renderHash, &position); err != nil {
							t.Fatal(err)
						}
						raw, err := canonicaljson.Canonicalize(raw)
						if err != nil {
							t.Fatal(err)
						}
						frozen, err := channeldelivery.Decode(raw, renderHash)
						if err != nil {
							t.Fatal(err)
						}
						card = frozen.DraftChoices[position-1].CardID
						if other == card {
							other = firstCard
						}
						before = selectedChannelDraftState(t, db, card)
					}
					var beforeOther [7]string
					if other != "" {
						beforeOther = selectedChannelDraftState(t, db, other)
					}
					var runID string
					if err := db.QueryRow(`SELECT run_id FROM decision_cards WHERE card_id=$1`, card).Scan(&runID); err != nil {
						t.Fatal(err)
					}
					conn, err := db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					if backend == servedparity.BackendDefaultSQLite {
						if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout=5000`); err != nil {
							t.Fatal(err)
						}
					}
					_, err = conn.ExecContext(context.Background(), `INSERT INTO run_fork_selected_contract_bindings
						(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,mode,created_at)
						VALUES ($1,$2,$2,'deployment_revision',1,'selected_contracts',$3)`, uuid.NewString(), runID, time.Now().UTC())
					if closeErr := conn.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
					if err != nil {
						t.Fatal(err)
					}
					if mode == "skip" {
						for page := 0; page < 5 && token == ""; page++ {
							p.mu.Lock()
							controls, _ := p.message(receipt)["controls"].([]any)
							for _, control := range controls {
								row, _ := control.(map[string]any)
								if row["name"] == "Skip field" {
									token, _ = row["value"].(string)
								}
							}
							p.mu.Unlock()
							if token != "" {
								break
							}
							more := waitObjectMessageControl(t, p, receipt, "More choices")
							postObjectInputAction(t, p, fmt.Sprintf("selected-skip-page-%d", page), receipt, more)
							waitObjectEditAfterAction(t, p, receipt, more)
						}
						if token == "" {
							t.Fatal("optional prompt cannot reach Skip field")
						}
					}
					// The controlled binding is not a public materialization. Actual
					// selected-fork restart is proved by the public source journey.
					for attempt := 0; attempt < 2; attempt++ {
						switch mode {
						case "chosen":
							postObjectInputAction(t, p, "selected-choice", chooser, token)
							waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "selected-choice", "unsupported")
							waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "selected-retained-answer", "unsupported")
						case "skip":
							postObjectInputAction(t, p, "selected-skip", receipt, token)
							waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "selected-skip", "unsupported")
						default:
							fact := map[string]any{"text": "refused answer"}
							if mode == "quoted" {
								fact["reply"] = map[string]any{"id": receipt}
							}
							postObjectChannelFact(t, p, "selected-text", fact)
							postObjectChannelFact(t, p, "selected-text", fact)
							waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "selected-text", "unsupported")
						}
						if after := selectedChannelDraftState(t, db, card); before != after {
							t.Fatalf("unsupported %s changed card/draft: before=%v after=%v", mode, before, after)
						}
						var decisions int
						if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, runID).Scan(&decisions); err != nil || decisions != 0 {
							t.Fatalf("unsupported input emitted decisions: %d %v", decisions, err)
						}
						if other != "" {
							if state := selectedChannelDraftState(t, db, other); state != beforeOther {
								t.Fatalf("chooser refusal consumed unrelated draft: %v", state)
							}
						}
					}
				})
			}
		}
	}
}

func selectedChannelDraftState(t *testing.T, db *sql.DB, card string) [7]string {
	t.Helper()
	var state [7]string
	if err := db.QueryRow(`SELECT d.status,CAST(d.input_fields AS TEXT),CAST(d.next_field_index AS TEXT),
		CAST(d.updated_at AS TEXT),c.status,COALESCE(c.verdict,''),CAST(c.updated_at AS TEXT)
		FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE d.card_id=$1`, card).
		Scan(&state[0], &state[1], &state[2], &state[3], &state[4], &state[5], &state[6]); err != nil {
		t.Fatal(err)
	}
	return state
}
