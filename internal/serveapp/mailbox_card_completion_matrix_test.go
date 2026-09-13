package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestMailboxMutationCompletionRollbackBothStores(t *testing.T) {
	requireMailboxCompletionFaultFunction(t)
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, anchor := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"} {
					t.Run(string(anchor)+"/"+method, func(t *testing.T) {
						f := mailboxCompletionFixtureInRuntime(t, rt, owner)
						card := mailboxCompletionAnchorCard(t, f, anchor)
						key := "matrix-" + card.CardID
						params := map[string]any{"card_id": card.CardID, "idempotency_key": key}
						inputVerdict := "reject"
						if anchor == decisioncard.AnchorKindProposedEffect {
							inputVerdict = "revise"
						}
						switch method {
						case "mailbox.decide":
							params["verdict"], params["observed_content_hash"] = "approve", card.CardContentHash
						case "mailbox.defer":
							params["until"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
						case "mailbox.begin_input":
							params["verdict"], params["observed_content_hash"] = inputVerdict, card.CardContentHash
						case "mailbox.cancel_input":
							var draft map[string]any
							requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": inputVerdict, "observed_content_hash": card.CardContentHash, "idempotency_key": "prerequisite-" + card.CardID}, &draft)
							params["input_draft_id"] = draft["input_draft_id"]
						}
						read := func() []string {
							t.Helper()
							var out []string
							for _, query := range []string{
								`SELECT status,COALESCE(verdict,''),COALESCE(decided_by,''),COALESCE(CAST(decision_event_id AS TEXT),''),CAST(updated_at AS TEXT) FROM decision_cards WHERE card_id=$1`,
								`SELECT CAST(input_draft_id AS TEXT),CAST(principal_id AS TEXT),status,CAST(expires_at AS TEXT),CAST(updated_at AS TEXT) FROM decision_card_input_drafts WHERE card_id=$1 ORDER BY input_draft_id`,
								`SELECT CAST(change_id AS TEXT),change_type,CAST(payload AS TEXT),'','' FROM decision_card_changes WHERE card_id=$1 ORDER BY change_id`,
							} {
								rows, err := f.rt.DB.Query(query, card.CardID)
								if err != nil {
									t.Fatal(err)
								}
								for rows.Next() {
									var a, b, c, d, e string
									if err := rows.Scan(&a, &b, &c, &d, &e); err != nil {
										t.Fatal(err)
									}
									out = append(out, a+"|"+b+"|"+c+"|"+d+"|"+e)
								}
								if err := rows.Err(); err != nil {
									t.Fatal(err)
								}
								rows.Close()
							}
							return out
						}
						before := read()
						beforeEffects := mailboxCompletionRunEffects(t, f.rt, card.RunID)
						assertCut, remove := installMailboxCompletionFaultWitness(t, f.rt, key)
						if failed := requestServedJSONRPC(t, f.rt.Endpoint, method, params); failed.Error == nil {
							t.Fatal("completion fault succeeded")
						}
						assertCut()
						if after := read(); !reflect.DeepEqual(before, after) {
							t.Fatalf("partial domain commit: before=%v after=%v", before, after)
						}
						if after := mailboxCompletionRunEffects(t, f.rt, card.RunID); !reflect.DeepEqual(beforeEffects, after) {
							t.Fatalf("response INSERT failure partially changed continuation/event/entity state:\nbefore=%v\nafter=%v", beforeEffects, after)
						}
						var count int
						if err := f.rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE idempotency_key=$1`, key).Scan(&count); err != nil || count != 0 {
							t.Fatalf("partial response: %d %v", count, err)
						}
						remove()
						original := mailboxCompletionLoseHTTPResponse(t, f.rt.Endpoint, method, params)
						var replay map[string]any
						waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, card.RunID)
						domain := read()
						effects := mailboxCompletionRunEffects(t, f.rt, card.RunID)
						requireServedJSONRPCResult(t, f.rt.Endpoint, method, params, &replay)
						waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, card.RunID)
						if original["idempotency_replayed"] != false || replay["idempotency_replayed"] != true {
							t.Fatalf("replay markers: %v %v", original, replay)
						}
						delete(original, "idempotency_replayed")
						delete(replay, "idempotency_replayed")
						if !reflect.DeepEqual(original, replay) || !reflect.DeepEqual(domain, read()) {
							t.Fatal("replay changed response or domain")
						}
						if after := mailboxCompletionRunEffects(t, f.rt, card.RunID); !reflect.DeepEqual(effects, after) {
							t.Fatalf("replay duplicated continuation/event/entity effects:\nbefore=%v\nafter=%v", effects, after)
						}
						var raw, principal, actor, actorKind string
						if err := f.rt.DB.QueryRow(`SELECT actor_kind,actor_id,CAST(response AS TEXT) FROM api_idempotency WHERE idempotency_key=$1`, key).Scan(&actorKind, &actor, &raw); err != nil {
							t.Fatal(err)
						}
						if err := f.rt.DB.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
							t.Fatal(err)
						}
						var stored map[string]any
						if err := json.Unmarshal([]byte(raw), &stored); err != nil {
							t.Fatal(err)
						}
						if actorKind != "operator_principal" || actor != principal || !reflect.DeepEqual(stored, original) {
							t.Fatalf("wrong completion: %s %s %v", actorKind, actor, stored)
						}
					})
				}
			}
		})
	}
}
