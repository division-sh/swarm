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
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, anchor := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
			for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"} {
				t.Run(string(backend)+"/"+string(anchor)+"/"+method, func(t *testing.T) {
					f := newMailboxCompletionFixture(t, backend)
					card := mailboxCompletionAnchorCard(t, f, anchor)
					params := map[string]any{"card_id": card.CardID, "idempotency_key": "matrix"}
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
						requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": inputVerdict, "observed_content_hash": card.CardContentHash, "idempotency_key": "prerequisite"}, &draft)
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
					install := []string{`CREATE TRIGGER matrix_completion_fault BEFORE INSERT ON api_idempotency WHEN NEW.idempotency_key = 'matrix' BEGIN SELECT RAISE(ABORT, 'matrix completion fault'); END`}
					remove := []string{`DROP TRIGGER matrix_completion_fault`}
					if backend == servedparity.BackendExplicitPostgres {
						install = []string{`CREATE FUNCTION matrix_completion_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.idempotency_key = 'matrix' THEN RAISE EXCEPTION 'matrix completion fault'; END IF; RETURN NEW; END $$`, `CREATE TRIGGER matrix_completion_fault BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION matrix_completion_fault()`}
						remove = []string{`DROP TRIGGER matrix_completion_fault ON api_idempotency`, `DROP FUNCTION matrix_completion_fault()`}
					}
					for _, q := range install {
						if _, err := f.rt.DB.Exec(q); err != nil {
							t.Fatal(err)
						}
					}
					if failed := requestServedJSONRPC(t, f.rt.Endpoint, method, params); failed.Error == nil {
						t.Fatal("completion fault succeeded")
					}
					if after := read(); !reflect.DeepEqual(before, after) {
						t.Fatalf("partial domain commit: before=%v after=%v", before, after)
					}
					var count int
					if err := f.rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE idempotency_key='matrix'`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("partial response: %d %v", count, err)
					}
					for _, q := range remove {
						if _, err := f.rt.DB.Exec(q); err != nil {
							t.Fatal(err)
						}
					}
					var original, replay map[string]any
					requireServedJSONRPCResult(t, f.rt.Endpoint, method, params, &original)
					domain := read()
					requireServedJSONRPCResult(t, f.rt.Endpoint, method, params, &replay)
					if original["idempotency_replayed"] != false || replay["idempotency_replayed"] != true {
						t.Fatalf("replay markers: %v %v", original, replay)
					}
					delete(original, "idempotency_replayed")
					delete(replay, "idempotency_replayed")
					if !reflect.DeepEqual(original, replay) || !reflect.DeepEqual(domain, read()) {
						t.Fatal("replay changed response or domain")
					}
					var raw, principal, actor, actorKind string
					if err := f.rt.DB.QueryRow(`SELECT actor_kind,actor_id,CAST(response AS TEXT) FROM api_idempotency WHERE idempotency_key='matrix'`).Scan(&actorKind, &actor, &raw); err != nil {
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
	}
}
