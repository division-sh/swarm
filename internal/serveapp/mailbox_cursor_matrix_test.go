package serveapp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

type cursorMailboxStore interface {
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error)
	CountUnreadInformationalNotices(context.Context) (int, error)
	MarkMailboxItemNotified(context.Context, string) error
}

type cursorMailboxFixture struct {
	rt      servedControlProofRuntime
	store   cursorMailboxStore
	ctx     context.Context
	base    decisioncard.Card
	eventID string
}

type cursorMailboxRow struct {
	Kind   string                               `json:"kind"`
	Card   decisioncard.ListItem                `json:"decision_card"`
	Notice mailbox.V1Item                       `json:"notice"`
	Effect *decisioncard.ProposedEffectReadback `json:"effect"`
}

func (r cursorMailboxRow) key() string {
	if r.Kind == decisioncard.KindNotice {
		return r.Kind + ":" + r.Notice.MailboxID
	}
	return r.Kind + ":" + r.Card.CardID
}

type cursorMailboxPage struct {
	Items  []cursorMailboxRow `json:"items"`
	Next   string             `json:"next_cursor"`
	Unread int                `json:"unread_informational_notices"`
}

func newCursorMailboxFixture(t *testing.T, backend servedparity.Backend) cursorMailboxFixture {
	t.Helper()
	rt, _, _, _, cards := startServedMailboxEntityFilterProof(t, backend)
	var owner cursorMailboxStore = rt.SQLite
	if rt.Postgres != nil {
		owner = rt.Postgres
	}
	f := cursorMailboxFixture{rt: rt, store: owner, ctx: servedControlProofAuthorActivityContext(t, rt)}
	for id := range cards {
		var err error
		f.base, err = owner.GetDecisionCard(f.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 ORDER BY created_at LIMIT 1`, f.base.RunID).Scan(&f.eventID); err != nil {
		t.Fatal(err)
	}
	return f
}

// Canonical writers create the rows. Only the notice timestamp is pinned by SQL,
// because that writer deliberately owns its clock; this is a tie/order fixture.
func (f cursorMailboxFixture) notice(t *testing.T, entity string, at time.Time) cursorMailboxRow {
	t.Helper()
	id, err := f.store.InsertMailboxItem(f.ctx, runtimetools.MailboxItem{ID: uuid.NewString(), EventID: f.eventID, EntityID: entity, FlowInstance: f.base.RunID, Type: runtimetools.NotifyHumanMailboxItemType, Priority: "normal", Status: "pending", Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.rt.DB.Exec(`UPDATE mailbox SET created_at=$1 WHERE item_id=$2`, at, id); err != nil {
		t.Fatal(err)
	}
	return cursorMailboxRow{Kind: decisioncard.KindNotice, Notice: mailbox.V1Item{MailboxID: id, CreatedAt: at.Format(time.RFC3339Nano)}}
}

func (f cursorMailboxFixture) card(t *testing.T, entity string, at time.Time, kind decisioncard.AnchorKind) cursorMailboxRow {
	t.Helper()
	card := f.base
	card.CardID, card.CreatedAt, card.UpdatedAt = uuid.NewString(), at, at
	scope := decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: f.base.RunID, EntityID: entity}
	source := eventtest.RootRoutingSource(entity)
	var err error
	switch kind {
	case decisioncard.AnchorKindStageGate:
		stage, _ := card.Anchor.StageGate()
		stage.EntityID, stage.StageActivationID, stage.Source = entity, uuid.NewString(), source
		card.Anchor, err = decisioncard.NewStageGateAnchor(stage)
	case decisioncard.AnchorKindHumanTask:
		card.Anchor, err = decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{RequesterAgentID: "reviewer", OperationID: uuid.NewString(), Category: "review", Scope: scope, Source: source})
	case decisioncard.AnchorKindProposedEffect:
		card.Anchor, err = decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{RequestEventID: uuid.NewString(), ActivityID: "send_reply", Decision: "review", Scope: scope, Source: source})
	}
	if err != nil {
		t.Fatal(err)
	}
	if kind == decisioncard.AnchorKindProposedEffect {
		card.WorkflowVersion = "1"
		anchor, _ := card.Anchor.ProposedEffect()
		input, err := canonicaljson.FromGo(map[string]any{"text": "review"})
		if err != nil {
			t.Fatal(err)
		}
		continuation := decisioncard.ProposedEffectContinuation{
			CardID: decisioncard.ProposedEffectCardID(anchor.RequestEventID, anchor.Decision), RunID: card.RunID, RequestEventID: anchor.RequestEventID,
			ActivityID: anchor.ActivityID, Tool: "telegram.send_message", Input: input, BundleHash: card.BundleHash, WorkflowVersion: card.WorkflowVersion,
			EffectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite, SuccessEvent: "reply.sent", FailureEvent: "reply.failed", RevisionEvent: "reply.revise", RejectedEvent: "reply.rejected",
			RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation, EntityID: entity, NodeID: activityidentity.MustNodeOwner(identitytest.RootNode(t, "reviewer")).Key(),
			FlowInstance: f.base.RunID, HandlerEventKey: "review.requested", SourceEventID: f.eventID, SourceRunID: card.RunID,
			ExecutionMode: "live", State: decisioncard.ProposedEffectPending, CreatedAt: at, UpdatedAt: at,
		}.Canonical()
		effect, err := continuation.EffectValue()
		if err != nil {
			t.Fatal(err)
		}
		continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
		if err != nil {
			t.Fatal(err)
		}
		card.CardID, card.EffectContentHash = continuation.CardID, continuation.EffectContentHash
		card.Snapshot, err = decisioncard.FreezeSnapshot("review", "Review", map[string]any{"input": input.Interface()}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve"}, "revise": {Verdict: "revise"}, "reject": {Verdict: "reject"}})
		if err != nil {
			t.Fatal(err)
		}
		card.CardContentHash, card.DecisionSchemaHash = "", ""
		card, err = decisioncard.New(card)
		if err == nil {
			err = f.store.CreateProposedEffectCard(f.ctx, card, continuation)
		}
		if err != nil {
			t.Fatal(err)
		}
	} else {
		card.CardContentHash, card.DecisionSchemaHash = "", ""
		card, err = decisioncard.New(card)
		if err == nil && kind == decisioncard.AnchorKindHumanTask {
			err = f.store.CreateHumanTaskCard(f.ctx, card, decisioncard.HumanTaskContinuation{CardID: card.CardID, RunID: card.RunID, RequesterRoute: source.Route(), SourceEventID: f.eventID,
				DeadlineAt: time.Now().Add(24 * time.Hour), BudgetBundleHash: card.BundleHash, BudgetLimit: 10000, BudgetWindowStart: at, BudgetWindowEnd: time.Now().Add(7 * 24 * time.Hour), State: decisioncard.HumanTaskContinuationPending, CreatedAt: at, UpdatedAt: at})
		} else if err == nil {
			err = f.store.CreateDecisionCard(f.ctx, card)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return cursorMailboxRow{Kind: decisioncard.KindDecisionCard, Card: decisioncard.ListItem{CardID: card.CardID, CreatedAt: at}}
}

func (f cursorMailboxFixture) list(t *testing.T, params map[string]any) cursorMailboxPage {
	t.Helper()
	var page cursorMailboxPage
	requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.list", params, &page)
	want, err := f.store.CountUnreadInformationalNotices(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if page.Items == nil || page.Unread != want {
		t.Fatalf("page envelope/count=%+v, want unread=%d", page, want)
	}
	for _, row := range page.Items {
		if row.Kind == decisioncard.KindDecisionCard && row.Card.Anchor.Kind() == decisioncard.AnchorKindProposedEffect && (row.Effect == nil || row.Effect.DispatchState != "held") {
			t.Fatalf("lost independent proposed-effect readback: %+v", row)
		}
	}
	return page
}

func cursorRowKeys(rows []cursorMailboxRow) []string {
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, r.key())
	}
	return keys
}

func cursorRowTime(r cursorMailboxRow) time.Time {
	if r.Kind == decisioncard.KindDecisionCard {
		return r.Card.CreatedAt
	}
	at, _ := time.Parse(time.RFC3339Nano, r.Notice.CreatedAt)
	return at
}

func (f cursorMailboxFixture) walk(t *testing.T, params map[string]any) []cursorMailboxRow {
	t.Helper()
	query := map[string]any{}
	for k, v := range params {
		query[k] = v
	}
	var rows []cursorMailboxRow
	for i := 0; i < 1000; i++ {
		page := f.list(t, query)
		rows = append(rows, page.Items...)
		if page.Next == "" {
			return rows
		}
		if page.Next == query["cursor"] || len(page.Items) == 0 {
			t.Fatal("nonadvancing page")
		}
		query["cursor"] = page.Next
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func TestMailboxCursorMergeMatrixParity(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newCursorMailboxFixture(t, backend)
			for _, tc := range []struct {
				name                  string
				notices, cards, limit int
				tied                  bool
				kind                  decisioncard.AnchorKind
			}{
				{"neither", 0, 0, 1, false, decisioncard.AnchorKindStageGate},
				{"notice_only", 4, 0, 1, false, decisioncard.AnchorKindStageGate},
				{"card_only", 0, 4, 1, false, decisioncard.AnchorKindStageGate},
				{"mixed_partial_selection", 5, 5, 3, false, decisioncard.AnchorKindStageGate},
				{"notice_exhausted_first", 1, 5, 1, false, decisioncard.AnchorKindStageGate},
				{"card_exhausted_first", 5, 1, 1, false, decisioncard.AnchorKindStageGate},
				{"ties_unselected_owner", 4, 4, 1, true, decisioncard.AnchorKindStageGate},
				{"human_task", 4, 4, 1, false, decisioncard.AnchorKindHumanTask},
				{"proposed_effect", 4, 4, 1, false, decisioncard.AnchorKindProposedEffect},
				{"default50", 27, 27, 0, true, decisioncard.AnchorKindStageGate},
				{"max200", 102, 102, 200, true, decisioncard.AnchorKindStageGate},
			} {
				t.Run(tc.name, func(t *testing.T) {
					entity := uuid.NewString()
					base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
					var want []cursorMailboxRow
					for i := 0; i < tc.notices; i++ {
						at := base
						if !tc.tied {
							at = at.Add(time.Duration(i*2) * time.Second)
						}
						want = append(want, f.notice(t, entity, at))
					}
					for i := 0; i < tc.cards; i++ {
						at := base
						if !tc.tied {
							at = at.Add(time.Duration(i*2+1) * time.Second)
						}
						want = append(want, f.card(t, entity, at, tc.kind))
					}
					sort.Slice(want, func(i, j int) bool {
						a, b := cursorRowTime(want[i]), cursorRowTime(want[j])
						if !a.Equal(b) {
							return a.Before(b)
						}
						return want[i].key() < want[j].key()
					})
					params := map[string]any{"entity_id": entity}
					if tc.limit != 0 {
						params["limit"] = tc.limit
					}
					first := f.list(t, params)
					cap := tc.limit
					if cap == 0 {
						cap = 50
					}
					if len(first.Items) != min(cap, len(want)) {
						t.Fatalf("first page size=%d", len(first.Items))
					}
					got := f.walk(t, params)
					if !reflect.DeepEqual(cursorRowKeys(got), cursorRowKeys(want)) {
						t.Fatalf("got %v want %v", cursorRowKeys(got), cursorRowKeys(want))
					}
					if tc.cards >= 4 {
						params["anchor_kind"] = string(tc.kind)
						params["limit"] = 1
						params["status"] = "pending"
						params["run_id"] = f.base.RunID
						filtered := f.walk(t, params)
						if len(filtered) != tc.cards {
							t.Fatalf("filtered got %d cards want %d", len(filtered), tc.cards)
						}
					}
				})
			}
		})
	}
}

func TestMailboxCursorAdmissionParity(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newCursorMailboxFixture(t, backend)
			encode := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
			card := decisioncard.EncodeCursor(time.Now(), "opaque")
			notice := mailbox.EncodeV1Cursor(time.Now(), "opaque")
			bad := []string{`null`, `[]`, `{}`, `{`, `{"card":null}`, `{"notice":null}`, `{"card":0}`, `{"notice":[]}`, `{"card":""}`, `{"notice":" "}`, `{"notice":"!"}`, `{"card":"!"}`,
				fmt.Sprintf(`{"card":%q,"card":%q}`, card, card), fmt.Sprintf(`{"card":%q,"extra":1}`, card), fmt.Sprintf(`{"card":%q}`, notice), fmt.Sprintf(`{"notice":%q}`, card)}
			for _, field := range []string{"card", "notice"} {
				id := "card_id"
				if field == "notice" {
					id = "mailbox_id"
				}
				for _, raw := range []string{`null`, `{}`, `{"created_at":null}`, fmt.Sprintf(`{"created_at":"2026-09-08T00:00:00Z",%q:"a",%q:"b"}`, id, id), `{"created_at":"2026-09-08T00:00:00Z","card_id":"c","mailbox_id":"n"}`, fmt.Sprintf(`{"created_at":"2026-09-08T00:00:00Z",%q:"a","extra":1}`, id)} {
					bad = append(bad, fmt.Sprintf(`{%q:%q}`, field, encode(raw)))
				}
			}
			for _, raw := range bad {
				for _, anchor := range []string{"", "stage_gate"} {
					err := requireServedJSONRPCError(t, f.rt.Endpoint, "mailbox.list", map[string]any{"cursor": encode(raw), "anchor_kind": anchor})
					if err.Code != -32602 {
						t.Fatalf("admission %s: %+v", raw, err)
					}
				}
			}
		})
	}
}

func TestServedMailboxCursorLiveContinuationParity(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			f := newCursorMailboxFixture(t, backend)
			base := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
			for _, initial := range []string{"card", "notice"} {
				t.Run("empty_owner_gains_"+initial, func(t *testing.T) {
					entity := uuid.NewString()
					add := func(which string, offset int) cursorMailboxRow {
						if which == "card" {
							return f.card(t, entity, base.Add(time.Duration(offset)*time.Second), decisioncard.AnchorKindStageGate)
						}
						return f.notice(t, entity, base.Add(time.Duration(offset)*time.Second))
					}
					firstRow := add(initial, 0)
					lastRow := add(initial, 2)
					params := map[string]any{"entity_id": entity, "limit": 1}
					first := f.list(t, params)
					if len(first.Items) != 1 || first.Items[0].key() != firstRow.key() || first.Next == "" {
						t.Fatalf("first=%+v", first)
					}
					other := "notice"
					if initial == "notice" {
						other = "card"
					}
					newRow := add(other, 1)
					params["cursor"] = first.Next
					second := f.list(t, params)
					if len(second.Items) != 1 || second.Items[0].key() != newRow.key() || second.Next == "" {
						t.Fatalf("new owner row lost: %+v", second)
					}
					decode := func(raw string) map[string]string {
						b, err := base64.RawURLEncoding.DecodeString(raw)
						if err != nil {
							t.Fatal(err)
						}
						var positions map[string]string
						if err := json.Unmarshal(b, &positions); err != nil {
							t.Fatal(err)
						}
						return positions
					}
					if decode(first.Next)[initial] != decode(second.Next)[initial] {
						t.Fatal("unselected owner position changed")
					}
					params["cursor"] = second.Next
					last := f.list(t, params)
					if len(last.Items) != 1 || last.Items[0].key() != lastRow.key() || last.Next != "" {
						t.Fatalf("last=%+v", last)
					}
				})
			}
			t.Run("state_changes_and_filter_composition", func(t *testing.T) {
				entity := uuid.NewString()
				var cards []decisioncard.Card
				for i := 0; i < 5; i++ {
					row := f.card(t, entity, base.Add(time.Duration(i)*time.Second), decisioncard.AnchorKindStageGate)
					card, err := f.store.GetDecisionCard(f.ctx, row.Card.CardID)
					if err != nil {
						t.Fatal(err)
					}
					cards = append(cards, card)
				}
				params := map[string]any{"entity_id": entity, "run_id": f.base.RunID, "status": "pending", "anchor_kind": "stage_gate", "limit": 1}
				first := f.list(t, params)
				now := time.Now().UTC()
				if _, err := f.store.DeferDecisionCard(f.ctx, decisioncard.DeferRequest{CardID: cards[1].CardID, ActorTokenID: "cursor-proof", Until: now.Add(time.Hour), Now: now}); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.DecideDecisionCard(f.ctx, decisioncard.DecideRequest{CardID: cards[2].CardID, ActorTokenID: "cursor-proof", Verdict: "approve", ObservedContentHash: cards[2].CardContentHash, DecisionEventID: uuid.NewString(), Now: now}); err != nil {
					t.Fatal(err)
				}
				anchor, _ := cards[3].Anchor.StageGate()
				if err := f.store.SupersedeDecisionCardsForStage(f.ctx, cards[3].RunID, entity, anchor.StageActivationID, "cursor-proof", now); err != nil {
					t.Fatal(err)
				}
				fresh := f.walk(t, params)
				if len(fresh) != 2 || fresh[0].Card.CardID != cards[0].CardID || fresh[1].Card.CardID != cards[4].CardID {
					t.Fatalf("fresh pending=%v", cursorRowKeys(fresh))
				}
				params["cursor"] = first.Next
				continued := f.walk(t, params)
				if !reflect.DeepEqual(cursorRowKeys(continued), cursorRowKeys(fresh[1:])) {
					t.Fatalf("continuation=%v fresh=%v", cursorRowKeys(continued), cursorRowKeys(fresh))
				}
				delete(params, "cursor")
				for _, status := range []string{"deferred", "decided", "superseded"} {
					params["status"] = status
					if rows := f.walk(t, params); len(rows) != 1 {
						t.Fatalf("%s=%v", status, cursorRowKeys(rows))
					}
				}
			})
			t.Run("notice_filters_and_global_ack_count", func(t *testing.T) {
				entity := uuid.NewString()
				n := f.notice(t, entity, base)
				c := f.card(t, entity, base.Add(time.Second), decisioncard.AnchorKindStageGate)
				params := map[string]any{"entity_id": entity, "run_id": f.base.RunID, "status": "pending", "type": "nonmatching", "priority": "critical", "limit": 1}
				rows := f.walk(t, params)
				if len(rows) != 1 || rows[0].key() != c.key() {
					t.Fatalf("notice-only filters changed cards: %v", cursorRowKeys(rows))
				}
				delete(params, "type")
				params["priority"] = "normal"
				if rows := f.walk(t, params); len(rows) != 2 {
					t.Fatalf("filtered mixed=%v", cursorRowKeys(rows))
				}
				before := f.list(t, params)
				if err := f.store.MarkMailboxItemNotified(f.ctx, n.Notice.MailboxID); err != nil {
					t.Fatal(err)
				}
				params["anchor_kind"] = "stage_gate"
				after := f.list(t, params)
				if after.Unread != before.Unread-1 {
					t.Fatalf("global unread %d -> %d", before.Unread, after.Unread)
				}
			})
		})
	}
}
