package runtimepersistence

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestDecisionCardEntityFilterParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			store, runID := decisionCardTestStore(t, backend)
			otherRun, entityID, otherEntity := uuid.NewString(), uuid.NewString(), uuid.NewString()
			now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
			if err := ensureEphemeralRunForTest(ctx, store, otherRun, now.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			var all, matching, sameEntity, otherEntityCards []decisioncard.Card
			byKind := map[decisioncard.AnchorKind][]decisioncard.Card{}
			seed := func(kind decisioncard.AnchorKind, run, entity string, at time.Time, global bool) decisioncard.Card {
				card := entityFilterCardForTest(t, kind, run, entity, at, global)
				if err := store.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				all = append(all, card)
				return card
			}
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				for i := 0; i < 5; i++ {
					card := seed(kind, runID, entityID, now.Add(time.Duration(i/2)*time.Second), false)
					matching = append(matching, card)
					sameEntity = append(sameEntity, card)
					byKind[kind] = append(byKind[kind], card)
				}
				sameEntity = append(sameEntity, seed(kind, otherRun, entityID, now, false))
				otherEntityCards = append(otherEntityCards, seed(kind, runID, otherEntity, now, false))
				if kind != decisioncard.AnchorKindStageGate {
					// A source route naming the entity is not an entity-scoped anchor.
					seed(kind, runID, entityID, now, true)
				}
			}
			decided := seed(decisioncard.AnchorKindStageGate, runID, entityID, now, false)
			if _, err := store.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: decided.CardID, Verdict: "approve", ObservedContentHash: decided.CardContentHash,
				ActorTokenID: "operator", DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			sameEntity = append(sameEntity, decided)

			check := func(t *testing.T, opts decisioncard.ListOptions, want []decisioncard.Card) {
				t.Helper()
				items, cursor, err := store.ListDecisionCards(ctx, opts)
				if err != nil {
					t.Fatalf("ListDecisionCards(%+v): %v", opts, err)
				}
				if cursor != "" {
					t.Fatalf("unexpected cursor %q for complete result", cursor)
				}
				got := make([]string, 0, len(items))
				for _, item := range items {
					got = append(got, item.CardID)
				}
				if expected := orderedEntityFilterCardIDs(want); !reflect.DeepEqual(got, expected) {
					t.Fatalf("card IDs = %v, want %v", got, expected)
				}
			}
			for _, tc := range []struct {
				name string
				opts decisioncard.ListOptions
				want []decisioncard.Card
			}{
				{"unfiltered", decisioncard.ListOptions{}, all},
				{"entity only", decisioncard.ListOptions{EntityID: entityID}, sameEntity},
				{"other entity", decisioncard.ListOptions{EntityID: otherEntity}, otherEntityCards},
				{"nonmatching entity", decisioncard.ListOptions{EntityID: uuid.NewString()}, nil},
				{"entity is a bound value", decisioncard.ListOptions{EntityID: "' OR 1=1 --"}, nil},
				{"run status entity", decisioncard.ListOptions{RunID: runID, EntityID: entityID, Status: "pending"}, matching},
				{"numbered status before entity", decisioncard.ListOptions{RunID: runID, EntityID: entityID, Status: "decided", AnchorKind: "stage_gate"}, []decisioncard.Card{decided}},
				{"nonmatching combined", decisioncard.ListOptions{RunID: otherRun, EntityID: otherEntity, Status: "pending", AnchorKind: "human_task"}, nil},
			} {
				t.Run(tc.name, func(t *testing.T) { check(t, tc.opts, tc.want) })
			}
			for kind, want := range byKind {
				t.Run(string(kind)+" combined and cursor", func(t *testing.T) {
					opts := decisioncard.ListOptions{RunID: runID, EntityID: entityID, Status: "pending", AnchorKind: string(kind)}
					check(t, opts, want)
					opts.Limit = 2
					var got []string
					seen := map[string]bool{}
					for page := 0; page < 3; page++ {
						items, cursor, err := store.ListDecisionCards(ctx, opts)
						if err != nil {
							t.Fatalf("page %d: %v", page, err)
						}
						wantSize := 2
						if page == 2 {
							wantSize = 1
						}
						if len(items) != wantSize || (cursor == "") != (page == 2) || cursor != "" && cursor == opts.Cursor {
							t.Fatalf("page %d: items=%d cursor=%q previous=%q", page, len(items), cursor, opts.Cursor)
						}
						for _, item := range items {
							if seen[item.CardID] || item.RunID != runID || item.Scope.EntityID != entityID || item.Anchor.Kind() != kind || item.Status != "pending" {
								t.Fatalf("page %d leaked or duplicated card: %+v", page, item)
							}
							seen[item.CardID] = true
							got = append(got, item.CardID)
						}
						opts.Cursor = cursor
					}
					if expected := orderedEntityFilterCardIDs(want); !reflect.DeepEqual(got, expected) {
						t.Fatalf("paginated IDs = %v, want %v", got, expected)
					}
				})
			}
			t.Run("malformed cursor unchanged", func(t *testing.T) {
				for _, filter := range []string{"", entityID} {
					_, _, err := store.ListDecisionCards(ctx, decisioncard.ListOptions{EntityID: filter, Cursor: "not-a-cursor"})
					if !errors.Is(err, decisioncard.ErrInvalidCursor) {
						t.Fatalf("malformed cursor with entity %q = %v, want ErrInvalidCursor", filter, err)
					}
				}
			})
		})
	}
}

func orderedEntityFilterCardIDs(cards []decisioncard.Card) []string {
	cards = append([]decisioncard.Card(nil), cards...)
	sort.Slice(cards, func(i, j int) bool {
		if cards[i].CreatedAt.Equal(cards[j].CreatedAt) {
			return cards[i].CardID < cards[j].CardID
		}
		return cards[i].CreatedAt.Before(cards[j].CreatedAt)
	})
	ids := make([]string, 0, len(cards))
	for _, card := range cards {
		ids = append(ids, card.CardID)
	}
	return ids
}

func entityFilterCardForTest(t *testing.T, kind decisioncard.AnchorKind, runID, entityID string, at time.Time, global bool) decisioncard.Card {
	t.Helper()
	path := "launch/" + uuid.NewString()
	source := eventtest.ConcreteTemplateRoutingSource("launch", path, entityID)
	scope := decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: path, EntityID: entityID}
	if global {
		scope = decisioncard.Scope{Kind: decisioncard.ScopeGlobal}
	}
	var anchor decisioncard.Anchor
	var err error
	outcome := runtimecontracts.WorkflowGateOutcomePlan{Verdict: "approve"}
	effectHash := ""
	switch kind {
	case decisioncard.AnchorKindStageGate:
		anchor = newDecisionCardTestStageAnchor(path, "launch", entityID, "waiting", uuid.NewString())
		outcome.AdvancesTo = "done"
	case decisioncard.AnchorKindHumanTask:
		anchor, err = decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{RequesterAgentID: "reviewer", OperationID: uuid.NewString(), Category: "review", Scope: scope, Source: source})
	case decisioncard.AnchorKindProposedEffect:
		anchor, err = decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{RequestEventID: uuid.NewString(), ActivityID: "review", Decision: "review", Scope: scope, Source: source})
		if err == nil {
			effectHash, err = canonicaljson.HashValue(semanticvalue.EmptyObject())
		}
	default:
		t.Fatalf("unknown anchor kind %q", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor, ExecutionMode: "live",
		Snapshot:          freezeDecisionCardTestSnapshot(t, "review", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": outcome}),
		EffectContentHash: effectHash, BundleHash: authorActivityTestBundleHash, WorkflowVersion: "1", CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card
}
