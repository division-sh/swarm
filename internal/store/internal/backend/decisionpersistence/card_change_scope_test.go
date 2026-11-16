package decisionpersistence

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

type cardScopeStory struct{ drafts []authoractivity.Draft }

func (s *cardScopeStory) Record(_ context.Context, draft authoractivity.Draft) error {
	s.drafts = append(s.drafts, draft)
	return nil
}

func (*cardScopeStory) PersistedOccurredAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

// Exercise the decision-change writer and its real card decoder; SQL is mocked,
// so this proves attribution selection/refusal, not native mutation admission.
func TestDecisionCardChangeStoryTargetAttribution(t *testing.T) {
	current := authoractivity.BundleScope(uuid.NewString(), "current")
	for _, postgres := range []bool{false, true} {
		for _, test := range []struct {
			name, change, wantError string
			kind                    decisioncard.AnchorKind
			scope                   authoractivity.Scope
		}{
			{"gate_decided", "decided", "", decisioncard.AnchorKindStageGate, current},
			{"gate_deferred", "deferred", "", decisioncard.AnchorKindStageGate, current},
			{"missing_scope", "decided", "exact runtime scope", decisioncard.AnchorKindStageGate, authoractivity.Scope{}},
			{"missing_runtime", "deferred", "exact runtime scope", decisioncard.AnchorKindStageGate, authoractivity.BundleScope("", "current")},
			{"global_scope", "decided", "cannot derive", decisioncard.AnchorKindStageGate, authoractivity.Scope{Kind: authoractivity.ScopeGlobal}},
			{"gate_created", "created", "conflicts with context", decisioncard.AnchorKindStageGate, current},
			{"gate_expired", "expired", "conflicts with context", decisioncard.AnchorKindStageGate, current},
			{"gate_superseded", "superseded", "conflicts with context", decisioncard.AnchorKindStageGate, current},
			{"human_decided", "decided", "conflicts with context", decisioncard.AnchorKindHumanTask, current},
			{"human_deferred", "deferred", "conflicts with context", decisioncard.AnchorKindHumanTask, current},
			{"effect_decided", "decided", "conflicts with context", decisioncard.AnchorKindProposedEffect, current},
			{"effect_deferred", "deferred", "conflicts with context", decisioncard.AnchorKindProposedEffect, current},
		} {
			dialect := "sqlite/"
			if postgres {
				dialect = "postgres/"
			}
			t.Run(dialect+test.name, func(t *testing.T) {
				card := cardScopeFixture(t, test.kind)
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				snapshot, err := decisioncard.SnapshotJSON(card)
				if err != nil {
					t.Fatal(err)
				}
				anchor, err := canonicaljson.Encode(card.Anchor.SemanticValue())
				if err != nil {
					t.Fatal(err)
				}
				cadence, err := json.Marshal(card.EffectiveCadence)
				if err != nil {
					t.Fatal(err)
				}
				rows := sqlmock.NewRows(strings.Fields("card_id run_id anchor_kind anchor status execution_mode snapshot content_hash effect_hash schema_hash bundle_hash workflow_version cadence provenance verdict fields decided_by decided_at deferred_until decision_event_id receipt_id render_hash reason created_at updated_at"))
				rows.AddRow(card.CardID, card.RunID, string(card.Anchor.Kind()), anchor, card.Status, string(card.ExecutionMode), snapshot, card.CardContentHash, card.EffectContentHash, card.DecisionSchemaHash, card.BundleHash, card.WorkflowVersion, cadence, "{}", "", "{}", "", nil, nil, "", "", "", "", card.CreatedAt, card.UpdatedAt)
				mock.ExpectQuery(regexp.QuoteMeta(decisionCardSelect)).WithArgs(card.CardID).WillReturnRows(rows)
				ctx := authoractivity.WithScope(t.Context(), test.scope)
				story := &cardScopeStory{}
				err = recordDecisionCardChangeAuthorActivity(ctx, story, db, 7, card.RunID, card.CardID, test.change, card.CreatedAt, postgres)
				if test.wantError != "" {
					if err == nil || !strings.Contains(err.Error(), test.wantError) || len(story.drafts) != 0 {
						t.Fatalf("writer refused incorrectly: err=%v drafts=%+v", err, story.drafts)
					}
				} else if err != nil || len(story.drafts) != 1 || story.drafts[0].Scope != authoractivity.BundleScope(current.RuntimeInstanceID, card.BundleHash) || story.drafts[0].Projection.CardID != card.CardID {
					t.Fatalf("writer lost target attribution: err=%v drafts=%+v", err, story.drafts)
				}
				if got, _ := authoractivity.ScopeFromContext(ctx); got != test.scope {
					t.Fatal("writer changed execution context scope")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func cardScopeFixture(t *testing.T, kind decisioncard.AnchorKind) decisioncard.Card {
	t.Helper()
	run, entity := uuid.NewString(), uuid.NewString()
	source := eventtest.RootRoutingSource(entity)
	var anchor decisioncard.Anchor
	var err error
	switch kind {
	case decisioncard.AnchorKindStageGate:
		anchor, err = decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{Route: flowidentity.RouteForInstancePath(entity), FlowID: ".", EntityID: entity, Source: source, Stage: "review", StageActivationID: uuid.NewString()})
	case decisioncard.AnchorKindHumanTask:
		operation, opErr := decisioncard.NewHumanTaskOperationID(run, "review")
		if opErr != nil {
			t.Fatal(opErr)
		}
		anchor, err = decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{RequesterAgentID: "operator", OperationID: operation, Category: "review", Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source})
	case decisioncard.AnchorKindProposedEffect:
		anchor, err = decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{RequestEventID: uuid.NewString(), ActivityID: "review", Decision: "review", Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source})
	}
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("review", "", nil, map[string]contracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}})
	if err != nil {
		t.Fatal(err)
	}
	card := decisioncard.Card{CardID: uuid.NewString(), RunID: run, Anchor: anchor, ExecutionMode: "live", BundleHash: "issued", Snapshot: snapshot, CreatedAt: time.Now().UTC()}
	if kind == decisioncard.AnchorKindProposedEffect {
		card.EffectContentHash = "sha256:" + strings.Repeat("a", 64)
	}
	card, err = decisioncard.New(card)
	if err != nil {
		t.Fatal(err)
	}
	return card
}
