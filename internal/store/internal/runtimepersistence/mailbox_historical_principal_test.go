package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

// This is the historical materialization port, not public run.fork atomicity.
func TestForkMaterializedDecisionPrincipalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, sourceRunID := decisionCardTestStore(t, backend)
			db := DatabaseForTest(selected)
			ctx := testAuthorActivityContext()
			now := time.Now().UTC().Truncate(time.Microsecond)
			principal, err := selected.(interface {
				EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
			}).EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			for _, status := range []string{"open", "committed", "superseded"} {
				t.Run(status, func(t *testing.T) {
					forkRunID, entityID := uuid.NewString(), uuid.NewString()
					if err := ensureEphemeralRunForTest(ctx, selected, forkRunID, now); err != nil {
						t.Fatal(err)
					}
					source, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", authorActivityTestBundleHash, testGateRoutes(t), "event-1", now)
					if err != nil {
						t.Fatal(err)
					}
					card, err := decisioncard.New(decisioncard.Card{
						CardID: source.CardID, RunID: sourceRunID, ExecutionMode: "live",
						Anchor: newDecisionCardTestStageAnchor("launch/review", "launch", entityID, source.Stage, source.ActivationID),
						Snapshot: freezeDecisionCardTestSnapshot(t, source.DecisionID, map[string]any{"summary": "historical"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
							"approve": {Verdict: "approve", AdvancesTo: "done"},
						}),
						BundleHash: source.BundleHash, WorkflowVersion: "1", CreatedAt: now,
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := selected.CreateDecisionCard(ctx, card); err != nil {
						t.Fatal(err)
					}
					fork, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", source.Stage, source.DecisionID, source.BundleHash, source.RoutesJSON, source.StartedByEvent, source.OpenedAt)
					if err != nil {
						t.Fatal(err)
					}
					if status == "committed" {
						eventID := uuid.NewString()
						if _, err := DecisionCardDomainForTest(selected).ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal.ID, ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: now.Add(time.Second)}); err != nil {
							t.Fatal(err)
						}
						if err := source.CommitDecision(eventID, now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if err := fork.CommitDecision(eventID, now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
					} else if status == "superseded" {
						if err := selected.SupersedeDecisionCardsForStage(ctx, sourceRunID, entityID, source.ActivationID, "stage_exited", now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if !source.Supersede("stage_exited", now.Add(time.Second)) || !fork.Supersede("stage_exited", now.Add(time.Second)) {
							t.Fatal("supersession fixture did not change gate state")
						}
					}
					before, err := selected.GetDecisionCard(ctx, card.CardID)
					if err != nil {
						t.Fatal(err)
					}
					projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, "launch/review")
					if err != nil {
						t.Fatal(err)
					}
					materialize := func(p runForkEntityProjection) error {
						bindings := []runForkGateActivationBinding{{Source: source, Fork: fork}}
						switch s := selected.(type) {
						case *PostgresStore:
							return s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
								return s.runForkPostgresOwner.MaterializeRunForkDecisionCardsTx(txctx, tx, story, forkRunID, p, bindings, now.Add(2*time.Second))
							})
						case *SQLiteRuntimeStore:
							return s.runPrivateAuthorActivityMutation(ctx, "historical mailbox principal proof", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
								return s.runForkSQLiteOwner.MaterializeRunForkDecisionCardsTx(txctx, tx, story, forkRunID, p, bindings, now.Add(2*time.Second))
							})
						default:
							panic("unexpected selected store")
						}
					}
					wrong := projection
					wrong.Source.EntityID = uuid.NewString()
					if err := materialize(wrong); err == nil {
						t.Fatal("historical actor copied through contradictory source ownership")
					}
					var count int
					if err := db.QueryRow(`SELECT count(*) FROM decision_cards WHERE run_id=$1`, forkRunID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("refusal created fork cards: count=%d err=%v", count, err)
					}
					if err := materialize(projection); err != nil {
						t.Fatal(err)
					}
					copied, err := selected.GetDecisionCard(ctx, fork.CardID)
					if err != nil {
						t.Fatal(err)
					}
					if copied.Status != before.Status || copied.DecidedBy != before.DecidedBy || copied.Verdict != before.Verdict || copied.SupersededReason != before.SupersededReason || copied.DecisionEventID != before.DecisionEventID {
						t.Fatalf("historical semantic actor/state changed: source=%+v copied=%+v", before, copied)
					}
					if status == "committed" && copied.DecidedBy != principal.ID {
						t.Fatal("historical principal was replaced by transport or fork identity")
					}
					after, err := selected.GetDecisionCard(ctx, card.CardID)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("historical copy changed source: %v", err)
					}
					for _, query := range []string{`SELECT count(*) FROM api_idempotency`, `SELECT count(*) FROM events WHERE run_id=$1`} {
						var args []any
						if query != `SELECT count(*) FROM api_idempotency` {
							args = []any{forkRunID}
						}
						if err := db.QueryRow(query, args...).Scan(&count); err != nil || count != 0 {
							t.Fatalf("history minted fresh response/publication: query=%s count=%d err=%v", query, count, err)
						}
					}
				})
			}
		})
	}
}
