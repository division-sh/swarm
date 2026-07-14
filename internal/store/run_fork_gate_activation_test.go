package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestMaterializeRunForkDecisionCardsCreatesForkLocalPendingAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", testGateRoutes(t), "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		Anchor:     newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot:   freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"summary": "source snapshot"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}),
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := &PostgresStore{DB: db}
	if err := cardStore.CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	humanCard, humanContinuation := newHumanTaskDecisionCardTestFixture(t, sourceRunID, "source-human-task", now, 10, now.Add(24*time.Hour))
	if err := cardStore.CreateHumanTaskCard(ctx, humanCard, humanContinuation); err != nil {
		t.Fatalf("create source human-task card: %v", err)
	}
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := cardStore.runAuthorActivityMutation(ctx, "test materialize fork decision cards", func(txctx context.Context, tx *sql.Tx) error {
		return materializeRunForkDecisionCards(txctx, tx, forkRunID, entityID, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(time.Minute))
	}); err != nil {
		t.Fatalf("materialize fork cards: %v", err)
	}
	forkCard, err := cardStore.GetDecisionCard(ctx, forkActivation.CardID)
	if err != nil {
		t.Fatalf("load fork card: %v", err)
	}
	forkAnchor := mustDecisionCardTestStageAnchor(t, forkCard)
	sourceAnchor := mustDecisionCardTestStageAnchor(t, sourceCard)
	if forkCard.RunID != forkRunID || forkCard.CardID == sourceCard.CardID || forkAnchor.StageActivationID == sourceAnchor.StageActivationID || forkCard.Status != decisioncard.StatusPending {
		t.Fatalf("fork card retained source authority: source=%#v fork=%#v", sourceCard, forkCard)
	}
	summary, _ := forkCard.Snapshot.Context.Lookup("summary")
	forkedFrom, _ := forkCard.Provenance.Lookup("forked_from_card_id")
	if forkCard.CardContentHash != sourceCard.CardContentHash || summary.Interface() != "source snapshot" || forkedFrom.Interface() != sourceCard.CardID {
		t.Fatalf("fork card snapshot/provenance = %#v", forkCard)
	}
	forkCards, _, err := cardStore.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
	if err != nil {
		t.Fatalf("list fork cards: %v", err)
	}
	if len(forkCards) != 1 || forkCards[0].CardID != forkCard.CardID || forkCards[0].Anchor.Kind() != decisioncard.AnchorKindStageGate {
		t.Fatalf("fork cards = %#v, want only materialized stage-gate authority", forkCards)
	}
	if sourceHuman, err := cardStore.GetDecisionCard(ctx, humanCard.CardID); err != nil || sourceHuman.RunID != sourceRunID || sourceHuman.Status != decisioncard.StatusPending {
		t.Fatalf("source human task changed during fork = %#v, %v", sourceHuman, err)
	}
}

func TestMaterializeRunForkDecisionCardsPreservesCommittedSemanticFields(t *testing.T) {
	const safeInteger = int64(9007199254740991)
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	sourceRunID, forkRunID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	sourceActivation, err := gateruntime.New(sourceRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", testGateRoutes(t), "event-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sourceCard, err := decisioncard.New(decisioncard.Card{
		CardID: sourceActivation.CardID, RunID: sourceRunID,
		Anchor: newDecisionCardTestStageAnchor("launch/review", "launch", entityID, sourceActivation.Stage, sourceActivation.ActivationID),
		Snapshot: freezeDecisionCardTestSnapshot(t, sourceActivation.DecisionID, map[string]any{"safe_integer": safeInteger}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", AdvancesTo: "done", Input: map[string]runtimecontracts.WorkflowGateInputField{"score": {Type: "integer", Required: true}}},
		}),
		BundleHash: sourceActivation.BundleHash, WorkflowVersion: "1", CreatedAt: now,
		Provenance: admitDecisionCardTestObject(t, map[string]any{"safe_integer": safeInteger}),
	})
	if err != nil {
		t.Fatal(err)
	}
	cardStore := &PostgresStore{DB: db}
	if err := cardStore.CreateDecisionCard(ctx, sourceCard); err != nil {
		t.Fatalf("create source card: %v", err)
	}
	decisionEventID := uuid.NewString()
	if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: sourceCard.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{"score": safeInteger}),
		ActorTokenID: "operator", ObservedContentHash: sourceCard.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("decide source card: %v", err)
	}
	if err := sourceActivation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	forkActivation, err := gateruntime.New(forkRunID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", sourceActivation.BundleHash, sourceActivation.RoutesJSON, sourceActivation.StartedByEvent, sourceActivation.OpenedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := forkActivation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := cardStore.runAuthorActivityMutation(ctx, "test materialize committed fork decision card", func(txctx context.Context, tx *sql.Tx) error {
		return materializeRunForkDecisionCards(txctx, tx, forkRunID, entityID, []runForkGateActivationBinding{{Source: sourceActivation, Fork: forkActivation}}, now.Add(2*time.Minute))
	}); err != nil {
		t.Fatalf("materialize committed fork card: %v", err)
	}
	forkCard, err := cardStore.GetDecisionCard(ctx, forkActivation.CardID)
	if err != nil {
		t.Fatal(err)
	}
	field, _ := forkCard.Fields.Lookup("score")
	fieldNumber, ok := field.Number()
	contextNumber, _ := forkCard.Snapshot.Context.Lookup("safe_integer")
	contextValue, _ := contextNumber.Number()
	provenanceNumber, _ := forkCard.Provenance.Lookup("safe_integer")
	provenanceValue, _ := provenanceNumber.Number()
	if forkCard.Status != decisioncard.StatusDecided || forkCard.DecisionEventID != decisionEventID || !ok || fieldNumber != float64(safeInteger) || contextValue != float64(safeInteger) || provenanceValue != float64(safeInteger) {
		t.Fatalf("committed fork card lost semantic authority: %#v", forkCard)
	}
}

func TestMaterializeRunForkProposedEffectCreatesFreshPendingAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	sourceRunID, forkRunID := uuid.NewString(), uuid.NewString()
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatal(err)
	}
	cards := &PostgresStore{DB: db}
	sourceCard, sourceContinuation := newProposedEffectTestCard(t, sourceRunID, now, attemptgeneration.Generation{})
	if err := cards.CreateProposedEffectCard(ctx, sourceCard, sourceContinuation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, entered_state_at, created_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 'root', 'default', 'operating', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $3, $3, $3)
	`, forkRunID, sourceContinuation.EntityID, now); err != nil {
		t.Fatal(err)
	}
	point := RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}
	if err := cards.runAuthorActivityMutation(ctx, "test materialize fork proposed-effect card", func(txctx context.Context, tx *sql.Tx) error {
		return materializeRunForkProposedEffectCards(txctx, tx, sourceRunID, forkRunID, sourceContinuation.EntityID, point, now.Add(2*time.Minute))
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: forkRunID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("fork proposed cards = %#v, %v", items, err)
	}
	forkCard, err := cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	forkContinuation, err := cards.LoadProposedEffectContinuation(ctx, forkCard.CardID)
	if err != nil {
		t.Fatal(err)
	}
	if sourceContinuation.ReplyContextID == "" || forkContinuation.ReplyContextID != "" {
		t.Fatalf("fork reply authority = source:%q fork:%q, want source-only", sourceContinuation.ReplyContextID, forkContinuation.ReplyContextID)
	}
	if forkCard.Status != decisioncard.StatusPending || forkCard.CardID == sourceCard.CardID || forkContinuation.RequestEventID == sourceContinuation.RequestEventID || forkContinuation.SourceRunID != forkRunID {
		t.Fatalf("fork authority retained source identity: source=%#v/%#v fork=%#v/%#v", sourceCard, sourceContinuation, forkCard, forkContinuation)
	}
	if forkContinuation.Input.Equal(sourceContinuation.Input) == false || forkContinuation.EffectContentHash == sourceContinuation.EffectContentHash {
		t.Fatalf("fork effect content = source:%#v fork:%#v", sourceContinuation, forkContinuation)
	}
	forkedFrom, ok := forkCard.Provenance.Lookup("forked_from_card_id")
	if value, stringOK := forkedFrom.String(); !ok || !stringOK || value != sourceCard.CardID {
		t.Fatalf("fork provenance = %#v", forkCard.Provenance)
	}
}

func TestPrepareRunForkApprovedProposedEffectRequiresUnambiguousTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		wantErr    string
		wantCopied bool
	}{
		{name: "succeeded", status: "succeeded", wantCopied: true},
		{name: "uncertain", status: "uncertain", wantErr: "ambiguous dispatch evidence"},
		{name: "started", status: "started", wantErr: "recorded evidence is not terminal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := context.Background()
			sourceRunID, forkRunID := uuid.NewString(), uuid.NewString()
			now := time.Date(2026, 7, 14, 19, 0, 0, 0, time.UTC)
			if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $3), ($2::uuid, 'running', $3)`, sourceRunID, forkRunID, now); err != nil {
				t.Fatal(err)
			}
			cards := &PostgresStore{DB: db}
			card, continuation := newProposedEffectTestCard(t, sourceRunID, now, attemptgeneration.Generation{})
			if err := cards.CreateProposedEffectCard(ctx, card, continuation); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := cards.CompleteProposedEffectRoute(ctx, card.CardID, uuid.NewString(), now.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, current_state,
					gates, fields, accumulator, entered_state_at, created_at, updated_at
				) VALUES ($1::uuid, $2::uuid, 'root', 'default', 'operating', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $3, $3, $3)
			`, forkRunID, continuation.EntityID, now); err != nil {
				t.Fatal(err)
			}
			resultEventID := activityidentity.ResultEventID(activityidentity.Fact{
				RunID: sourceRunID, SourceEventID: continuation.SourceEventID, EntityID: continuation.EntityID,
				FlowID: continuation.FlowID, NodeID: continuation.NodeID, HandlerEventKey: continuation.HandlerEventKey,
				ActivityID: continuation.ActivityID, Tool: continuation.Tool, Attempt: 1,
			}, continuation.SuccessEvent)
			var storedResultEventID any = resultEventID
			var resultEventType any = continuation.SuccessEvent
			var resultPayload any = `{"activity_id":"send_support_reply","result":{"ok":true}}`
			var failure any
			var completedAt any = now.Add(3 * time.Minute)
			switch tc.status {
			case "uncertain":
				resultEventType = continuation.FailureEvent
				resultPayload = `{"activity_id":"send_support_reply","failure":{"code":"provider_outcome_uncertain"}}`
				failure = `{"schema_version":"platform.failure/v1","class":"platform.outcome_uncertain","detail":{"code":"provider_outcome_uncertain"},"retryable":false,"deterministic":false,"message":"Provider outcome is uncertain.","remediation":"Inspect provider state.","component":"activity-runtime","operation":"execute"}`
			case "started":
				storedResultEventID, resultEventType, resultPayload, failure, completedAt = nil, nil, nil, nil, nil
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO activity_attempts (
					request_event_id, run_id, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
					activity_id, tool, effect_class, attempt, status, success_event, failure_event,
					result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage,
					started_at, completed_at, updated_at
				) VALUES (
					$1::uuid, $2::uuid, $3::uuid, $4::uuid, 'root', $5, $6,
					$7, $8, 'non_idempotent_write', 1, $9, $10, $11,
					$12::uuid, $13, $14::jsonb, $15::jsonb, 'input-hash', '{}'::jsonb, '', $16, $17, $16
				)
			`, continuation.RequestEventID, sourceRunID, continuation.SourceEventID, continuation.EntityID,
				continuation.NodeID, continuation.HandlerEventKey, continuation.ActivityID, continuation.Tool, tc.status,
				continuation.SuccessEvent, continuation.FailureEvent, storedResultEventID, resultEventType,
				resultPayload, failure, now.Add(3*time.Minute), completedAt); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]any{
				"activity_id": continuation.ActivityID, "tool": continuation.Tool, "input": continuation.Input.Interface(),
				"effect_class": string(continuation.EffectClass), "success_event": continuation.SuccessEvent,
				"failure_event": continuation.FailureEvent, "fork_policy": string(continuation.ForkPolicy),
				"entity_id": continuation.EntityID, "node_id": continuation.NodeID, "flow_id": continuation.FlowID,
				"handler_event_key": continuation.HandlerEventKey, "source_event_id": continuation.SourceEventID,
				"source_run_id": sourceRunID, "attempt": 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			var prepared RunForkSelectedContractSourceEvent
			err = cards.runAuthorActivityMutation(ctx, "test prepare fork proposed-effect source event", func(txctx context.Context, tx *sql.Tx) error {
				var inner error
				prepared, inner = prepareRunForkSelectedContractSourceEvent(txctx, tx, forkRunID, RunForkSelectedContractSourceEvent{
					SourceEventID: continuation.RequestEventID, EventName: runForkActivityRequestEvent,
					EntityID: continuation.EntityID, FlowInstance: "root", Payload: payload,
				})
				return inner
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("prepare error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var forkPayload runForkActivityRequestPayload
			if err := json.Unmarshal(prepared.Payload, &forkPayload); err != nil {
				t.Fatal(err)
			}
			forkRequestID := activityidentity.RequestEventID(activityidentity.Fact{
				RunID: forkRunID, SourceEventID: forkPayload.SourceEventID, ParentEventID: forkPayload.ParentEventID,
				EntityID: forkPayload.EntityID, FlowID: forkPayload.FlowID, NodeID: forkPayload.NodeID,
				HandlerEventKey: forkPayload.HandlerEventKey, ActivityID: forkPayload.ActivityID, Tool: forkPayload.Tool, Attempt: 1,
			})
			var copiedStatus string
			if err := db.QueryRowContext(ctx, `SELECT status FROM activity_attempts WHERE request_event_id = $1::uuid`, forkRequestID).Scan(&copiedStatus); err != nil {
				t.Fatal(err)
			}
			if !tc.wantCopied || copiedStatus != tc.status {
				t.Fatalf("copied status = %q", copiedStatus)
			}
		})
	}
}
