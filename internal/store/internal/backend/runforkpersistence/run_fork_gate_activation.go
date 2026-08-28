package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type runForkDecisionMaterializer interface {
	LoadTx(context.Context, *sql.Tx, string, bool) (decisioncard.Card, error)
	InsertTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.Card) error
	DecideTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error)
	SupersedeStageTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, string, string, string, string, time.Time) (bool, error)
	LoadProposedEffectTx(context.Context, *sql.Tx, string, bool) (decisioncard.ProposedEffectContinuation, error)
	InsertProposedEffectTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.Card, decisioncard.ProposedEffectContinuation) error
}

func materializeRunForkDecisionCards(ctx context.Context, decisions runForkDecisionMaterializer, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID string, projection runForkEntityProjection, bindings []runForkGateActivationBinding, now time.Time) error {
	if story == nil {
		return fmt.Errorf("fork decision-card materialization requires private story ownership")
	}
	if projection.Source.EntityID == "" || projection.Source.FlowInstance == "" || projection.Fork.EntityID == "" || projection.Fork.FlowInstance == "" {
		return fmt.Errorf("fork decision-card materialization requires exact source and fork entity ownership")
	}
	for _, binding := range bindings {
		sourceCard, err := decisions.LoadTx(ctx, tx, binding.Source.CardID, false)
		if err != nil {
			return fmt.Errorf("load source decision card %s for fork: %w", binding.Source.CardID, err)
		}
		sourceVerdict := sourceCard.Verdict
		sourceFields := sourceCard.Fields
		sourceActor := sourceCard.DecidedBy
		sourceReceipt := sourceCard.DeliveryReceiptID
		sourceRenderHash := sourceCard.DeliveryRenderHash

		forkCard := sourceCard
		forkCard.CardID = binding.Fork.CardID
		forkCard.RunID = strings.TrimSpace(forkRunID)
		sourceAnchor, err := sourceCard.Anchor.StageGate()
		if err != nil {
			return fmt.Errorf("source decision card %s anchor: %w", sourceCard.CardID, err)
		}
		if sourceAnchor.EntityID != projection.Source.EntityID || sourceAnchor.Route.InstancePath != projection.Source.FlowInstance {
			return fmt.Errorf("source decision card %s owner does not match fork source entity ownership", sourceCard.CardID)
		}
		forkRoute := runtimeflowidentity.RouteForInstancePath(projection.Fork.FlowInstance)
		forkSource, err := forkDecisionCardExecutionSource(sourceAnchor.Source, projection.Fork.FlowInstance, projection.Fork.EntityID)
		if err != nil {
			return fmt.Errorf("construct fork stage_gate source: %w", err)
		}
		forkCard.Anchor, err = decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
			Route: forkRoute, FlowID: sourceAnchor.FlowID,
			EntityID: projection.Fork.EntityID, Source: forkSource, Stage: sourceAnchor.Stage,
			StageActivationID: binding.Fork.ActivationID,
		})
		if err != nil {
			return fmt.Errorf("construct fork stage_gate anchor: %w", err)
		}
		forkCard.Status = decisioncard.StatusPending
		forkCard.Verdict = ""
		forkCard.Fields = semanticvalue.EmptyObject()
		forkCard.DecidedBy = ""
		forkCard.DecidedAt = time.Time{}
		forkCard.DeferredUntil = time.Time{}
		forkCard.DecisionEventID = ""
		forkCard.DeliveryReceiptID = ""
		forkCard.DeliveryRenderHash = ""
		forkCard.SupersededReason = ""
		forkCard.CreatedAt = now.UTC()
		forkCard.UpdatedAt = now.UTC()
		forkedFromCardID, err := semanticvalue.String(sourceCard.CardID)
		if err != nil {
			return fmt.Errorf("admit source decision card identity: %w", err)
		}
		forkCard.Provenance, err = sourceCard.Provenance.With("forked_from_card_id", forkedFromCardID)
		if err != nil {
			return fmt.Errorf("extend fork decision card provenance: %w", err)
		}
		forkedFromActivationID, err := semanticvalue.String(binding.Source.ActivationID)
		if err != nil {
			return fmt.Errorf("admit source gate activation identity: %w", err)
		}
		forkCard.Provenance, err = forkCard.Provenance.With("forked_from_stage_activation_id", forkedFromActivationID)
		if err != nil {
			return fmt.Errorf("extend fork decision card provenance: %w", err)
		}
		forkCard, err = decisioncard.New(forkCard)
		if err != nil {
			return fmt.Errorf("construct fork decision card: %w", err)
		}
		if err := decisions.InsertTx(ctx, story, tx, forkCard); err != nil {
			return fmt.Errorf("insert fork decision card: %w", err)
		}
		switch binding.Fork.Status {
		case gateruntime.StatusOpen:
		case gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted:
			if strings.TrimSpace(sourceVerdict) == "" || strings.TrimSpace(binding.Fork.DecisionEventID) == "" {
				return fmt.Errorf("source decision card %s lacks committed verdict evidence", sourceCard.CardID)
			}
			if _, err := decisions.DecideTx(ctx, story, tx, decisioncard.DecideRequest{
				CardID: forkCard.CardID, Verdict: sourceVerdict, Fields: sourceFields, ActorTokenID: sourceActor,
				ObservedContentHash: forkCard.CardContentHash, DeliveryReceiptID: sourceReceipt, DeliveryRenderHash: sourceRenderHash,
				DecisionEventID: binding.Fork.DecisionEventID, Now: now,
			}); err != nil {
				return fmt.Errorf("restore committed fork decision card: %w", err)
			}
		case gateruntime.StatusSuperseded:
			if _, err := decisions.SupersedeStageTx(ctx, story, tx, forkRunID, projection.Fork.EntityID, binding.Fork.ActivationID, binding.Fork.SupersededReason, now); err != nil {
				return fmt.Errorf("restore superseded fork decision card: %w", err)
			}
		}
	}
	return nil
}

func (s *RunForkPostgresOwner) MaterializeRunForkDecisionCardsTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID string, projection RunForkEntityProjection, bindings []RunForkGateActivationBinding, now time.Time) error {
	return materializeRunForkDecisionCards(ctx, s.DecisionPostgresOwner, tx, story, forkRunID, projection, bindings, now)
}

func (s *RunForkSQLiteOwner) MaterializeRunForkDecisionCardsTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID string, projection RunForkEntityProjection, bindings []RunForkGateActivationBinding, now time.Time) error {
	return materializeRunForkDecisionCards(ctx, s.DecisionSQLiteOwner, tx, story, forkRunID, projection, bindings, now)
}

type runForkProposedEffectMaterializer func(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, string, string, runForkEntityProjection, runfork.RunForkPoint, time.Time) error

const postgresRunForkProposedEffectCardIDsQuery = `
	SELECT p.card_id
	FROM proposed_effect_continuations p
	JOIN decision_cards c ON c.card_id = p.card_id
	WHERE p.run_id = $1::uuid
	  AND p.effect->>'entity_id' = $2
	  AND c.created_at <= $3
	ORDER BY c.created_at, p.card_id
	FOR UPDATE OF p, c
`

const sqliteRunForkProposedEffectCardIDsQuery = `
	SELECT p.card_id
	FROM proposed_effect_continuations p
	JOIN decision_cards c ON c.card_id = p.card_id
	WHERE p.run_id = $1
	  AND CAST(json_extract(p.effect, '$.entity_id') AS TEXT) = $2
	  AND c.created_at <= $3
	ORDER BY c.created_at, p.card_id
`

func materializeRunForkProposedEffectCards(ctx context.Context, decisions runForkDecisionMaterializer, cardIDsQuery string, tx *sql.Tx, story runtimeauthoractivity.Mutation, sourceRunID, forkRunID string, projection runForkEntityProjection, forkPoint runfork.RunForkPoint, now time.Time) error {
	if story == nil {
		return fmt.Errorf("fork proposed-effect materialization requires private story ownership")
	}
	rows, err := tx.QueryContext(ctx, cardIDsQuery, strings.TrimSpace(sourceRunID), projection.Source.EntityID, forkPoint.Timestamp.UTC())
	if err != nil {
		return fmt.Errorf("load source proposed effects for fork: %w", err)
	}
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			_ = rows.Close()
			return err
		}
		cardIDs = append(cardIDs, cardID)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(cardIDs) == 0 {
		return nil
	}
	forkGenerations, err := loadRunForkEntityGenerations(ctx, tx, forkRunID, projection.Fork.EntityID)
	if err != nil {
		return err
	}
	for _, cardID := range cardIDs {
		sourceCard, err := decisions.LoadTx(ctx, tx, cardID, false)
		if err != nil {
			return fmt.Errorf("load source proposed-effect card %s: %w", cardID, err)
		}
		pendingAtFork := sourceCard.Status == decisioncard.StatusPending
		if !sourceCard.DecidedAt.IsZero() {
			pendingAtFork = sourceCard.DecidedAt.After(forkPoint.Timestamp)
		} else if sourceCard.Status == decisioncard.StatusSuperseded {
			pendingAtFork = sourceCard.UpdatedAt.After(forkPoint.Timestamp)
		}
		if !pendingAtFork {
			continue
		}
		sourceContinuation, err := decisions.LoadProposedEffectTx(ctx, tx, cardID, false)
		if err != nil {
			return fmt.Errorf("load source proposed-effect continuation %s: %w", cardID, err)
		}
		forkCard, forkContinuation, err := forkPendingProposedEffect(sourceCard, sourceContinuation, forkRunID, projection, forkGenerations, now)
		if err != nil {
			return err
		}
		if err := decisions.InsertProposedEffectTx(ctx, story, tx, forkCard, forkContinuation); err != nil {
			return fmt.Errorf("insert fork-local proposed effect: %w", err)
		}
	}
	return nil
}

func (s *RunForkPostgresOwner) MaterializeRunForkProposedEffectCardsTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, sourceRunID, forkRunID string, projection RunForkEntityProjection, forkPoint runfork.RunForkPoint, now time.Time) error {
	return materializeRunForkProposedEffectCards(ctx, s.DecisionPostgresOwner, postgresRunForkProposedEffectCardIDsQuery, tx, story, sourceRunID, forkRunID, projection, forkPoint, now)
}

func (s *RunForkSQLiteOwner) MaterializeRunForkProposedEffectCardsTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, sourceRunID, forkRunID string, projection RunForkEntityProjection, forkPoint runfork.RunForkPoint, now time.Time) error {
	return materializeRunForkProposedEffectCards(ctx, s.DecisionSQLiteOwner, sqliteRunForkProposedEffectCardIDsQuery, tx, story, sourceRunID, forkRunID, projection, forkPoint, now)
}

func forkPendingProposedEffect(sourceCard decisioncard.Card, source decisioncard.ProposedEffectContinuation, forkRunID string, projection runForkEntityProjection, forkGenerations []attemptgeneration.Generation, now time.Time) (decisioncard.Card, decisioncard.ProposedEffectContinuation, error) {
	source = source.Canonical()
	if source.EntityID != projection.Source.EntityID || source.FlowInstance != projection.Source.FlowInstance {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("source proposed effect %s owner does not match fork source entity ownership", source.ActivityID)
	}
	fork := source
	fork.RunID = strings.TrimSpace(forkRunID)
	fork.SourceRunID = fork.RunID
	fork.EntityID = projection.Fork.EntityID
	fork.FlowInstance = projection.Fork.FlowInstance
	fork.ReplyContextID = ""
	fork.SourceEventID = activityidentity.ForkLineageEventID(fork.RunID, source.SourceEventID)
	if source.ParentEventID != "" {
		fork.ParentEventID = activityidentity.ForkLineageEventID(fork.RunID, source.ParentEventID)
	}
	if source.Generation.Valid() {
		matched := false
		for _, generation := range forkGenerations {
			if strings.TrimSpace(generation.LoopID) == strings.TrimSpace(source.Generation.LoopID) {
				fork.Generation = generation.Normalize()
				matched = true
				break
			}
		}
		if !matched {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("fork proposed effect %s has no fork-local loop generation", source.ActivityID)
		}
	}
	owner, err := activityidentity.ParseOwnerKey(fork.NodeID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("fork proposed effect %s owner identity: %w", fork.ActivityID, err)
	}
	fact := activityidentity.Fact{
		RunID: fork.RunID, SourceEventID: fork.SourceEventID, ParentEventID: fork.ParentEventID,
		EntityID: fork.EntityID, Owner: owner, ExecutionFlowID: fork.FlowID,
		HandlerEventKey: fork.HandlerEventKey, ActivityID: fork.ActivityID, Tool: fork.Tool,
		Attempt: fork.Attempt, RevisionID: fork.Generation.RevisionID,
	}
	fork.RequestEventID = activityidentity.RequestEventID(fact)
	sourceAnchor, err := sourceCard.Anchor.ProposedEffect()
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	fork.CardID = decisioncard.ProposedEffectCardID(fork.RequestEventID, sourceAnchor.Decision)
	fork.State = decisioncard.ProposedEffectPending
	fork.Verdict = ""
	fork.DecisionEventID = ""
	fork.RouteEventID = ""
	fork.SupersededReason = ""
	fork.CreatedAt = now.UTC()
	fork.UpdatedAt = now.UTC()
	effect, err := fork.EffectValue()
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	fork.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	scope := sourceAnchor.Scope
	scope.FlowInstance = fork.FlowInstance
	scope.EntityID = fork.EntityID
	forkSource, err := forkDecisionCardExecutionSource(sourceAnchor.Source, fork.FlowInstance, fork.EntityID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: fork.RequestEventID, ActivityID: fork.ActivityID, Decision: sourceAnchor.Decision, Scope: scope,
		Source: forkSource,
	})
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkCard := sourceCard
	forkCard.CardID = fork.CardID
	forkCard.RunID = fork.RunID
	forkCard.Anchor = anchor
	forkCard.EffectContentHash = fork.EffectContentHash
	forkCard.Status = decisioncard.StatusPending
	forkCard.Verdict = ""
	forkCard.Fields = semanticvalue.EmptyObject()
	forkCard.DecidedBy = ""
	forkCard.DecidedAt = time.Time{}
	forkCard.DeferredUntil = time.Time{}
	forkCard.DecisionEventID = ""
	forkCard.DeliveryReceiptID = ""
	forkCard.DeliveryRenderHash = ""
	forkCard.SupersededReason = ""
	forkCard.CreatedAt = now.UTC()
	forkCard.UpdatedAt = now.UTC()
	forkedFromCardID, _ := semanticvalue.String(sourceCard.CardID)
	forkCard.Provenance, err = sourceCard.Provenance.With("forked_from_card_id", forkedFromCardID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkedFromRequestID, _ := semanticvalue.String(source.RequestEventID)
	forkCard.Provenance, err = forkCard.Provenance.With("forked_from_request_event_id", forkedFromRequestID)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	forkCard, err = decisioncard.New(forkCard)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	if err := fork.Validate(forkCard); err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	return forkCard, fork, nil
}

func forkDecisionCardExecutionSource(source events.RoutingSource, flowInstance, entityID string) (events.RoutingSource, error) {
	route := source.Route()
	route.FlowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	route.EntityID = strings.TrimSpace(entityID)
	switch source.Kind() {
	case events.RoutingSourceRoot:
		return events.NewRootRoutingSource(route.EntityID)
	case events.RoutingSourceStaticFlow:
		return events.NewStaticFlowRoutingSource(route)
	case events.RoutingSourceConcreteTemplateInstance:
		return events.NewConcreteTemplateInstanceRoutingSource(route)
	default:
		return events.RoutingSource{}, fmt.Errorf("fork decision-card source kind %q is not an execution source", source.Kind().StorageCode())
	}
}
