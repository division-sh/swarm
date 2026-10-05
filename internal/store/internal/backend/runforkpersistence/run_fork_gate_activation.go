package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type runForkDecisionMaterializer interface {
	LoadTx(context.Context, *mutationprotocol.Attempt, string, bool) (decisioncard.Card, error)
	InsertTx(context.Context, *mutationprotocol.Attempt, decisioncard.Card) error
	DecideTx(context.Context, *mutationprotocol.Attempt, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error)
	SupersedeStageTx(context.Context, *mutationprotocol.Attempt, string, string, string, string, time.Time) (bool, error)
	LoadProposedEffectTx(context.Context, *mutationprotocol.Attempt, string, bool) (decisioncard.ProposedEffectContinuation, error)
	InsertProposedEffectTx(context.Context, *mutationprotocol.Attempt, decisioncard.Card, decisioncard.ProposedEffectContinuation) error
}

func materializeRunForkDecisionCards(ctx context.Context, decisions runForkDecisionMaterializer, attempt *mutationprotocol.Attempt, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, bindings []runForkGateActivationBinding, now time.Time) error {
	if attempt == nil {
		return fmt.Errorf("fork decision-card materialization requires private story ownership")
	}
	if projection.Source.EntityID == "" || projection.Source.FlowInstance == "" || projection.Fork.EntityID == "" || projection.Fork.FlowInstance == "" {
		return fmt.Errorf("fork decision-card materialization requires exact source and fork entity ownership")
	}
	for _, binding := range bindings {
		sourceCard, err := decisions.LoadTx(ctx, attempt, binding.Source.CardID, false)
		if err != nil {
			return fmt.Errorf("load source decision card %s for fork: %w", binding.Source.CardID, err)
		}
		if sourceCard.BundleHash != binding.Source.BundleHash || binding.Fork.BundleHash != target.BundleHash || strings.TrimSpace(target.WorkflowVersion) == "" {
			return fmt.Errorf("fork decision-card execution source disagrees with source/target activation")
		}
		forkCard := sourceCard
		forkCard.CardID = binding.Fork.CardID
		forkCard.RunID = strings.TrimSpace(forkRunID)
		forkCard.BundleHash, forkCard.WorkflowVersion = target.BundleHash, target.WorkflowVersion
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
		if err := decisions.InsertTx(ctx, attempt, forkCard); err != nil {
			return fmt.Errorf("insert fork decision card: %w", err)
		}
		persisted, err := decisions.LoadTx(ctx, attempt, forkCard.CardID, false)
		if err != nil {
			return err
		}
		if err := validateForkDecisionCardRepeat(persisted, forkCard, target); err != nil {
			return err
		}
		if err := restoreForkDecisionCardDisposition(ctx, decisions, attempt, forkRunID, projection.Fork.EntityID, binding.Fork, sourceCard, forkCard, persisted, now); err != nil {
			return err
		}
	}
	return nil
}

func validateForkDecisionCardRepeat(persisted, expected decisioncard.Card, target contracts.BundleIdentity) error {
	if persisted.RunID != expected.RunID || persisted.BundleHash != target.BundleHash || persisted.WorkflowVersion != target.WorkflowVersion || persisted.ExecutionMode != expected.ExecutionMode || persisted.CardContentHash != expected.CardContentHash || persisted.EffectContentHash != expected.EffectContentHash || !persisted.Anchor.SemanticValue().Equal(expected.Anchor.SemanticValue()) {
		return fmt.Errorf("fork decision card repeats contradictory execution authority")
	}
	return nil
}

func restoreForkDecisionCardDisposition(ctx context.Context, decisions runForkDecisionMaterializer, attempt *mutationprotocol.Attempt, forkRunID, entityID string, activation gateruntime.Activation, source, fork, persisted decisioncard.Card, now time.Time) error {
	switch activation.Status {
	case gateruntime.StatusOpen:
	case gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted:
		if strings.TrimSpace(source.Verdict) == "" || strings.TrimSpace(activation.DecisionEventID) == "" {
			return fmt.Errorf("source decision card %s lacks committed verdict evidence", source.CardID)
		}
		if persisted.Status == decisioncard.StatusDecided {
			if persisted.Verdict != source.Verdict || !persisted.Fields.Equal(source.Fields) || persisted.DecidedBy != source.DecidedBy || persisted.DecisionEventID != activation.DecisionEventID || persisted.DeliveryReceiptID != source.DeliveryReceiptID || persisted.DeliveryRenderHash != source.DeliveryRenderHash {
				return fmt.Errorf("fork decision card repeats contradictory committed evidence")
			}
			return nil
		}
		if _, err := decisions.DecideTx(ctx, attempt, decisioncard.DecideRequest{
			CardID: fork.CardID, Verdict: source.Verdict, Fields: source.Fields, PrincipalID: source.DecidedBy,
			ObservedContentHash: fork.CardContentHash, DeliveryReceiptID: source.DeliveryReceiptID, DeliveryRenderHash: source.DeliveryRenderHash,
			DecisionEventID: activation.DecisionEventID, Now: now,
		}); err != nil {
			return fmt.Errorf("restore committed fork decision card: %w", err)
		}
	case gateruntime.StatusSuperseded:
		if _, err := decisions.SupersedeStageTx(ctx, attempt, forkRunID, entityID, activation.ActivationID, activation.SupersededReason, now); err != nil {
			return fmt.Errorf("restore superseded fork decision card: %w", err)
		}
	}
	return nil
}

func (s *RunForkPostgresOwner) MaterializeRunForkDecisionCardsTx(ctx context.Context, attempt *mutationprotocol.Attempt, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, bindings []RunForkGateActivationBinding, now time.Time) error {
	return materializeRunForkDecisionCards(ctx, s.DecisionPostgresOwner, attempt, forkRunID, target, projection, bindings, now)
}

func (s *RunForkSQLiteOwner) MaterializeRunForkDecisionCardsTx(ctx context.Context, attempt *mutationprotocol.Attempt, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, bindings []RunForkGateActivationBinding, now time.Time) error {
	return materializeRunForkDecisionCards(ctx, s.DecisionSQLiteOwner, attempt, forkRunID, target, projection, bindings, now)
}

type runForkProposedEffectMaterializer func(context.Context, *mutationprotocol.Attempt, string, string, contracts.BundleIdentity, runfork.EntityProjection, runfork.RunForkPoint, *loopruntime.ForkCorrespondence, time.Time) error

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

func materializeRunForkProposedEffectCards(ctx context.Context, decisions runForkDecisionMaterializer, cardIDsQuery string, postgres bool, attempt *mutationprotocol.Attempt, sourceRunID, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, forkPoint runfork.RunForkPoint, correspondence *loopruntime.ForkCorrespondence, now time.Time) error {
	if attempt == nil {
		return fmt.Errorf("fork proposed-effect materialization requires private story ownership")
	}
	if err := correspondence.RequireDestination(forkRunID, projection.Fork.EntityID); err != nil {
		return err
	}
	var cardIDs []string
	var forkActivations []loopruntime.Activation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, cardIDsQuery, strings.TrimSpace(sourceRunID), projection.Source.EntityID, forkPoint.Timestamp.UTC())
		if err != nil {
			return fmt.Errorf("load source proposed effects for fork: %w", err)
		}
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
		return nil
	})
	if err != nil {
		return err
	}
	if len(cardIDs) == 0 {
		return nil
	}
	for _, cardID := range cardIDs {
		sourceCard, err := decisions.LoadTx(ctx, attempt, cardID, false)
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
		sourceContinuation, err := decisions.LoadProposedEffectTx(ctx, attempt, cardID, false)
		if err != nil {
			return fmt.Errorf("load source proposed-effect continuation %s: %w", cardID, err)
		}
		if sourceCard.RunID != sourceRunID || sourceContinuation.RunID != sourceRunID {
			return fmt.Errorf("source proposed effect %s belongs to another run", cardID)
		}
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			forkActivations, err = loadRunForkEntityActivations(ctx, tx, postgres, forkRunID, projection.Fork.EntityID, projection.Fork.FlowInstance, sourceContinuation.FlowID)
			return err
		}); err != nil {
			return err
		}
		forkCard, forkContinuation, err := forkPendingProposedEffect(sourceCard, sourceContinuation, forkRunID, target, projection, correspondence, forkActivations, now)
		if err != nil {
			return err
		}
		if err := decisions.InsertProposedEffectTx(ctx, attempt, forkCard, forkContinuation); err != nil {
			return fmt.Errorf("insert fork-local proposed effect: %w", err)
		}
	}
	return nil
}

func (s *RunForkPostgresOwner) MaterializeRunForkProposedEffectCardsTx(ctx context.Context, attempt *mutationprotocol.Attempt, sourceRunID, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, forkPoint runfork.RunForkPoint, correspondence *loopruntime.ForkCorrespondence, now time.Time) error {
	return materializeRunForkProposedEffectCards(ctx, s.DecisionPostgresOwner, postgresRunForkProposedEffectCardIDsQuery, true, attempt, sourceRunID, forkRunID, target, projection, forkPoint, correspondence, now)
}

func (s *RunForkSQLiteOwner) MaterializeRunForkProposedEffectCardsTx(ctx context.Context, attempt *mutationprotocol.Attempt, sourceRunID, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, forkPoint runfork.RunForkPoint, correspondence *loopruntime.ForkCorrespondence, now time.Time) error {
	return materializeRunForkProposedEffectCards(ctx, s.DecisionSQLiteOwner, sqliteRunForkProposedEffectCardIDsQuery, false, attempt, sourceRunID, forkRunID, target, projection, forkPoint, correspondence, now)
}

func forkPendingProposedEffect(sourceCard decisioncard.Card, source decisioncard.ProposedEffectContinuation, forkRunID string, target contracts.BundleIdentity, projection runfork.EntityProjection, correspondence *loopruntime.ForkCorrespondence, forkActivations []loopruntime.Activation, now time.Time) (decisioncard.Card, decisioncard.ProposedEffectContinuation, error) {
	if err := correspondence.RequireDestination(forkRunID, projection.Fork.EntityID); err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	sourceGeneration := source.Generation
	source = source.Canonical()
	if err := source.Validate(sourceCard); err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("source proposed effect disagrees with frozen card: %w", err)
	}
	if strings.TrimSpace(target.BundleHash) == "" || strings.TrimSpace(target.WorkflowVersion) == "" {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("fork proposed effect requires admitted execution source")
	}
	if source.Generation != sourceGeneration {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("source proposed effect has noncanonical generation")
	}
	if source.EntityID != projection.Source.EntityID || source.FlowInstance != projection.Source.FlowInstance {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("source proposed effect %s owner does not match fork source entity ownership", source.ActivityID)
	}
	fork := source
	fork.BundleHash, fork.WorkflowVersion = target.BundleHash, target.WorkflowVersion
	fork.RunID = strings.TrimSpace(forkRunID)
	fork.SourceRunID = fork.RunID
	fork.EntityID = projection.Fork.EntityID
	fork.FlowInstance = projection.Fork.FlowInstance
	fork.ReplyContextID = ""
	fork.SourceEventID = activityidentity.ForkLineageEventID(fork.RunID, source.SourceEventID)
	if source.ParentEventID != "" {
		fork.ParentEventID = activityidentity.ForkLineageEventID(fork.RunID, source.ParentEventID)
	}
	if source.Generation != (attemptgeneration.Generation{}) {
		ref, err := correspondence.AdmitSource(source.Generation)
		if err != nil {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
		}
		child, err := correspondence.Bind(ref)
		if err != nil {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
		}
		if err := correspondence.ValidateChild(child, forkActivations); err != nil {
			return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
		}
		fork.Generation = child.Generation()
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
	forkCard.BundleHash, forkCard.WorkflowVersion = target.BundleHash, target.WorkflowVersion
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
