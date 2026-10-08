package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func commitCanceledTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, publications canceledTurnPublicationOwner, command runtimeeffects.CanceledTurnCommand, selectedRecoveryExecutionID string) (runtimeeffects.CanceledTurnCommit, error) {
	if err := command.Validate(); err != nil {
		return runtimeeffects.CanceledTurnCommit{}, err
	}
	if command.Attempt == nil {
		return commitUnstartedCanceledTurn(ctx, mutation, postgres, delivery, directives, command.Origin, selectedRecoveryExecutionID)
	}
	attempt := *command.Attempt
	if attempt.Kind != runtimeeffects.KindProviderTurn || !attempt.Authority.HasBusinessTurnOrigin() || attempt.Origin.Validate() != nil {
		return runtimeeffects.CanceledTurnCommit{}, fmt.Errorf("canceled turn requires its exact admitted business origin")
	}
	result := runtimeeffects.CanceledTurnCommit{Origin: attempt.Origin}
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		pending, err := providerTurnPendingTx(ctx, tx, attempt, delivery, directives)
		if err != nil {
			return err
		}
		facts, err := prepareCanceledTurnSettlementTx(ctx, tx, postgres, attempt)
		if err != nil {
			return err
		}
		result.Cancellation = facts.intent
		result.Cancellation.OriginSettled = true
		plan, err := prepareCanceledTurnReactionTx(ctx, tx, postgres, publications, command, facts.reason)
		if err != nil {
			return err
		}
		switch attempt.Origin.Kind {
		case runtimeeffects.CompletionOriginDelivery:
			result.Delivery, err = settleCanceledDeliveryTurn(ctx, mutation, postgres, delivery, attempt, selectedRecoveryExecutionID)
		case runtimeeffects.CompletionOriginDirective:
			result.Directive, err = settleCanceledDirectiveTurn(ctx, mutation, postgres, directives, attempt)
		}
		if err != nil {
			return fmt.Errorf("commit exact canceled origin: %w", err)
		}
		if command.Publication != nil {
			committed, err := publications.CommitPublicationTx(ctx, mutation, plan.PublicationCommand())
			if err != nil {
				return fmt.Errorf("commit exact cancellation reaction: %w", err)
			}
			if !pending && committed.AppendOutcome != runtimebus.EventAppendExactDuplicate {
				return fmt.Errorf("settled timeout origin lacks its atomically committed reaction")
			}
			result.Publication, err = runtimebus.NewCommittedEnginePublication(plan, committed)
			return err
		}
		return nil
	})
	return result, err
}

func prepareCanceledTurnReactionTx(ctx context.Context, tx *sql.Tx, postgres bool, publications canceledTurnPublicationOwner, command runtimeeffects.CanceledTurnCommand, reason deliverylifecycle.CancellationReason) (runtimebus.EnginePublicationPlan, error) {
	attempt := *command.Attempt
	var plan runtimebus.EnginePublicationPlan
	if reason == deliverylifecycle.CancellationTurnTimeout {
		var ok bool
		plan, ok = command.Publication.(runtimebus.EnginePublicationPlan)
		if !ok || publications == nil {
			return runtimebus.EnginePublicationPlan{}, fmt.Errorf("turn timeout settlement requires its admitted reaction publication")
		}
		if err := validateTurnReactionTx(ctx, tx, postgres, attempt, plan); err != nil {
			return runtimebus.EnginePublicationPlan{}, err
		}
	} else if command.Publication != nil {
		return runtimebus.EnginePublicationPlan{}, fmt.Errorf("terminate cancellation cannot invent a timeout reaction")
	}
	return plan, nil
}

func validateTurnReactionTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, plan runtimebus.EnginePublicationPlan) error {
	if err := plan.ValidateDurablePublicationPlan(); err != nil {
		return err
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return err
	}
	query := `SELECT bound_emit,timeout_event_id::text,cancel_cause_event_id::text,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid`
	if !postgres {
		query = `SELECT bound_emit,timeout_event_id,cancel_cause_event_id,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var emit, timeoutID, cause string
	var rawRequested any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&emit, &timeoutID, &cause, &rawRequested); err != nil {
		return err
	}
	event := plan.PublicationCommand().Commit.Event.Event()
	if event.ID() != timeoutID || event.ID() != cause || string(event.Type()) != emit || event.RunID() != attempt.Authority.Target.RunID {
		return fmt.Errorf("canceled turn reaction contradicts its persisted declaration or exact origin")
	}
	requestedAt, valid, err := sqliteTimeValue(rawRequested)
	// Event occurrence time uses the event owner's canonical microsecond
	// precision. This does not round the logical launch clock or its deadline.
	if err != nil || !valid || !event.CreatedAt().Equal(requestedAt.UTC().Truncate(time.Microsecond)) {
		return fmt.Errorf("canceled turn reaction changed its persisted occurrence time")
	}
	flowID, _, path, err := attempt.Authority.BusinessTurnCoordinates()
	if err != nil {
		return err
	}
	source := event.RoutingSource()
	if source.Kind() != events.RoutingSourceFlowOwnedControl ||
		source.Route().FlowID != flowID || source.Route().FlowInstance != path ||
		source.Route().EntityID != attempt.Authority.Target.EntityID ||
		event.ProducerType() != events.EventProducerPlatform || event.Producer().ID() != runtimeeffects.TurnTimeoutProducerID() {
		return fmt.Errorf("canceled turn reaction changed its exact flow-owned producer: expected=%+v source_kind=%s source=%+v target=%+v producer=%+v", events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: attempt.Authority.Target.EntityID}, source.Kind().StorageCode(), source.Route(), event.TargetRoute(), event.Producer())
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload(), &fields); err != nil || fields == nil || len(fields) != 0 {
		return fmt.Errorf("canceled turn reaction must retain its declared empty payload")
	}
	// The command event is the journal's receiver projection, not the producer's
	// target claim. Validate the original targetless publication against the
	// planner's immutable admitted source, as grouped publication already does.
	originalSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: attempt.Authority.Target.EntityID})
	if err != nil {
		return err
	}
	original, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		RunID: attempt.Authority.Target.RunID,
		Facts: events.EventFacts{
			ID: cause, Type: events.EventType(emit), Payload: []byte(`{}`),
			Producer:      events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeeffects.TurnTimeoutProducerID()},
			RoutingSource: originalSource, CreatedAt: requestedAt,
			Envelope: events.EventEnvelope{EntityID: attempt.Authority.Target.EntityID}, ExecutionMode: attempt.Authority.ExecutionMode,
		},
	})
	if err != nil {
		return err
	}
	if err := plan.ValidatePreparedFanOutEvent(original); err != nil {
		return fmt.Errorf("validate original timeout publication: %w", err)
	}
	return nil
}

func AcknowledgeCanceledTurn(result runtimeeffects.CanceledTurnCommit, acknowledged bool) runtimeeffects.CanceledTurnCommit {
	if !acknowledged {
		return runtimeeffects.CanceledTurnCommit{}
	}
	result.Acknowledged = true
	result.Cancellation.Committed = true
	result.Directive.Acknowledged = result.Origin.Kind == runtimeeffects.CompletionOriginDirective
	if result.Publication != nil {
		result.Publication = result.Publication.(runtimebus.CommittedEnginePublication).WithCommitAcknowledgment()
	}
	return result
}

func (s *EffectPostgresOwner) CommitCanceledTurn(ctx context.Context, command runtimeeffects.CanceledTurnCommand) (runtimeeffects.CanceledTurnCommit, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.CanceledTurnCommit{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (runtimeeffects.CanceledTurnCommit, error) {
		return commitCanceledTurn(ctx, mutation, true, s.delivery, s.directives, s.publications, command, "")
	})
	value, acknowledged := result.Value()
	value = AcknowledgeCanceledTurn(value, acknowledged)
	if !acknowledged {
		return value, result.Err()
	}
	return value, errors.Join(result.Err(), value.Validate())
}

func (s *EffectSQLiteOwner) CommitCanceledTurn(ctx context.Context, command runtimeeffects.CanceledTurnCommand) (runtimeeffects.CanceledTurnCommit, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.CanceledTurnCommit{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite commit canceled turn reaction", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (runtimeeffects.CanceledTurnCommit, error) {
		return commitCanceledTurn(ctx, mutation, false, s.delivery, s.directives, s.publications, command, "")
	})
	value, acknowledged := result.Value()
	value = AcknowledgeCanceledTurn(value, acknowledged)
	if !acknowledged {
		return value, result.Err()
	}
	return value, errors.Join(result.Err(), value.Validate())
}
