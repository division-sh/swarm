package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/apiidempotency"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type decisionCardMutationTxOwner interface {
	LoadTx(context.Context, *sql.Tx, string, bool) (decisioncard.Card, error)
	DecideTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error)
	DeferTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error)
	BeginInputTx(context.Context, *sql.Tx, decisioncard.BeginInputRequest) (decisioncard.InputDraft, error)
	CancelInputTx(context.Context, *sql.Tx, decisioncard.CancelInputRequest) (decisioncard.InputDraft, error)
}

func commitDecisionCardOperation(
	ctx context.Context,
	store eventCommitTxStore,
	decisions decisionCardMutationTxOwner,
	postgres bool,
	effects *revisionEffects,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	command runtimepipeline.DecisionCardMutationCommand,
	storeCompletion func(context.Context, *sql.Tx, apiidempotency.Completion) error,
) (runtimepipeline.CommittedDecisionCardMutation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedDecisionCardMutation{}, err
	}
	result := runtimepipeline.CommittedDecisionCardMutation{Kind: command.Mutation.Kind()}
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		if err := admitDecisionCardAnchorTx(txctx, tx, decisions, command.Mutation, postgres); err != nil {
			return err
		}
		var selected runtimeengine.DurablePublicationPlan
		switch command.Mutation.Kind() {
		case runtimepipeline.DecisionCardMutationDecide:
			req, ok := command.Mutation.Decision()
			if !ok {
				return fmt.Errorf("decision-card decision request is missing")
			}
			outcome, err := decisions.DecideTx(txctx, runtimeAuthorActivityMutation(story), tx, req)
			if err != nil {
				return err
			}
			result.Outcome = outcome
			txctx = runtimecorrelation.WithRunID(txctx, outcome.Card.RunID)
			if outcome.ForcedDeferred {
				if command.GateState != nil {
					return fmt.Errorf("forced decision-card deferral cannot commit gate state")
				}
				selected = command.ForcedDeferralPublication
				if selected == nil {
					return fmt.Errorf("forced decision-card deferral requires its publication plan")
				}
			} else {
				selected = command.Publication
				if err := validateDecisionCardGateState(command.GateState, outcome.Card); err != nil {
					return err
				}
				if command.GateState != nil {
					if err := commitDecisionCardGateState(txctx, tx, story, store, postgres, effects, *command.GateState); err != nil {
						return err
					}
				}
			}
		case runtimepipeline.DecisionCardMutationDefer:
			req, ok := command.Mutation.Deferral()
			if !ok {
				return fmt.Errorf("decision-card deferral request is missing")
			}
			outcome, err := decisions.DeferTx(txctx, runtimeAuthorActivityMutation(story), tx, req)
			if err != nil {
				return err
			}
			result.Outcome = outcome
			txctx = runtimecorrelation.WithRunID(txctx, outcome.Card.RunID)
			selected = command.Publication
		case runtimepipeline.DecisionCardMutationBeginInput:
			req, _, ok := command.Mutation.InputBegin()
			if !ok {
				return fmt.Errorf("decision-card input request is missing")
			}
			draft, err := decisions.BeginInputTx(txctx, tx, req)
			if err != nil {
				return err
			}
			result.Draft = draft
		case runtimepipeline.DecisionCardMutationCancelInput:
			req, ok := command.Mutation.InputCancellation()
			if !ok {
				return fmt.Errorf("decision-card input cancellation is missing")
			}
			draft, err := decisions.CancelInputTx(txctx, tx, req)
			if err != nil {
				return err
			}
			result.Draft = draft
		default:
			return fmt.Errorf("decision-card mutation kind is required")
		}
		if selected != nil {
			plan, ok := selected.(runtimebus.EnginePublicationPlan)
			if !ok {
				return fmt.Errorf("decision-card publication has unexpected type %T", selected)
			}
			committed, err := store.commitPublicationTx(txctx, tx, story, effects, plan.PublicationCommand(), nil)
			if err != nil {
				return err
			}
			evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
			if err != nil {
				return err
			}
			result.Publication = evidence
			result.HasPublication = true
		}
		completion, err := result.ProjectCompletion()
		if err != nil {
			return err
		}
		if err := storeCompletion(txctx, tx, completion); err != nil {
			return err
		}
		result.Completion = completion
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedDecisionCardMutation{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedDecisionCardMutation{}, err
	}
	return result, nil
}

func admitDecisionCardAnchorTx(ctx context.Context, tx *sql.Tx, decisions decisionCardMutationTxOwner, mutation runtimepipeline.DecisionCardMutation, postgres bool) error {
	var cardID string
	switch mutation.Kind() {
	case runtimepipeline.DecisionCardMutationDecide:
		req, _ := mutation.Decision()
		cardID = req.CardID
	case runtimepipeline.DecisionCardMutationDefer:
		req, _ := mutation.Deferral()
		cardID = req.CardID
	case runtimepipeline.DecisionCardMutationBeginInput:
		req, _, _ := mutation.InputBegin()
		cardID = req.CardID
	case runtimepipeline.DecisionCardMutationCancelInput:
		return nil // Cancellation is draft authority, not a fresh card decision.
	default:
		return fmt.Errorf("decision-card mutation kind is required")
	}
	// The run/source fence is already held. Read immutable card identity, then
	// lock the workflow anchor before the card mutation takes its row lock.
	card, err := decisions.LoadTx(ctx, tx, cardID, false)
	if err != nil {
		return err
	}
	if err := card.RequirePendingMutation(); err != nil {
		return err
	}
	if card.Anchor.Kind() != decisioncard.AnchorKindStageGate {
		return nil
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	query := sqliteWorkflowInstanceSelect + ` WHERE es.run_id = ? AND es.entity_id = ? AND es.flow_instance = ?`
	if postgres {
		query = postgresWorkflowInstanceSelect + ` WHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid AND es.flow_instance = $3 FOR UPDATE OF es, fi`
	}
	rows, err := tx.QueryContext(ctx, query, card.RunID, anchor.EntityID, anchor.Route.InstancePath)
	if err != nil {
		return err
	}
	defer rows.Close()
	var instances []runtimepipeline.WorkflowInstance
	if postgres {
		instances, err = scanPostgresWorkflowInstances(rows)
	} else {
		instances, err = scanSQLiteWorkflowInstances(rows)
	}
	if err != nil {
		return err
	}
	if len(instances) != 1 {
		return fmt.Errorf("decision card workflow instance is missing or non-singular")
	}
	return runtimepipeline.ValidateDecisionCardGateMutation(card, instances[0])
}

func validateDecisionCardGateState(state *runtimepipeline.WorkflowEngineStateRecord, card decisioncard.Card) error {
	if card.Anchor.Kind() != decisioncard.AnchorKindStageGate {
		if state != nil {
			return fmt.Errorf("non-gate decision card cannot commit workflow gate state")
		}
		return nil
	}
	if state == nil {
		return fmt.Errorf("stage-gate decision requires exact workflow gate state")
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	exactRoute := runtimeflowidentity.StoredRoute(anchor.Route.ScopeKey, anchor.Route.InstanceID, anchor.Route.InstancePath)
	if strings.TrimSpace(state.Identity.RunID) != strings.TrimSpace(card.RunID) || state.Identity.Route != exactRoute {
		return fmt.Errorf("decision-card gate state does not match the authoritative card scope")
	}
	return nil
}

func commitDecisionCardGateState(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
	record runtimepipeline.WorkflowEngineStateRecord,
) error {
	before, err := loadWorkflowEngineStateProjection(ctx, tx, postgres, record)
	if err != nil {
		return err
	}
	if err := commitWorkflowEngineState(ctx, tx, postgres, effects, record); err != nil {
		return err
	}
	before, err = commitWorkflowEngineInitialValues(ctx, tx, story, store, postgres, effects, record, before)
	if err != nil {
		return err
	}
	return commitWorkflowEngineMutationLog(ctx, tx, story, store, postgres, effects, record, before)
}

var _ runtimepipeline.DecisionCardMutationOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.DecisionCardMutationOwner = (*PipelineSQLiteOwner)(nil)
