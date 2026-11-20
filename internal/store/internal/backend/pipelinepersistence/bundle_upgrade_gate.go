package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func lockUpgradeGateInstances(ctx context.Context, tx *sql.Tx, sqlite bool, plan *runbundle.UpgradePlan) error {
	var owners []flowidentity.RunScopedFlowInstance
	for _, runID := range slices.Sorted(maps.Keys(plan.Runs)) {
		instances, err := listWorkflowInstances(ctx, tx, sqlite, runID)
		if err != nil {
			return err
		}
		for _, instance := range instances {
			persisted, err := flowidentity.StoredPersisted(plan.Sources[plan.Runs[runID]], instance.WorkflowName, instance.StorageRef, instance.StorageRef, instance.InstanceID, instance.EntityID, instance.ParentEntityID)
			if err != nil {
				return err
			}
			owners = append(owners, flowidentity.RunScopedFlowInstance{RunID: runID, Route: persisted.Route()})
		}
	}
	// All run/source fences precede this common sorted header/field footprint.
	return lockWorkflowPublicationFootprintTx(ctx, tx, !sqlite, owners, nil)
}

func replaceUpgradeGateCards(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, owner bundleUpgradeMutationOwner, sqlite bool, runID string, current, candidate semanticview.Source, cadence decisioncard.CadencePolicy) error {
	instances, err := listWorkflowInstances(ctx, tx, sqlite, runID)
	if err != nil {
		return err
	}
	var original []decisioncard.ListItem
	for cursor := ""; ; {
		cards, next, err := owner.ListDecisionCardsTx(ctx, tx, decisioncard.ListOptions{RunID: runID, Cursor: cursor, Limit: 200})
		if err != nil {
			return err
		}
		original = append(original, cards...)
		if next == "" {
			break
		}
		cursor = next
	}
	// Never discover this attempt's new successors through a later input page.
	for _, card := range original {
		if card.Status != decisioncard.StatusPending || card.Anchor.Kind() != decisioncard.AnchorKindStageGate {
			continue
		}
		anchor, err := card.Anchor.StageGate()
		if err != nil {
			return err
		}
		index := slices.IndexFunc(instances, func(instance runtimepipeline.WorkflowInstance) bool {
			return instance.EntityID == anchor.EntityID && instance.StorageRef == anchor.Route.InstancePath
		})
		if index < 0 {
			return fmt.Errorf("pending upgrade card %s lacks its exact instance", card.CardID)
		}
		if err := replaceUpgradeGateCard(ctx, attempt, owner.workflowDecisionLifecycleOwner(), !sqlite, current, candidate, cadence, card, instances[index]); err != nil {
			return err
		}
	}
	return nil
}

func replaceUpgradeGateCard(ctx context.Context, attempt *mutationprotocol.Attempt, decisions workflowDecisionLifecycleTxOwner, postgres bool, current, candidate semanticview.Source, cadence decisioncard.CadencePolicy, listed decisioncard.ListItem, instance runtimepipeline.WorkflowInstance) error {
	if decisions == nil {
		return fmt.Errorf("gate replacement requires its existing decision owner")
	}
	anchor, err := listed.Anchor.StageGate()
	if err != nil {
		return err
	}
	card, err := decisions.LoadByActivationTx(ctx, attempt, listed.RunID, anchor.EntityID, anchor.StageActivationID)
	if err != nil {
		return err
	}
	if card.CardID != listed.CardID {
		return fmt.Errorf("upgrade card changed before its locked read")
	}
	now := decisioncard.CanonicalTimestamp(time.Now())
	prepared, err := runtimepipeline.PrepareGateUpgrade(ctx, current, candidate, card, instance, cadence, now)
	if err != nil || prepared == nil {
		return err
	}
	if _, err := commitWorkflowEngineGateCardMutation(ctx, attempt, decisions, runtimepipeline.WorkflowGateCardMutation{
		Kind: runtimepipeline.WorkflowGateCardMutationSupersede, Card: prepared.Previous, EntityID: anchor.EntityID,
		ActivationID: anchor.StageActivationID, Reason: decisioncard.SupersededByUpdate, OccurredAt: now,
	}); err != nil {
		return err
	}
	inserted, err := decisions.InsertTx(ctx, attempt, prepared.Replacement)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf("changed gate replacement did not create its exact successor")
	}
	return commitWorkflowEngineState(ctx, attempt, postgres, prepared.State)
}
