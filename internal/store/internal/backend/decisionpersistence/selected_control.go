package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func RequireCardControlTx(ctx context.Context, tx *sql.Tx, cardID string, operation runfork.SelectedControl, postgres, lock bool) error {
	if tx == nil {
		return fmt.Errorf("card control requires selected transaction")
	}
	card, err := loadDecisionCard(ctx, tx, cardID, postgres, false)
	if err != nil {
		return err
	}
	return RequireCardRunControl(ctx, tx, card.RunID, operation, postgres, lock)
}

func RequireCardRunControl(ctx context.Context, q runstate.RowQueryer, runID string, operation runfork.SelectedControl, postgres, lock bool) error {
	if ctx == nil || q == nil {
		return fmt.Errorf("card control requires a caller and selected reader")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, err := runstate.LoadSelectedContractBinding(ctx, q, runID)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	admission, _ := managedexecution.FromContext(ctx)
	if err := runfork.RequireDecisionControlForBinding(runID, operation, binding, exists, admission); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	tx, transactional := q.(*sql.Tx)
	if !transactional || tx == nil {
		return fmt.Errorf("selected card control requires its native transaction snapshot")
	}
	evidence, present := startupownership.GenerationGrantEvidenceFromContext(ctx)
	if !present || evidence.SelectedFork == nil || evidence.SelectedFork.BindingID != binding.BindingID ||
		evidence.SelectedFork.ForkRunID != runID || evidence.BundleHash != admission.BundleHash ||
		!admission.AuthorizesSelected(evidence.SelectedFork.ExecutionID, runID, evidence.SelectedFork.ExecutionGeneration) {
		return fmt.Errorf("selected card control requires its own admitted generation grant")
	}
	var ownership manager.RunExecutionOwnership
	if lock {
		ownership, err = agentpersistence.InspectRunExecutionOwnershipTx(ctx, tx, evidence, runID, !postgres)
	} else {
		ownership, err = agentpersistence.ObserveRunExecutionOwnershipTx(ctx, tx, evidence, runID, !postgres)
	}
	if err != nil {
		return err
	}
	if ownership != manager.RunExecutionOwned {
		return fmt.Errorf("selected card control generation does not own its exact run")
	}
	return ctx.Err()
}
