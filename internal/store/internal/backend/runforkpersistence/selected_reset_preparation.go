package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

func ValidateSelectedResetHistoryTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) error {
	for _, prepared := range req.SelectedHistory {
		if err := ValidateSelectedResetPreparationTx(ctx, tx, prepared, sqlite); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// The cleanup caller retains its existing reset/generation fence through this
// recapture and the eventual detachment commit. Preparation alone is not proof.
func ValidateSelectedResetPreparationTx(ctx context.Context, tx *sql.Tx, prepared runfork.SelectedResetPreparation, sqlite bool) error {
	if tx == nil {
		return errors.New("selected reset preparation requires its cleanup transaction")
	}
	if err := prepared.Binding.ValidateExecutionBinding(prepared.Binding.ForkRunID); err != nil {
		return err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, prepared.Binding.ForkRunID)
	if err != nil {
		return err
	}
	if binding.Owner != prepared.Binding.Owner || binding.BindingID != prepared.Binding.BindingID ||
		binding.SourceRunID != prepared.Binding.SourceRunID || binding.ForkRunID != prepared.Binding.ForkRunID ||
		binding.ContractSelection != prepared.Binding.ContractSelection || binding.ForkEventID != prepared.Binding.ForkEventID ||
		!binding.CreatedAt.Equal(prepared.Binding.CreatedAt) || !sameSelectedForkPointIdentity(binding.ForkPoint, prepared.Binding.ForkPoint) {
		return errors.New("selected reset preparation has a changed or foreign binding")
	}
	load, validate, resolve := sourceadmission.LoadPostgresTx, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	if sqlite {
		load, validate, resolve = sourceadmission.LoadSQLiteTx, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	}
	fact, err := load(ctx, tx, binding.SourceRunID, false, true)
	if err != nil {
		return err
	}
	if !fact.Matches(prepared.Source) {
		return errors.New("selected reset preparation source artifact changed")
	}
	if err := prepared.Original.RequireSource(fact.BundleHash()); err != nil {
		return err
	}
	if !sameSelectedForkPointIdentity(binding.ForkPoint, prepared.Plan.ForkPoint) {
		return errors.New("selected reset preparation does not name the binding's fixed cut")
	}
	plan, err := planRunForkSnapshotWithActivityOrigins(ctx, tx, runfork.RunForkPlanRequest{
		SourceRunID: binding.SourceRunID, ResolvedPoint: &prepared.Plan.ForkPoint, AtStart: binding.ForkPoint.Kind == runfork.RunForkPointRunStart,
	}, validate, resolve, func(ctx context.Context, tx *sql.Tx, runID string) (correlation.SourceArtifactFact, error) {
		return load(ctx, tx, runID, false, true)
	}, !sqlite)
	if err != nil {
		return err
	}
	events, present := plan.HistoricalEventIDs(binding.ForkPoint.Revision)
	prior, captured := prepared.Plan.HistoricalEventIDs(binding.ForkPoint.Revision)
	if !present || !captured || prepared.Plan.SourceRunID != plan.SourceRunID ||
		!sameSelectedForkPointIdentity(plan.ForkPoint, prepared.Plan.ForkPoint) || !slices.Equal(events, prior) {
		return errors.New("selected reset preparation fixed cut changed")
	}
	origins, priorOrigins := plan.ActivityRequestOrigins(), prepared.Plan.ActivityRequestOrigins()
	if len(origins) != len(priorOrigins) {
		return errors.New("selected reset preparation activity membership changed")
	}
	for i := range origins {
		if !origins[i].Matches(priorOrigins[i]) {
			return errors.New("selected reset preparation activity origin changed")
		}
	}
	return ctx.Err()
}
