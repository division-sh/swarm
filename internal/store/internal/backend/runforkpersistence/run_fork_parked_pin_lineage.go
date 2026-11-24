package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	lifecycleowner "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

func isParkedForkPinTx(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, plan runfork.RunForkPlan, pin runtimedata.Pin) (bool, error) {
	if plan.IsParkedDeploymentPin(pin) {
		return true, nil
	}
	if pin.Selection != "fork_inherited" {
		return false, nil
	}
	// A missing intent is not a parking fact. Follow only unchanged pins in
	// acknowledged fork operations until an exact named cut proves parking.
	seen := make(map[string]bool)
	for runID := plan.SourceRunID; ; {
		if seen[runID] {
			return false, fmt.Errorf("parked pin has cyclic fork lineage")
		}
		seen[runID] = true
		op, err := loadParkedPinLineageOperationTx(ctx, tx, runID)
		if err != nil || op == nil || op.Status != runfork.ForkOperationActivated {
			return false, err
		}
		retained := findUnchangedForkPin(op.Result.DataPins, pin)
		if retained.Selection == "" {
			return false, nil
		}
		parent, err := planParkedPinBirthCut(ctx, tx, data, op.Request)
		if err != nil {
			return false, err
		}
		if parent.IsParkedDeploymentPin(retained) {
			return true, nil
		}
		for _, obligation := range parent.FanOutObligations {
			if d := obligation.Intent.Request.Deployment; d != nil && d.Declaration == pin.Declaration {
				return false, nil
			}
		}
		if retained.Selection != "fork_inherited" {
			return false, nil
		}
		runID = op.Request.SourceRunID
	}
}

func findUnchangedForkPin(pins []runtimedata.Pin, want runtimedata.Pin) runtimedata.Pin {
	for _, pin := range pins {
		if pin.Declaration == want.Declaration && pin.VersionID == want.VersionID && pin.SchemaDigest == want.SchemaDigest {
			return pin
		}
	}
	return runtimedata.Pin{}
}

func loadParkedPinLineageOperationTx(ctx context.Context, tx *sql.Tx, runID string) (*runfork.ForkOperationRecord, error) {
	origin, err := lifecycleowner.ReadImmutableRunOriginTx(ctx, tx, runID)
	if err != nil || origin.Kind() != runlifecycle.OriginForkMaterialization {
		return nil, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	want, err := runlifecycle.ForkMaterializationRunOrigin(binding.SourceRunID, binding.ForkPoint.Kind, binding.ForkPoint.Revision, binding.ForkEventID)
	if err != nil || !origin.Equal(want) {
		return nil, fmt.Errorf("parked pin lineage disagrees with the immutable fork origin")
	}
	return loadBoundSelectedForkOperationTx(ctx, tx, binding, nil, false)
}

func planParkedPinBirthCut(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, op runfork.ForkOperationRequest) (runfork.RunForkPlan, error) {
	plan := runfork.RunForkPlan{SourceRunID: op.SourceRunID}
	cut, err := loadRunForkPointSnapshot(ctx, tx, op.SourceRunID, *op.ResolvedPoint)
	if err != nil {
		return plan, err
	}
	plan.PendingWork, err = loadRunForkPendingWorkFromRevision(cut)
	if err != nil {
		return plan, err
	}
	plan.FanOutObligations, err = loadRunForkFanOutObligationsFromRevision(cut, plan.PendingWork)
	if err != nil || op.PublicationID == "" || op.ResolvedPoint.Kind != runfork.RunForkPointStateRevision {
		return plan, err
	}
	point, member, err := resolveRunForkPublicationPoint(ctx, tx, data, op.SourceRunID, op.PublicationID, resolveRunForkRevisionPoint)
	if err != nil {
		return plan, err
	}
	if !point.SameIdentity(*op.ResolvedPoint) {
		return plan, fmt.Errorf("parked pin lineage disagrees with the retained publication cut")
	}
	if member {
		err = selectRunForkDeploymentFeed(ctx, tx, data, &plan, op.PublicationID)
	}
	return plan, err
}
