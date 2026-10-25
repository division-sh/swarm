package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	lifecycleowner "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

// Execution ownership follows the exact materialized obligation at each
// immutable fork cut, never an ancestor selected by content hash or trigger ID.
func retainFanOutExecutionSources(ctx context.Context, tx *sql.Tx, snapshot *runForkRevisionSnapshot, plan runfork.RunForkPlan, postgres bool) error {
	requirements, err := runfork.FanOutContinuationRequirements(plan)
	if err != nil {
		return err
	}
	resolver := capturedConstructionResolver{tx: tx, snapshots: make(map[constructionCut]*runForkRevisionSnapshot), active: make(map[string]bool)}
	for i := range plan.FanOutObligations {
		obligation := &plan.FanOutObligations[i]
		if obligation.Intent.Request.Deployment != nil || requirements[obligation.Intent.Request.Key] {
			continue
		}
		obligation.ExecutionSource, err = fanOutExecutionSourceAtCut(ctx, &resolver, snapshot, *obligation, postgres, make(map[string]bool))
		if err != nil {
			return fmt.Errorf("retain fan-out execution source %s: %w", obligation.Intent.Request.Key.String(), err)
		}
	}
	return nil
}

func fanOutExecutionSourceAtCut(ctx context.Context, resolver *capturedConstructionResolver, snapshot *runForkRevisionSnapshot, obligation runfork.RunForkFanOutObligation, postgres bool, visiting map[string]bool) (sourceartifact.RootedSource, error) {
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	request := obligation.Intent.Request
	if snapshot == nil || snapshot.RunID != request.Key.RunID || visiting[snapshot.RunID] {
		return sourceartifact.RootedSource{}, fmt.Errorf("fan-out execution history requires an acyclic exact fixed-cut owner")
	}
	if err := request.ValidateSourceProjection(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	visiting[snapshot.RunID] = true
	defer delete(visiting, snapshot.RunID)
	if request.Capsule.Lineage.RunID != snapshot.RunID {
		return inheritedFanOutExecutionSourceAtCut(ctx, resolver, snapshot, obligation, postgres, visiting)
	}
	for _, delivery := range snapshot.Deliveries {
		if delivery.Snapshot.RunID == snapshot.RunID && delivery.Snapshot.DeliveryID == request.Key.TriggeringDeliveryID && delivery.Snapshot.EventID == request.Capsule.Lineage.ParentEventID {
			return loadFanOutExecutionRunRoot(ctx, resolver.tx, snapshot.RunID, request.PlanRef.BundleHash, postgres)
		}
	}
	return sourceartifact.RootedSource{}, fmt.Errorf("native fan-out execution lacks its exact triggering delivery at the fixed cut")
}

func inheritedFanOutExecutionSourceAtCut(ctx context.Context, resolver *capturedConstructionResolver, snapshot *runForkRevisionSnapshot, obligation runfork.RunForkFanOutObligation, postgres bool, visiting map[string]bool) (sourceartifact.RootedSource, error) {
	origin, err := lifecycleowner.ReadImmutableRunOriginTx(ctx, resolver.tx, snapshot.RunID)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if origin.Kind() != runlifecycle.OriginForkMaterialization {
		return sourceartifact.RootedSource{}, fmt.Errorf("inherited fan-out execution lacks immutable fork origin")
	}
	parent, err := resolver.sourceCut(ctx, origin)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	pending, err := loadRunForkPendingWorkFromRevision(parent)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	previous, err := loadRunForkFanOutObligationsFromRevision(parent, pending)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	requirements, err := runfork.FanOutContinuationRequirements(runfork.RunForkPlan{SourceRunID: parent.RunID, PendingWork: pending, FanOutObligations: previous})
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	request, key := obligation.Intent.Request, obligation.Intent.Request.Key
	key.RunID = parent.RunID
	for _, prior := range previous {
		if prior.Intent.Request.Key != key {
			continue
		}
		original := prior.Intent.Request
		if original.Source != request.Source || prior.Intent.Source != obligation.Intent.Source || original.Cardinality != request.Cardinality || original.Capsule.Lineage != request.Capsule.Lineage || original.Capsule.SourcePlanRef != request.Capsule.SourcePlanRef {
			return sourceartifact.RootedSource{}, fmt.Errorf("materialized fan-out contradicts its immutable source obligation")
		}
		if !requirements[key] {
			if original.PlanRef != request.PlanRef {
				return sourceartifact.RootedSource{}, fmt.Errorf("inert fan-out history changed its retained execution reference")
			}
			return fanOutExecutionSourceAtCut(ctx, resolver, parent, prior, postgres, visiting)
		}
		return continuedFanOutExecutionSourceAtCut(ctx, resolver, snapshot, parent, origin, obligation, prior, postgres, visiting)
	}
	return sourceartifact.RootedSource{}, fmt.Errorf("inherited fan-out has no exact obligation at its immutable source cut")
}

func continuedFanOutExecutionSourceAtCut(ctx context.Context, resolver *capturedConstructionResolver, snapshot, parent *runForkRevisionSnapshot, origin runlifecycle.RunOrigin, obligation, prior runfork.RunForkFanOutObligation, postgres bool, visiting map[string]bool) (sourceartifact.RootedSource, error) {
	rooted, err := loadFanOutExecutionRunRoot(ctx, resolver.tx, snapshot.RunID, obligation.Intent.Request.PlanRef.BundleHash, postgres)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if _, err := loadRunForkSelectedContractBinding(ctx, resolver.tx, snapshot.RunID); err == nil {
		return loadSelectedForkRootedSourceTx(ctx, resolver.tx, snapshot.RunID, origin, rooted.Admission().Reference(), postgres)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return sourceartifact.RootedSource{}, err
	}
	if prior.Intent.Request.PlanRef != obligation.Intent.Request.PlanRef {
		return sourceartifact.RootedSource{}, fmt.Errorf("changed fan-out execution lacks its selected binding")
	}
	ordinaryParent, err := fanOutExecutionSourceAtCut(ctx, resolver, parent, prior, postgres, visiting)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if !ordinaryParent.Admission().Equal(rooted.Admission()) {
		return sourceartifact.RootedSource{}, fmt.Errorf("ordinary fan-out fork changed root without a selected binding")
	}
	return rooted, nil
}

func loadFanOutExecutionRunRoot(ctx context.Context, tx *sql.Tx, runID, hash string, postgres bool) (sourceartifact.RootedSource, error) {
	rooted, err := lifecycleowner.LoadRunRootedSourceTx(ctx, tx, runID, postgres)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if rooted.Admission().BundleHash() != hash {
		return sourceartifact.RootedSource{}, fmt.Errorf("fan-out execution reference contradicts its proven native run root")
	}
	return rooted, nil
}
