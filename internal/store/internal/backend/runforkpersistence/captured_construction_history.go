package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	lifecycleowner "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type constructionCut struct {
	runID    string
	revision int64
}

type capturedConstructionResolver struct {
	tx        *sql.Tx
	snapshots map[constructionCut]*runForkRevisionSnapshot
	active    map[string]bool
}

func (r *capturedConstructionResolver) resolve(ctx context.Context, snapshot *runForkRevisionSnapshot) (*runForkRevisionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cut := constructionCut{runID: snapshot.RunID, revision: snapshot.Revision}
	if r.active[snapshot.RunID] {
		return nil, fmt.Errorf("captured construction has cyclic immutable lineage")
	}
	if resolved := r.snapshots[cut]; resolved != nil {
		return resolved, nil
	}
	r.active[snapshot.RunID] = true
	defer delete(r.active, snapshot.RunID)
	var origin runlifecycle.RunOrigin
	var original *runForkRevisionSnapshot
	for i := range snapshot.EntityMetadata {
		metadata := &snapshot.EntityMetadata[i]
		if metadata.ConstructionKind != "constructed" {
			continue
		}
		if origin.Kind() == "" {
			var err error
			origin, err = lifecycleowner.ReadImmutableRunOriginTx(ctx, r.tx, snapshot.RunID)
			if err != nil {
				return nil, err
			}
		}
		raw := bytes.TrimSpace(metadata.InitialMaterialization)
		native := len(raw) != 0 && !bytes.Equal(raw, []byte("null"))
		if native {
			if metadata.InitialProjectionVersion != pipeline.FlowConstructionReceiptVersion {
				return nil, fmt.Errorf("native captured header requires its immutable constructor receipt version")
			}
			receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(raw, snapshot.RunID, metadata.EntityID, metadata.FlowInstance, metadata.FlowTemplate)
			if err != nil {
				return nil, err
			}
			if !receipt.OccurredAt.Equal(metadata.CreatedAt) || receipt.Persisted.Control.EntityType != metadata.EntityType {
				return nil, fmt.Errorf("native captured constructor contradicts fixed header birth or field contract")
			}
		}
		if origin.Kind() == runlifecycle.OriginForkMaterialization {
			if original == nil {
				var err error
				original, err = r.sourceCut(ctx, origin)
				if err != nil {
					return nil, err
				}
			}
			witness, found, err := findProjectedFlowInstanceMetadata(original, snapshot.RunID, metadata.FlowInstance)
			if err != nil {
				return nil, err
			}
			if found {
				if native {
					return nil, fmt.Errorf("captured construction has conflicting native and historical evidence")
				}
				entity, err := loadRunForkConstructedEntityState(original, witness.EntityID)
				if err != nil {
					return nil, err
				}
				metadata.HistoricalConstruction = &runfork.RunForkCapturedHistoricalConstruction{SourceRunID: original.RunID, SourceRevision: original.Revision, SourceEntity: entity}
			}
		}
		if !native && metadata.HistoricalConstruction == nil {
			return nil, fmt.Errorf("captured construction lacks its native receipt or proven historical origin")
		}
		if err := validateResolvedCapturedConstruction(snapshot, *metadata); err != nil {
			return nil, err
		}
	}
	r.snapshots[cut] = snapshot
	return snapshot, nil
}

func (r *capturedConstructionResolver) sourceCut(ctx context.Context, origin runlifecycle.RunOrigin) (*runForkRevisionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runID, revision := origin.SourceRunID(), origin.ForkRevision()
	cut := constructionCut{runID: runID, revision: revision}
	if resolved := r.snapshots[cut]; resolved != nil && !r.active[runID] {
		return resolved, validateCapturedOriginBoundary(origin, resolved)
	}
	if r.active[runID] {
		return nil, fmt.Errorf("captured construction has cyclic immutable lineage")
	}
	var recorded int64
	if err := r.tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, runID, revision).Scan(&recorded); err != nil {
		return nil, fmt.Errorf("read captured construction lineage cut: %w", err)
	}
	snapshot, err := loadRunForkRevisionSnapshotRaw(ctx, r.tx, runID, recorded, false)
	if err != nil {
		return nil, err
	}
	if err := validateCapturedOriginBoundary(origin, snapshot); err != nil {
		return nil, err
	}
	return r.resolve(ctx, snapshot)
}

func validateCapturedOriginBoundary(origin runlifecycle.RunOrigin, snapshot *runForkRevisionSnapshot) error {
	if snapshot.RunID != origin.SourceRunID() || snapshot.Revision != origin.ForkRevision() {
		return fmt.Errorf("captured construction lineage cut disagrees with its immutable origin")
	}
	if origin.ForkPointKind() == runlifecycle.ForkOriginPointDeploymentRevision {
		return nil
	}
	for _, event := range snapshot.Events {
		if event.RunID == snapshot.RunID && event.EventID == origin.SourceEventID() && event.FirstRevision > 0 && event.FirstRevision <= snapshot.Revision {
			return nil
		}
	}
	return fmt.Errorf("captured construction lineage cut lacks its exact originating event")
}

func validateResolvedCapturedConstruction(snapshot *runForkRevisionSnapshot, metadata runForkRevisionEntityMetadata) error {
	entity, err := loadRunForkConstructedEntityState(snapshot, metadata.EntityID)
	if err != nil {
		return err
	}
	route := flowidentity.StoredRoute(metadata.FlowTemplate, flowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)
	_, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Revision: snapshot.Revision}, Entities: []runfork.RunForkEntityState{entity}}, route)
	if err != nil || !found {
		return fmt.Errorf("admit exact captured constructor: found=%t: %w", found, err)
	}
	return nil
}
