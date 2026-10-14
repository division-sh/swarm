package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type dynamicFlowReadinessInspection struct {
	postgres   bool
	historical HistoricalFlowInstanceReader
	read       func(context.Context, func(context.Context, *sql.Tx) error) error
	loadRun    func(context.Context, *sql.Tx, string) (runlifecycle.Snapshot, error)
}

func (s *PipelinePostgresOwner) readinessInspection() dynamicFlowReadinessInspection {
	return dynamicFlowReadinessInspection{
		postgres: true, historical: s.historicalInstances, read: s.backend.RunReadTransaction,
		loadRun: func(ctx context.Context, tx *sql.Tx, runID string) (runlifecycle.Snapshot, error) {
			return s.LoadSnapshotTx(ctx, tx, runID, false)
		},
	}
}

func (s *PipelineSQLiteOwner) readinessInspection() dynamicFlowReadinessInspection {
	return dynamicFlowReadinessInspection{
		historical: s.historicalInstances, read: s.backend.RunReadTransaction, loadRun: s.LoadSnapshotTx,
	}
}

func (s *PipelinePostgresOwner) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.backend == nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("postgres dynamic flow source projection reader is required")
	}
	return s.readinessInspection().source(ctx, source)
}

func (s *PipelineSQLiteOwner) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.backend == nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("sqlite dynamic flow source projection reader is required")
	}
	return s.readinessInspection().source(ctx, source)
}

func (s *PipelinePostgresOwner) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source correlation.SourceArtifactFact) ([]pipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres dynamic flow run projection reader is required")
	}
	return s.readinessInspection().run(ctx, runID, source)
}

func (s *PipelineSQLiteOwner) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source correlation.SourceArtifactFact) ([]pipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite dynamic flow run projection reader is required")
	}
	return s.readinessInspection().run(ctx, runID, source)
}

func (inspection dynamicFlowReadinessInspection) source(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	items, err := inspection.snapshot(ctx, source, "")
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	projection := pipeline.DynamicFlowRuntimeReadinessProjection{}
	for _, item := range items {
		planSource, err := correlation.DecodeSourceArtifactFact(item.Plan.BundleHash)
		if err != nil {
			return pipeline.DynamicFlowRuntimeReadinessProjection{}, err
		}
		switch {
		case !planSource.Matches(source):
			projection.SourceTransitionRequired = append(projection.SourceTransitionRequired, item)
		case item.Pending():
			projection.CurrentPending = append(projection.CurrentPending, item)
		default:
			projection.CurrentCompleted = append(projection.CurrentCompleted, item)
		}
	}
	return projection, nil
}

func (inspection dynamicFlowReadinessInspection) run(ctx context.Context, runID string, source correlation.SourceArtifactFact) ([]pipeline.DynamicFlowRuntimeReadiness, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("dynamic flow run projection requires valid run_id: %w", err)
	}
	return inspection.snapshot(ctx, source, runID)
}

func (inspection dynamicFlowReadinessInspection) snapshot(ctx context.Context, source correlation.SourceArtifactFact, runID string) ([]pipeline.DynamicFlowRuntimeReadiness, error) {
	if err := source.Validate(); err != nil {
		return nil, fmt.Errorf("dynamic flow readiness projection source: %w", err)
	}
	if inspection.historical == nil {
		return nil, fmt.Errorf("dynamic flow readiness inspection requires its construction history owner")
	}
	items := make([]pipeline.DynamicFlowRuntimeReadiness, 0)
	err := inspection.read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		runs, err := inspection.scopedRuns(ctx, tx, source, runID)
		if err != nil {
			return err
		}
		for _, id := range runs {
			run, err := inspection.loadRun(ctx, tx, id)
			if err != nil {
				return err
			}
			predicate := `LOWER(TRIM(fi.status))='active' AND fi.terminated_at IS NULL ORDER BY fi.instance_path`
			headers, err := readFlowInstanceIndexHeaders(ctx, tx, inspection.postgres, predicate, run.RunID)
			if err != nil {
				return err
			}
			for _, header := range headers {
				readiness, present, err := inspection.construction(ctx, tx, run, header)
				if err != nil {
					return err
				}
				if present {
					items = append(items, readiness)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inspect construction-scoped dynamic flow readiness: %w", err)
	}
	return items, nil
}

func (inspection dynamicFlowReadinessInspection) scopedRuns(ctx context.Context, tx *sql.Tx, source correlation.SourceArtifactFact, runID string) ([]string, error) {
	query := `SELECT run_id FROM runs WHERE bundle_hash=$1 AND LOWER(TRIM(status)) IN ('running','paused')`
	args := []any{source.BundleHash()}
	if runID != "" {
		query += ` AND run_id=$2`
		args = append(args, runID)
	}
	rows, err := tx.QueryContext(ctx, query+` ORDER BY run_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		runs = append(runs, id)
	}
	return runs, rows.Err()
}

func (inspection dynamicFlowReadinessInspection) construction(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	corrupt := func(err error) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
		return pipeline.DynamicFlowRuntimeReadiness{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: run.RunID, FlowID: header.WorkflowName, InstancePath: header.StorageRef, Cause: err}
	}
	history, projected, err := inspection.historical.ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, header)
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	receipt, recorded, err := ReadStoredFlowConstructionReceiptTx(ctx, tx, run.RunID, header)
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	if !projected && !recorded {
		return corrupt(fmt.Errorf("native header lacks its captured construction receipt"))
	}
	identity, key := history.Identity, history.InstanceKey
	if recorded {
		if projected && (receipt.Identity != history.Identity || receipt.InstanceKey != history.InstanceKey) {
			return corrupt(fmt.Errorf("present native receipt crosses fixed historical construction"))
		}
		if err := receipt.ValidateStoredMode(header.Mode); err != nil {
			return corrupt(err)
		}
		identity, key = receipt.Identity, receipt.InstanceKey
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: run.RunID, Route: identity.Route()}
	stored, err := header.ConstructionIdentity(owner)
	if err != nil || stored != identity || key != header.InstanceKey {
		return corrupt(errors.Join(fmt.Errorf("header contradicts its immutable construction identity or key"), err))
	}
	readiness, present, err := loadDynamicFlowRuntimeReadiness(ctx, tx, inspection.postgres, run.RunID, identity.Route(), false)
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	if !present && !projected {
		return corrupt(fmt.Errorf("native header lacks its desired attachment evidence"))
	}
	if present && readiness.Plan.Identity != identity {
		return corrupt(fmt.Errorf("desired attachment contradicts its immutable construction identity"))
	}
	return readiness, present, nil
}
