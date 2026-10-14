package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

const flowInstanceCompanionBatchSize = 64

const flowInstanceCompanionSelect = `SELECT
	fi.instance_path, fi.flow_template, fi.entity_id, fi.parent_instance, fi.instance_key,
	m.entity_id, m.projection,
	r.instance_path, r.plan, r.plan_hash, r.activation_attempt_id, r.phase, r.activation_attempt_state, r.creation_event_emitted_at,
	run.bundle_hash, run.status, fi.status, fi.terminated_at
	FROM flow_instances fi
	JOIN runs run ON run.run_id=fi.run_id
	LEFT JOIN workflow_instance_initial_materializations m
		ON m.run_id=fi.run_id AND m.instance_path=fi.instance_path AND m.entity_id=fi.entity_id
	LEFT JOIN flow_instance_runtime_readiness r ON r.run_id=fi.run_id AND r.instance_path=fi.instance_path
	WHERE fi.run_id=$1 AND fi.instance_path IN (`

// Raw companions and their decoded readiness never leave this owned read
// transaction. Every requested selector still admits its own observation.
type flowInstanceCompanions struct {
	construction        flowConstructionPublicationRecord
	constructionPresent bool
	readiness           pipeline.DynamicFlowRuntimeReadinessPersistenceRecord
	readinessPresent    bool
	creationAt, endedAt any
	decoded             *pipeline.DynamicFlowRuntimeReadiness
	decodeErr           error
	decodedOnce         bool
}

func (c *flowInstanceCompanions) observeReadiness() (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if !c.readinessPresent {
		return pipeline.DynamicFlowRuntimeReadiness{}, false, nil
	}
	if !c.decodedOnce {
		value, err := decodeStoredDynamicFlowRuntimeReadiness(c.readiness, c.creationAt, c.endedAt)
		c.decoded, c.decodeErr, c.decodedOnce = &value, err, true
	}
	return *c.decoded, c.decodeErr == nil, c.decodeErr
}

func (s *flowInstanceIndexSnapshot) preloadCompanions(ctx context.Context, headers []pipeline.WorkflowInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.companions == nil {
		s.companions = make(map[string]*flowInstanceCompanions)
	}
	var paths []string
	seen := make(map[string]bool)
	for _, header := range headers {
		if s.companions[header.StorageRef] == nil && !seen[header.StorageRef] {
			seen[header.StorageRef] = true
			paths = append(paths, header.StorageRef)
		}
	}
	for start := 0; start < len(paths); start += flowInstanceCompanionBatchSize {
		end := min(start+flowInstanceCompanionBatchSize, len(paths))
		if err := s.readCompanionBatch(ctx, paths[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *flowInstanceIndexSnapshot) readCompanionBatch(ctx context.Context, paths []string) error {
	args := make([]any, len(paths)+1)
	args[0] = s.run.RunID
	placeholders := make([]string, len(paths))
	expected := make(map[string]bool, len(paths))
	for index, path := range paths {
		args[index+1], placeholders[index], expected[path] = path, "$"+strconv.Itoa(index+2), true
	}
	rows, err := s.tx.QueryContext(ctx, flowInstanceCompanionSelect+strings.Join(placeholders, ",")+")", args...)
	if err != nil {
		return fmt.Errorf("read instance companions: %w", err)
	}
	defer rows.Close()
	loaded := make(map[string]*flowInstanceCompanions, len(paths))
	for rows.Next() {
		path, companions, err := scanFlowInstanceCompanions(rows, s.run.RunID)
		if err != nil {
			return err
		}
		if !expected[path] || loaded[path] != nil {
			return &pipeline.FlowInstanceConstructionCorruption{RunID: s.run.RunID, InstancePath: path, Cause: fmt.Errorf("instance companions have duplicate or crossed coordinates")}
		}
		loaded[path] = companions
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(loaded) != len(paths) {
		return &pipeline.FlowInstanceConstructionCorruption{RunID: s.run.RunID, Cause: fmt.Errorf("instance companion read lost an existing header")}
	}
	for path, companions := range loaded {
		s.companions[path] = companions
	}
	return nil
}

func scanFlowInstanceCompanions(rows *sql.Rows, runID string) (string, *flowInstanceCompanions, error) {
	var path string
	var parent, key, receiptEntity, readyPath, planHash, phase, attemptState sql.NullString
	var attempt sql.NullInt64
	c := &flowInstanceCompanions{}
	if err := rows.Scan(&path, &c.construction.templateID, &c.construction.entityID, &parent, &key,
		&receiptEntity, &c.construction.raw, &readyPath, &c.readiness.Plan, &planHash, &attempt, &phase, &attemptState, &c.creationAt,
		&c.readiness.OwningRunBundleHash, &c.readiness.RunStatus, &c.readiness.InstanceStatus, &c.endedAt); err != nil {
		return "", nil, fmt.Errorf("scan instance companions: %w", err)
	}
	if attempt.Valid && attempt.Int64 < 0 {
		return "", nil, fmt.Errorf("instance readiness has a negative attachment attempt")
	}
	c.construction.parentInstance, c.construction.instanceKey = parent.String, key.String
	c.constructionPresent, c.readinessPresent = receiptEntity.Valid, readyPath.Valid
	c.readiness.RunID, c.readiness.InstancePath = runID, path
	c.readiness.PlanHash, c.readiness.Phase = planHash.String, pipeline.FlowAttachmentPhase(phase.String)
	c.readiness.AttemptOrdinal, c.readiness.AttemptState = uint64(attempt.Int64), attemptState.String
	return path, c, nil
}
