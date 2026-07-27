package pipeline

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

const dynamicFlowRuntimeReadinessVersion = 1

type DynamicFlowRuntimeAgentExpectation struct {
	AgentID        string `json:"agent_id"`
	ConfigRevision string `json:"config_revision"`
}

type DynamicFlowRuntimeCreationEventPlan struct {
	EventID         string                 `json:"event_id"`
	EventType       string                 `json:"event_type"`
	RunID           string                 `json:"run_id"`
	ParentEventID   string                 `json:"parent_event_id"`
	ExecutionMode   executionmode.Mode     `json:"execution_mode"`
	Payload         json.RawMessage        `json:"payload"`
	CreatedAt       time.Time              `json:"created_at"`
	DeliveryContext events.DeliveryContext `json:"delivery_context,omitempty"`
}

// DynamicFlowRuntimeReadinessPlan is the durable desired topology and
// creation occurrence for one dynamic flow instance.
type DynamicFlowRuntimeReadinessPlan struct {
	Version         int                                  `json:"version"`
	Identity        runtimeflowidentity.Instance         `json:"identity"`
	RunID           string                               `json:"run_id"`
	WorkflowVersion string                               `json:"workflow_version"`
	Agents          []DynamicFlowRuntimeAgentExpectation `json:"agents"`
	CreationEvent   *DynamicFlowRuntimeCreationEventPlan `json:"creation_event,omitempty"`
}

type DynamicFlowRuntimeReadiness struct {
	InstancePath           string
	Plan                   DynamicFlowRuntimeReadinessPlan
	TopologyReadyAt        time.Time
	CreationEventEmittedAt time.Time
}

type dynamicFlowRuntimeReadinessTime struct {
	Time  time.Time
	Valid bool
}

func (t *dynamicFlowRuntimeReadinessTime) Scan(value any) error {
	if t == nil {
		return fmt.Errorf("dynamic flow runtime readiness time scanner is required")
	}
	switch value := value.(type) {
	case nil:
		t.Time = time.Time{}
		t.Valid = false
		return nil
	case time.Time:
		if value.IsZero() {
			t.Time = time.Time{}
			t.Valid = false
			return nil
		}
		t.Time = value.UTC()
		t.Valid = true
		return nil
	case string:
		return t.scanString(value)
	case []byte:
		return t.scanString(string(value))
	default:
		return fmt.Errorf("unsupported dynamic flow runtime readiness time value %T", value)
	}
}

func (t *dynamicFlowRuntimeReadinessTime) scanString(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		t.Time = time.Time{}
		t.Valid = false
		return nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			t.Time = parsed.UTC()
			t.Valid = true
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("parse dynamic flow runtime readiness time %q: %w", value, lastErr)
}

func (p DynamicFlowRuntimeReadinessPlan) Normalized() (DynamicFlowRuntimeReadinessPlan, error) {
	if p.Version != 0 && p.Version != dynamicFlowRuntimeReadinessVersion {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("unsupported dynamic flow runtime readiness version %d", p.Version)
	}
	p.Version = dynamicFlowRuntimeReadinessVersion
	p.RunID = strings.TrimSpace(p.RunID)
	p.WorkflowVersion = strings.TrimSpace(p.WorkflowVersion)
	p.Identity.TemplateID = strings.TrimSpace(p.Identity.TemplateID)
	p.Identity.ScopeKey = strings.Trim(strings.TrimSpace(p.Identity.ScopeKey), "/")
	p.Identity.InstanceID = strings.Trim(strings.TrimSpace(p.Identity.InstanceID), "/")
	p.Identity.InstancePath = strings.Trim(strings.TrimSpace(p.Identity.InstancePath), "/")
	p.Identity.EntityID = strings.TrimSpace(p.Identity.EntityID)
	p.Identity.ParentEntityID = strings.TrimSpace(p.Identity.ParentEntityID)
	p.Identity.ParentRoute = p.Identity.ParentRoute.Normalized()
	if !p.Identity.HasStoredPath || !p.Identity.Route().Valid() || p.Identity.TemplateID == "" || p.Identity.EntityID == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires exact flow identity")
	}
	if p.RunID == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires run_id")
	}
	if _, err := uuid.Parse(p.RunID); err != nil {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness run_id: %w", err)
	}
	if p.WorkflowVersion == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires workflow_version")
	}
	agents := append([]DynamicFlowRuntimeAgentExpectation(nil), p.Agents...)
	for idx := range agents {
		agents[idx].AgentID = strings.TrimSpace(agents[idx].AgentID)
		agents[idx].ConfigRevision = strings.TrimSpace(agents[idx].ConfigRevision)
		if agents[idx].AgentID == "" || agents[idx].ConfigRevision == "" {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness agent identity and config revision are required")
		}
		decodedRevision, err := hex.DecodeString(agents[idx].ConfigRevision)
		if err != nil || len(decodedRevision) != 32 {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness agent %s has invalid config revision", agents[idx].AgentID)
		}
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].AgentID < agents[j].AgentID })
	for idx := 1; idx < len(agents); idx++ {
		if agents[idx-1].AgentID == agents[idx].AgentID {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness has duplicate agent %s", agents[idx].AgentID)
		}
	}
	p.Agents = agents
	if p.CreationEvent != nil {
		event := *p.CreationEvent
		event.EventID = strings.TrimSpace(event.EventID)
		event.EventType = strings.TrimSpace(event.EventType)
		event.RunID = strings.TrimSpace(event.RunID)
		event.ParentEventID = strings.TrimSpace(event.ParentEventID)
		event.CreatedAt = canonicalWorkflowInstancePersistedTime(event.CreatedAt)
		event.DeliveryContext = event.DeliveryContext.Normalized()
		if _, err := uuid.Parse(event.EventID); err != nil {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event_id: %w", err)
		}
		if event.EventType == "" || event.RunID != p.RunID || event.ParentEventID == "" || !event.ExecutionMode.Valid() || event.CreatedAt.IsZero() {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event requires exact type, lineage, execution mode, and occurrence time")
		}
		if len(event.Payload) == 0 || !json.Valid(event.Payload) {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event requires valid payload")
		}
		p.CreationEvent = &event
	}
	return p, nil
}

func (s *WorkflowInstanceStore) insertDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	instancePath string,
	plan DynamicFlowRuntimeReadinessPlan,
	createdAt time.Time,
) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil {
		return fmt.Errorf("dynamic flow runtime readiness creation requires selected mutation")
	}
	normalized, err := plan.Normalized()
	if err != nil {
		return err
	}
	if normalized.Identity.InstancePath != strings.Trim(strings.TrimSpace(instancePath), "/") {
		return fmt.Errorf("dynamic flow runtime readiness identity disagrees with flow instance")
	}
	encoded, err := canonicaljson.Bytes(normalized)
	if err != nil {
		return fmt.Errorf("encode dynamic flow runtime readiness: %w", err)
	}
	if s.isSQLite() {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES (?, ?, ?, NULL, NULL, ?, ?)
		`, normalized.RunID, instancePath, encoded, createdAt.UTC(), createdAt.UTC())
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)
		`, normalized.RunID, instancePath, encoded, createdAt.UTC())
	}
	if err != nil {
		return fmt.Errorf("persist dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	return nil
}

func (s *WorkflowInstanceStore) dynamicFlowRuntimeReadinessPlanEqual(
	ctx context.Context,
	instancePath string,
	expected *DynamicFlowRuntimeReadinessPlan,
) (bool, error) {
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	var normalized DynamicFlowRuntimeReadinessPlan
	if expected != nil {
		var err error
		normalized, err = expected.Normalized()
		if err != nil {
			return false, err
		}
		if runID != "" && runID != normalized.RunID {
			return false, fmt.Errorf("dynamic flow runtime readiness run_id conflicts with selected mutation")
		}
		runID = normalized.RunID
	}
	if runID == "" {
		return false, fmt.Errorf("dynamic flow runtime readiness comparison requires run_id")
	}
	actual, found, err := s.LoadDynamicFlowRuntimeReadiness(ctx, runID, instancePath)
	if err != nil {
		return false, err
	}
	if expected == nil {
		return !found, nil
	}
	if !found {
		return false, nil
	}
	actualJSON, actualErr := canonicaljson.Bytes(actual.Plan)
	if actualErr != nil {
		return false, fmt.Errorf("encode persisted dynamic flow runtime readiness: %w", actualErr)
	}
	expectedJSON, expectedErr := canonicaljson.Bytes(normalized)
	if expectedErr != nil {
		return false, fmt.Errorf("encode expected dynamic flow runtime readiness: %w", expectedErr)
	}
	return string(actualJSON) == string(expectedJSON), nil
}

func (s *WorkflowInstanceStore) LoadDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	instancePath string,
) (DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.db == nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("workflow instance store is required")
	}
	runID = strings.TrimSpace(runID)
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if _, err := uuid.Parse(runID); err != nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires valid run_id: %w", err)
	}
	if instancePath == "" {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires instance path")
	}
	query := `
		SELECT plan, topology_ready_at, creation_event_emitted_at
		FROM flow_instance_runtime_readiness
		WHERE run_id = $1::uuid AND instance_id = $2
	`
	if s.isSQLite() {
		query = `
			SELECT plan, topology_ready_at, creation_event_emitted_at
			FROM flow_instance_runtime_readiness
			WHERE run_id = ? AND instance_id = ?
		`
	}
	var raw []byte
	var topologyReadyAt, creationEventEmittedAt dynamicFlowRuntimeReadinessTime
	var err error
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		err = tx.QueryRowContext(ctx, query, runID, instancePath).Scan(&raw, &topologyReadyAt, &creationEventEmittedAt)
	} else {
		err = dbQueryRowContext(ctx, s.db, query, runID, instancePath).Scan(&raw, &topologyReadyAt, &creationEventEmittedAt)
	}
	if err == sql.ErrNoRows {
		return DynamicFlowRuntimeReadiness{}, false, nil
	}
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("load dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	item, err := decodeDynamicFlowRuntimeReadiness(runID, instancePath, raw, topologyReadyAt, creationEventEmittedAt)
	return item, err == nil, err
}

func (s *WorkflowInstanceStore) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) = 'running'
		ORDER BY readiness.run_id, readiness.instance_id
	`
	if s.isSQLite() {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) = 'running'
			ORDER BY readiness.run_id, readiness.instance_id
		`
	}
	rows, err := dbQueryContext(ctx, s.db, query)
	if err != nil {
		return nil, fmt.Errorf("list dynamic flow runtime readiness: %w", err)
	}
	defer rows.Close()
	var out []DynamicFlowRuntimeReadiness
	for rows.Next() {
		var runID, instancePath string
		var raw []byte
		var topologyReadyAt, creationEventEmittedAt dynamicFlowRuntimeReadinessTime
		if err := rows.Scan(&runID, &instancePath, &raw, &topologyReadyAt, &creationEventEmittedAt); err != nil {
			return nil, err
		}
		item, err := decodeDynamicFlowRuntimeReadiness(runID, instancePath, raw, topologyReadyAt, creationEventEmittedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func decodeDynamicFlowRuntimeReadiness(
	runID string,
	instancePath string,
	raw []byte,
	topologyReadyAt dynamicFlowRuntimeReadinessTime,
	creationEventEmittedAt dynamicFlowRuntimeReadinessTime,
) (DynamicFlowRuntimeReadiness, error) {
	var plan DynamicFlowRuntimeReadinessPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("decode dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	if plan.Version != dynamicFlowRuntimeReadinessVersion {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf(
			"dynamic flow runtime readiness %s has unsupported version %d",
			instancePath,
			plan.Version,
		)
	}
	normalized, err := plan.Normalized()
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("validate dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	if normalized.RunID != strings.TrimSpace(runID) {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness run identity mismatch for %s", instancePath)
	}
	if normalized.Identity.InstancePath != strings.Trim(strings.TrimSpace(instancePath), "/") {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness identity mismatch for %s", instancePath)
	}
	item := DynamicFlowRuntimeReadiness{InstancePath: instancePath, Plan: normalized}
	if topologyReadyAt.Valid {
		item.TopologyReadyAt = topologyReadyAt.Time.UTC()
	}
	if creationEventEmittedAt.Valid {
		if item.TopologyReadyAt.IsZero() {
			return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness %s emitted creation event before topology readiness", instancePath)
		}
		item.CreationEventEmittedAt = creationEventEmittedAt.Time.UTC()
	}
	return item, nil
}

func (s *WorkflowInstanceStore) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, runID, instancePath string, readyAt time.Time) error {
	return s.markDynamicFlowRuntimeReadiness(ctx, runID, instancePath, "topology_ready_at", readyAt)
}

func (s *WorkflowInstanceStore) MarkDynamicFlowRuntimeCreationEventEmitted(ctx context.Context, runID, instancePath string, emittedAt time.Time) error {
	return s.markDynamicFlowRuntimeReadiness(ctx, runID, instancePath, "creation_event_emitted_at", emittedAt)
}

func (s *WorkflowInstanceStore) markDynamicFlowRuntimeReadiness(ctx context.Context, runID, instancePath, column string, observedAt time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	runID = strings.TrimSpace(runID)
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	observedAt = observedAt.UTC()
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness transition requires valid run_id: %w", err)
	}
	if instancePath == "" || observedAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime readiness transition requires exact instance and time")
	}
	if column != "topology_ready_at" && column != "creation_event_emitted_at" {
		return fmt.Errorf("unsupported dynamic flow runtime readiness transition")
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var result sql.Result
		var err error
		if s.isSQLite() {
			query := `UPDATE flow_instance_runtime_readiness SET ` + column + ` = COALESCE(` + column + `, ?), updated_at = ? WHERE run_id = ? AND instance_id = ?`
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			result, err = tx.ExecContext(txctx, query, observedAt, observedAt, runID, instancePath)
		} else {
			query := `UPDATE flow_instance_runtime_readiness SET ` + column + ` = COALESCE(` + column + `, $1), updated_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			result, err = tx.ExecContext(txctx, query, observedAt, runID, instancePath)
		}
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("dynamic flow runtime readiness %s transition %s requires one active record", instancePath, column)
		}
		return nil
	})
}
