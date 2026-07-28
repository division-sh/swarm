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

const dynamicFlowRuntimeReadinessVersion = 2

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

type DynamicFlowRuntimeCreationOccurrencePublisher interface {
	PublishInMutation(context.Context, events.Event) error
}

type DynamicFlowRuntimeCreationOccurrenceRequest struct {
	RunID        string
	InstancePath string
	Plan         DynamicFlowRuntimeReadinessPlan
	Event        events.Event
	OccurredAt   time.Time
}

// DynamicFlowRuntimeReadinessPlan is the durable desired topology and
// creation occurrence for one dynamic flow instance.
type DynamicFlowRuntimeReadinessPlan struct {
	Version         int                                  `json:"version"`
	Identity        runtimeflowidentity.Instance         `json:"identity"`
	RunID           string                               `json:"run_id"`
	BundleHash      string                               `json:"bundle_hash"`
	BundleSource    string                               `json:"bundle_source"`
	WorkflowVersion string                               `json:"workflow_version"`
	Agents          []DynamicFlowRuntimeAgentExpectation `json:"agents"`
	CreationEvent   *DynamicFlowRuntimeCreationEventPlan `json:"creation_event,omitempty"`
}

type DynamicFlowRuntimeReadiness struct {
	InstancePath           string
	Plan                   DynamicFlowRuntimeReadinessPlan
	RunStatus              string
	InstanceStatus         string
	InstanceTerminatedAt   time.Time
	TopologyReadyAt        time.Time
	CreationEventEmittedAt time.Time
}

type DynamicFlowRuntimeReadinessKey struct {
	RunID        string
	InstancePath string
}

func (r DynamicFlowRuntimeReadiness) Eligible() bool {
	runStatus := strings.ToLower(strings.TrimSpace(r.RunStatus))
	return (runStatus == "running" || runStatus == "paused") &&
		strings.EqualFold(strings.TrimSpace(r.InstanceStatus), "active") &&
		r.InstanceTerminatedAt.IsZero()
}

func (r DynamicFlowRuntimeReadiness) Pending() bool {
	if !r.Eligible() || r.TopologyReadyAt.IsZero() {
		return r.Eligible()
	}
	return r.Plan.CreationEvent != nil && r.CreationEventEmittedAt.IsZero()
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
	sourceFact, err := runtimecorrelation.DecodeBundleSourceFact(p.BundleHash, p.BundleSource)
	if err != nil {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness bundle source: %w", err)
	}
	p.BundleHash, p.BundleSource = sourceFact.StorageValues()
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

// ReconcileDynamicFlowRuntimeReadinessPlan advances the durable topology owner
// when an existing flow instance is ensured against a revised semantic source.
func (s *WorkflowInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	expected DynamicFlowRuntimeReadinessPlan,
	observedAt time.Time,
) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("workflow instance lifecycle store is required")
	}
	normalized, err := expected.Normalized()
	if err != nil {
		return false, err
	}
	observedAt = canonicalWorkflowInstancePersistedTime(observedAt)
	if observedAt.IsZero() {
		return false, fmt.Errorf("dynamic flow runtime readiness reconciliation requires an exact occurrence time")
	}

	changed := false
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		activeRunID, err := s.requireActiveWorkflowRun(txctx, tx)
		if err != nil {
			return err
		}
		if activeRunID != normalized.RunID {
			return fmt.Errorf(
				"dynamic flow runtime readiness reconciliation run identity changed: expected=%s actual=%s",
				normalized.RunID,
				activeRunID,
			)
		}
		instancePath := normalized.Identity.InstancePath
		if err := s.lockDynamicFlowRuntimeCreationEligibility(txctx, tx, normalized.RunID, instancePath); err != nil {
			return err
		}
		current, found, err := s.LoadDynamicFlowRuntimeReadiness(txctx, normalized.RunID, instancePath)
		if err != nil {
			return err
		}
		if !found || !current.Eligible() {
			return fmt.Errorf(
				"dynamic flow runtime readiness reconciliation requires one active eligible record: %s",
				instancePath,
			)
		}
		if current.Plan.Identity != normalized.Identity || current.Plan.RunID != normalized.RunID {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation identity changed for %s", instancePath)
		}
		actualJSON, err := canonicaljson.Bytes(current.Plan)
		if err != nil {
			return fmt.Errorf("encode persisted dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		expectedJSON, err := canonicaljson.Bytes(normalized)
		if err != nil {
			return fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		if string(actualJSON) == string(expectedJSON) {
			return nil
		}
		if !current.CreationEventEmittedAt.IsZero() {
			actualCreationJSON, err := canonicaljson.Bytes(current.Plan.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode emitted dynamic flow creation plan %s: %w", instancePath, err)
			}
			expectedCreationJSON, err := canonicaljson.Bytes(normalized.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode revised dynamic flow creation plan %s: %w", instancePath, err)
			}
			if string(actualCreationJSON) != string(expectedCreationJSON) {
				return fmt.Errorf("dynamic flow runtime readiness cannot revise emitted creation occurrence for %s", instancePath)
			}
		}
		if s.isSQLite() {
			result, err := tx.ExecContext(txctx, `
				UPDATE flow_instance_runtime_readiness
				SET plan = ?,
				    topology_ready_at = NULL,
				    updated_at = ?
				WHERE run_id = ? AND instance_id = ?
			`, expectedJSON, observedAt, normalized.RunID, instancePath)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("count dynamic flow runtime readiness reconciliation rows for %s: %w", instancePath, err)
			}
			if rows != 1 {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation changed %d rows for %s", rows, instancePath)
			}
		} else {
			result, err := tx.ExecContext(txctx, `
				UPDATE flow_instance_runtime_readiness
				SET plan = $1::jsonb,
				    topology_ready_at = NULL,
				    updated_at = $2
				WHERE run_id = $3::uuid AND instance_id = $4
			`, expectedJSON, observedAt, normalized.RunID, instancePath)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("count dynamic flow runtime readiness reconciliation rows for %s: %w", instancePath, err)
			}
			if rows != 1 {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation changed %d rows for %s", rows, instancePath)
			}
		}
		changed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
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
		SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.status, instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid AND readiness.instance_id = $2
	`
	if s.isSQLite() {
		query = `
			SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.status, instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_id = ?
		`
	}
	var raw []byte
	var runStatus, instanceStatus string
	var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt dynamicFlowRuntimeReadinessTime
	var err error
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		err = tx.QueryRowContext(ctx, query, runID, instancePath).Scan(
			&raw, &topologyReadyAt, &creationEventEmittedAt,
			&runStatus, &instanceStatus, &instanceTerminatedAt,
		)
	} else {
		err = dbQueryRowContext(ctx, s.db, query, runID, instancePath).Scan(
			&raw, &topologyReadyAt, &creationEventEmittedAt,
			&runStatus, &instanceStatus, &instanceTerminatedAt,
		)
	}
	if err == sql.ErrNoRows {
		return DynamicFlowRuntimeReadiness{}, false, nil
	}
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("load dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	item, err := decodeDynamicFlowRuntimeReadiness(
		runID, instancePath, raw, runStatus, instanceStatus, instanceTerminatedAt,
		topologyReadyAt, creationEventEmittedAt,
	)
	return item, err == nil, err
}

func (s *WorkflowInstanceStore) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan,
		       readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.status, instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY readiness.run_id, readiness.instance_id
	`
	if s.isSQLite() {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan,
			       readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.status, instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
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
		var runStatus, instanceStatus string
		var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt dynamicFlowRuntimeReadinessTime
		if err := rows.Scan(
			&runID, &instancePath, &raw,
			&topologyReadyAt, &creationEventEmittedAt,
			&runStatus, &instanceStatus, &instanceTerminatedAt,
		); err != nil {
			return nil, err
		}
		item, err := decodeDynamicFlowRuntimeReadiness(
			runID, instancePath, raw, runStatus, instanceStatus, instanceTerminatedAt,
			topologyReadyAt, creationEventEmittedAt,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *WorkflowInstanceStore) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]DynamicFlowRuntimeReadinessKey, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	query := `
		SELECT run_id::text, instance_id
		FROM flow_instance_runtime_readiness
		ORDER BY run_id, instance_id
	`
	if s.isSQLite() {
		query = `
			SELECT run_id, instance_id
			FROM flow_instance_runtime_readiness
			ORDER BY run_id, instance_id
		`
	}
	rows, err := dbQueryContext(ctx, s.db, query)
	if err != nil {
		return nil, fmt.Errorf("list dynamic flow runtime readiness keys: %w", err)
	}
	defer rows.Close()
	var keys []DynamicFlowRuntimeReadinessKey
	for rows.Next() {
		var key DynamicFlowRuntimeReadinessKey
		if err := rows.Scan(&key.RunID, &key.InstancePath); err != nil {
			return nil, err
		}
		key.RunID = strings.TrimSpace(key.RunID)
		key.InstancePath = strings.Trim(strings.TrimSpace(key.InstancePath), "/")
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func decodeDynamicFlowRuntimeReadiness(
	runID string,
	instancePath string,
	raw []byte,
	runStatus string,
	instanceStatus string,
	instanceTerminatedAt dynamicFlowRuntimeReadinessTime,
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
	item := DynamicFlowRuntimeReadiness{
		InstancePath:   instancePath,
		Plan:           normalized,
		RunStatus:      strings.TrimSpace(runStatus),
		InstanceStatus: strings.TrimSpace(instanceStatus),
	}
	if instanceTerminatedAt.Valid {
		item.InstanceTerminatedAt = instanceTerminatedAt.Time.UTC()
	}
	if topologyReadyAt.Valid {
		item.TopologyReadyAt = topologyReadyAt.Time.UTC()
	}
	if creationEventEmittedAt.Valid {
		item.CreationEventEmittedAt = creationEventEmittedAt.Time.UTC()
	}
	return item, nil
}

func (s *WorkflowInstanceStore) MarkDynamicFlowRuntimeTopologyReady(
	ctx context.Context,
	expected DynamicFlowRuntimeReadinessPlan,
	readyAt time.Time,
) error {
	normalized, err := expected.Normalized()
	if err != nil {
		return fmt.Errorf("normalize dynamic flow runtime topology readiness plan: %w", err)
	}
	expectedJSON, err := canonicaljson.Bytes(normalized)
	if err != nil {
		return fmt.Errorf("encode dynamic flow runtime topology readiness plan: %w", err)
	}
	return s.markDynamicFlowRuntimeReadiness(
		ctx,
		normalized.RunID,
		normalized.Identity.InstancePath,
		"topology_ready_at",
		expectedJSON,
		readyAt,
	)
}

func (s *WorkflowInstanceStore) CommitDynamicFlowRuntimeCreationOccurrence(
	ctx context.Context,
	req DynamicFlowRuntimeCreationOccurrenceRequest,
	publisher DynamicFlowRuntimeCreationOccurrencePublisher,
) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	if publisher == nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence publisher is required")
	}
	runID := strings.TrimSpace(req.RunID)
	instancePath := strings.Trim(strings.TrimSpace(req.InstancePath), "/")
	occurredAt := req.OccurredAt.UTC()
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires valid run_id: %w", err)
	}
	if instancePath == "" || occurredAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires exact instance and time")
	}
	expected, err := req.Plan.Normalized()
	if err != nil {
		return fmt.Errorf("normalize dynamic flow runtime creation occurrence plan: %w", err)
	}
	if expected.RunID != runID || expected.Identity.InstancePath != instancePath {
		return fmt.Errorf("dynamic flow runtime creation occurrence identity does not match readiness plan")
	}
	if expected.CreationEvent == nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence plan is missing creation event")
	}
	if strings.TrimSpace(req.Event.ID()) != expected.CreationEvent.EventID ||
		strings.TrimSpace(string(req.Event.Type())) != expected.CreationEvent.EventType ||
		strings.TrimSpace(req.Event.RunID()) != runID {
		return fmt.Errorf("dynamic flow runtime creation occurrence event does not match readiness plan")
	}

	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.lockDynamicFlowRuntimeCreationEligibility(txctx, tx, runID, instancePath); err != nil {
			return err
		}
		current, found, err := s.LoadDynamicFlowRuntimeReadiness(txctx, runID, instancePath)
		if err != nil {
			return err
		}
		if !found || !current.Eligible() {
			return fmt.Errorf("dynamic flow runtime creation occurrence requires one active eligible record: %s", instancePath)
		}
		if current.TopologyReadyAt.IsZero() {
			return fmt.Errorf("dynamic flow runtime creation occurrence requires topology readiness: %s", instancePath)
		}
		actualJSON, err := canonicaljson.Bytes(current.Plan)
		if err != nil {
			return fmt.Errorf("encode persisted dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		expectedJSON, err := canonicaljson.Bytes(expected)
		if err != nil {
			return fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		if string(actualJSON) != string(expectedJSON) {
			return fmt.Errorf("dynamic flow runtime creation occurrence readiness plan changed for %s", instancePath)
		}
		if !current.CreationEventEmittedAt.IsZero() {
			return nil
		}
		if err := publisher.PublishInMutation(txctx, req.Event); err != nil {
			return fmt.Errorf("append dynamic flow runtime creation occurrence: %w", err)
		}
		if err := s.markDynamicFlowRuntimeReadiness(txctx, runID, instancePath, "creation_event_emitted_at", nil, occurredAt); err != nil {
			return fmt.Errorf("mark dynamic flow runtime creation occurrence complete: %w", err)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) lockDynamicFlowRuntimeCreationEligibility(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	instancePath string,
) error {
	if tx == nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires selected-store transaction")
	}
	activeRunID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return err
	}
	if activeRunID != runID {
		return fmt.Errorf("dynamic flow runtime creation occurrence run identity changed: expected=%s actual=%s", runID, activeRunID)
	}
	if s.isSQLite() {
		result, err := tx.ExecContext(ctx, `
			UPDATE flow_instances
			SET status = status
			WHERE instance_id = ?
		`, instancePath)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("dynamic flow runtime creation occurrence lifecycle row not found: %s", instancePath)
		}
		return nil
	}
	var lockedInstancePath string
	if err := tx.QueryRowContext(ctx, `
		SELECT instance_id
		FROM flow_instances
		WHERE instance_id = $1
		FOR UPDATE
	`, instancePath).Scan(&lockedInstancePath); err != nil {
		return fmt.Errorf("lock dynamic flow runtime creation instance: %w", err)
	}
	if strings.TrimSpace(lockedInstancePath) != instancePath {
		return fmt.Errorf("dynamic flow runtime creation occurrence instance identity changed")
	}
	return nil
}

func (s *WorkflowInstanceStore) markDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	instancePath string,
	column string,
	expectedPlan []byte,
	observedAt time.Time,
) error {
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
			args := []any{observedAt, observedAt, runID, instancePath}
			if len(expectedPlan) != 0 {
				query += ` AND plan = ?`
				args = append(args, expectedPlan)
			}
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			query += `
				AND EXISTS (
					SELECT 1
					FROM flow_instances AS instance
					JOIN runs AS run ON run.run_id = flow_instance_runtime_readiness.run_id
					WHERE instance.instance_id = flow_instance_runtime_readiness.instance_id
					  AND LOWER(TRIM(instance.status)) = 'active'
					  AND instance.terminated_at IS NULL
					  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
				)`
			result, err = tx.ExecContext(txctx, query, args...)
		} else {
			query := `UPDATE flow_instance_runtime_readiness SET ` + column + ` = COALESCE(` + column + `, $1), updated_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
			args := []any{observedAt, runID, instancePath}
			if len(expectedPlan) != 0 {
				query += ` AND plan = $4::jsonb`
				args = append(args, expectedPlan)
			}
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			query += `
				AND EXISTS (
					SELECT 1
					FROM flow_instances AS instance
					JOIN runs AS run ON run.run_id = flow_instance_runtime_readiness.run_id
					WHERE instance.instance_id = flow_instance_runtime_readiness.instance_id
					  AND LOWER(BTRIM(instance.status)) = 'active'
					  AND instance.terminated_at IS NULL
					  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
				)`
			result, err = tx.ExecContext(txctx, query, args...)
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
