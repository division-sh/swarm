package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type WorkflowInstance struct {
	InstanceID        string
	StorageRef        string
	WorkflowName      string
	WorkflowVersion   string
	CurrentState      string
	EnteredStageAt    time.Time
	TransitionHistory []WorkflowTransitionRecord
	StateBuckets      map[string]any
	TimerState        []WorkflowTimerState
	Metadata          map[string]any
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type WorkflowTransitionRecord struct {
	TransitionID    string    `json:"transition_id"`
	From            string    `json:"from"`
	To              string    `json:"to"`
	TriggerEventID  string    `json:"trigger_event_id"`
	FiredAt         time.Time `json:"fired_at"`
	GuardsEvaluated []string  `json:"guards_evaluated"`
}

type WorkflowTimerState struct {
	TimerID   string    `json:"timer_id"`
	EventType string    `json:"event_type,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	FiresAt   time.Time `json:"fires_at"`
	StartedBy string    `json:"started_by,omitempty"`
	Recurring bool      `json:"recurring,omitempty"`
	Cancelled bool      `json:"cancelled,omitempty"`
}

type WorkflowInstanceStore struct {
	db *sql.DB
}

var workflowInstancePathNamespace = uuid.MustParse("5e7507c8-bd4f-46e0-a098-b016dc31df23")

func NewWorkflowInstanceStore(db *sql.DB) *WorkflowInstanceStore {
	return &WorkflowInstanceStore{db: db}
}

func (s *WorkflowInstanceStore) Enabled() bool {
	return s != nil && s.db != nil
}

func (s *WorkflowInstanceStore) Load(ctx context.Context, instanceID string) (WorkflowInstance, bool, error) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil {
		return WorkflowInstance{}, false, nil
	}
	var (
		out               WorkflowInstance
		transitionHistory []byte
		accumulatorState  []byte
		timerState        []byte
		metadata          []byte
	)
	keys := workflowInstanceLookupKeys(instanceID)
	if len(keys) == 0 {
		return WorkflowInstance{}, false, nil
	}
	err := dbQueryRowContext(ctx, s.db, `
		SELECT
			instance_id::text,
			workflow_name,
			workflow_version,
			current_state,
			entered_stage_at,
			COALESCE(transition_history, '[]'::jsonb),
			COALESCE(accumulator_state, '{}'::jsonb),
			COALESCE(timer_state, '[]'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			created_at,
			updated_at
		FROM workflow_instances
		WHERE instance_id = ANY($1::uuid[])
		ORDER BY created_at DESC, instance_id DESC
		LIMIT 1
	`, pqStringArray(keys)).Scan(
		&out.InstanceID,
		&out.WorkflowName,
		&out.WorkflowVersion,
		&out.CurrentState,
		&out.EnteredStageAt,
		&transitionHistory,
		&accumulatorState,
		&timerState,
		&metadata,
		&out.CreatedAt,
		&out.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return WorkflowInstance{}, false, nil
	}
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(transitionHistory, &out.TransitionHistory); err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(accumulatorState, &out.StateBuckets); err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(timerState, &out.TimerState); err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(metadata, &out.Metadata); err != nil {
		return WorkflowInstance{}, false, err
	}
	if out.StateBuckets == nil {
		out.StateBuckets = map[string]any{}
	}
	if out.Metadata == nil {
		out.Metadata = map[string]any{}
	}
	out.StorageRef = workflowInstanceStorageRef(out)
	out.InstanceID = workflowInstanceLogicalID(out.InstanceID, out.Metadata)
	return out, true, nil
}

func (s *WorkflowInstanceStore) List(ctx context.Context) ([]WorkflowInstance, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			instance_id::text,
			workflow_name,
			workflow_version,
			current_state,
			entered_stage_at,
			COALESCE(transition_history, '[]'::jsonb),
			COALESCE(accumulator_state, '{}'::jsonb),
			COALESCE(timer_state, '[]'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			created_at,
			updated_at
		FROM workflow_instances
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]WorkflowInstance, 0, 32)
	for rows.Next() {
		var (
			item              WorkflowInstance
			transitionHistory []byte
			accumulatorState  []byte
			timerState        []byte
			metadata          []byte
		)
		if err := rows.Scan(
			&item.InstanceID,
			&item.WorkflowName,
			&item.WorkflowVersion,
			&item.CurrentState,
			&item.EnteredStageAt,
			&transitionHistory,
			&accumulatorState,
			&timerState,
			&metadata,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(transitionHistory, &item.TransitionHistory); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(accumulatorState, &item.StateBuckets); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(timerState, &item.TimerState); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metadata, &item.Metadata); err != nil {
			return nil, err
		}
		if item.StateBuckets == nil {
			item.StateBuckets = map[string]any{}
		}
		if item.Metadata == nil {
			item.Metadata = map[string]any{}
		}
		item.StorageRef = workflowInstanceStorageRef(item)
		item.InstanceID = workflowInstanceLogicalID(item.InstanceID, item.Metadata)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *WorkflowInstanceStore) Upsert(ctx context.Context, instance WorkflowInstance) error {
	if s == nil || s.db == nil {
		return nil
	}
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	instance.WorkflowName = strings.TrimSpace(instance.WorkflowName)
	instance.WorkflowVersion = strings.TrimSpace(instance.WorkflowVersion)
	instance.CurrentState = strings.TrimSpace(instance.CurrentState)
	if instance.InstanceID == "" || instance.WorkflowName == "" || instance.WorkflowVersion == "" || instance.CurrentState == "" {
		return fmt.Errorf(
			"workflow instance requires instance_id, workflow_name, workflow_version, and current_state (id=%q workflow=%q version=%q state=%q)",
			instance.InstanceID,
			instance.WorkflowName,
			instance.WorkflowVersion,
			instance.CurrentState,
		)
	}
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = time.Now().UTC()
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	instance.StorageRef = strings.TrimSpace(instance.StorageRef)
	storageRef := workflowInstanceStorageRef(instance)
	if storageRef == "" {
		return nil
	}
	rowID := workflowInstanceRowID(storageRef)
	if rowID == "" {
		return nil
	}
	if strings.TrimSpace(asString(instance.Metadata["instance_id"])) == "" {
		instance.Metadata["instance_id"] = instance.InstanceID
	}
	instance.Metadata["storage_ref"] = storageRef
	transitionHistory, err := json.Marshal(instance.TransitionHistory)
	if err != nil {
		return err
	}
	accumulatorState, err := json.Marshal(instance.StateBuckets)
	if err != nil {
		return err
	}
	timerState, err := json.Marshal(instance.TimerState)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(instance.Metadata)
	if err != nil {
		return err
	}
	_, err = dbExecContext(ctx, s.db, `
		INSERT INTO workflow_instances (
			instance_id,
			workflow_name,
			workflow_version,
			current_state,
			entered_stage_at,
			transition_history,
			accumulator_state,
			timer_state,
			metadata,
			created_at,
			updated_at
		)
		VALUES (
			$1::uuid,
			$2,
			$3,
			$4,
			$5,
			$6::jsonb,
			$7::jsonb,
			$8::jsonb,
			$9::jsonb,
			now(),
			now()
		)
		ON CONFLICT (instance_id) DO UPDATE SET
			workflow_name = EXCLUDED.workflow_name,
			workflow_version = EXCLUDED.workflow_version,
			current_state = EXCLUDED.current_state,
			entered_stage_at = EXCLUDED.entered_stage_at,
			transition_history = EXCLUDED.transition_history,
			accumulator_state = EXCLUDED.accumulator_state,
			timer_state = EXCLUDED.timer_state,
			metadata = EXCLUDED.metadata,
			updated_at = now()
	`,
		rowID,
		instance.WorkflowName,
		instance.WorkflowVersion,
		instance.CurrentState,
		instance.EnteredStageAt,
		jsonOrDefault(transitionHistory, "[]"),
		jsonOrDefault(accumulatorState, "{}"),
		jsonOrDefault(timerState, "[]"),
		jsonOrDefault(metadata, "{}"),
	)
	return err
}

func (s *WorkflowInstanceStore) Mutate(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil || fn == nil {
		return nil
	}
	instance, ok, err := s.Load(ctx, instanceID)
	if err != nil {
		return err
	}
	if !ok {
		instance = WorkflowInstance{InstanceID: instanceID}
	}
	fn(&instance)
	return s.Upsert(ctx, instance)
}

func (s *WorkflowInstanceStore) Delete(ctx context.Context, instanceID string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil {
		return nil
	}
	keys := workflowInstanceLookupKeys(instanceID)
	if len(keys) == 0 {
		return nil
	}
	_, err := dbExecContext(ctx, s.db, `DELETE FROM workflow_instances WHERE instance_id = ANY($1::uuid[])`, pqStringArray(keys))
	return err
}

func workflowInstanceStorageRef(instance WorkflowInstance) string {
	if storageRef := strings.TrimSpace(instance.StorageRef); storageRef != "" {
		return storageRef
	}
	if flowPath := strings.TrimSpace(asString(instance.Metadata["flow_path"])); flowPath != "" {
		return flowPath
	}
	if storageRef := strings.TrimSpace(asString(instance.Metadata["storage_ref"])); storageRef != "" {
		return storageRef
	}
	return strings.TrimSpace(instance.InstanceID)
}

func workflowInstanceLogicalID(fallback string, metadata map[string]any) string {
	if logicalID := strings.TrimSpace(asString(metadata["instance_id"])); logicalID != "" {
		return logicalID
	}
	if flowPath := strings.TrimSpace(asString(metadata["flow_path"])); flowPath != "" {
		return strings.TrimSpace(path.Base(flowPath))
	}
	if storageRef := strings.TrimSpace(asString(metadata["storage_ref"])); storageRef != "" {
		if strings.Contains(storageRef, "/") {
			return strings.TrimSpace(path.Base(storageRef))
		}
		return storageRef
	}
	return strings.TrimSpace(fallback)
}

func workflowInstanceLookupKeys(ref string) []string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	keys := make([]string, 0, 2)
	if parsed, err := uuid.Parse(ref); err == nil {
		keys = append(keys, parsed.String())
	}
	if rowID := workflowInstanceRowID(ref); rowID != "" && !containsString(keys, rowID) {
		keys = append(keys, rowID)
	}
	return keys
}

func workflowInstanceRowID(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if !strings.Contains(ref, "/") {
		if parsed, err := uuid.Parse(ref); err == nil {
			return parsed.String()
		}
	}
	return uuid.NewSHA1(workflowInstancePathNamespace, []byte(ref)).String()
}

func containsString(items []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}

type pqStringArray []string

func (a pqStringArray) Value() (driver.Value, error) {
	return pq.Array([]string(a)).Value()
}

func jsonOrDefault(raw []byte, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
