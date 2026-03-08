package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

type WorkflowInstance struct {
	InstanceID        string
	WorkflowName      string
	WorkflowVersion   string
	CurrentStage      string
	EnteredStageAt    time.Time
	TransitionHistory []WorkflowTransitionRecord
	AccumulatorState  map[string]any
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
	CreatedAt time.Time `json:"created_at"`
	FiresAt   time.Time `json:"fires_at"`
	Cancelled bool      `json:"cancelled,omitempty"`
}

type WorkflowInstanceStore struct {
	db *sql.DB
}

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
	ctx = withoutSQLTxContext(ctx)
	var (
		out               WorkflowInstance
		transitionHistory []byte
		accumulatorState  []byte
		timerState        []byte
		metadata          []byte
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT
			instance_id::text,
			workflow_name,
			workflow_version,
			current_stage,
			entered_stage_at,
			COALESCE(transition_history, '[]'::jsonb),
			COALESCE(accumulator_state, '{}'::jsonb),
			COALESCE(timer_state, '[]'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			created_at,
			updated_at
		FROM workflow_instances
		WHERE instance_id = $1::uuid
	`, instanceID).Scan(
		&out.InstanceID,
		&out.WorkflowName,
		&out.WorkflowVersion,
		&out.CurrentStage,
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
	if err := json.Unmarshal(accumulatorState, &out.AccumulatorState); err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(timerState, &out.TimerState); err != nil {
		return WorkflowInstance{}, false, err
	}
	if err := json.Unmarshal(metadata, &out.Metadata); err != nil {
		return WorkflowInstance{}, false, err
	}
	if out.AccumulatorState == nil {
		out.AccumulatorState = map[string]any{}
	}
	if out.Metadata == nil {
		out.Metadata = map[string]any{}
	}
	return out, true, nil
}

func (s *WorkflowInstanceStore) Upsert(ctx context.Context, instance WorkflowInstance) error {
	if s == nil || s.db == nil {
		return nil
	}
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	instance.WorkflowName = strings.TrimSpace(instance.WorkflowName)
	instance.WorkflowVersion = strings.TrimSpace(instance.WorkflowVersion)
	instance.CurrentStage = strings.TrimSpace(instance.CurrentStage)
	if instance.InstanceID == "" || instance.WorkflowName == "" || instance.WorkflowVersion == "" || instance.CurrentStage == "" {
		return nil
	}
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = time.Now().UTC()
	}
	transitionHistory, err := json.Marshal(instance.TransitionHistory)
	if err != nil {
		return err
	}
	accumulatorState, err := json.Marshal(instance.AccumulatorState)
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
	ctx = withoutSQLTxContext(ctx)
	_, err = dbExecContext(ctx, s.db, `
		INSERT INTO workflow_instances (
			instance_id,
			workflow_name,
			workflow_version,
			current_stage,
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
			current_stage = EXCLUDED.current_stage,
			entered_stage_at = EXCLUDED.entered_stage_at,
			transition_history = EXCLUDED.transition_history,
			accumulator_state = EXCLUDED.accumulator_state,
			timer_state = EXCLUDED.timer_state,
			metadata = EXCLUDED.metadata,
			updated_at = now()
	`,
		instance.InstanceID,
		instance.WorkflowName,
		instance.WorkflowVersion,
		instance.CurrentStage,
		instance.EnteredStageAt,
		jsonOrDefault(transitionHistory, "[]"),
		jsonOrDefault(accumulatorState, "{}"),
		jsonOrDefault(timerState, "[]"),
		jsonOrDefault(metadata, "{}"),
	)
	return err
}

func (s *WorkflowInstanceStore) Delete(ctx context.Context, instanceID string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil {
		return nil
	}
	ctx = withoutSQLTxContext(ctx)
	_, err := dbExecContext(ctx, s.db, `DELETE FROM workflow_instances WHERE instance_id = $1::uuid`, instanceID)
	return err
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
