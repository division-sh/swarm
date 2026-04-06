package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimemutationlog "swarm/internal/runtime/mutationlog"
)

type SchedulePersistence interface {
	UpsertSchedule(ctx context.Context, sc Schedule) error
	CancelScheduleExact(ctx context.Context, sc Schedule) error
	LoadActiveSchedules(ctx context.Context) ([]Schedule, error)
	MarkScheduleFiredExact(ctx context.Context, sc Schedule) error
	ClaimSchedule(ctx context.Context, sc Schedule) (bool, error)
	ReleaseSchedule(ctx context.Context, sc Schedule) error
	ReleaseScheduleClaims(ctx context.Context) error
}

type WorkflowInstance struct {
	InstanceID        string
	SubjectID         string
	StorageRef        string
	WorkflowName      string
	WorkflowVersion   string
	CurrentState      string
	Config            map[string]any
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
	keys := workflowInstanceLookupKeys(instanceID)
	if len(keys) == 0 {
		return WorkflowInstance{}, false, nil
	}
	return s.loadSpec(ctx, keys, false)
}

func (s *WorkflowInstanceStore) List(ctx context.Context) ([]WorkflowInstance, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	return s.listSpec(ctx)
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
	instance.SubjectID = strings.TrimSpace(firstNonEmptyString(instance.SubjectID, asString(instance.Metadata["subject_id"])))
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
	return s.upsertSpec(ctx, rowID, storageRef, instance)
}

func (s *WorkflowInstanceStore) Mutate(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil || fn == nil {
		return nil
	}
	tx, ownedTx, err := workflowInstanceStoreTx(ctx, s.db)
	if err != nil {
		return err
	}
	committed := !ownedTx
	if ownedTx {
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
	}
	ctx = withSQLTxContext(ctx, tx)
	if err := lockWorkflowInstanceMutation(ctx, tx, instanceID); err != nil {
		return err
	}
	instance, ok, err := s.loadSpec(ctx, workflowInstanceLookupKeys(instanceID), true)
	if err != nil {
		return err
	}
	if !ok {
		instance = WorkflowInstance{InstanceID: instanceID}
	}
	fn(&instance)
	if err := s.Upsert(ctx, instance); err != nil {
		return err
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
	}
	return nil
}

func (s *WorkflowInstanceStore) MarkTerminated(ctx context.Context, storageRef string, terminatedAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	storageRef = strings.TrimSpace(storageRef)
	if storageRef == "" {
		return fmt.Errorf("workflow instance storage_ref is required")
	}
	if terminatedAt.IsZero() {
		terminatedAt = time.Now().UTC()
	}
	tx, ownedTx, err := workflowInstanceStoreTx(ctx, s.db)
	if err != nil {
		return err
	}
	committed := !ownedTx
	if ownedTx {
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated',
		    terminated_at = COALESCE(terminated_at, $2)
		WHERE instance_id = $1
	`, storageRef, terminatedAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("flow instance not found: %s", storageRef)
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
	}
	return nil
}

func (s *WorkflowInstanceStore) Delete(context.Context, string) error {
	return fmt.Errorf("workflow instance deletion is unsupported: entity_state writes must stay on the mutation-logged upsert path")
}

func (s *WorkflowInstanceStore) loadSpec(ctx context.Context, keys []string, forUpdate bool) (WorkflowInstance, bool, error) {
	var (
		item         WorkflowInstance
		gatesRaw     []byte
		fieldsRaw    []byte
		configRaw    []byte
		accRaw       []byte
		subjectID    sql.NullString
		flowInstance string
		entityType   string
		slug         sql.NullString
		name         sql.NullString
	)
	query := `
		SELECT
			es.entity_id::text,
			es.subject_id::text,
			COALESCE(fi.flow_template, ''),
			COALESCE(fi.config->>'workflow_version', ''),
			es.current_state,
			es.entered_state_at,
			COALESCE(es.gates, '{}'::jsonb),
			COALESCE(es.fields, '{}'::jsonb),
			COALESCE(es.accumulator, '{}'::jsonb),
			COALESCE(fi.config, '{}'::jsonb),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			es.slug,
			es.name,
			es.created_at,
			es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.entity_id = ANY($1::uuid[])
		ORDER BY es.created_at DESC, es.entity_id DESC
		LIMIT 1
	`
	if forUpdate {
		query += ` FOR UPDATE OF es`
	}
	err := dbQueryRowContext(ctx, s.db, query, pqStringArray(keys)).Scan(
		&item.InstanceID,
		&subjectID,
		&item.WorkflowName,
		&item.WorkflowVersion,
		&item.CurrentState,
		&item.EnteredStageAt,
		&gatesRaw,
		&fieldsRaw,
		&accRaw,
		&configRaw,
		&flowInstance,
		&entityType,
		&slug,
		&name,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return WorkflowInstance{}, false, nil
	}
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	item.SubjectID = strings.TrimSpace(subjectID.String)
	if err := json.Unmarshal(accRaw, &item.StateBuckets); err != nil {
		return WorkflowInstance{}, false, err
	}
	item.Metadata = workflowInstanceMetadataFromSpec(fieldsRaw, gatesRaw, configRaw, slug.String, name.String, entityType, flowInstance, item.SubjectID)
	item.StorageRef = strings.TrimSpace(flowInstance)
	item.InstanceID = workflowInstanceLogicalID(item.InstanceID, item.Metadata)
	timers, err := s.loadWorkflowTimersSpec(ctx, keys[0])
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	item.TimerState = timers
	item.TransitionHistory = workflowInstanceTransitionHistory(item.Metadata)
	if item.StateBuckets == nil {
		item.StateBuckets = map[string]any{}
	}
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}
	return item, true, nil
}

func (s *WorkflowInstanceStore) listSpec(ctx context.Context) ([]WorkflowInstance, error) {
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			es.entity_id::text,
			es.subject_id::text,
			COALESCE(fi.flow_template, ''),
			COALESCE(fi.config->>'workflow_version', ''),
			es.current_state,
			es.entered_state_at,
			COALESCE(es.gates, '{}'::jsonb),
			COALESCE(es.fields, '{}'::jsonb),
			COALESCE(es.accumulator, '{}'::jsonb),
			COALESCE(fi.config, '{}'::jsonb),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			es.slug,
			es.name,
			es.created_at,
			es.updated_at
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		ORDER BY es.created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkflowInstance, 0, 32)
	for rows.Next() {
		var (
			item         WorkflowInstance
			gatesRaw     []byte
			fieldsRaw    []byte
			configRaw    []byte
			accRaw       []byte
			subjectID    sql.NullString
			flowInstance string
			entityType   string
			slug         sql.NullString
			name         sql.NullString
		)
		if err := rows.Scan(
			&item.InstanceID,
			&subjectID,
			&item.WorkflowName,
			&item.WorkflowVersion,
			&item.CurrentState,
			&item.EnteredStageAt,
			&gatesRaw,
			&fieldsRaw,
			&accRaw,
			&configRaw,
			&flowInstance,
			&entityType,
			&slug,
			&name,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.SubjectID = strings.TrimSpace(subjectID.String)
		if err := json.Unmarshal(accRaw, &item.StateBuckets); err != nil {
			return nil, err
		}
		item.Metadata = workflowInstanceMetadataFromSpec(fieldsRaw, gatesRaw, configRaw, slug.String, name.String, entityType, flowInstance, item.SubjectID)
		item.StorageRef = strings.TrimSpace(flowInstance)
		item.InstanceID = workflowInstanceLogicalID(item.InstanceID, item.Metadata)
		item.TransitionHistory = workflowInstanceTransitionHistory(item.Metadata)
		timers, err := s.loadWorkflowTimersSpec(ctx, runtimeflowidentity.EntityID(item.StorageRef))
		if err != nil {
			return nil, err
		}
		item.TimerState = timers
		if item.StateBuckets == nil {
			item.StateBuckets = map[string]any{}
		}
		if item.Metadata == nil {
			item.Metadata = map[string]any{}
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *WorkflowInstanceStore) upsertSpec(ctx context.Context, rowID, storageRef string, instance WorkflowInstance) error {
	tx, ownedTx, err := workflowInstanceStoreTx(ctx, s.db)
	if err != nil {
		return err
	}
	committed := !ownedTx
	if ownedTx {
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
	}
	previous, err := loadTrackedEntityStateProjection(ctx, tx, rowID)
	if err != nil {
		return err
	}

	metadata := cloneStringAnyMap(instance.Metadata)
	config := workflowInstanceConfigPayload(instance, storageRef)
	fields, gates, slug, name, entityType := workflowInstanceEntityProjection(metadata)
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	gatesJSON, err := json.Marshal(gates)
	if err != nil {
		return err
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}
	accumulatorState, err := json.Marshal(instance.StateBuckets)
	if err != nil {
		return err
	}
	mode := workflowInstanceMode(storageRef)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instances (
			instance_id, flow_template, mode, config, status, created_at
		)
		VALUES (
			$1, $2, $3, $4::jsonb, 'active', now()
		)
		ON CONFLICT (instance_id) DO UPDATE SET
			flow_template = EXCLUDED.flow_template,
			mode = EXCLUDED.mode,
			config = EXCLUDED.config,
			status = CASE WHEN flow_instances.status = 'terminated' THEN flow_instances.status ELSE 'active' END
	`, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}")); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, subject_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, $3, $4, NULLIF($5,''), NULLIF($6,''),
			$7, $8::jsonb, $9::jsonb, $10::jsonb, 1,
			$11, now(), now()
		)
		ON CONFLICT (entity_id) DO UPDATE SET
			subject_id = COALESCE(entity_state.subject_id, EXCLUDED.subject_id),
			flow_instance = EXCLUDED.flow_instance,
			entity_type = EXCLUDED.entity_type,
			slug = EXCLUDED.slug,
			name = EXCLUDED.name,
			current_state = EXCLUDED.current_state,
			gates = EXCLUDED.gates,
			fields = EXCLUDED.fields,
			accumulator = EXCLUDED.accumulator,
			revision = entity_state.revision + 1,
			entered_state_at = EXCLUDED.entered_state_at,
			updated_at = now()
	`, rowID, instance.SubjectID, storageRef, entityType, slug, name, instance.CurrentState,
		jsonOrDefault(gatesJSON, "{}"),
		jsonOrDefault(fieldsJSON, "{}"),
		jsonOrDefault(accumulatorState, "{}"),
		instance.EnteredStageAt,
	); err != nil {
		return err
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, rowID, previous, runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(instance.CurrentState),
		Fields:       fields,
		Gates:        gates,
		Accumulator:  cloneStringAnyMap(instance.StateBuckets),
	}, runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "workflow_instance_store",
		HandlerStep: "upsert",
	}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM timers
		WHERE entity_id = $1::uuid
		  AND flow_instance = $2
		  AND owner_agent IS NULL
	`, rowID, storageRef); err != nil {
		return err
	}
	for _, timer := range instance.TimerState {
		payloadJSON, err := json.Marshal(map[string]any{
			"started_by": strings.TrimSpace(timer.StartedBy),
			"timer_id":   strings.TrimSpace(timer.TimerID),
		})
		if err != nil {
			return err
		}
		status := "active"
		if timer.Cancelled {
			status = "cancelled"
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, owner_node, task_type, status, created_at
			)
			VALUES (
				$1::uuid, $2, $3::uuid, $4, $5, $6::jsonb,
				$7, $8, NULL, $9, $10, $11
			)
		`, workflowInstanceTimerRowID(strings.TrimSpace(timer.TimerID), rowID), strings.TrimSpace(timer.TimerID), rowID, storageRef,
			strings.TrimSpace(timer.EventType), jsonOrDefault(payloadJSON, "{}"), timer.FiresAt, timer.Recurring,
			workflowInstanceTimerTaskType(timer), status, workflowTimeOrNow(timer.CreatedAt),
		); err != nil {
			return err
		}
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
	}
	return nil
}

func workflowInstanceStoreTx(ctx context.Context, db *sql.DB) (*sql.Tx, bool, error) {
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		return tx, false, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	return tx, true, nil
}

func lockWorkflowInstanceMutation(ctx context.Context, tx *sql.Tx, instanceID string) error {
	if tx == nil {
		return nil
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "workflow-instance:"+instanceID); err != nil {
		return fmt.Errorf("lock workflow instance mutation: %w", err)
	}
	return nil
}

func loadTrackedEntityStateProjection(ctx context.Context, tx *sql.Tx, entityID string) (runtimemutationlog.EntityStateProjection, error) {
	if tx == nil || strings.TrimSpace(entityID) == "" {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	var (
		currentState sql.NullString
		fieldsRaw    []byte
		gatesRaw     []byte
		accRaw       []byte
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			current_state,
			COALESCE(fields, '{}'::jsonb),
			COALESCE(gates, '{}'::jsonb),
			COALESCE(accumulator, '{}'::jsonb)
		FROM entity_state
		WHERE entity_id = $1::uuid
		FOR UPDATE
	`, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw)
	if err == sql.ErrNoRows {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState.String),
		Fields:       jsonObjectMap(fieldsRaw),
		Gates:        jsonObjectMap(gatesRaw),
		Accumulator:  jsonObjectMap(accRaw),
	}, nil
}

func jsonObjectMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *WorkflowInstanceStore) loadWorkflowTimersSpec(ctx context.Context, entityID string) ([]WorkflowTimerState, error) {
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			timer_name,
			fire_event,
			created_at,
			fire_at,
			COALESCE(fire_payload->>'started_by', ''),
			recurring,
			status = 'cancelled'
		FROM timers
		WHERE entity_id = $1::uuid
		  AND owner_agent IS NULL
		ORDER BY created_at ASC, timer_name ASC
	`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkflowTimerState, 0, 4)
	for rows.Next() {
		var timer WorkflowTimerState
		if err := rows.Scan(
			&timer.TimerID,
			&timer.EventType,
			&timer.CreatedAt,
			&timer.FiresAt,
			&timer.StartedBy,
			&timer.Recurring,
			&timer.Cancelled,
		); err != nil {
			return nil, err
		}
		out = append(out, timer)
	}
	return out, rows.Err()
}

func workflowInstanceMetadataFromSpec(fieldsRaw, gatesRaw, configRaw []byte, slug, name, entityType, flowInstance, subjectID string) map[string]any {
	metadata := map[string]any{}
	var fields map[string]any
	if len(fieldsRaw) > 0 {
		_ = json.Unmarshal(fieldsRaw, &fields)
	}
	for key, value := range fields {
		metadata[key] = value
	}
	if strings.TrimSpace(slug) != "" {
		metadata["slug"] = strings.TrimSpace(slug)
	}
	if strings.TrimSpace(name) != "" {
		metadata["name"] = strings.TrimSpace(name)
	}
	if strings.TrimSpace(entityType) != "" {
		metadata["entity_type"] = strings.TrimSpace(entityType)
	}
	if strings.TrimSpace(flowInstance) != "" {
		metadata["storage_ref"] = strings.TrimSpace(flowInstance)
	}
	if strings.TrimSpace(subjectID) != "" {
		metadata["subject_id"] = strings.TrimSpace(subjectID)
	}
	var gates map[string]any
	if len(gatesRaw) > 0 && json.Unmarshal(gatesRaw, &gates) == nil && len(gates) > 0 {
		metadata["gates"] = gates
	}
	var config map[string]any
	if len(configRaw) > 0 && json.Unmarshal(configRaw, &config) == nil {
		for _, key := range []string{"instance_id", "flow_path", "storage_ref", "instance_kind", "template_version", "last_source_event", "status", "parent_entity_id"} {
			if value, ok := config[key]; ok {
				metadata[key] = value
			}
		}
		if value, ok := config["transition_history"]; ok {
			metadata["transition_history"] = value
		}
	}
	return metadata
}

func workflowInstanceTransitionHistory(metadata map[string]any) []WorkflowTransitionRecord {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata["transition_history"]
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var out []WorkflowTransitionRecord
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil
	}
	return out
}

func workflowInstanceConfigPayload(instance WorkflowInstance, storageRef string) map[string]any {
	config := map[string]any{
		"workflow_version": strings.TrimSpace(instance.WorkflowVersion),
		"instance_id":      strings.TrimSpace(instance.InstanceID),
		"storage_ref":      strings.TrimSpace(storageRef),
	}
	for key, value := range instance.Config {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		config[key] = value
	}
	metadata := cloneStringAnyMap(instance.Metadata)
	for _, key := range []string{"flow_path", "instance_kind", "template_version", "status", "last_source_event", "parent_entity_id"} {
		if value, ok := metadata[key]; ok {
			config[key] = value
		}
	}
	if len(instance.TransitionHistory) > 0 {
		config["transition_history"] = instance.TransitionHistory
	}
	return config
}

func workflowInstanceEntityProjection(metadata map[string]any) (map[string]any, map[string]any, string, string, string) {
	metadata = cloneStringAnyMap(metadata)
	gates := payloadMap(metadata["gates"])
	delete(metadata, "gates")
	slug := strings.TrimSpace(asString(metadata["slug"]))
	name := strings.TrimSpace(asString(metadata["name"]))
	entityType := strings.TrimSpace(asString(metadata["entity_type"]))
	for _, key := range []string{"slug", "name", "entity_type", "subject_id", "parent_entity_id", "instance_id", "storage_ref", "flow_path", "instance_kind", "template_version", "workflow_version", "transition_history"} {
		delete(metadata, key)
	}
	if entityType == "" {
		entityType = "default"
	}
	return metadata, gates, slug, name, entityType
}

func workflowInstanceMode(storageRef string) string {
	if strings.Contains(strings.TrimSpace(storageRef), "/") {
		return "template"
	}
	return "static"
}

func workflowInstanceTimerRowID(timerID, entityID string) string {
	return uuid.NewSHA1(workflowInstancePathNamespace, []byte(strings.TrimSpace(entityID)+":"+strings.TrimSpace(timerID))).String()
}

func workflowInstanceTimerTaskType(timer WorkflowTimerState) string {
	if timer.Recurring {
		return "scheduled_task"
	}
	return "timer"
}

func workflowTimeOrNow(v time.Time) time.Time {
	if v.IsZero() {
		return time.Now().UTC()
	}
	return v.UTC()
}

func shouldFallbackLegacyWorkflowInstanceSchema(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	for _, token := range []string{
		`relation "entity_state" does not exist`,
		`relation "flow_instances" does not exist`,
		`relation "timers" does not exist`,
		`column "flow_instance" does not exist`,
		`column "entered_state_at" does not exist`,
		`column "accumulator" does not exist`,
		`column "config" does not exist`,
	} {
		if strings.Contains(msg, token) {
			return true
		}
	}
	return false
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
		return runtimeflowidentity.LogicalInstanceID(flowPath)
	}
	if storageRef := strings.TrimSpace(asString(metadata["storage_ref"])); storageRef != "" {
		if strings.Contains(storageRef, "/") {
			return runtimeflowidentity.LogicalInstanceID(storageRef)
		}
		return storageRef
	}
	return strings.TrimSpace(fallback)
}

func workflowInstanceLookupKeys(ref string) []string {
	return runtimeflowidentity.LookupKeys(ref)
}

func workflowInstanceRowID(ref string) string {
	return runtimeflowidentity.EntityID(ref)
}

func FlowInstanceEntityID(ref string) string {
	return runtimeflowidentity.EntityID(ref)
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
