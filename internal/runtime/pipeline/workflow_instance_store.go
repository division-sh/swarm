package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/lib/pq"
)

type SchedulePersistence interface {
	UpsertSchedule(ctx context.Context, sc Schedule) error
	CancelScheduleExact(ctx context.Context, sc Schedule) error
	CancelScheduleExactTerminal(ctx context.Context, sc Schedule) error
	LoadActiveSchedules(ctx context.Context) ([]Schedule, error)
	MarkScheduleFiredExact(ctx context.Context, sc Schedule) error
	CompleteScheduleFireExact(ctx context.Context, sc Schedule) error
	ClaimSchedule(ctx context.Context, sc Schedule) (bool, error)
	ReleaseSchedule(ctx context.Context, sc Schedule) error
	ReleaseScheduleClaims(ctx context.Context) error
}

type WorkflowInstance struct {
	InstanceID        string
	StorageRef        string
	WorkflowName      string
	WorkflowVersion   string
	Status            string
	TerminatedAt      time.Time
	CurrentState      string
	Config            map[string]any
	EnteredStageAt    time.Time
	TransitionHistory []WorkflowTransitionRecord
	StateBuckets      map[string]any
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

type workflowInstancePersistedProjection struct {
	Fields      map[string]any
	Gates       map[string]bool
	Accumulator map[string]any
	Config      map[string]any
	Control     workflowInstancePersistedControl
}

type workflowInstancePersistedControl struct {
	StorageRef         string
	Slug               string
	Name               string
	EntityType         string
	InstanceID         string
	FlowPath           string
	InstanceKind       string
	TemplateVersion    string
	LastSourceEvent    string
	Status             string
	ParentFlowID       string
	ParentFlowInstance string
	ParentEntityID     string
	TransitionHistory  []WorkflowTransitionRecord
}

type WorkflowInstanceStore struct {
	db              *sql.DB
	dialect         workflowStoreDialect
	runtimeMutation RuntimeMutationRunner
	deliveryStore   runtimedelivery.Store
	decisionCards   decisioncard.Store
	gateEvents      workflowGateMutationPublisher
}

type standingGenerationRebindContextKey struct{}

// WithStandingGenerationRebind authorizes a reset generation to reuse its
// stable declaration-owned flow-instance identity with fresh run-scoped state.
func WithStandingGenerationRebind(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, standingGenerationRebindContextKey{}, true)
}

func standingGenerationRebindAllowed(ctx context.Context) bool {
	allowed, _ := ctx.Value(standingGenerationRebindContextKey{}).(bool)
	return allowed
}

type RuntimeMutationRunner interface {
	RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error
}

type WorkflowInstanceFieldSelector struct {
	Field string
	Value any
}

type workflowInstanceFieldSelector = WorkflowInstanceFieldSelector

type workflowStoreDialect string

const (
	workflowStoreDialectPostgres workflowStoreDialect = "postgres"
	workflowStoreDialectSQLite   workflowStoreDialect = "sqlite"
)

var errSQLiteWorkflowInstanceStoreRuntimeMutationRunnerRequired = errors.New("sqlite workflow instance store requires runtime mutation runner")

type workflowCreateEntityInitialValuesContextKey struct{}

type workflowCreateEntityInitialValues struct {
	Fields map[string]any
}

func NewWorkflowInstanceStore(db *sql.DB) *WorkflowInstanceStore {
	return &WorkflowInstanceStore{db: db, dialect: workflowStoreDialectPostgres}
}

func NewSQLiteWorkflowInstanceStore(db *sql.DB) *WorkflowInstanceStore {
	return &WorkflowInstanceStore{db: db, dialect: workflowStoreDialectSQLite}
}

func NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db *sql.DB, runner RuntimeMutationRunner) *WorkflowInstanceStore {
	return &WorkflowInstanceStore{db: db, dialect: workflowStoreDialectSQLite, runtimeMutation: runner}
}

func (s *WorkflowInstanceStore) ConfigureRuntimeMutationRunner(runner RuntimeMutationRunner) {
	if s != nil {
		s.runtimeMutation = runner
	}
}

func (s *WorkflowInstanceStore) ConfigureDeliveryLifecycleStore(store runtimedelivery.Store) {
	if s != nil {
		s.deliveryStore = store
	}
}

func (s *WorkflowInstanceStore) DeliveryLifecycleStore() runtimedelivery.Store {
	if s == nil {
		return nil
	}
	return s.deliveryStore
}

func (s *WorkflowInstanceStore) ConfigureDecisionCardLifecycle(cards decisioncard.Store, publishers ...workflowGateMutationPublisher) {
	if s != nil {
		s.decisionCards = cards
		if len(publishers) > 0 {
			s.gateEvents = publishers[0]
		}
	}
}

func withWorkflowCreateEntityInitialValues(ctx context.Context, fields map[string]any) context.Context {
	if len(fields) == 0 {
		return ctx
	}
	return context.WithValue(ctx, workflowCreateEntityInitialValuesContextKey{}, workflowCreateEntityInitialValues{
		Fields: cloneStringAnyMap(fields),
	})
}

func workflowCreateEntityInitialValuesFromContext(ctx context.Context) (workflowCreateEntityInitialValues, bool) {
	if ctx == nil {
		return workflowCreateEntityInitialValues{}, false
	}
	raw := ctx.Value(workflowCreateEntityInitialValuesContextKey{})
	info, ok := raw.(workflowCreateEntityInitialValues)
	if !ok || len(info.Fields) == 0 {
		return workflowCreateEntityInitialValues{}, false
	}
	return workflowCreateEntityInitialValues{Fields: cloneStringAnyMap(info.Fields)}, true
}

func (s *WorkflowInstanceStore) Enabled() bool {
	return s != nil && s.db != nil
}

func (s *WorkflowInstanceStore) Load(ctx context.Context, instanceID string) (WorkflowInstance, bool, error) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil {
		return WorkflowInstance{}, false, nil
	}
	if s.isSQLite() {
		return s.loadSQLite(ctx, instanceID)
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
	if s.isSQLite() {
		return s.listSQLite(ctx)
	}
	return s.listSpec(ctx)
}

func (s *WorkflowInstanceStore) selectActiveByFields(ctx context.Context, scopeKey string, selectors []workflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	return s.SelectActiveByFields(ctx, scopeKey, selectors, excludedStates)
}

func (s *WorkflowInstanceStore) SelectActiveByFields(ctx context.Context, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if s.isSQLite() {
		return s.selectActiveByFieldsSQLite(ctx, scopeKey, selectors, excludedStates)
	}
	return s.selectActiveByFieldsSpec(ctx, scopeKey, selectors, excludedStates)
}

func (s *WorkflowInstanceStore) requireActiveWorkflowRun(ctx context.Context, tx *sql.Tx) (string, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return "", err
	}
	dialect := storerunlifecycle.DialectPostgres
	if s.isSQLite() {
		dialect = storerunlifecycle.DialectSQLite
	}
	if err := storerunlifecycle.RequireActive(ctx, tx, runID, dialect); err != nil {
		return "", err
	}
	return runID, nil
}

func (s *WorkflowInstanceStore) Upsert(ctx context.Context, instance WorkflowInstance) error {
	if s == nil || s.db == nil {
		return nil
	}
	if s.isSQLite() {
		return s.upsertSQLite(ctx, instance)
	}
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		return s.upsertSpec(txctx, identity.RowID(), identity.StorageRef, instance)
	})
}

func (s *WorkflowInstanceStore) Create(ctx context.Context, instance WorkflowInstance) error {
	if s == nil || s.db == nil {
		return nil
	}
	if s.isSQLite() {
		return s.createSQLite(ctx, instance)
	}
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		return s.createSpec(txctx, identity.RowID(), identity.StorageRef, instance)
	})
}

func normalizeWorkflowInstanceForPersistence(instance WorkflowInstance) (WorkflowInstance, runtimeflowidentity.Persisted, bool, error) {
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	instance.WorkflowName = strings.TrimSpace(instance.WorkflowName)
	instance.WorkflowVersion = strings.TrimSpace(instance.WorkflowVersion)
	instance.CurrentState = strings.TrimSpace(instance.CurrentState)
	if instance.InstanceID == "" || instance.WorkflowName == "" || instance.WorkflowVersion == "" || instance.CurrentState == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, fmt.Errorf(
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
	identity, err := workflowInstancePersistedIdentity(nil, instance)
	if err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, err
	}
	if strings.TrimSpace(identity.StorageRef) == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, nil
	}
	if strings.TrimSpace(identity.RowID()) == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, nil
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	instance.StorageRef = identity.StorageRef
	instance.InstanceID = identity.InstanceID
	instance.Metadata["storage_ref"] = identity.StorageRef
	instance.Metadata["instance_id"] = identity.InstanceID
	if identity.HasStoredPath && identity.InstancePath != "" {
		instance.Metadata["flow_path"] = identity.InstancePath
	} else {
		delete(instance.Metadata, "flow_path")
	}
	if identity.ParentRoute.FlowID != "" {
		instance.Metadata["parent_flow_id"] = identity.ParentRoute.FlowID
	}
	if identity.ParentRoute.FlowInstance != "" {
		instance.Metadata["parent_flow_instance"] = identity.ParentRoute.FlowInstance
	}
	return instance, identity, true, nil
}

func (s *WorkflowInstanceStore) Mutate(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error {
	if fn == nil {
		return nil
	}
	return s.MutateE(ctx, instanceID, func(instance *WorkflowInstance) error {
		fn(instance)
		return nil
	})
}

func (s *WorkflowInstanceStore) MutateE(ctx context.Context, instanceID string, fn func(*WorkflowInstance) error) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || s == nil || s.db == nil || fn == nil {
		return nil
	}
	if s.isSQLite() {
		return s.mutateSQLiteE(ctx, instanceID, fn)
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := s.requireActiveWorkflowRun(txctx, tx); err != nil {
			return err
		}
		if err := lockWorkflowInstanceMutation(txctx, tx, instanceID); err != nil {
			return err
		}
		instance, ok, err := s.loadSpec(txctx, workflowInstanceLookupKeys(instanceID), true)
		if err != nil {
			return err
		}
		if !ok {
			instance = WorkflowInstance{InstanceID: instanceID}
		}
		if err := fn(&instance); err != nil {
			return err
		}
		return s.Upsert(txctx, instance)
	})
}

func (s *WorkflowInstanceStore) MarkTerminated(ctx context.Context, storageRef string, terminatedAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	if s.decisionCards != nil {
		return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			if _, err := s.requireActiveWorkflowRun(txctx, tx); err != nil {
				return err
			}
			if err := s.MutateE(txctx, storageRef, func(instance *WorkflowInstance) error {
				return s.supersedeWorkflowInstanceGates(txctx, instance, "flow_terminated", terminatedAt)
			}); err != nil {
				return err
			}
			if s.isSQLite() {
				return s.markTerminatedSQLiteTx(txctx, tx, storageRef, terminatedAt)
			}
			return markWorkflowInstanceTerminatedSpecTx(txctx, tx, storageRef, terminatedAt)
		})
	}
	if s.isSQLite() {
		return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			if _, err := s.requireActiveWorkflowRun(txctx, tx); err != nil {
				return err
			}
			return s.markTerminatedSQLiteTx(txctx, tx, storageRef, terminatedAt)
		})
	}
	storageRef = strings.TrimSpace(storageRef)
	if storageRef == "" {
		return fmt.Errorf("workflow instance storage_ref is required")
	}
	if terminatedAt.IsZero() {
		terminatedAt = time.Now().UTC()
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if _, err := s.requireActiveWorkflowRun(txctx, tx); err != nil {
			return err
		}
		result, err := tx.ExecContext(txctx, `
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
		return nil
	})
}

func markWorkflowInstanceTerminatedSpecTx(ctx context.Context, tx *sql.Tx, storageRef string, terminatedAt time.Time) error {
	storageRef = strings.TrimSpace(storageRef)
	if storageRef == "" {
		return fmt.Errorf("workflow instance storage_ref is required")
	}
	if terminatedAt.IsZero() {
		terminatedAt = time.Now().UTC()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE flow_instances
		SET status = 'terminated', terminated_at = COALESCE(terminated_at, $2)
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
	return nil
}

func (s *WorkflowInstanceStore) Delete(context.Context, string) error {
	return fmt.Errorf("workflow instance deletion is unsupported: entity_state writes must stay on the mutation-logged upsert path")
}

func (s *WorkflowInstanceStore) isSQLite() bool {
	return s != nil && s.dialect == workflowStoreDialectSQLite
}

func (s *WorkflowInstanceStore) RunPipelineMutation(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
		return fn(txctx)
	})
}

func (s *WorkflowInstanceStore) runInPipelineTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		if runtimeauthoractivity.InMutation(ctx, tx) {
			return fn(ctx, tx)
		}
		if !runtimeauthoractivity.FinalizedMutation(ctx, tx) {
			return fmt.Errorf("pipeline mutation entered from a raw transaction without author activity ownership")
		}
		ctx = WithoutPipelineSQLTxContext(ctx)
	}
	if s.runtimeMutation != nil {
		return s.runtimeMutation.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
			tx, ok := sqlTxFromContext(txctx)
			if !ok || tx == nil {
				return fmt.Errorf("selected runtime mutation did not provide pipeline transaction")
			}
			return fn(txctx, tx)
		})
	}
	if s.isSQLite() {
		return errSQLiteWorkflowInstanceStoreRuntimeMutationRunnerRequired
	}
	return s.runInPipelineTransactionOnce(ctx, fn)
}

func (s *WorkflowInstanceStore) runInPipelineTransactionOnce(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if s == nil || s.db == nil {
		return fn(ctx, nil)
	}
	conn, borrowed := PipelineSQLConnFromContext(ctx)
	if !borrowed {
		var err error
		conn, err = s.db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	postCommit := make([]OwnerAction, 0, 4)
	rollbackActions := make([]OwnerAction, 0, 4)
	txctx := WithPipelineSQLConnContext(ctx, conn)
	txctx = WithPipelineSQLTxContext(txctx, tx)
	txctx = withPipelinePostCommitActions(txctx, &postCommit)
	txctx = withPipelineRollbackActions(txctx, &rollbackActions)
	storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(storyctx, tx); err != nil {
		_ = tx.Rollback()
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := CapturePipelineRunForkRevisionChanges(storyctx, tx); err != nil {
		_ = tx.Rollback()
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := runtimeauthoractivity.Finalize(storyctx); err != nil {
		_ = tx.Rollback()
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := tx.Commit(); err != nil {
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	flushPipelinePostCommitActions(postCommit)
	return nil
}

func (s *WorkflowInstanceStore) QueryEntityCount(ctx context.Context, runID string, source semanticview.Source, contract entityruntime.Contract, predicate workflowEntityQueryPredicate) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if s.isSQLite() {
		return s.queryEntityStateCountSQLite(ctx, runID, source, contract, predicate)
	}
	return queryEntityStateCount(runID, s.db, source, contract, predicate)
}

func (s *WorkflowInstanceStore) loadSpec(ctx context.Context, keys []string, forUpdate bool) (WorkflowInstance, bool, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	var (
		item         WorkflowInstance
		gatesRaw     []byte
		fieldsRaw    []byte
		configRaw    []byte
		accRaw       []byte
		flowInstance string
		entityType   string
		slug         sql.NullString
		name         sql.NullString
		status       sql.NullString
		terminatedAt sql.NullTime
	)
	query := `
		SELECT
			es.entity_id::text,
			COALESCE(fi.flow_template, ''),
			COALESCE(fi.config->>'workflow_version', ''),
			COALESCE(fi.status, ''),
			fi.terminated_at,
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
		  AND es.run_id = $2::uuid
		ORDER BY es.created_at DESC, es.entity_id DESC
		LIMIT 1
	`
	if forUpdate {
		query += ` FOR UPDATE OF es`
	}
	err = dbQueryRowContext(ctx, s.db, query, pqStringArray(keys), runID).Scan(
		&item.InstanceID,
		&item.WorkflowName,
		&item.WorkflowVersion,
		&status,
		&terminatedAt,
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
	item.Status = strings.TrimSpace(status.String)
	if terminatedAt.Valid {
		item.TerminatedAt = terminatedAt.Time
	}
	projection, err := decodeWorkflowInstancePersistedProjection(fieldsRaw, gatesRaw, accRaw, configRaw, workflowInstancePersistedControl{
		StorageRef: strings.TrimSpace(flowInstance),
		Slug:       slug.String,
		Name:       name.String,
		EntityType: entityType,
	})
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	item.StateBuckets = projection.Accumulator
	item.Config = projection.Config
	item.Metadata = projection.Metadata()
	persistedIdentity, err := workflowInstancePersistedIdentity(nil, WorkflowInstance{
		InstanceID:   item.InstanceID,
		StorageRef:   projection.Control.StorageRef,
		WorkflowName: item.WorkflowName,
		Metadata:     item.Metadata,
	})
	if err != nil {
		return WorkflowInstance{}, false, err
	}
	item.StorageRef = persistedIdentity.StorageRef
	item.InstanceID = persistedIdentity.InstanceID
	item.TransitionHistory = append([]WorkflowTransitionRecord{}, projection.Control.TransitionHistory...)
	if item.StateBuckets == nil {
		item.StateBuckets = map[string]any{}
	}
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}
	return item, true, nil
}

func (s *WorkflowInstanceStore) listSpec(ctx context.Context) ([]WorkflowInstance, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	return s.querySpec(ctx, runID, `
		WHERE es.run_id = $1::uuid
		ORDER BY es.created_at ASC
	`, runID)
}

func (s *WorkflowInstanceStore) selectActiveByFieldsSpec(ctx context.Context, scopeKey string, selectors []workflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return nil, err
	}
	scopeKey = strings.Trim(strings.TrimSpace(scopeKey), "/")
	if scopeKey == "" {
		return nil, nil
	}
	selectors = normalizeWorkflowInstanceFieldSelectors(selectors)
	if len(selectors) == 0 {
		return nil, nil
	}
	args := []any{runID, scopeKey, scopeKey + "/%"}
	var where strings.Builder
	where.WriteString(`
		WHERE es.run_id = $1::uuid
		  AND EXISTS (
			SELECT 1
			FROM runs run
			WHERE run.run_id = es.run_id
			  AND run.status IN ('running', 'paused')
		  )
		  AND (es.flow_instance = $2 OR es.flow_instance LIKE $3)
		  AND COALESCE(fi.status, 'active') NOT IN ('terminated', 'inactive')
		  AND fi.terminated_at IS NULL
	`)
	terminalStates := normalizeWorkflowInstanceExcludedStates(excludedStates)
	if len(terminalStates) > 0 {
		args = append(args, pq.Array(terminalStates))
		where.WriteString(fmt.Sprintf(`
		  AND NOT (LOWER(COALESCE(es.current_state, '')) = ANY($%d::text[]))
		`, len(args)))
	}
	for _, selector := range selectors {
		segments := workflowInstanceFieldSelectorPath(selector.Field)
		if len(segments) == 0 {
			return nil, fmt.Errorf("workflow instance selector field is required")
		}
		valueJSON, err := json.Marshal(selector.Value)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow instance selector %s: %w", selector.Field, err)
		}
		args = append(args, pq.Array(segments), string(valueJSON))
		where.WriteString(fmt.Sprintf(`
		  AND es.fields #> $%d::text[] = $%d::jsonb
		`, len(args)-1, len(args)))
	}
	where.WriteString(`
		ORDER BY es.created_at ASC
	`)
	return s.querySpec(ctx, runID, where.String(), args...)
}

func (s *WorkflowInstanceStore) querySpec(ctx context.Context, runID, where string, args ...any) ([]WorkflowInstance, error) {
	rows, err := dbQueryContext(ctx, s.db, `
		SELECT
			es.entity_id::text,
			COALESCE(fi.flow_template, ''),
			COALESCE(fi.config->>'workflow_version', ''),
			COALESCE(fi.status, ''),
			fi.terminated_at,
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
	`+where, args...)
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
			flowInstance string
			entityType   string
			slug         sql.NullString
			name         sql.NullString
			status       sql.NullString
			terminatedAt sql.NullTime
		)
		if err := rows.Scan(
			&item.InstanceID,
			&item.WorkflowName,
			&item.WorkflowVersion,
			&status,
			&terminatedAt,
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
		item.Status = strings.TrimSpace(status.String)
		if terminatedAt.Valid {
			item.TerminatedAt = terminatedAt.Time
		}
		projection, err := decodeWorkflowInstancePersistedProjection(fieldsRaw, gatesRaw, accRaw, configRaw, workflowInstancePersistedControl{
			StorageRef: strings.TrimSpace(flowInstance),
			Slug:       slug.String,
			Name:       name.String,
			EntityType: entityType,
		})
		if err != nil {
			return nil, err
		}
		item.StateBuckets = projection.Accumulator
		item.Config = projection.Config
		item.Metadata = projection.Metadata()
		persistedIdentity, err := workflowInstancePersistedIdentity(nil, WorkflowInstance{
			InstanceID:   item.InstanceID,
			StorageRef:   projection.Control.StorageRef,
			WorkflowName: item.WorkflowName,
			Metadata:     item.Metadata,
		})
		if err != nil {
			return nil, err
		}
		item.StorageRef = persistedIdentity.StorageRef
		item.InstanceID = persistedIdentity.InstanceID
		item.TransitionHistory = append([]WorkflowTransitionRecord{}, projection.Control.TransitionHistory...)
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

func normalizeWorkflowInstanceFieldSelectors(selectors []workflowInstanceFieldSelector) []workflowInstanceFieldSelector {
	out := make([]workflowInstanceFieldSelector, 0, len(selectors))
	for _, selector := range selectors {
		field := strings.TrimSpace(selector.Field)
		if field == "" {
			continue
		}
		out = append(out, workflowInstanceFieldSelector{Field: field, Value: selector.Value})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Field < out[j].Field
	})
	return out
}

func normalizeWorkflowInstanceExcludedStates(states []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(states))
	for _, state := range states {
		state = strings.ToLower(strings.TrimSpace(state))
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	sort.Strings(out)
	return out
}

func workflowInstanceFieldSelectorPath(field string) []string {
	parts := strings.Split(strings.TrimSpace(field), ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func (s *WorkflowInstanceStore) upsertSpec(ctx context.Context, rowID, storageRef string, instance WorkflowInstance) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !runtimeauthoractivity.InMutation(ctx, tx) {
		return fmt.Errorf("workflow instance upsert requires the pipeline story transaction owner")
	}
	runID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return err
	}
	previous, err := loadTrackedEntityStateProjection(ctx, tx, runID, rowID)
	if err != nil {
		return err
	}

	projection, err := workflowInstancePersistedProjectionFromInstance(instance, storageRef)
	if err != nil {
		return err
	}
	fieldsJSON, err := json.Marshal(projection.Fields)
	if err != nil {
		return err
	}
	gatesJSON, err := json.Marshal(projection.GatesAny())
	if err != nil {
		return err
	}
	config := projection.ConfigPayload(instance.WorkflowVersion)
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}
	accumulatorState, err := json.Marshal(projection.Accumulator)
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
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5,''), NULLIF($6,''),
			$7, $8::jsonb, $9::jsonb, $10::jsonb, 1,
			$11, now(), now()
		)
		ON CONFLICT (run_id, entity_id) DO UPDATE SET
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
	`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name, instance.CurrentState,
		jsonOrDefault(gatesJSON, "{}"),
		jsonOrDefault(fieldsJSON, "{}"),
		jsonOrDefault(accumulatorState, "{}"),
		instance.EnteredStageAt,
	); err != nil {
		return err
	}
	afterProjection := runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(instance.CurrentState),
		Fields:       projection.Fields,
		Gates:        projection.GatesAny(),
		Accumulator:  projection.Accumulator,
	}
	previousForDiff := previous
	if createInfo, ok := workflowCreateEntityInitialValuesFromContext(ctx); ok {
		nextPrevious, err := insertWorkflowCreateEntityInitialValueMutations(ctx, tx, rowID, previous, afterProjection, createInfo.Fields)
		if err != nil {
			return err
		}
		previousForDiff = nextPrevious
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, rowID, previousForDiff, afterProjection, runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "workflow_instance_store",
		HandlerStep: "upsert",
	}); err != nil {
		return err
	}
	return nil
}

func (s *WorkflowInstanceStore) createSpec(ctx context.Context, rowID, storageRef string, instance WorkflowInstance) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !runtimeauthoractivity.InMutation(ctx, tx) {
		return fmt.Errorf("workflow instance create requires the pipeline story transaction owner")
	}
	runID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return err
	}
	if err := lockWorkflowInstanceMutation(ctx, tx, storageRef); err != nil {
		return err
	}
	exists, err := workflowInstanceCreateTargetExists(ctx, tx, runID, rowID, storageRef)
	if err != nil {
		return err
	}
	allowRebind := standingGenerationRebindAllowed(ctx)
	if exists && !allowRebind {
		return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-store", "create", map[string]any{"flow_instance": storageRef})
	}
	if allowRebind {
		if err := admitStandingGenerationRebindPostgres(ctx, tx, runID, rowID, storageRef, instance.WorkflowName); err != nil {
			return err
		}
	}
	projection, err := workflowInstancePersistedProjectionFromInstance(instance, storageRef)
	if err != nil {
		return err
	}
	fieldsJSON, err := json.Marshal(projection.Fields)
	if err != nil {
		return err
	}
	gatesJSON, err := json.Marshal(projection.GatesAny())
	if err != nil {
		return err
	}
	config := projection.ConfigPayload(instance.WorkflowVersion)
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}
	accumulatorState, err := json.Marshal(projection.Accumulator)
	if err != nil {
		return err
	}
	mode := workflowInstanceMode(storageRef)
	flowInstanceInsert := `
		INSERT INTO flow_instances (
			instance_id, flow_template, mode, config, status, created_at
		)
		VALUES (
			$1, $2, $3, $4::jsonb, 'active', now()
		)
	`
	if allowRebind {
		flowInstanceInsert += `
			ON CONFLICT (instance_id) DO UPDATE SET
				flow_template = EXCLUDED.flow_template,
				mode = EXCLUDED.mode,
				config = EXCLUDED.config,
				status = 'active',
				terminated_at = NULL
		`
	}
	if _, err := tx.ExecContext(ctx, flowInstanceInsert, storageRef, instance.WorkflowName, mode, jsonOrDefault(configJSON, "{}")); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5,''), NULLIF($6,''),
			$7, $8::jsonb, $9::jsonb, $10::jsonb, 1,
			$11, now(), now()
		)
	`, runID, rowID, storageRef, projection.Control.EntityType, projection.Control.Slug, projection.Control.Name, instance.CurrentState,
		jsonOrDefault(gatesJSON, "{}"),
		jsonOrDefault(fieldsJSON, "{}"),
		jsonOrDefault(accumulatorState, "{}"),
		instance.EnteredStageAt,
	); err != nil {
		return err
	}
	afterProjection := runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(instance.CurrentState),
		Fields:       projection.Fields,
		Gates:        projection.GatesAny(),
		Accumulator:  projection.Accumulator,
	}
	previousForDiff := runtimemutationlog.EntityStateProjection{}
	if createInfo, ok := workflowCreateEntityInitialValuesFromContext(ctx); ok {
		nextPrevious, err := insertWorkflowCreateEntityInitialValueMutations(ctx, tx, rowID, previousForDiff, afterProjection, createInfo.Fields)
		if err != nil {
			return err
		}
		previousForDiff = nextPrevious
	}
	if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, rowID, previousForDiff, afterProjection, runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "workflow_instance_store",
		HandlerStep: "create",
	}); err != nil {
		return err
	}
	return nil
}

func admitStandingGenerationRebindPostgres(ctx context.Context, tx *sql.Tx, runID, rowID, storageRef, workflowName string) error {
	var sameRunExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid)`, runID, rowID).Scan(&sameRunExists); err != nil {
		return err
	}
	if sameRunExists {
		return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-store", "create", map[string]any{"flow_instance": storageRef})
	}
	var existingTemplate string
	err := tx.QueryRowContext(ctx, `SELECT flow_template FROM flow_instances WHERE instance_id = $1 FOR UPDATE`, storageRef).Scan(&existingTemplate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(existingTemplate) != strings.TrimSpace(workflowName) {
		return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-store", "create", map[string]any{"flow_instance": storageRef})
	}
	return nil
}

func workflowInstanceCreateTargetExists(ctx context.Context, tx *sql.Tx, runID, rowID, storageRef string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM flow_instances
			WHERE instance_id = $1
		)
	`, storageRef).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM entity_state
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
		)
	`, runID, rowID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
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

func insertWorkflowCreateEntityInitialValueMutations(
	ctx context.Context,
	tx *sql.Tx,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	initialValues map[string]any,
) (runtimemutationlog.EntityStateProjection, error) {
	if len(initialValues) == 0 {
		return before, nil
	}
	adjusted := runtimemutationlog.EntityStateProjection{
		CurrentState: before.CurrentState,
		Fields:       cloneStringAnyMap(before.Fields),
		Gates:        cloneStringAnyMap(before.Gates),
		Accumulator:  cloneStringAnyMap(before.Accumulator),
	}
	if adjusted.Fields == nil {
		adjusted.Fields = map[string]any{}
	}
	for field, declared := range initialValues {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		finalValue, ok := after.Fields[field]
		oldValue, hadOld := adjusted.Fields[field]
		if hadOld {
			continue
		}
		if err := runtimemutationlog.Insert(ctx, tx, runtimemutationlog.Record{
			EntityID:    entityID,
			Field:       field,
			OldValue:    oldValueOrNil(oldValue, hadOld),
			NewValue:    declared,
			WriterType:  "platform",
			WriterID:    "entity_initial_value",
			HandlerStep: "create_entity",
		}); err != nil {
			return runtimemutationlog.EntityStateProjection{}, err
		}
		if ok && workflowJSONValuesEqual(finalValue, declared) {
			adjusted.Fields[field] = declared
			continue
		}
		adjusted.Fields[field] = declared
	}
	return adjusted, nil
}

func oldValueOrNil(value any, ok bool) any {
	if !ok {
		return nil
	}
	return value
}

func loadTrackedEntityStateProjection(ctx context.Context, tx *sql.Tx, runID, entityID string) (runtimemutationlog.EntityStateProjection, error) {
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
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		FOR UPDATE
	`, runID, entityID).Scan(&currentState, &fieldsRaw, &gatesRaw, &accRaw)
	if err == sql.ErrNoRows {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("entity_state.gates", gatesRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	accumulator, err := decodeWorkflowInstanceJSONMap("entity_state.accumulator", accRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState.String),
		Fields:       fields,
		Gates:        workflowBoolGatesAsMap(gates),
		Accumulator:  accumulator,
	}, nil
}

func decodeWorkflowInstancePersistedProjection(
	fieldsRaw, gatesRaw, accRaw, configRaw []byte,
	control workflowInstancePersistedControl,
) (workflowInstancePersistedProjection, error) {
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("entity_state.gates", gatesRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	accumulator, err := decodeWorkflowInstanceJSONMap("entity_state.accumulator", accRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	config, control, err := decodeWorkflowInstanceConfigPayload(configRaw, control)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	if strings.TrimSpace(control.EntityType) == "" {
		control.EntityType = "default"
	}
	return workflowInstancePersistedProjection{
		Fields:      fields,
		Gates:       gates,
		Accumulator: accumulator,
		Config:      config,
		Control:     control,
	}, nil
}

func workflowInstancePersistedProjectionFromInstance(instance WorkflowInstance, storageRef string) (workflowInstancePersistedProjection, error) {
	metadata := cloneStringAnyMap(instance.Metadata)
	gates, err := workflowInstanceMetadataGates(metadata)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	delete(metadata, "gates")
	persistedIdentity, err := workflowInstancePersistedIdentity(nil, instance)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	if strings.TrimSpace(storageRef) != "" && strings.TrimSpace(persistedIdentity.StorageRef) != "" && strings.TrimSpace(storageRef) != strings.TrimSpace(persistedIdentity.StorageRef) {
		return workflowInstancePersistedProjection{}, fmt.Errorf("workflow instance storage_ref %q disagrees with canonical storage_ref %q", storageRef, persistedIdentity.StorageRef)
	}
	control := workflowInstancePersistedControl{
		StorageRef:         strings.TrimSpace(persistedIdentity.StorageRef),
		Slug:               strings.TrimSpace(asString(instance.Metadata["slug"])),
		Name:               strings.TrimSpace(asString(instance.Metadata["name"])),
		EntityType:         strings.TrimSpace(asString(instance.Metadata["entity_type"])),
		InstanceID:         strings.TrimSpace(persistedIdentity.InstanceID),
		InstanceKind:       strings.TrimSpace(asString(instance.Metadata["instance_kind"])),
		TemplateVersion:    strings.TrimSpace(asString(instance.Metadata["template_version"])),
		LastSourceEvent:    strings.TrimSpace(asString(instance.Metadata["last_source_event"])),
		Status:             strings.TrimSpace(asString(instance.Config["status"])),
		ParentFlowID:       strings.TrimSpace(persistedIdentity.ParentRoute.FlowID),
		ParentFlowInstance: strings.Trim(strings.TrimSpace(persistedIdentity.ParentRoute.FlowInstance), "/"),
		ParentEntityID:     strings.TrimSpace(asString(instance.Metadata["parent_entity_id"])),
		TransitionHistory:  append([]WorkflowTransitionRecord{}, instance.TransitionHistory...),
	}
	if persistedIdentity.HasStoredPath {
		control.FlowPath = strings.TrimSpace(persistedIdentity.InstancePath)
	}
	for _, key := range []string{
		"slug", "name", "entity_type", "parent_flow_id", "parent_flow_instance", "parent_entity_id",
		"instance_id", "storage_ref", "flow_path", "instance_kind",
		"template_version", "workflow_version", "transition_history",
	} {
		delete(metadata, key)
	}
	if control.EntityType == "" {
		control.EntityType = "default"
	}
	return workflowInstancePersistedProjection{
		Fields:      metadata,
		Gates:       gates,
		Accumulator: cloneStringAnyMap(instance.StateBuckets),
		Config:      cloneStringAnyMap(instance.Config),
		Control:     control,
	}, nil
}

func (p workflowInstancePersistedProjection) Metadata() map[string]any {
	metadata := cloneStringAnyMap(p.Fields)
	if len(p.Gates) > 0 {
		metadata["gates"] = p.GatesAny()
	}
	if strings.TrimSpace(p.Control.Slug) != "" {
		metadata["slug"] = strings.TrimSpace(p.Control.Slug)
	}
	if strings.TrimSpace(p.Control.Name) != "" {
		metadata["name"] = strings.TrimSpace(p.Control.Name)
	}
	if strings.TrimSpace(p.Control.EntityType) != "" {
		metadata["entity_type"] = strings.TrimSpace(p.Control.EntityType)
	}
	if strings.TrimSpace(p.Control.StorageRef) != "" {
		metadata["storage_ref"] = strings.TrimSpace(p.Control.StorageRef)
	}
	if strings.TrimSpace(p.Control.InstanceID) != "" {
		metadata["instance_id"] = strings.TrimSpace(p.Control.InstanceID)
	}
	if strings.TrimSpace(p.Control.FlowPath) != "" {
		metadata["flow_path"] = strings.TrimSpace(p.Control.FlowPath)
	}
	if strings.TrimSpace(p.Control.InstanceKind) != "" {
		metadata["instance_kind"] = strings.TrimSpace(p.Control.InstanceKind)
	}
	if strings.TrimSpace(p.Control.TemplateVersion) != "" {
		metadata["template_version"] = strings.TrimSpace(p.Control.TemplateVersion)
	}
	if strings.TrimSpace(p.Control.LastSourceEvent) != "" {
		metadata["last_source_event"] = strings.TrimSpace(p.Control.LastSourceEvent)
	}
	if strings.TrimSpace(p.Control.ParentFlowID) != "" {
		metadata["parent_flow_id"] = strings.TrimSpace(p.Control.ParentFlowID)
	}
	if strings.TrimSpace(p.Control.ParentFlowInstance) != "" {
		metadata["parent_flow_instance"] = strings.Trim(strings.TrimSpace(p.Control.ParentFlowInstance), "/")
	}
	if strings.TrimSpace(p.Control.ParentEntityID) != "" {
		metadata["parent_entity_id"] = strings.TrimSpace(p.Control.ParentEntityID)
	}
	if len(p.Control.TransitionHistory) > 0 {
		metadata["transition_history"] = append([]WorkflowTransitionRecord{}, p.Control.TransitionHistory...)
	}
	return metadata
}

func (p workflowInstancePersistedProjection) ConfigPayload(workflowVersion string) map[string]any {
	config := cloneStringAnyMap(p.Config)
	if config == nil {
		config = map[string]any{}
	}
	config["workflow_version"] = strings.TrimSpace(workflowVersion)
	config["instance_id"] = strings.TrimSpace(p.Control.InstanceID)
	config["storage_ref"] = strings.TrimSpace(p.Control.StorageRef)
	if strings.TrimSpace(p.Control.FlowPath) != "" {
		config["flow_path"] = strings.TrimSpace(p.Control.FlowPath)
	}
	if strings.TrimSpace(p.Control.InstanceKind) != "" {
		config["instance_kind"] = strings.TrimSpace(p.Control.InstanceKind)
	}
	if strings.TrimSpace(p.Control.TemplateVersion) != "" {
		config["template_version"] = strings.TrimSpace(p.Control.TemplateVersion)
	}
	if strings.TrimSpace(p.Control.LastSourceEvent) != "" {
		config["last_source_event"] = strings.TrimSpace(p.Control.LastSourceEvent)
	}
	if strings.TrimSpace(p.Control.Status) != "" {
		config["status"] = strings.TrimSpace(p.Control.Status)
	}
	if strings.TrimSpace(p.Control.ParentFlowID) != "" {
		config["parent_flow_id"] = strings.TrimSpace(p.Control.ParentFlowID)
	}
	if strings.TrimSpace(p.Control.ParentFlowInstance) != "" {
		config["parent_flow_instance"] = strings.Trim(strings.TrimSpace(p.Control.ParentFlowInstance), "/")
	}
	if strings.TrimSpace(p.Control.ParentEntityID) != "" {
		config["parent_entity_id"] = strings.TrimSpace(p.Control.ParentEntityID)
	}
	if len(p.Control.TransitionHistory) > 0 {
		config["transition_history"] = append([]WorkflowTransitionRecord{}, p.Control.TransitionHistory...)
	}
	return config
}

func (p workflowInstancePersistedProjection) GatesAny() map[string]any {
	return workflowBoolGatesAsMap(p.Gates)
}

func decodeWorkflowInstanceJSONMap(label string, raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s must be a JSON object: %w", label, err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func decodeWorkflowInstanceJSONBoolMap(label string, raw []byte) (map[string]bool, error) {
	if len(raw) == 0 {
		return map[string]bool{}, nil
	}
	var out map[string]bool
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s must be an object of booleans: %w", label, err)
	}
	if out == nil {
		return map[string]bool{}, nil
	}
	return out, nil
}

func workflowInstanceMetadataGates(metadata map[string]any) (map[string]bool, error) {
	if metadata == nil {
		return map[string]bool{}, nil
	}
	raw, ok := metadata["gates"]
	if !ok || raw == nil {
		return map[string]bool{}, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("workflow instance metadata.gates must be JSON-serializable: %w", err)
	}
	return decodeWorkflowInstanceJSONBoolMap("workflow instance metadata.gates", encoded)
}

func decodeWorkflowInstanceConfigPayload(raw []byte, control workflowInstancePersistedControl) (map[string]any, workflowInstancePersistedControl, error) {
	config, err := decodeWorkflowInstanceJSONMap("flow_instances.config", raw)
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	instanceID, err := workflowInstanceOptionalString(config, "instance_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	flowPath, err := workflowInstanceOptionalString(config, "flow_path")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	configStorageRef, err := workflowInstanceOptionalString(config, "storage_ref")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	instanceKind, err := workflowInstanceOptionalString(config, "instance_kind")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	templateVersion, err := workflowInstanceOptionalString(config, "template_version")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	lastSourceEvent, err := workflowInstanceOptionalString(config, "last_source_event")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	status, err := workflowInstanceOptionalString(config, "status")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentFlowID, err := workflowInstanceOptionalString(config, "parent_flow_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentFlowInstance, err := workflowInstanceOptionalString(config, "parent_flow_instance")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentEntityID, err := workflowInstanceOptionalString(config, "parent_entity_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	transitionHistory, err := workflowInstanceTransitionHistoryFromConfig(config)
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	delete(config, "workflow_version")
	delete(config, "instance_id")
	delete(config, "storage_ref")
	delete(config, "flow_path")
	delete(config, "instance_kind")
	delete(config, "template_version")
	delete(config, "last_source_event")
	delete(config, "parent_flow_id")
	delete(config, "parent_flow_instance")
	delete(config, "parent_entity_id")
	delete(config, "transition_history")
	control.InstanceID = strings.TrimSpace(instanceID)
	control.FlowPath = strings.TrimSpace(flowPath)
	if strings.TrimSpace(control.StorageRef) == "" {
		control.StorageRef = strings.TrimSpace(configStorageRef)
	}
	if strings.TrimSpace(control.StorageRef) != "" && strings.TrimSpace(configStorageRef) != "" && strings.TrimSpace(control.StorageRef) != strings.TrimSpace(configStorageRef) {
		return nil, workflowInstancePersistedControl{}, fmt.Errorf("flow_instances.config storage_ref %q disagrees with canonical storage_ref %q", configStorageRef, control.StorageRef)
	}
	control.InstanceKind = strings.TrimSpace(instanceKind)
	control.TemplateVersion = strings.TrimSpace(templateVersion)
	control.LastSourceEvent = strings.TrimSpace(lastSourceEvent)
	control.Status = strings.TrimSpace(status)
	control.ParentFlowID = strings.TrimSpace(parentFlowID)
	control.ParentFlowInstance = strings.Trim(strings.TrimSpace(parentFlowInstance), "/")
	control.ParentEntityID = strings.TrimSpace(parentEntityID)
	control.TransitionHistory = transitionHistory
	return config, control, nil
}

func workflowInstanceOptionalString(config map[string]any, key string) (string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return "", nil
	}
	typed, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("flow_instances.config %s must be a string", key)
	}
	return strings.TrimSpace(typed), nil
}

func workflowInstanceTransitionHistoryFromConfig(config map[string]any) ([]WorkflowTransitionRecord, error) {
	raw, ok := config["transition_history"]
	if !ok || raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("flow_instances.config transition_history must be JSON-serializable: %w", err)
	}
	var out []WorkflowTransitionRecord
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, fmt.Errorf("flow_instances.config transition_history must be an array of workflow transition records: %w", err)
	}
	return out, nil
}

func workflowInstanceMode(storageRef string) string {
	if strings.Contains(strings.TrimSpace(storageRef), "/") {
		return "template"
	}
	return "static"
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
