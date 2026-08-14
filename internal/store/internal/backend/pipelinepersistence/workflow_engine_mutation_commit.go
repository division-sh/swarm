package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	gaterouteadapter "github.com/division-sh/swarm/internal/store/internal/backend/gateroute"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
)

func commitWorkflowEngineState(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
	rebindExistingRoute bool,
) error {
	if tx == nil {
		return fmt.Errorf("workflow engine state commit requires private transaction")
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if postgres {
		if err := requirePostgresRunActive(ctx, tx, record.RunID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, record.RunID+":"+record.Route.InstancePath); err != nil {
			return fmt.Errorf("lock workflow engine state route: %w", err)
		}
		return commitPostgresWorkflowEngineState(ctx, tx, record, rebindExistingRoute)
	}
	if err := requireSQLiteRunActive(ctx, tx, record.RunID); err != nil {
		return err
	}
	return commitSQLiteWorkflowEngineState(ctx, tx, record, rebindExistingRoute)
}

func commitPostgresWorkflowEngineState(ctx context.Context, tx *sql.Tx, record runtimepipeline.WorkflowEngineStateRecord, rebindExistingRoute bool) error {
	if record.Create {
		query := `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6)
		`
		if rebindExistingRoute {
			query += `
				ON CONFLICT (instance_id) DO UPDATE SET
					flow_template = EXCLUDED.flow_template,
					mode = EXCLUDED.mode,
					config = EXCLUDED.config,
					status = EXCLUDED.status,
					terminated_at = NULL
			`
		} else {
			query += ` ON CONFLICT (instance_id) DO NOTHING`
		}
		result, err := tx.ExecContext(ctx, query, record.Route.InstancePath, record.WorkflowName, record.Mode, string(record.Config), record.Status, record.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert workflow engine flow instance: %w", err)
		}
		if !rebindExistingRoute {
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("read workflow engine flow insertion: %w", err)
			}
			if rows == 0 {
				if err := verifyWorkflowEngineFlowDescriptor(ctx, tx, true, record); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, bookkeeping, accumulator, revision,
				entered_state_at, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
				$7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, 1,
				$12, $13, $13
			)
		`, record.RunID, record.EntityID, record.Route.InstancePath, record.EntityType, record.Slug, record.Name,
			record.CurrentState, string(record.Gates), string(record.Fields), string(record.Bookkeeping), string(record.Accumulator),
			record.EnteredStageAt, record.CreatedAt); err != nil {
			return fmt.Errorf("insert workflow engine entity state: %w", err)
		}
		return nil
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE entity_state
		SET entity_type = $1,
		    slug = NULLIF($2, ''),
		    name = NULLIF($3, ''),
		    current_state = $4,
		    gates = $5::jsonb,
		    fields = $6::jsonb,
		    bookkeeping = $7::jsonb,
		    accumulator = $8::jsonb,
		    revision = revision + 1,
		    entered_state_at = $9,
		    updated_at = $10
		WHERE run_id = $11::uuid
		  AND entity_id = $12::uuid
		  AND flow_instance = $13
		  AND revision = $14
		  AND current_state = $15
	`, record.EntityType, record.Slug, record.Name, record.CurrentState, string(record.Gates), string(record.Fields), string(record.Bookkeeping), string(record.Accumulator),
		record.EnteredStageAt, record.UpdatedAt, record.RunID, record.EntityID, record.Route.InstancePath, record.ExpectedRevision, record.ExpectedState)
	if err != nil {
		return fmt.Errorf("update workflow engine entity state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read workflow engine state update: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("workflow engine state changed before commit: route=%s revision=%d state=%s", record.Route.InstancePath, record.ExpectedRevision, record.ExpectedState)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE flow_instances
		SET flow_template = $1,
		    config = $2::jsonb,
		    status = $3,
		    terminated_at = CASE WHEN $3 = 'terminated' THEN COALESCE(terminated_at, $4) ELSE NULL END
		WHERE instance_id = $5
	`, record.WorkflowName, string(record.Config), record.Status, nullableWorkflowTerminationTime(record.TerminatedAt), record.Route.InstancePath)
	if err != nil {
		return fmt.Errorf("update workflow engine flow instance: %w", err)
	}
	rows, err = result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return fmt.Errorf("read workflow engine flow update: %w", err)
		}
		return fmt.Errorf("workflow engine flow instance is missing: %s", record.Route.InstancePath)
	}
	return nil
}

func commitSQLiteWorkflowEngineState(ctx context.Context, tx *sql.Tx, record runtimepipeline.WorkflowEngineStateRecord, rebindExistingRoute bool) error {
	if record.Create {
		query := `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`
		if rebindExistingRoute {
			query += `
				ON CONFLICT(instance_id) DO UPDATE SET
					flow_template = excluded.flow_template,
					mode = excluded.mode,
					config = excluded.config,
					status = excluded.status,
					terminated_at = NULL
			`
		} else {
			query += ` ON CONFLICT(instance_id) DO NOTHING`
		}
		result, err := tx.ExecContext(ctx, query, record.Route.InstancePath, record.WorkflowName, record.Mode, string(record.Config), record.Status, record.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert workflow engine flow instance: %w", err)
		}
		if !rebindExistingRoute {
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("read workflow engine flow insertion: %w", err)
			}
			if rows == 0 {
				if err := verifyWorkflowEngineFlowDescriptor(ctx, tx, false, record); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, bookkeeping, accumulator, revision,
				entered_state_at, created_at, updated_at
			)
			VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, 1, ?, ?, ?)
		`, record.RunID, record.EntityID, record.Route.InstancePath, record.EntityType, record.Slug, record.Name,
			record.CurrentState, string(record.Gates), string(record.Fields), string(record.Bookkeeping), string(record.Accumulator),
			record.EnteredStageAt, record.CreatedAt, record.CreatedAt); err != nil {
			return fmt.Errorf("insert workflow engine entity state: %w", err)
		}
		return nil
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE entity_state
		SET entity_type = ?,
		    slug = NULLIF(?, ''),
		    name = NULLIF(?, ''),
		    current_state = ?,
		    gates = ?,
		    fields = ?,
		    bookkeeping = ?,
		    accumulator = ?,
		    revision = revision + 1,
		    entered_state_at = ?,
		    updated_at = ?
		WHERE run_id = ?
		  AND entity_id = ?
		  AND flow_instance = ?
		  AND revision = ?
		  AND current_state = ?
	`, record.EntityType, record.Slug, record.Name, record.CurrentState, string(record.Gates), string(record.Fields), string(record.Bookkeeping), string(record.Accumulator),
		record.EnteredStageAt, record.UpdatedAt, record.RunID, record.EntityID, record.Route.InstancePath, record.ExpectedRevision, record.ExpectedState)
	if err != nil {
		return fmt.Errorf("update workflow engine entity state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read workflow engine state update: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("workflow engine state changed before commit: route=%s revision=%d state=%s", record.Route.InstancePath, record.ExpectedRevision, record.ExpectedState)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE flow_instances
		SET flow_template = ?,
		    config = ?,
		    status = ?,
		    terminated_at = CASE WHEN ? = 'terminated' THEN COALESCE(terminated_at, ?) ELSE NULL END
		WHERE instance_id = ?
	`, record.WorkflowName, string(record.Config), record.Status, record.Status, nullableWorkflowTerminationTime(record.TerminatedAt), record.Route.InstancePath)
	if err != nil {
		return fmt.Errorf("update workflow engine flow instance: %w", err)
	}
	rows, err = result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return fmt.Errorf("read workflow engine flow update: %w", err)
		}
		return fmt.Errorf("workflow engine flow instance is missing: %s", record.Route.InstancePath)
	}
	return nil
}

func verifyWorkflowEngineFlowDescriptor(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowEngineStateRecord) error {
	query := `SELECT flow_template, mode, config, status FROM flow_instances WHERE instance_id = ?`
	if postgres {
		query = `SELECT flow_template, mode, config, status FROM flow_instances WHERE instance_id = $1`
	}
	var workflowName, mode, status string
	var config any
	if err := tx.QueryRowContext(ctx, query, record.Route.InstancePath).Scan(&workflowName, &mode, &config, &status); err != nil {
		return fmt.Errorf("load existing workflow engine flow descriptor: %w", err)
	}
	var configBytes []byte
	switch value := config.(type) {
	case []byte:
		configBytes = append(configBytes, value...)
	case string:
		configBytes = append(configBytes, value...)
	default:
		return fmt.Errorf("existing workflow engine flow descriptor has unsupported config type %T", config)
	}
	if workflowName != record.WorkflowName || mode != record.Mode || status != record.Status || !workflowCommitJSONEqual(configBytes, record.Config) {
		return fmt.Errorf("workflow engine flow descriptor conflicts at route %s", record.Route.InstancePath)
	}
	return nil
}

func nullableWorkflowTerminationTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func workflowEngineStateProjection(record runtimepipeline.WorkflowEngineStateRecord) (runtimemutationlog.EntityStateProjection, error) {
	decode := func(name string, raw json.RawMessage) (map[string]any, error) {
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode workflow engine %s projection: %w", name, err)
		}
		if value == nil {
			value = map[string]any{}
		}
		return value, nil
	}
	fields, err := decode("fields", record.Fields)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	bookkeeping, err := decode("bookkeeping", record.Bookkeeping)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	gates, err := decode("gates", record.Gates)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	accumulator, err := decode("accumulator", record.Accumulator)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: record.CurrentState,
		Fields:       fields,
		Bookkeeping:  bookkeeping,
		Gates:        gates,
		Accumulator:  accumulator,
	}, nil
}

func loadWorkflowEngineStateProjection(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
) (runtimemutationlog.EntityStateProjection, error) {
	if record.Create {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	query := `SELECT current_state, fields, bookkeeping, gates, accumulator FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
	args := []any{record.RunID, record.EntityID, record.Route.InstancePath}
	if postgres {
		query = `SELECT current_state, fields, bookkeeping, gates, accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3 FOR UPDATE`
	}
	var currentState string
	var fieldsRaw, bookkeepingRaw, gatesRaw, accumulatorRaw any
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&currentState, &fieldsRaw, &bookkeepingRaw, &gatesRaw, &accumulatorRaw); err != nil {
		if err == sql.ErrNoRows {
			return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("workflow engine state route is missing: %s", record.Route.InstancePath)
		}
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("load workflow engine state projection: %w", err)
	}
	fields, err := storeentity.DecodeJSONMap(fieldsRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine persisted fields: %w", err)
	}
	bookkeeping, err := storeentity.DecodeJSONMap(bookkeepingRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine persisted bookkeeping: %w", err)
	}
	gates, err := storeentity.DecodeJSONMap(gatesRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine persisted gates: %w", err)
	}
	accumulator, err := storeentity.DecodeJSONMap(accumulatorRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine persisted accumulator: %w", err)
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: currentState,
		Fields:       fields,
		Bookkeeping:  bookkeeping,
		Gates:        gates,
		Accumulator:  accumulator,
	}, nil
}

func commitWorkflowEngineMutationLog(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
	before runtimemutationlog.EntityStateProjection,
) error {
	after, err := workflowEngineStateProjection(record)
	if err != nil {
		return err
	}
	writer := runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: map[bool]string{true: "create", false: "mutate"}[record.Create]}
	if postgres {
		selected, ok := store.(*PipelinePostgresOwner)
		if !ok {
			return fmt.Errorf("workflow engine PostgreSQL mutation requires PostgreSQL selected store")
		}
		return privatemutationlog.InsertEntityStateDiffWithStory(
			ctx, tx, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
				return selected.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), runtimeAuthorActivityMutation(story),
			record.EntityID, before, after, writer,
		)
	}
	_, ok := store.(*PipelineSQLiteOwner)
	if !ok {
		return fmt.Errorf("workflow engine SQLite mutation requires SQLite selected store")
	}
	return storeentity.InsertSQLiteEntityStateDiff(ctx, runtimeAuthorActivityMutation(story), tx, record.RunID, record.EntityID, before, after, writer, record.UpdatedAt)
}

func commitWorkflowEngineInitialValues(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
	before runtimemutationlog.EntityStateProjection,
) (runtimemutationlog.EntityStateProjection, error) {
	var initial map[string]any
	if err := json.Unmarshal(record.InitialFields, &initial); err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine initial fields: %w", err)
	}
	if !record.Create || len(initial) == 0 {
		return before, nil
	}
	adjusted := runtimemutationlog.EntityStateProjection{
		CurrentState: before.CurrentState,
		Fields:       copyWorkflowEngineProjectionMap(before.Fields),
		Bookkeeping:  copyWorkflowEngineProjectionMap(before.Bookkeeping),
		Gates:        copyWorkflowEngineProjectionMap(before.Gates),
		Accumulator:  copyWorkflowEngineProjectionMap(before.Accumulator),
	}
	keys := make([]string, 0, len(initial))
	for field := range initial {
		if field != "" {
			keys = append(keys, field)
		}
	}
	sort.Strings(keys)
	for _, field := range keys {
		if _, exists := adjusted.Fields[field]; exists {
			continue
		}
		next := adjusted
		next.Fields = copyWorkflowEngineProjectionMap(adjusted.Fields)
		next.Fields[field] = initial[field]
		writer := runtimemutationlog.Writer{Type: "platform", ID: "entity_initial_value", HandlerStep: "create_entity"}
		if postgres {
			selected, ok := store.(*PipelinePostgresOwner)
			if !ok {
				return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("workflow engine initial values require PostgreSQL selected store")
			}
			if err := privatemutationlog.InsertEntityStateDiffWithStory(ctx, tx, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
				return selected.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), runtimeAuthorActivityMutation(story), record.EntityID, adjusted, next, writer); err != nil {
				return runtimemutationlog.EntityStateProjection{}, err
			}
		} else if err := storeentity.InsertSQLiteEntityStateDiff(ctx, runtimeAuthorActivityMutation(story), tx, record.RunID, record.EntityID, adjusted, next, writer, record.UpdatedAt); err != nil {
			return runtimemutationlog.EntityStateProjection{}, err
		}
		adjusted = next
	}
	return adjusted, nil
}

func copyWorkflowEngineProjectionMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func commitWorkflowEngineMutation(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	command runtimepipeline.WorkflowEngineMutationCommand,
) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	defer handoff.Rollback()
	result := runtimepipeline.CommittedWorkflowEngineMutation{
		Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(command.Publications)),
		PostCommit:   command.PostCommit,
	}
	entityless := !command.EntitylessTarget.Empty()
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		if runID := strings.TrimSpace(command.GateRouteAdmissionRunID); runID != "" {
			if postgres {
				err = gaterouteadapter.RequirePostgres(txctx, tx, runID)
			} else {
				err = gaterouteadapter.RequireSQLite(txctx, tx, runID)
			}
			if err != nil {
				return err
			}
		}
		var before runtimemutationlog.EntityStateProjection
		if entityless {
			if postgres {
				err = requirePostgresRunActive(txctx, tx, command.EntitylessRunID)
			} else {
				err = requireSQLiteRunActive(txctx, tx, command.EntitylessRunID)
			}
			if err != nil {
				return err
			}
		} else {
			before, err = loadWorkflowEngineStateProjection(txctx, tx, postgres, command.State)
			if err != nil {
				return err
			}
			if err := commitWorkflowEngineState(txctx, tx, postgres, command.State, false); err != nil {
				return err
			}
		}
		if !entityless && command.RouteRetirement != nil {
			sets := []runtimebus.FlowInstanceRouteRecordSet{{Identity: command.RouteRetirement.Route}}
			if _, err := replaceFlowInstanceRouteTopologyTx(txctx, tx, postgres, sets); err != nil {
				return fmt.Errorf("retire terminal workflow route: %w", err)
			}
			retirement := *command.RouteRetirement
			result.RouteRetirement = &retirement
		}
		runtimeStory := runtimeAuthorActivityMutation(story)
		if !entityless {
			before, err = commitWorkflowEngineInitialValues(txctx, tx, story, store, postgres, command.State, before)
			if err != nil {
				return err
			}
			result.Lifecycle, err = commitWorkflowEngineLifecycle(txctx, tx, runtimeStory, store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), postgres, command.Lifecycle)
			if err != nil {
				return err
			}
			if command.Lifecycle.RequestCompletionCandidate {
				candidate, err := requestCandidate(txctx, tx, command.State.RunID)
				if err != nil {
					return err
				}
				if err := prepare(handoff, candidate); err != nil {
					return err
				}
			}
			if err := commitWorkflowEngineMutationLog(txctx, tx, story, store, postgres, command.State, before); err != nil {
				return err
			}
			for index, proposed := range command.ProposedEffects {
				if err := store.workflowDecisionLifecycleOwner().InsertProposedEffectTx(txctx, runtimeStory, tx, proposed.Card, proposed.Continuation); err != nil {
					return fmt.Errorf("commit workflow engine proposed effect %d: %w", index, err)
				}
			}
		}
		for index, value := range command.Publications {
			plan, ok := value.(runtimebus.EnginePublicationPlan)
			if !ok {
				return fmt.Errorf("workflow engine publication %d has unexpected type %T", index, value)
			}
			committed, err := store.commitPublicationTx(txctx, tx, story, plan.PublicationCommand(), handoff)
			if err != nil {
				return fmt.Errorf("commit workflow engine publication %d: %w", index, err)
			}
			evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
			if err != nil {
				return err
			}
			result.Publications = append(result.Publications, evidence)
		}
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	if err := handoff.Commit(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	return result, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	return commitWorkflowEngineMutation(ctx, s, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, reserveRunLifecycleCandidateHandoff, func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
		return reservation.Prepare(s.runLifecycleCandidates, result)
	}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
		return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
	}, command)
}

func (s *PipelineSQLiteOwner) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	return commitWorkflowEngineMutation(ctx, s, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite workflow engine mutation", fn)
	}, reserveRunLifecycleCandidateHandoff, func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
		return reservation.Prepare(s.runLifecycleCandidates, result)
	}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
		return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
	}, command)
}

var _ runtimepipeline.WorkflowEngineMutationOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowEngineMutationOwner = (*PipelineSQLiteOwner)(nil)
