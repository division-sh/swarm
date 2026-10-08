package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	gaterouteadapter "github.com/division-sh/swarm/internal/store/internal/backend/gateroute"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func commitWorkflowEngineState(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
) error {
	if err := record.Validate(); err != nil {
		return err
	}
	decision, err := decideWorkflowEngineState(record.Transition)
	if err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var written workflowEngineStateFact
		if postgres {
			if err := requirePostgresRunActive(ctx, tx, record.Identity.RunID); err != nil {
				return err
			}
			lockIdentity := fmt.Sprintf("%d:%s%s", len(record.Identity.RunID), record.Identity.RunID, record.Identity.Route.InstancePath)
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockIdentity); err != nil {
				return fmt.Errorf("lock workflow engine state route: %w", err)
			}
			if record.Transition.PreservesState() {
				return requireWorkflowHeaderProjection(ctx, tx, true, record, record.ExpectedRevision)
			}
			written, err = commitPostgresWorkflowEngineState(ctx, tx, record, decision)
		} else {
			if err := requireSQLiteRunActive(ctx, tx, record.Identity.RunID); err != nil {
				return err
			}
			if record.Transition.PreservesState() {
				return requireWorkflowHeaderProjection(ctx, tx, false, record, record.ExpectedRevision)
			}
			written, err = commitSQLiteWorkflowEngineState(ctx, tx, record, decision)
		}
		if err != nil {
			return err
		}
		return attempt.AddFact(written.runID, privaterunforkrevision.FamilyEntityMetadata, written.entityID)
	})
}

type workflowEngineStateDecision struct {
	createState bool
	historyStep string
}

type workflowEngineStateFact struct {
	runID    string
	entityID string
}

func decideWorkflowEngineState(transition runtimepipeline.WorkflowEngineStateTransition) (workflowEngineStateDecision, error) {
	switch transition {
	case runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion:
		return workflowEngineStateDecision{createState: true, historyStep: "create"}, nil
	case runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion:
		return workflowEngineStateDecision{historyStep: "mutate"}, nil
	case runtimepipeline.WorkflowEngineStateTransitionPreserveStateAndCompanion:
		return workflowEngineStateDecision{}, nil
	default:
		return workflowEngineStateDecision{}, fmt.Errorf("workflow engine state requires a closed transition")
	}
}

func commitPostgresWorkflowEngineState(ctx context.Context, tx *sql.Tx, record runtimepipeline.WorkflowEngineStateRecord, decision workflowEngineStateDecision) (workflowEngineStateFact, error) {
	return commitWorkflowHeaderAndFields(ctx, tx, true, record, decision)
}

func commitSQLiteWorkflowEngineState(ctx context.Context, tx *sql.Tx, record runtimepipeline.WorkflowEngineStateRecord, decision workflowEngineStateDecision) (workflowEngineStateFact, error) {
	return commitWorkflowHeaderAndFields(ctx, tx, false, record, decision)
}

func commitWorkflowHeaderAndFields(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowEngineStateRecord, decision workflowEngineStateDecision) (workflowEngineStateFact, error) {
	fact, err := commitWorkflowInstanceHeader(ctx, tx, postgres, record, decision.createState)
	if err != nil {
		return workflowEngineStateFact{}, err
	}
	if record.EntityType == "" {
		return fact, requireFieldlessWorkflowStateAbsent(ctx, tx, postgres, record)
	}
	var query string
	var args []any
	if decision.createState {
		query = `INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, 1, ?, ?, ?)`
		if postgres {
			query = `INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, bookkeeping, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
				$7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, 1, $12, $13, $14)`
		}
		args = []any{record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, record.EntityType,
			record.Slug, record.Name, record.CurrentState, string(record.Gates), string(record.Fields),
			string(record.Bookkeeping), string(record.Accumulator), record.EnteredStageAt, record.CreatedAt, record.UpdatedAt}
	} else {
		query = `UPDATE entity_state SET slug = NULLIF(?, ''), name = NULLIF(?, ''), current_state = ?,
			gates = ?, fields = ?, bookkeeping = ?, accumulator = ?, revision = revision + 1,
			entered_state_at = ?, updated_at = ? WHERE run_id = ? AND entity_id = ? AND flow_instance = ? AND entity_type = ?`
		if postgres {
			query = `UPDATE entity_state SET slug = NULLIF($1, ''), name = NULLIF($2, ''), current_state = $3,
				gates = $4::jsonb, fields = $5::jsonb, bookkeeping = $6::jsonb, accumulator = $7::jsonb,
				revision = revision + 1, entered_state_at = $8, updated_at = $9
				WHERE run_id = $10::uuid AND entity_id = $11::uuid AND flow_instance = $12 AND entity_type = $13`
		}
		args = []any{record.Slug, record.Name, record.CurrentState, string(record.Gates), string(record.Fields),
			string(record.Bookkeeping), string(record.Accumulator), record.EnteredStageAt, record.UpdatedAt,
			record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, record.EntityType}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return workflowEngineStateFact{}, fmt.Errorf("commit declared workflow fields: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return workflowEngineStateFact{}, fmt.Errorf("read declared workflow fields commit: %w", err)
	}
	if rows != 1 {
		return workflowEngineStateFact{}, fmt.Errorf("constructed workflow %s has no required field row", record.Identity.Route.InstancePath)
	}
	return fact, nil
}

func workflowEngineStateRevisionConflict(record runtimepipeline.WorkflowEngineStateRecord) error {
	return runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"workflow_engine_state_revision_conflict",
		"workflow-engine-persistence",
		"commit_state",
		map[string]any{
			"route": record.Identity.Route.InstancePath, "expected_revision": record.ExpectedRevision,
			"expected_state": record.ExpectedState,
		},
	)
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
		if _, err := runtimecanonicaljson.Decode(raw); err != nil {
			return nil, fmt.Errorf("validate workflow engine %s projection: %w", name, err)
		}
		if err := runtimecanonicaljson.DecodePreservingNumberLexemes(raw, &value); err != nil {
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
	decision, err := decideWorkflowEngineState(record.Transition)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	if decision.createState {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	query := `SELECT fi.current_state, es.fields, fi.bookkeeping, fi.gates, fi.accumulator
		FROM flow_instances fi LEFT JOIN entity_state es
			ON es.run_id = fi.run_id AND es.entity_id = fi.entity_id AND es.flow_instance = fi.instance_path
		WHERE fi.run_id = ? AND fi.entity_id = ? AND fi.instance_path = ?`
	args := []any{record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath}
	if postgres {
		query = `SELECT fi.current_state, es.fields, fi.bookkeeping, fi.gates, fi.accumulator
			FROM flow_instances fi LEFT JOIN entity_state es
				ON es.run_id = fi.run_id AND es.entity_id = fi.entity_id AND es.flow_instance = fi.instance_path
			WHERE fi.run_id = $1::uuid AND fi.entity_id = $2::uuid AND fi.instance_path = $3 FOR UPDATE OF fi`
	}
	var currentState string
	var fieldsRaw, bookkeepingRaw, gatesRaw, accumulatorRaw any
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&currentState, &fieldsRaw, &bookkeepingRaw, &gatesRaw, &accumulatorRaw); err != nil {
		if err == sql.ErrNoRows {
			return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("workflow engine state route is missing: %s", record.Identity.Route.InstancePath)
		}
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("load workflow engine state projection: %w", err)
	}
	if record.EntityType != "" && fieldsRaw == nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("constructed workflow %s has no required field row", record.Identity.Route.InstancePath)
	}
	if record.EntityType == "" && fieldsRaw != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("fieldless workflow %s has an unexpected field row", record.Identity.Route.InstancePath)
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
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
	before runtimemutationlog.EntityStateProjection,
	attribution ...*runtimemutationlog.Writer,
) error {
	if record.Transition.PreservesState() {
		return nil
	}
	decision, err := decideWorkflowEngineState(record.Transition)
	if err != nil {
		return err
	}
	after, err := workflowEngineStateProjection(record)
	if err != nil {
		return err
	}
	writer := runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: decision.historyStep}
	if len(attribution) != 0 && attribution[0] != nil {
		writer = *attribution[0]
	}
	return insertWorkflowEngineStateDiff(ctx, attempt, store, postgres, record.EntityID, before, after, writer, record.UpdatedAt)
}

func insertWorkflowEngineStateDiff(ctx context.Context, attempt *mutationprotocol.Attempt, store eventCommitTxStore, postgres bool, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, occurredAt time.Time) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if postgres {
			selected, ok := store.(*PipelinePostgresOwner)
			if !ok {
				return fmt.Errorf("workflow engine PostgreSQL mutation requires PostgreSQL selected store")
			}
			return privatemutationlog.InsertEntityStateDiff(ctx, attempt, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
				return selected.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), entityID, before, after, writer)
		}
		selected, ok := store.(*PipelineSQLiteOwner)
		if !ok {
			return fmt.Errorf("workflow engine SQLite mutation requires SQLite selected store")
		}
		return privatemutationlog.InsertSQLiteEntityStateDiff(ctx, attempt, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return selected.RunLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
		}), entityID, before, after, writer, occurredAt)
	})
}

func commitWorkflowEngineInitialValues(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	postgres bool,
	record runtimepipeline.WorkflowEngineStateRecord,
	before runtimemutationlog.EntityStateProjection,
) (runtimemutationlog.EntityStateProjection, error) {
	decision, err := decideWorkflowEngineState(record.Transition)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	var initial map[string]any
	if _, err := runtimecanonicaljson.Decode(record.InitialFields); err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("validate workflow engine initial fields: %w", err)
	}
	if err := runtimecanonicaljson.DecodePreservingNumberLexemes(record.InitialFields, &initial); err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode workflow engine initial fields: %w", err)
	}
	if !decision.createState || len(initial) == 0 {
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
		if err := insertWorkflowEngineStateDiff(ctx, attempt, store, postgres, record.EntityID, adjusted, next, writer, record.UpdatedAt); err != nil {
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
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowEngineMutation, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowEngineMutation],
	candidateWriter completionCandidateWriter,
	command runtimepipeline.WorkflowEngineMutationCommand,
) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	if command.State.Transition.PreservesState() && command.DeliverySuccess == nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("accepted-event preservation requires exact inbound delivery settlement")
	}
	stage, err := runtimepipeline.CommittedWorkflowStage(command.State)
	if err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
		result := runtimepipeline.CommittedWorkflowEngineMutation{
			Stage:        stage,
			Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(command.Publications)),
			PostCommit:   command.PostCommit,
		}
		var err error
		err = attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			// Run authority precedes the paired header/field projection. A stop
			// holding that authority must never wait for this writer's header lock.
			current, err := store.RequireActiveSourceTx(txctx, tx, command.State.Identity.RunID)
			if err != nil {
				return err
			}
			if command.Writer != nil {
				if !current.Matches(command.WriterSource) {
					return fmt.Errorf("entity mutation source does not match active run source")
				}
			}
			if !command.State.Transition.PreservesState() {
				transactiontest.Mark(txctx, transactiontest.WorkflowMutation)
			} else if err := requirePreservedWorkflowAcceptedEvent(txctx, tx, store, command); err != nil {
				return err
			}
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
			before, err := loadWorkflowEngineStateProjection(txctx, tx, postgres, command.State)
			if err != nil {
				return err
			}
			if err := commitWorkflowEngineState(txctx, attempt, postgres, command.State); err != nil {
				return err
			}
			if command.RouteRetirement != nil {
				retirement := *command.RouteRetirement
				attemptQuery := `SELECT activation_attempt_id::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
				if !postgres {
					attemptQuery = `SELECT activation_attempt_id FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
				}
				var attemptID sql.NullString
				if err := tx.QueryRowContext(txctx, attemptQuery, retirement.Identity.RunID, retirement.Identity.Route.InstancePath).Scan(&attemptID); err != nil && err != sql.ErrNoRows {
					return fmt.Errorf("load committed flow route activation attempt: %w", err)
				}
				if attemptID.Valid {
					retirement.ActivationAttemptID = attemptID.String
				}
				sets := []runtimebus.FlowInstanceRouteRecordSet{{Identity: command.RouteRetirement.Identity}}
				if _, err := replaceFlowInstanceRouteTopologyTx(txctx, tx, postgres, sets); err != nil {
					return fmt.Errorf("retire terminal workflow route: %w", err)
				}
				result.RouteRetirement = &retirement
			}
			before, err = commitWorkflowEngineInitialValues(txctx, attempt, store, postgres, command.State, before)
			if err != nil {
				return err
			}
			result.Lifecycle, err = commitWorkflowEngineLifecycle(txctx, attempt, store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), store.workflowTurnTerminationOwner(), postgres, command.Lifecycle)
			if err != nil {
				return err
			}
			if command.Lifecycle.RequestCompletionCandidate {
				if _, err := attempt.RequestCompletion(txctx, candidateWriter, command.State.Identity.RunID, nil); err != nil {
					return err
				}
			}
			if err := commitWorkflowEngineMutationLog(txctx, attempt, store, postgres, command.State, before, command.Writer); err != nil {
				return err
			}
			for index, proposed := range command.ProposedEffects {
				if err := store.workflowDecisionLifecycleOwner().InsertProposedEffectTx(txctx, attempt, proposed.Card, proposed.Continuation); err != nil {
					return fmt.Errorf("commit workflow engine proposed effect %d: %w", index, err)
				}
			}
			if command.FanOutIntent != nil {
				runID := command.State.Identity.RunID
				fields := command.State.Fields
				triggerEventID := command.FanOutIntent.Capsule.Lineage.ParentEventID
				createdAt := command.State.UpdatedAt
				if err := commitFanOutIntentTx(txctx, attempt, postgres, store.resourceSourceOwner(), *command.FanOutIntent, runID, fields, triggerEventID, createdAt); err != nil {
					return err
				}
				if command.FanOutBarrier != nil {
					if err := commitFanOutBarrierRegistrationTx(txctx, attempt, postgres, *command.FanOutBarrier); err != nil {
						return err
					}
				}
			}
			for index, value := range command.Publications {
				plan, ok := value.(runtimebus.EnginePublicationPlan)
				if !ok {
					return fmt.Errorf("workflow engine publication %d has unexpected type %T", index, value)
				}
				publication, err := plan.PublicationCommandForMutation(command.State, command.Lifecycle)
				if err != nil {
					return err
				}
				committed, err := store.commitPublicationTx(txctx, attempt, publication)
				if err != nil {
					return fmt.Errorf("commit workflow engine publication %d: %w", index, err)
				}
				evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
				if err != nil {
					return err
				}
				result.Publications = append(result.Publications, evidence)
			}
			if command.FanOutBarrierCompletion != nil {
				runID := command.State.Identity.RunID
				updatedAt := command.State.UpdatedAt
				if err := commitFanOutBarrierCompletionTx(txctx, attempt, postgres, runID, *command.FanOutBarrierCompletion, updatedAt); err != nil {
					return err
				}
			}
			if success := command.DeliverySuccess; success != nil {
				settled, err := store.SettleWorkflowNodeSuccessTx(
					txctx,
					attempt,
					success.Claim,
					append([]string(nil), success.SideEffects...),
					success.Duration,
					success.RuleSelection,
				)
				if err != nil {
					return fmt.Errorf("settle workflow node delivery with engine mutation: %w", err)
				}
				if !command.Lifecycle.RequestCompletionCandidate {
					if _, err := attempt.RequestCompletion(txctx, candidateWriter, success.Claim.RunID(), nil); err != nil {
						return err
					}
				}
				claim := success.Claim
				if err := persistWorkflowHandlerStageReceiptTx(txctx, tx, claim, settled, stage); err != nil {
					return err
				}
				result.DeliverySuccess = &claim
			}
			return nil
		})
		return result, err
	})
	result, committed := outcome.Value()
	if !committed {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, outcome.Err()
	}
	result.Committed = true
	result.Lifecycle = result.Lifecycle.WithCommitAcknowledgment()
	for index, publication := range result.Publications {
		result.Publications[index] = publication.(runtimebus.CommittedEnginePublication).WithCommitAcknowledgment()
	}
	return result, errors.Join(outcome.Err(), result.Validate())
}

// This bounded cross-domain projection binds the lifecycle cause to its exact
// inbound delivery. Fencing, expiry and renewal remain the delivery owner's work.
func requirePreservedWorkflowAcceptedEvent(ctx context.Context, tx *sql.Tx, store eventCommitTxStore, command runtimepipeline.WorkflowEngineMutationCommand) error {
	current, err := store.RequireActiveSourceTx(ctx, tx, command.State.Identity.RunID)
	if err != nil {
		return err
	}
	if !current.Matches(command.AcceptedEventSource) {
		return fmt.Errorf("preserved workflow accepted-event source disagrees with current run authority")
	}
	return store.RequireWorkflowAcceptedEventTx(ctx, tx, command.DeliverySuccess.Claim, *command.AcceptedEvent)
}

func (s *PipelinePostgresOwner) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	return commitWorkflowEngineMutation(ctx, s, true, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowEngineMutation, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowEngineMutation] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecyclePostgresOwner, command)
}

func (s *PipelineSQLiteOwner) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.CommittedWorkflowEngineMutation{}, err
	}
	return commitWorkflowEngineMutation(ctx, s, false, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowEngineMutation, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowEngineMutation] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite workflow engine mutation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecycleSQLiteOwner, command)
}

var _ runtimepipeline.WorkflowEngineMutationOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowEngineMutationOwner = (*PipelineSQLiteOwner)(nil)
