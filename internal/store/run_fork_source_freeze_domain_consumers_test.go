package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type forkedDomainConsumerSurface interface {
	CreateEntity(context.Context, runtimetools.EntityCreateRecord) error
	SaveEntityField(context.Context, runtimetools.EntityFieldUpdate) (int, error)
	RecordSpend(context.Context, budgetspend.SpendRecord) error
	ListBudgetProjectionTargets(context.Context, []string) ([]budgetspend.ProjectionTarget, error)
	UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error
	RollbackFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error
	ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error)
	RecordDeadLetter(context.Context, runtimedeadletters.Record) error
}

func TestForkedSourceEntityMutationLogBudgetRouteAndDeadLetterConsumersRefuse(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			ctx := runtimecorrelation.WithRunID(context.Background(), fixture.sourceRun)
			var surface forkedDomainConsumerSurface
			if fixture.postgres != nil {
				surface = fixture.postgres
			} else {
				surface = fixture.sqlite
			}

			entityID := uuid.NewString()
			entity := runtimetools.EntityCreateRecord{
				RunID: fixture.sourceRun, EntityID: entityID, FlowInstance: "freeze/domain", EntityType: "work_item",
				CurrentState: "active", FieldsJSON: json.RawMessage(`{"value":1}`), CreatedAt: fixture.forkedAt.Add(-time.Minute),
				Writer: runtimetools.EntityMutationWriter{Type: "platform", ID: "source-freeze"},
			}
			if err := surface.CreateEntity(ctx, entity); err != nil {
				t.Fatal(err)
			}
			mutationQuery := `SELECT COUNT(*) FROM entity_mutations WHERE entity_id = ?`
			if fixture.postgres != nil {
				mutationQuery = `SELECT COUNT(*) FROM entity_mutations WHERE entity_id = $1::uuid`
			}
			var baselineMutationRows int
			if err := fixture.db.QueryRowContext(ctx, mutationQuery, entityID).Scan(&baselineMutationRows); err != nil {
				t.Fatal(err)
			}
			seedForkedFlowInstance(t, fixture, entity.FlowInstance)
			route := runtimebus.FlowInstanceRouteRecord{
				Identity: runtimeflowidentity.DeriveRoute("freeze", "domain"), EventPattern: "freeze/domain/input",
				SubscriberType: "node", SubscriberID: "freeze-node", SourceFlow: "freeze",
			}
			if err := surface.UpsertFlowInstanceRoute(ctx, route); err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			insertForkedConsumerEvent(t, fixture, eventID, "freeze.domain", fixture.forkedAt.Add(-time.Minute))

			fixture.freeze(t)

			lateEntity := entity
			lateEntity.EntityID = uuid.NewString()
			requireForkedSourceRefusal(t, "create entity", surface.CreateEntity(ctx, lateEntity))
			_, err := surface.SaveEntityField(ctx, runtimetools.EntityFieldUpdate{
				RunID: fixture.sourceRun, EntityID: entityID, FieldPath: "value", ValueJSON: json.RawMessage(`2`),
				Writer: runtimetools.EntityMutationWriter{Type: "platform", ID: "source-freeze"},
			})
			requireForkedSourceRefusal(t, "save entity field and mutation log", err)
			requireForkedSourceRefusal(t, "record spend", surface.RecordSpend(ctx, budgetspend.SpendRecord{
				ExecutionMode: runtimeeffects.ExecutionModeLive, EntityID: entityID, FlowInstance: entity.FlowInstance,
				AgentID: "freeze-agent", Model: "test", ModelAlias: "regular", BackendProfile: "test",
				Provider: "test", Transport: "test", ResolvedModel: "test", InputTokens: 1, OutputTokens: 1,
				CostUSD: 0.01, InvocationType: "test", UsageAccounting: "exact", RecordedAt: fixture.forkedAt,
			}))
			targets, err := surface.ListBudgetProjectionTargets(ctx, []string{"done"})
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range targets {
				if target.RunID == fixture.sourceRun {
					t.Fatalf("budget recovery selected frozen source: %#v", target)
				}
			}

			requireForkedSourceRefusal(t, "upsert flow route", surface.UpsertFlowInstanceRoute(ctx, route))
			requireForkedSourceRefusal(t, "delete flow route", surface.DeleteFlowInstanceRoute(ctx, route.Identity))
			requireForkedSourceRefusal(t, "rollback flow route", surface.RollbackFlowInstanceRoute(ctx, route.Identity))
			routes, err := surface.ListFlowInstanceRoutes(ctx)
			if err != nil {
				t.Fatal(err)
			}
			foundRoute := false
			for _, listed := range routes {
				foundRoute = foundRoute || listed.InstancePath == route.Identity.InstancePath
			}
			if !foundRoute {
				t.Fatalf("run-independent structural route disappeared after source freeze: %#v", routes)
			}

			requireForkedSourceRefusal(t, "record dead letter", surface.RecordDeadLetter(ctx, runtimedeadletters.Record{
				OriginalEventID: eventID,
				Failure:         testFailureEnvelope(runtimefailures.ClassRetryExhausted, "frozen_source", nil),
				RetryCount:      1,
				HandlerNode:     "freeze-node",
			}))

			var entityRevision, mutationRows, spendRows, deadLetterRows int
			query := `SELECT revision FROM entity_state WHERE run_id = ? AND entity_id = ?`
			spendQuery := `SELECT COUNT(*) FROM spend_ledger WHERE entity_id = ?`
			deadLetterQuery := `SELECT COUNT(*) FROM dead_letters WHERE original_event_id = ?`
			if fixture.postgres != nil {
				query = `SELECT revision FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
				spendQuery = `SELECT COUNT(*) FROM spend_ledger WHERE entity_id = $1::uuid`
				deadLetterQuery = `SELECT COUNT(*) FROM dead_letters WHERE original_event_id = $1::uuid`
			}
			if err := fixture.db.QueryRowContext(ctx, query, fixture.sourceRun, entityID).Scan(&entityRevision); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, mutationQuery, entityID).Scan(&mutationRows); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, spendQuery, entityID).Scan(&spendRows); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRowContext(ctx, deadLetterQuery, eventID).Scan(&deadLetterRows); err != nil {
				t.Fatal(err)
			}
			if entityRevision != 1 || mutationRows != baselineMutationRows || spendRows != 0 || deadLetterRows != 0 {
				t.Fatalf("frozen domain rows revision=%d mutations=%d spend=%d dead_letters=%d", entityRevision, mutationRows, spendRows, deadLetterRows)
			}
		})
	}
}

func seedForkedFlowInstance(t *testing.T, fixture *forkedConsumerTestBackend, instancePath string) {
	t.Helper()
	query := `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES (?, 'freeze', 'template', '{}', 'active', ?)`
	if fixture.postgres != nil {
		query = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES ($1, 'freeze', 'template', '{}'::jsonb, 'active', $2)`
	}
	if _, err := fixture.db.ExecContext(context.Background(), query, instancePath, fixture.forkedAt.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
}

type forkedDirectiveConsumerSurface interface {
	ReserveDirectiveOperation(context.Context, agentcontrol.ReserveDirectiveOperationRequest) (agentcontrol.DirectiveOperationReservation, error)
	AdmitDirectiveExecution(context.Context, string, string, time.Time, time.Duration) (agentcontrol.DirectiveOperation, error)
	RenewDirectiveExecutionLease(context.Context, string, string, time.Time, time.Duration) error
	RecordDirectiveExecuted(context.Context, string, string, json.RawMessage, time.Time) (agentcontrol.DirectiveOperation, error)
	FinalizeDirectiveSuccess(context.Context, string, time.Time, time.Duration) (agentcontrol.DirectiveOperation, error)
	FinalizeDirectiveFailure(context.Context, string, string, runtimefailures.Envelope, time.Time, time.Duration) (agentcontrol.DirectiveOperation, error)
	ReconcileDirectiveOperations(context.Context, time.Time, time.Duration) (agentcontrol.DirectiveOperationReconcileResult, error)
	ReconcileDirectiveOperation(context.Context, string, time.Time, time.Duration) (agentcontrol.DirectiveOperation, bool, error)
}

func TestForkedSourceDirectiveReservationTransitionsAndRecoveryRefuse(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			fixture.freeze(t)
			var surface forkedDirectiveConsumerSurface
			if fixture.postgres != nil {
				surface = fixture.postgres
			} else {
				surface = fixture.sqlite
			}
			ctx := context.Background()
			now := fixture.forkedAt.Add(time.Minute)
			request := forkedDirectiveReservation(t, fixture.sourceRun, now)
			_, err := surface.ReserveDirectiveOperation(ctx, request)
			requireForkedSourceRefusal(t, "reserve directive", err)

			operationID, ownerID := uuid.NewString(), uuid.NewString()
			seedForkedDirectiveOperation(t, fixture, operationID, ownerID, now)
			_, err = surface.AdmitDirectiveExecution(ctx, operationID, ownerID, now, time.Minute)
			requireForkedSourceRefusal(t, "admit directive", err)
			requireForkedSourceRefusal(t, "renew directive", surface.RenewDirectiveExecutionLease(ctx, operationID, ownerID, now, time.Minute))
			_, err = surface.RecordDirectiveExecuted(ctx, operationID, ownerID, json.RawMessage(`{"ok":true}`), now)
			requireForkedSourceRefusal(t, "record directive executed", err)
			_, err = surface.FinalizeDirectiveSuccess(ctx, operationID, now, time.Hour)
			requireForkedSourceRefusal(t, "finalize directive success", err)
			_, err = surface.FinalizeDirectiveFailure(ctx, operationID, ownerID, agentcontrol.DirectiveExecutionLeaseExpiredFailure(), now, time.Hour)
			requireForkedSourceRefusal(t, "finalize directive failure", err)
			summary, err := surface.ReconcileDirectiveOperations(ctx, now, time.Hour)
			if err != nil || summary != (agentcontrol.DirectiveOperationReconcileResult{}) {
				t.Fatalf("directive recovery selected frozen operation = %#v, %v", summary, err)
			}
			_, _, err = surface.ReconcileDirectiveOperation(ctx, operationID, now, time.Hour)
			requireForkedSourceRefusal(t, "reconcile exact directive", err)

			var state string
			query := `SELECT state FROM agent_directive_operations WHERE operation_id = ?`
			if fixture.postgres != nil {
				query = `SELECT state FROM agent_directive_operations WHERE operation_id = $1::uuid`
			}
			if err := fixture.db.QueryRowContext(ctx, query, operationID).Scan(&state); err != nil || state != "executing" {
				t.Fatalf("frozen directive state = %q, %v", state, err)
			}
		})
	}
}

func forkedDirectiveReservation(t *testing.T, runID string, now time.Time) agentcontrol.ReserveDirectiveOperationRequest {
	t.Helper()
	operationID, eventID := uuid.NewString(), uuid.NewString()
	request := agentcontrol.SendDirectiveRequest{
		AgentID: "freeze-agent", Directive: "continue", RunID: runID, Source: agentcontrol.DirectiveSourceV1RPC, OperatorID: "operator",
	}
	event, err := agentcontrol.NewDirectiveEvent(request, agentcontrol.RunTargetResolution{RunID: runID, Mode: agentcontrol.RunResolutionSpecified}, operationID, eventID, now)
	if err != nil {
		t.Fatal(err)
	}
	return agentcontrol.ReserveDirectiveOperationRequest{
		Operation: agentcontrol.DirectiveOperation{
			OperationID: operationID, Method: agentcontrol.DirectiveOperationMethod, ActorTokenID: "operator",
			RequestHash: "frozen-request", AgentID: request.AgentID, Directive: request.Directive,
			RequestedRunID: runID, ResolvedRunID: runID, RunIDResolution: agentcontrol.RunResolutionSpecified,
			Source: request.Source, OperatorID: request.OperatorID, DirectiveEventID: eventID, State: agentcontrol.DirectiveOperationPrepared,
		},
		Event: event, Now: now,
	}
}

func seedForkedDirectiveOperation(t *testing.T, fixture *forkedConsumerTestBackend, operationID, ownerID string, now time.Time) {
	t.Helper()
	eventID := uuid.NewString()
	insertForkedConsumerEvent(t, fixture, eventID, "agent.directive", now)
	query := `INSERT INTO agent_directive_operations (
		operation_id, method, actor_token_id, request_hash, agent_id, directive_text, resolved_run_id,
		run_id_resolution, source, directive_event_id, state, execution_owner_id, execution_admitted_at,
		execution_lease_expires_at, created_at, updated_at
	) VALUES (?, 'agent.send_directive', 'operator', 'frozen-request', 'freeze-agent', 'continue', ?,
		'specified', 'v1_rpc', ?, 'executing', ?, ?, ?, ?, ?)`
	if fixture.postgres != nil {
		query = `INSERT INTO agent_directive_operations (
			operation_id, method, actor_token_id, request_hash, agent_id, directive_text, resolved_run_id,
			run_id_resolution, source, directive_event_id, state, execution_owner_id, execution_admitted_at,
			execution_lease_expires_at, created_at, updated_at
		) VALUES ($1::uuid, 'agent.send_directive', 'operator', 'frozen-request', 'freeze-agent', 'continue', $2::uuid,
			'specified', 'v1_rpc', $3::uuid, 'executing', $4, $5, $6, $5, $5)`
	}
	args := []any{operationID, fixture.sourceRun, eventID, ownerID, now, now.Add(-time.Second), now, now}
	if fixture.postgres != nil {
		args = args[:6]
	}
	if _, err := fixture.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

type forkedEffectConsumerSurface interface {
	IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error)
	AuthorizeExternalAttempt(context.Context, runtimeeffects.Authority, runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error)
	MarkExternalAttemptLaunched(context.Context, runtimeeffects.Attempt, time.Time) error
	HeartbeatCompletionAttempt(context.Context, runtimeeffects.Attempt, time.Time, time.Duration) error
	MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error
	SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error
	ReconcileExternalEffectAttempts(context.Context, time.Time) (runtimeeffects.RecoverySummary, error)
}

func TestForkedSourceManagedExternalEffectAdmissionTransitionsAndRecoveryRefuse(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			fixture.freeze(t)
			var surface forkedEffectConsumerSurface
			if fixture.postgres != nil {
				surface = fixture.postgres
			} else {
				surface = fixture.sqlite
			}
			now := fixture.forkedAt.Add(time.Minute)
			token := runtimeeffects.LifecycleToken{RuntimeEpoch: 1, AgentID: "freeze-effect-agent", Generation: 1}
			authority := runtimeeffects.NormalAgentAuthority(token, "freeze-worker", now.Add(time.Minute))
			authority.Target = runtimeeffects.UsageTarget{
				Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: fixture.sourceRun,
				AgentID: token.AgentID, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "freeze/effect",
			}
			ctx := runtimecorrelation.WithRunID(context.Background(), fixture.sourceRun)
			if current, err := surface.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("frozen external authority current=%v err=%v", current, err)
			}
			req := runtimeeffects.AuthorizeRequest{
				OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: runtimeeffects.KindNativeCommand,
				Class: runtimeeffects.EffectWriteOrUnknown, Adapter: "native_command", Transport: "process",
				RequestFingerprint: "sha256:frozen", Lineage: map[string]string{"run_id": fixture.sourceRun}, Now: now,
			}
			_, err := surface.AuthorizeExternalAttempt(ctx, authority, req)
			requireForkedSourceRefusal(t, "authorize external attempt", err)
			attempt := runtimeeffects.Attempt{
				OperationID: req.OperationID, AttemptID: req.AttemptID, Token: token, Authority: authority,
				Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport, Ordinal: 1, AuthorizedAt: now,
			}
			requireForkedSourceRefusal(t, "mark external launch", surface.MarkExternalAttemptLaunched(ctx, attempt, now))
			requireForkedSourceRefusal(t, "heartbeat external attempt", surface.HeartbeatCompletionAttempt(ctx, attempt, now, time.Minute))
			requireForkedSourceRefusal(t, "mark external response", surface.MarkExternalAttemptResponseObserved(ctx, attempt, map[string]any{"ok": true}, now))
			requireForkedSourceRefusal(t, "settle external attempt", surface.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
				OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: authority,
				State: runtimeeffects.StateSettled, Evidence: map[string]any{"ok": true}, Now: now,
			}))

			operationID, attemptID := seedForkedExternalEffectAttempt(t, fixture, now)
			summary, err := surface.ReconcileExternalEffectAttempts(ctx, now.Add(time.Hour))
			if err != nil || summary != (runtimeeffects.RecoverySummary{}) {
				t.Fatalf("external-effect recovery selected frozen source = %#v, %v", summary, err)
			}
			var operationState, attemptState string
			query := `SELECT o.state, a.state FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id WHERE o.operation_id = ? AND a.attempt_id = ?`
			if fixture.postgres != nil {
				query = `SELECT o.state, a.state FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id WHERE o.operation_id = $1::uuid AND a.attempt_id = $2::uuid`
			}
			if err := fixture.db.QueryRowContext(ctx, query, operationID, attemptID).Scan(&operationState, &attemptState); err != nil || operationState != "launched" || attemptState != "launched" {
				t.Fatalf("frozen external effect states = %s/%s, %v", operationState, attemptState, err)
			}
		})
	}
}

func seedForkedExternalEffectAttempt(t *testing.T, fixture *forkedConsumerTestBackend, now time.Time) (string, string) {
	t.Helper()
	agentID, operationID, attemptID := "frozen-recovery-agent-"+uuid.NewString(), uuid.NewString(), uuid.NewString()
	agentQuery := `INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, status, created_at) VALUES (?, 'freeze/effect', 'worker', 'standard', 'mock', TRUE, 'authored', 'active', ?)`
	operationQuery := `INSERT INTO runtime_external_effect_operations (
		operation_id, effect_kind, effect_class, execution_mode, authority_kind, authority_id, agent_id,
		runtime_epoch, generation, authority_evidence, lineage, request_fingerprint, state, created_at, updated_at
	) VALUES (?, 'native_command', 'write_or_unknown', 'live', 'normal_agent', ?, ?, 1, 1, '{}', ?, 'frozen-fingerprint', 'launched', ?, ?)`
	attemptQuery := `INSERT INTO runtime_external_effect_attempts (
		attempt_id, operation_id, attempt_ordinal, adapter, transport, execution_mode, runtime_epoch, generation,
		execution_owner, lease_expires_at, fence_generation, state, evidence, authorized_at, launched_at, updated_at
	) VALUES (?, ?, 1, 'native_command', 'process', 'live', 1, 1, 'freeze-worker', ?, 1, 'launched', '{}', ?, ?, ?)`
	lineage := `{"run_id":"` + fixture.sourceRun + `"}`
	if fixture.postgres != nil {
		agentQuery = `INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, status, created_at) VALUES ($1, 'freeze/effect', 'worker', 'standard', 'mock', TRUE, 'authored', 'active', $2)`
		operationQuery = `INSERT INTO runtime_external_effect_operations (
			operation_id, effect_kind, effect_class, execution_mode, authority_kind, authority_id, agent_id,
			runtime_epoch, generation, authority_evidence, lineage, request_fingerprint, state, created_at, updated_at
		) VALUES ($1::uuid, 'native_command', 'write_or_unknown', 'live', 'normal_agent', $2, $3, 1, 1, '{}'::jsonb, $4::jsonb, 'frozen-fingerprint', 'launched', $5, $5)`
		attemptQuery = `INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, execution_mode, runtime_epoch, generation,
			execution_owner, lease_expires_at, fence_generation, state, evidence, authorized_at, launched_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 1, 'native_command', 'process', 'live', 1, 1, 'freeze-worker', $3, 1, 'launched', '{}'::jsonb, $4, $4, $4)`
	}
	if _, err := fixture.db.ExecContext(context.Background(), agentQuery, agentID, now); err != nil {
		t.Fatal(err)
	}
	operationArgs := []any{operationID, agentID, agentID, lineage, now, now}
	if fixture.postgres != nil {
		operationArgs = operationArgs[:5]
	}
	if _, err := fixture.db.ExecContext(context.Background(), operationQuery, operationArgs...); err != nil {
		t.Fatal(err)
	}
	attemptArgs := []any{attemptID, operationID, now.Add(-time.Minute), now, now, now}
	if fixture.postgres != nil {
		attemptArgs = attemptArgs[:4]
	}
	if _, err := fixture.db.ExecContext(context.Background(), attemptQuery, attemptArgs...); err != nil {
		t.Fatal(err)
	}
	return operationID, attemptID
}
