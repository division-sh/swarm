package runtimepersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// These forwarding methods are confined to same-package atomicity fixtures.
func (s *PostgresStore) AppendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.eventPostgresOwner.AppendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *SQLiteRuntimeStore) AppendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.eventSQLiteOwner.AppendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *PostgresStore) RequirePipelinePublicationClaimTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim) error {
	return s.pipelinePostgresOwner.RequirePipelinePublicationClaimTx(ctx, tx, eventID, claim)
}

func (s *SQLiteRuntimeStore) RequirePipelinePublicationClaimTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim) error {
	return s.pipelineSQLiteOwner.RequirePipelinePublicationClaimTx(ctx, tx, eventID, claim)
}

func (s *PostgresStore) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	return s.deliveryPostgresOwner.CommitInitialDeliveryObligationsTx(ctx, attempt, eventID, runID, routes, authority)
}

func (s *SQLiteRuntimeStore) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	return s.deliverySQLiteOwner.CommitInitialDeliveryObligationsTx(ctx, attempt, eventID, runID, routes, authority)
}

func (s *PostgresStore) CommitInitialPipelineScopeTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	return s.pipelinePostgresOwner.CommitInitialPipelineScopeTx(ctx, attempt, eventID, scope)
}

func (s *SQLiteRuntimeStore) CommitInitialPipelineScopeTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	return s.pipelineSQLiteOwner.CommitInitialPipelineScopeTx(ctx, attempt, eventID, scope)
}

func (s *PostgresStore) CommitInitialPipelineDispositionTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	return s.pipelinePostgresOwner.CommitInitialPipelineDispositionTx(ctx, attempt, eventID, claim, disposition)
}

func (s *SQLiteRuntimeStore) CommitInitialPipelineDispositionTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID string, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	return s.pipelineSQLiteOwner.CommitInitialPipelineDispositionTx(ctx, attempt, eventID, claim, disposition)
}

func (s *PostgresStore) RecordDeadLetterTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimedeadletters.Record, requireActive bool) error {
	return s.deliveryPostgresOwner.RecordDeadLetterTx(ctx, attempt, record, requireActive)
}

func (s *SQLiteRuntimeStore) RecordDeadLetterTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimedeadletters.Record, requireActive bool) error {
	return s.deliverySQLiteOwner.RecordDeadLetterTx(ctx, attempt, record, requireActive)
}

func (s *PostgresStore) CreateWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.replyPostgresOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *SQLiteRuntimeStore) CreateWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.replySQLiteOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *PostgresStore) ClaimWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.replyPostgresOwner.ClaimWithinTransaction(ctx, attempt, command)
}

func (s *SQLiteRuntimeStore) ClaimWithinTransaction(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.replySQLiteOwner.ClaimWithinTransaction(ctx, attempt, command)
}

func (s *PostgresStore) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return s.pipelinePostgresOwner.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, request)
}

func (s *SQLiteRuntimeStore) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return s.pipelineSQLiteOwner.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, request)
}

func (s *PostgresStore) CommitFlowInstanceActivationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return s.pipelinePostgresOwner.CommitFlowInstanceActivationsTx(ctx, attempt, plans)
}

func (s *SQLiteRuntimeStore) CommitFlowInstanceActivationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return s.pipelineSQLiteOwner.CommitFlowInstanceActivationsTx(ctx, attempt, plans)
}

func (s *PostgresStore) ReplaceFlowInstanceRouteTopologyTx(ctx context.Context, tx *sql.Tx, sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	return s.pipelinePostgresOwner.ReplaceFlowInstanceRouteTopologyTx(ctx, tx, sets)
}

func (s *SQLiteRuntimeStore) ReplaceFlowInstanceRouteTopologyTx(ctx context.Context, tx *sql.Tx, sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	return s.pipelineSQLiteOwner.ReplaceFlowInstanceRouteTopologyTx(ctx, tx, sets)
}

func (s *PostgresStore) MarkDynamicFlowCreationOccurrenceCommittedTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error {
	return s.pipelinePostgresOwner.MarkDynamicFlowCreationOccurrenceCommittedTx(ctx, tx, request)
}

func (s *SQLiteRuntimeStore) MarkDynamicFlowCreationOccurrenceCommittedTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error {
	return s.pipelineSQLiteOwner.MarkDynamicFlowCreationOccurrenceCommittedTx(ctx, tx, request)
}

func (s *PostgresStore) TerminalizeRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, disposition runtimepipelineobligation.Disposition, at time.Time) (int, error) {
	return s.pipelinePostgresOwner.TerminalizeRunTx(ctx, attempt, runID, disposition, at)
}

func (s *SQLiteRuntimeStore) TerminalizeRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, disposition runtimepipelineobligation.Disposition, at time.Time) (int, error) {
	return s.pipelineSQLiteOwner.TerminalizeRunTx(ctx, attempt, runID, disposition, at)
}
