package runtimepersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// These forwarding methods expose raw transaction hooks only to same-package
// atomicity fixtures. They are absent from every production method set.
func (s *PostgresStore) AppendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.eventPostgresOwner.AppendAdmittedEventTxOutcome(ctx, tx, story, privaterunforkrevision.NewEffects(), admitted, settlement)
}

func (s *SQLiteRuntimeStore) AppendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.eventSQLiteOwner.AppendAdmittedEventTxOutcome(ctx, tx, story, privaterunforkrevision.NewEffects(), admitted, settlement)
}

func (s *PostgresStore) RequirePipelinePublicationClaimTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim) error {
	return s.pipelinePostgresOwner.RequirePipelinePublicationClaimTx(ctx, tx, eventID, claim)
}

func (s *SQLiteRuntimeStore) RequirePipelinePublicationClaimTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim) error {
	return s.pipelineSQLiteOwner.RequirePipelinePublicationClaimTx(ctx, tx, eventID, claim)
}

func (s *PostgresStore) CommitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	return s.deliveryPostgresOwner.CommitInitialDeliveryObligationsTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, runID, routes, authority)
}

func (s *SQLiteRuntimeStore) CommitInitialDeliveryObligationsTx(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	return s.deliverySQLiteOwner.CommitInitialDeliveryObligationsTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, runID, routes, authority)
}

func (s *PostgresStore) CommitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	return s.pipelinePostgresOwner.CommitInitialPipelineScopeTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, scope)
}

func (s *SQLiteRuntimeStore) CommitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	return s.pipelineSQLiteOwner.CommitInitialPipelineScopeTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, scope)
}

func (s *PostgresStore) CommitInitialPipelineDispositionTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	return s.pipelinePostgresOwner.CommitInitialPipelineDispositionTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, claim, disposition)
}

func (s *SQLiteRuntimeStore) CommitInitialPipelineDispositionTx(ctx context.Context, tx *sql.Tx, eventID string, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	return s.pipelineSQLiteOwner.CommitInitialPipelineDispositionTx(ctx, tx, privaterunforkrevision.NewEffects(), eventID, claim, disposition)
}

func (s *PostgresStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, record runtimedeadletters.Record, requireActive bool) error {
	return s.deliveryPostgresOwner.RecordDeadLetterTx(ctx, tx, story, privaterunforkrevision.NewEffects(), record, requireActive)
}

func (s *SQLiteRuntimeStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, record runtimedeadletters.Record, requireActive bool) error {
	return s.deliverySQLiteOwner.RecordDeadLetterTx(ctx, tx, story, privaterunforkrevision.NewEffects(), record, requireActive)
}

func (s *PostgresStore) CreateWithinTransaction(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.replyPostgresOwner.CreateWithinTransaction(ctx, tx, privaterunforkrevision.NewEffects(), record)
}

func (s *SQLiteRuntimeStore) CreateWithinTransaction(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.replySQLiteOwner.CreateWithinTransaction(ctx, tx, privaterunforkrevision.NewEffects(), record)
}

func (s *PostgresStore) ClaimWithinTransaction(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.replyPostgresOwner.ClaimWithinTransaction(ctx, tx, privaterunforkrevision.NewEffects(), command)
}

func (s *SQLiteRuntimeStore) ClaimWithinTransaction(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.replySQLiteOwner.ClaimWithinTransaction(ctx, tx, privaterunforkrevision.NewEffects(), command)
}

func (s *PostgresStore) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return s.pipelinePostgresOwner.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, request)
}

func (s *SQLiteRuntimeStore) PrepareDynamicFlowCreationOccurrenceCommitTx(ctx context.Context, tx *sql.Tx, request runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error) {
	return s.pipelineSQLiteOwner.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, request)
}

func (s *PostgresStore) CommitFlowInstanceActivationsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return s.pipelinePostgresOwner.CommitFlowInstanceActivationsTx(ctx, tx, story, privaterunforkrevision.NewEffects(), plans)
}

func (s *SQLiteRuntimeStore) CommitFlowInstanceActivationsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return s.pipelineSQLiteOwner.CommitFlowInstanceActivationsTx(ctx, tx, story, privaterunforkrevision.NewEffects(), plans)
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

func (s *PostgresStore) TerminalizeRunTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, disposition runtimepipelineobligation.Disposition, at time.Time) (int, error) {
	return s.pipelinePostgresOwner.TerminalizeRunTx(ctx, tx, effects, runID, disposition, at)
}

func (s *SQLiteRuntimeStore) TerminalizeRunTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, disposition runtimepipelineobligation.Disposition, at time.Time) (int, error) {
	return s.pipelineSQLiteOwner.TerminalizeRunTx(ctx, tx, effects, runID, disposition, at)
}
