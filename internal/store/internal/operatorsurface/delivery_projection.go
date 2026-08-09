package operatorsurface

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type deliveryLifecycleEventMetadata struct {
	EventName string
	RunID     string
	EntityID  string
}

func (s *OperatorPostgres) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return operatorPostgresDelivery.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *OperatorSQLite) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return operatorSQLiteDelivery.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *OperatorPostgres) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorPostgresDelivery.LifecycleSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *OperatorSQLite) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorSQLiteDelivery.LifecycleSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *OperatorPostgres) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorPostgresDelivery.DiagnosticSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *OperatorSQLite) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorSQLiteDelivery.DiagnosticSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *OperatorPostgres) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return operatorPostgresDelivery.DiagnosticCountsForAgentSince(ctx, s.backend, identity, since)
}

func (s *OperatorSQLite) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return operatorSQLiteDelivery.DiagnosticCountsForAgentSince(ctx, s.backend, identity, since)
}

func (s *OperatorPostgres) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return operatorPostgresDelivery.RunDiagnosticCounts(ctx, s.backend, runID)
}

func (s *OperatorSQLite) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return operatorSQLiteDelivery.RunDiagnosticCounts(ctx, s.backend, runID)
}

func (s *OperatorPostgres) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return operatorPostgresDelivery.RunDiagnosticFailures(ctx, s.backend, runID, limit)
}

func (s *OperatorSQLite) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return operatorSQLiteDelivery.RunDiagnosticFailures(ctx, s.backend, runID, limit)
}

func (s *OperatorPostgres) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return operatorPostgresDelivery.RunTraceReferencePage(ctx, s.backend, query)
}

func (s *OperatorSQLite) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return operatorSQLiteDelivery.RunTraceReferencePage(ctx, s.backend, query)
}

func (s *OperatorPostgres) summarizeDeliveryRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return operatorPostgresDelivery.SummarizeRun(ctx, s.backend, runID)
}

func (s *OperatorSQLite) summarizeDeliveryRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return operatorSQLiteDelivery.SummarizeRun(ctx, s.backend, runID)
}
