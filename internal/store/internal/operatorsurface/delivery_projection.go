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

func (s *ObservabilityPostgres) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return operatorPostgresDelivery.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *ObservabilitySQLite) deliverySnapshotsForEvent(ctx context.Context, eventID string) ([]runtimedelivery.Snapshot, error) {
	return operatorSQLiteDelivery.SnapshotsForEvent(ctx, s.backend, eventID)
}

func (s *AgentPostgres) DeliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return s.deliveryLifecycleSnapshotPageForAgent(ctx, query)
}

func (s *AgentSQLite) DeliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return s.deliveryLifecycleSnapshotPageForAgent(ctx, query)
}

func (s *AgentPostgres) DeliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return s.deliveryDiagnosticSnapshotPageForAgent(ctx, query)
}

func (s *AgentSQLite) DeliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return s.deliveryDiagnosticSnapshotPageForAgent(ctx, query)
}

func (s *AgentPostgres) DeliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return s.deliveryDiagnosticCountsForAgentSince(ctx, identity, since)
}

func (s *AgentSQLite) DeliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return s.deliveryDiagnosticCountsForAgentSince(ctx, identity, since)
}

func (s *AgentPostgres) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorPostgresDelivery.LifecycleSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *AgentSQLite) deliveryLifecycleSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorSQLiteDelivery.LifecycleSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *AgentPostgres) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorPostgresDelivery.DiagnosticSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *AgentSQLite) deliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error) {
	return operatorSQLiteDelivery.DiagnosticSnapshotPageForAgent(ctx, s.backend, query)
}

func (s *AgentPostgres) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return operatorPostgresDelivery.DiagnosticCountsForAgentSince(ctx, s.backend, identity, since)
}

func (s *AgentSQLite) deliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (runtimedelivery.AgentDiagnosticCounts, error) {
	return operatorSQLiteDelivery.DiagnosticCountsForAgentSince(ctx, s.backend, identity, since)
}

func (s *RunPostgres) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return operatorPostgresDelivery.RunDiagnosticCounts(ctx, s.backend, runID)
}

func (s *RunSQLite) deliveryRunDiagnosticCounts(ctx context.Context, runID string) ([]runtimedelivery.RunDiagnosticCount, error) {
	return operatorSQLiteDelivery.RunDiagnosticCounts(ctx, s.backend, runID)
}

func (s *RunPostgres) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return operatorPostgresDelivery.RunDiagnosticFailures(ctx, s.backend, runID, limit)
}

func (s *RunSQLite) deliveryRunDiagnosticFailures(ctx context.Context, runID string, limit int) ([]runtimedelivery.Snapshot, error) {
	return operatorSQLiteDelivery.RunDiagnosticFailures(ctx, s.backend, runID, limit)
}

func (s *RunPostgres) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return operatorPostgresDelivery.RunTraceReferencePage(ctx, s.backend, query)
}

func (s *RunSQLite) deliveryRunTraceReferencePage(ctx context.Context, query runtimedelivery.RunTracePageQuery) (runtimedelivery.RunTraceReferencePage, error) {
	return operatorSQLiteDelivery.RunTraceReferencePage(ctx, s.backend, query)
}

func (s *RunPostgres) summarizeDeliveryRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return operatorPostgresDelivery.SummarizeRun(ctx, s.backend, runID)
}

func (s *RunSQLite) summarizeDeliveryRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return operatorSQLiteDelivery.SummarizeRun(ctx, s.backend, runID)
}
