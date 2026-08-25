package operatorsurface

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type agentLifecycleDeliveryRecord struct {
	AgentIdentity   agentidentity.Identity
	Status          string
	ActiveSessionID string
	CreatedAt       time.Time
	DeliveredAt     sql.NullTime
}

func (s *AgentPostgres) ListAgentDeliveryLifecycleFacts(ctx context.Context, identities []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized, err := normalizePendingAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts{}, nil
	}
	asOf, err := operatorPostgresDelivery.CaptureSnapshotTime(ctx, s.backend)
	if err != nil {
		return nil, err
	}
	records, err := s.listAgentLifecycleRecordsSpec(ctx, normalized, asOf)
	if err != nil {
		return nil, err
	}
	out := make(map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, len(normalized))
	for _, identity := range normalized {
		out[identity] = operatorread.AgentDeliveryLifecycleFacts{}
	}
	grouped := make(map[agentidentity.Identity][]agentLifecycleDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentIdentity] = append(grouped[record.AgentIdentity], record)
	}
	for _, identity := range normalized {
		out[identity] = canonicalAgentDeliveryLifecycleFactsFromRecords(grouped[identity])
	}
	return out, nil
}

func (s *AgentPostgres) listAgentLifecycleRecordsSpec(ctx context.Context, identities []agentidentity.Identity, asOf time.Time) ([]agentLifecycleDeliveryRecord, error) {
	snapshots, err := operatorPostgresDelivery.CurrentAgentSnapshots(ctx, s.backend, identities, asOf)
	if err != nil {
		return nil, err
	}
	return agentLifecycleRecordsFromSnapshots(snapshots), nil
}

func agentDeliveryLifecycleFactsFromSnapshots(identities []agentidentity.Identity, snapshots []runtimedelivery.Snapshot) map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts {
	out := make(map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, len(identities))
	grouped := make(map[agentidentity.Identity][]agentLifecycleDeliveryRecord, len(identities))
	for _, record := range agentLifecycleRecordsFromSnapshots(snapshots) {
		grouped[record.AgentIdentity] = append(grouped[record.AgentIdentity], record)
	}
	for _, identity := range identities {
		out[identity] = canonicalAgentDeliveryLifecycleFactsFromRecords(grouped[identity])
	}
	return out
}

func agentLifecycleRecordsFromSnapshots(snapshots []runtimedelivery.Snapshot) []agentLifecycleDeliveryRecord {
	out := make([]agentLifecycleDeliveryRecord, 0, len(snapshots))
	for _, snapshot := range snapshots {
		record := agentLifecycleDeliveryRecord{
			AgentIdentity: snapshot.Route.AgentIdentity, Status: string(snapshot.Status),
			ActiveSessionID: snapshot.ActiveSessionID, CreatedAt: snapshot.CreatedAt,
		}
		if !snapshot.SettledAt.IsZero() {
			record.DeliveredAt = sql.NullTime{Time: snapshot.SettledAt, Valid: true}
		}
		out = append(out, record)
	}
	return out
}

type agentLifecycleCandidate struct {
	facts      operatorread.AgentDeliveryLifecycleFacts
	observedAt time.Time
	priority   int
}

func canonicalAgentDeliveryLifecycleFactsFromRecords(records []agentLifecycleDeliveryRecord) operatorread.AgentDeliveryLifecycleFacts {
	var live *agentLifecycleCandidate
	var exhausted *agentLifecycleCandidate
	for _, record := range records {
		state, ok := runtimedelivery.StateFromDelivery(record.Status, record.ActiveSessionID)
		if !ok {
			continue
		}
		candidate := agentLifecycleCandidate{
			facts: operatorread.AgentDeliveryLifecycleFacts{
				CurrentState:  string(state),
				BlockingLayer: agentLifecycleBlockingLayer(state),
			},
			observedAt: agentLifecycleObservedAt(record),
			priority:   agentLifecyclePriority(state),
		}
		switch state {
		case runtimedelivery.StateQueued, runtimedelivery.StateLaunching, runtimedelivery.StateActive, runtimedelivery.StateRetrying:
			if live == nil || candidate.priority > live.priority || (candidate.priority == live.priority && candidate.observedAt.After(live.observedAt)) {
				live = &candidate
			}
		case runtimedelivery.StateExhausted:
			if exhausted == nil || candidate.observedAt.After(exhausted.observedAt) {
				exhausted = &candidate
			}
		}
	}
	if live != nil {
		return live.facts
	}
	if exhausted != nil {
		return exhausted.facts
	}
	return operatorread.AgentDeliveryLifecycleFacts{}
}

func agentLifecycleObservedAt(record agentLifecycleDeliveryRecord) time.Time {
	if record.DeliveredAt.Valid {
		return record.DeliveredAt.Time
	}
	return record.CreatedAt
}

func agentLifecyclePriority(state runtimedelivery.State) int {
	switch state {
	case runtimedelivery.StateRetrying:
		return 4
	case runtimedelivery.StateLaunching:
		return 3
	case runtimedelivery.StateActive:
		return 2
	case runtimedelivery.StateQueued:
		return 1
	default:
		return 0
	}
}

func agentLifecycleBlockingLayer(state runtimedelivery.State) string {
	switch state {
	case runtimedelivery.StateQueued:
		return "delivery_queue"
	case runtimedelivery.StateLaunching:
		return "session_launch"
	case runtimedelivery.StateActive:
		return "session_execution"
	case runtimedelivery.StateRetrying:
		return "delivery_retry"
	case runtimedelivery.StateExhausted:
		return "delivery_terminal"
	default:
		return ""
	}
}

func AgentLifecycleBlockingLayer(state runtimedelivery.State) string {
	return agentLifecycleBlockingLayer(state)
}
