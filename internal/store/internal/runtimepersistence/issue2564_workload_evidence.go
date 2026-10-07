package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storeevent "github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

type Issue2564WorkloadReader interface {
	operatorread.RunReader
	SummarizeRun(context.Context, string) (deliverylifecycle.RunSummary, error)
	Close() error
}

type Issue2564WorkloadObservation struct {
	Reader Issue2564WorkloadReader
}

func (o *Issue2564WorkloadObservation) CloseForTest() error { return o.Reader.Close() }

type H1FlowAccountingEvidence = storepipeline.H1FlowAccountingEvidence

func ObserveH1FlowAccountingForTest(ctx context.Context, selected any, runID string) (H1FlowAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1FlowAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1FlowAccountingForTest(ctx, runID)
	default:
		return H1FlowAccountingEvidence{}, fmt.Errorf("H1FlowAccounting requires exact selected owner, got %T", selected)
	}
}

type H1TurnAccountingEvidence = storellm.H1TurnAccountingEvidence

func ObserveH1TurnAccountingForTest(ctx context.Context, selected any, runID string) (H1TurnAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.lLMPostgresOwner.ObserveH1TurnAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.lLMSQLiteOwner.ObserveH1TurnAccountingForTest(ctx, runID)
	default:
		return H1TurnAccountingEvidence{}, fmt.Errorf("H1TurnAccounting requires exact selected owner, got %T", selected)
	}
}

type H1BumpAccountingEvidence = storeevent.H1BumpAccountingEvidence

func ObserveH1BumpAccountingForTest(ctx context.Context, selected any, runID string) (H1BumpAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.eventPostgresOwner.ObserveH1BumpAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.eventSQLiteOwner.ObserveH1BumpAccountingForTest(ctx, runID)
	default:
		return H1BumpAccountingEvidence{}, fmt.Errorf("H1BumpAccounting requires exact selected owner, got %T", selected)
	}
}

type H1DeliveryAccountingEvidence = storedelivery.H1DeliveryAccountingEvidence

func ObserveH1DeliveryAccountingForTest(ctx context.Context, selected any, runID string) (H1DeliveryAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH1DeliveryAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH1DeliveryAccountingForTest(ctx, runID)
	default:
		return H1DeliveryAccountingEvidence{}, fmt.Errorf("H1DeliveryAccounting requires exact selected owner, got %T", selected)
	}
}

type H2DeliveryAccountingEvidence = storedelivery.H2DeliveryAccountingEvidence

func ObserveH2DeliveryAccountingForTest(ctx context.Context, selected any, runID string) (H2DeliveryAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH2DeliveryAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH2DeliveryAccountingForTest(ctx, runID)
	default:
		return H2DeliveryAccountingEvidence{}, fmt.Errorf("H2DeliveryAccounting requires exact selected owner, got %T", selected)
	}
}

type H2ConstructionAccountingEvidence = storepipeline.H2ConstructionAccountingEvidence

func ObserveH2ConstructionAccountingForTest(ctx context.Context, selected any, runID string) (H2ConstructionAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH2ConstructionAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH2ConstructionAccountingForTest(ctx, runID)
	default:
		return H2ConstructionAccountingEvidence{}, fmt.Errorf("H2ConstructionAccounting requires exact selected owner, got %T", selected)
	}
}

type H2EventAccountingEvidence = storeevent.H2EventAccountingEvidence

func ObserveH2EventAccountingForTest(ctx context.Context, selected any, runID string) (H2EventAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.eventPostgresOwner.ObserveH2EventAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.eventSQLiteOwner.ObserveH2EventAccountingForTest(ctx, runID)
	default:
		return H2EventAccountingEvidence{}, fmt.Errorf("H2EventAccounting requires exact selected owner, got %T", selected)
	}
}

type H2PendingAccountingEvidence = storedelivery.H2PendingAccountingEvidence

func ObserveH2PendingAccountingForTest(ctx context.Context, selected any, runID string) (H2PendingAccountingEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH2PendingAccountingForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH2PendingAccountingForTest(ctx, runID)
	default:
		return H2PendingAccountingEvidence{}, fmt.Errorf("H2PendingAccounting requires exact selected owner, got %T", selected)
	}
}

type H1RunOverlapEvidence = storepipeline.H1RunOverlapEvidence

func ObserveH1RunOverlapForTest(ctx context.Context, selected any, runID string) (H1RunOverlapEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1RunOverlapForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1RunOverlapForTest(ctx, runID)
	default:
		return H1RunOverlapEvidence{}, fmt.Errorf("H1RunOverlap requires exact selected owner, got %T", selected)
	}
}

type H1HubFieldsEvidence = storepipeline.H1HubFieldsEvidence

func ObserveH1HubFieldsForTest(ctx context.Context, selected any, runID string) ([]H1HubFieldsEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1HubFieldsForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1HubFieldsForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1HubFields requires exact selected owner, got %T", selected)
	}
}

type H1AttributedMutationsEvidence = storepipeline.H1AttributedMutationsEvidence

func ObserveH1AttributedMutationsForTest(ctx context.Context, selected any, runID, bundleHash string) ([]H1AttributedMutationsEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1AttributedMutationsForTest(ctx, runID, bundleHash)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1AttributedMutationsForTest(ctx, runID, bundleHash)
	default:
		return nil, fmt.Errorf("H1AttributedMutations requires exact selected owner, got %T", selected)
	}
}

type H1SameEntityOverlapEvidence = storepipeline.H1SameEntityOverlapEvidence

func ObserveH1SameEntityOverlapForTest(ctx context.Context, selected any, runID string) ([]H1SameEntityOverlapEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1SameEntityOverlapForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1SameEntityOverlapForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1SameEntityOverlap requires exact selected owner, got %T", selected)
	}
}

type H1FailureMutationsEvidence = storepipeline.H1FailureMutationsEvidence

func ObserveH1FailureMutationsForTest(ctx context.Context, selected any, runID string) ([]H1FailureMutationsEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1FailureMutationsForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1FailureMutationsForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1FailureMutations requires exact selected owner, got %T", selected)
	}
}

type H1NodeFailuresEvidence = storedelivery.H1NodeFailuresEvidence

func ObserveH1NodeFailuresForTest(ctx context.Context, selected any, runID string) ([]H1NodeFailuresEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH1NodeFailuresForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH1NodeFailuresForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1NodeFailures requires exact selected owner, got %T", selected)
	}
}

type H1AttemptFailuresEvidence = storedelivery.H1AttemptFailuresEvidence

func ObserveH1AttemptFailuresForTest(ctx context.Context, selected any, runID string) ([]H1AttemptFailuresEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH1AttemptFailuresForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH1AttemptFailuresForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1AttemptFailures requires exact selected owner, got %T", selected)
	}
}

type H1DeadLettersEvidence = storedelivery.H1DeadLettersEvidence

func ObserveH1DeadLettersForTest(ctx context.Context, selected any, runID string) ([]H1DeadLettersEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH1DeadLettersForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH1DeadLettersForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1DeadLetters requires exact selected owner, got %T", selected)
	}
}

type H1BumpHistoryEvidence = storepipeline.H1BumpHistoryEvidence

func ObserveH1BumpHistoryForTest(ctx context.Context, selected any, runID string) ([]H1BumpHistoryEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH1BumpHistoryForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH1BumpHistoryForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H1BumpHistory requires exact selected owner, got %T", selected)
	}
}

type H2HubsEvidence = storepipeline.H2HubsEvidence

func ObserveH2HubsForTest(ctx context.Context, selected any, runID string) ([]H2HubsEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH2HubsForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH2HubsForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H2Hubs requires exact selected owner, got %T", selected)
	}
}

type H2OccurrencesEvidence = storepipeline.H2OccurrencesEvidence

func ObserveH2OccurrencesForTest(ctx context.Context, selected any, runID string) ([]H2OccurrencesEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH2OccurrencesForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH2OccurrencesForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H2Occurrences requires exact selected owner, got %T", selected)
	}
}

type H2ResponseQueueEvidence = storedelivery.H2ResponseQueueEvidence

func ObserveH2ResponseQueueForTest(ctx context.Context, selected any, runID string) ([]H2ResponseQueueEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH2ResponseQueueForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH2ResponseQueueForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H2ResponseQueue requires exact selected owner, got %T", selected)
	}
}

type H2CounterMutationsEvidence = storepipeline.H2CounterMutationsEvidence

func ObserveH2CounterMutationsForTest(ctx context.Context, selected any, runID string) ([]H2CounterMutationsEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.pipelinePostgresOwner.ObserveH2CounterMutationsForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.pipelineSQLiteOwner.ObserveH2CounterMutationsForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H2CounterMutations requires exact selected owner, got %T", selected)
	}
}

type H2NodeDeliveriesEvidence = storedelivery.H2NodeDeliveriesEvidence

func ObserveH2NodeDeliveriesForTest(ctx context.Context, selected any, runID string) ([]H2NodeDeliveriesEvidence, error) {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.deliveryPostgresOwner.ObserveH2NodeDeliveriesForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return s.deliverySQLiteOwner.ObserveH2NodeDeliveriesForTest(ctx, runID)
	default:
		return nil, fmt.Errorf("H2NodeDeliveries requires exact selected owner, got %T", selected)
	}
}

type H2TimersEvidence = storepipeline.H2TimersEvidence
type H2WorkloadSnapshotEvidence struct {
	Hubs   []H2HubsEvidence
	Timers []H2TimersEvidence
	Events []H2OccurrencesEvidence
}

// All three original H2 joins share the same repeatable read, including the
// published-but-unadvanced SIGKILL cut and restart readback.
func ObserveH2WorkloadSnapshotForTest(ctx context.Context, selected any, runID string) (H2WorkloadSnapshotEvidence, error) {
	var out H2WorkloadSnapshotEvidence
	var err error
	switch s := selected.(type) {
	case *PostgresStore:
		err = s.InspectSnapshot(ctx, func(ctx context.Context) error {
			var err error
			if out.Hubs, err = s.pipelinePostgresOwner.ObserveH2HubsForTest(ctx, runID); err != nil {
				return err
			}
			if out.Timers, err = s.pipelinePostgresOwner.ObserveH2TimersForTest(ctx, runID); err != nil {
				return err
			}
			out.Events, err = s.pipelinePostgresOwner.ObserveH2OccurrencesForTest(ctx, runID)
			return err
		})
	case *SQLiteRuntimeStore:
		err = s.InspectSnapshot(ctx, func(ctx context.Context) error {
			var err error
			if out.Hubs, err = s.pipelineSQLiteOwner.ObserveH2HubsForTest(ctx, runID); err != nil {
				return err
			}
			if out.Timers, err = s.pipelineSQLiteOwner.ObserveH2TimersForTest(ctx, runID); err != nil {
				return err
			}
			out.Events, err = s.pipelineSQLiteOwner.ObserveH2OccurrencesForTest(ctx, runID)
			return err
		})
	default:
		err = fmt.Errorf("H2 snapshot requires exact selected native owner, got %T", selected)
	}
	if err != nil {
		return H2WorkloadSnapshotEvidence{}, err
	}
	return out, nil
}

type H2SessionEvidence = postgresbackend.H2SessionEvidence
type H2SessionObservation interface {
	SampleForTest(context.Context, []string) ([]H2SessionEvidence, error)
	CloseForTest() error
}

func ObserveH2ServerCapacityForTest(ctx context.Context, selected any) (int, error) {
	s, ok := selected.(*PostgresStore)
	if !ok {
		return 0, fmt.Errorf("H2 native capacity requires exact PostgreSQL owner")
	}
	return s.backend.ObserveH2ServerCapacityForTest(ctx)
}

func BeginH2SessionObservationForTest(ctx context.Context, selected any, runID string, count int) (H2SessionObservation, error) {
	s, ok := selected.(*PostgresStore)
	if !ok {
		return nil, fmt.Errorf("H2 session observation requires exact PostgreSQL owner")
	}
	actor, err := s.postgresOwner.ObserveH2WorkloadActorForTest(ctx)
	if err != nil {
		return nil, err
	}
	return s.backend.BeginH2SessionObservationForTest(ctx, runID, count, actor.Kind, actor.ID)
}
