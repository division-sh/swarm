package eventpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *EventPostgresOwner) InsertCanonicalEventRecordForTest(ctx context.Context, record eventrecord.Record) (bool, error) {
	var inserted bool
	err := runNativeEventFixtureMutation(ctx, s, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecord(txctx, attempt, true, record)
		return err
	})
	return inserted, err
}

func (s *EventSQLiteOwner) InsertCanonicalEventRecordForTest(ctx context.Context, record eventrecord.Record) (bool, error) {
	var inserted bool
	err := runNativeEventFixtureMutation(ctx, s, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecord(txctx, attempt, false, record)
		return err
	})
	return inserted, err
}

func (s *EventPostgresOwner) SetEventChainDepthForTest(ctx context.Context, eventID string, depth int) error {
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return eventrecord.SetFixtureEventChainDepthTx(txctx, tx, true, eventID, depth)
	})
}

func (s *EventSQLiteOwner) SetEventChainDepthForTest(ctx context.Context, eventID string, depth int) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(txctx context.Context, tx *sql.Tx) error {
		return eventrecord.SetFixtureEventChainDepthTx(txctx, tx, false, eventID, depth)
	})
}

func (s *EventPostgresOwner) CommitSemanticEventFixtureForTest(ctx context.Context, record eventrecord.Record, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	return commitSelectedSemanticEventFixture(ctx, s, true, record, routes, scope, disposition, revisioned, captureForkFrontier)
}

func (s *EventSQLiteOwner) CommitSemanticEventFixtureForTest(ctx context.Context, record eventrecord.Record, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	return commitSelectedSemanticEventFixture(ctx, s, false, record, routes, scope, disposition, revisioned, captureForkFrontier)
}

func runNativeEventFixtureMutation(ctx context.Context, selected any, write func(context.Context, *mutationprotocol.Attempt) error) error {
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return mutationprotocol.RunPostgres(ctx, owner.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, owner.RunLifecyclePostgresOwner.CompletionCandidateCoordinator(), apply).Err()
	case *EventSQLiteOwner:
		return mutationprotocol.RunSQLite(ctx, owner.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, owner.RunLifecycleSQLiteOwner.CompletionCandidateCoordinator(), apply).Err()
	default:
		return fmt.Errorf("event fixture owner %T is unsupported", selected)
	}
}

func runNativeUnrevisionedEventFixtureTransaction(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return owner.backend.RunTransaction(ctx, write)
	case *EventSQLiteOwner:
		return owner.backend.RunTransaction(ctx, "unrevisioned event fixture", write)
	default:
		return fmt.Errorf("event fixture owner %T is unsupported", selected)
	}
}

func insertCanonicalFixtureRecord(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, record eventrecord.Record) (bool, error) {
	var inserted bool
	var err error
	if postgres {
		inserted, err = eventrecordpostgres.Insert(ctx, attempt, record)
	} else {
		inserted, err = eventrecordsqlite.Insert(ctx, attempt, record)
	}
	if err != nil || inserted {
		return inserted, err
	}
	var existing eventrecord.Record
	var found bool
	err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if postgres {
			existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
		} else {
			existing, found, err = eventrecordsqlite.Load(ctx, tx, record.EventID)
		}
		return err
	})
	if err != nil {
		return false, err
	}
	if !found || !record.Equal(existing) {
		return false, fmt.Errorf("canonical event fixture %s conflicts with its persisted record", record.EventID)
	}
	return false, nil
}

func commitSelectedSemanticEventFixture(ctx context.Context, selected any, postgres bool, record eventrecord.Record, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	dialect := storedelivery.DialectSQLite
	if postgres {
		dialect = storedelivery.DialectPostgres
	}
	adapter, err := storedelivery.NewAdapter(dialect)
	if err != nil {
		return false, err
	}
	if !revisioned {
		return commitUnrevisionedSemanticEventFixture(ctx, selected, postgres, record, routes, scope, disposition, adapter)
	}
	inserted := false
	err = runNativeEventFixtureMutation(ctx, selected, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecord(txctx, attempt, postgres, record)
		if err != nil || !inserted {
			return err
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = fixtureExecutionAuthorityTx(txctx, selected, tx, record.RunID)
			return err
		}); err != nil {
			return err
		}
		if _, err := adapter.CommitInitial(txctx, attempt, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
			return err
		}
		if err := commitRevisionedFixtureFacts(txctx, selected, attempt, record.EventID, scope, disposition, time.Now().UTC()); err != nil {
			return err
		}
		if captureForkFrontier {
			if !postgres {
				return fmt.Errorf("semantic fork frontier fixture requires PostgreSQL, got %T", selected)
			}
			for _, family := range []runforkrevision.Family{runforkrevision.FamilyEvents, runforkrevision.FamilyEventDeliveries, runforkrevision.FamilyCommittedReplayScopes, runforkrevision.FamilyEventReceipts} {
				if err := attempt.AddWholeFamily(record.RunID, family); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

func fixtureExecutionAuthorityTx(ctx context.Context, selected any, tx *sql.Tx, runID string) (runtimedelivery.ExecutionAuthority, error) {
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return owner.DeliveryPostgresOwner.FixtureExecutionAuthorityTx(ctx, tx, runID)
	case *EventSQLiteOwner:
		return owner.DeliverySQLiteOwner.FixtureExecutionAuthorityTx(ctx, tx, runID)
	default:
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("event fixture owner %T is unsupported", selected)
	}
}

func commitRevisionedFixtureFacts(ctx context.Context, selected any, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return owner.PipelinePostgresOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	case *EventSQLiteOwner:
		return owner.PipelineSQLiteOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture owner %T is unsupported", selected)
	}
}

func commitUnrevisionedFixtureFacts(ctx context.Context, selected any, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return owner.PipelinePostgresOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	case *EventSQLiteOwner:
		return owner.PipelineSQLiteOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture owner %T is unsupported", selected)
	}
}

func commitUnrevisionedSemanticEventFixture(ctx context.Context, selected any, postgres bool, record eventrecord.Record, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, adapter *storedelivery.Adapter) (bool, error) {
	inserted := false
	var authority runtimedelivery.ExecutionAuthority
	err := runNativeUnrevisionedEventFixtureTransaction(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if postgres {
			inserted, err = eventrecordpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		} else {
			inserted, err = eventrecordsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		}
		if err != nil || !inserted {
			return err
		}
		authority, err = fixtureExecutionAuthorityTx(ctx, selected, tx, record.RunID)
		if err != nil {
			return err
		}
		if err := events.ValidateDeliveryRoutes(routes); err != nil {
			return err
		}
		admitted, err := record.Decode()
		if err != nil {
			return err
		}
		if err := events.ValidateReceiverMaterializations(admitted.Event(), routes); err != nil {
			return err
		}
		for _, route := range events.NormalizeDeliveryRoutes(routes) {
			obligation, err := runtimedelivery.NewObligation(record.EventID, record.RunID, route, authority)
			if err != nil {
				return err
			}
			if _, err := adapter.InsertUnrevisionedFixtureObligationTx(ctx, tx, obligation); err != nil {
				return err
			}
		}
		return commitUnrevisionedFixtureFacts(ctx, selected, tx, record.EventID, scope, disposition, time.Now().UTC())
	})
	if err != nil || !inserted {
		return false, err
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		obligation, err := runtimedelivery.NewObligation(record.EventID, record.RunID, route, authority)
		if err != nil {
			return false, err
		}
		snapshot, err := fixtureDeliverySnapshot(ctx, selected, obligation.DeliveryID())
		if err != nil {
			return false, fmt.Errorf("read unrevisioned delivery fixture %s: %w", obligation.DeliveryID(), err)
		}
		wantRoute, err := json.Marshal(obligation.Route())
		if err != nil {
			return false, err
		}
		gotRoute, err := json.Marshal(snapshot.Route.Normalized())
		if err != nil {
			return false, err
		}
		if snapshot.DeliveryID != obligation.DeliveryID() || snapshot.EventID != record.EventID || snapshot.RunID != record.RunID ||
			events.EncodeDeliveryRouteIdentity(snapshot.RouteIdentity) != events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()) ||
			snapshot.SubscriberClass != obligation.SubscriberClass() || snapshot.SubscriberID != obligation.SubscriberID() ||
			snapshot.Status != runtimedelivery.StatusPending || snapshot.RetryCount != 0 || snapshot.MaxRetries != obligation.MaxRetries() ||
			snapshot.ClaimVersion != 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.Authority.Equal(authority) || !bytes.Equal(gotRoute, wantRoute) {
			return false, fmt.Errorf("unrevisioned delivery fixture %s differs from canonical route/authority projection", obligation.DeliveryID())
		}
	}
	return true, nil
}

func fixtureDeliverySnapshot(ctx context.Context, selected any, deliveryID string) (runtimedelivery.Snapshot, error) {
	switch owner := selected.(type) {
	case *EventPostgresOwner:
		return owner.DeliveryPostgresOwner.Snapshot(ctx, deliveryID)
	case *EventSQLiteOwner:
		return owner.DeliverySQLiteOwner.Snapshot(ctx, deliveryID)
	default:
		return runtimedelivery.Snapshot{}, fmt.Errorf("unrevisioned semantic fixture store %T cannot read delivery snapshots", selected)
	}
}
