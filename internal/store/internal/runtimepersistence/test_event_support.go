package runtimepersistence

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
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

// CommitSemanticEventFixtureForTest persists the exact fixture facts without
// adding a fork revision. Writer admission still belongs to the selected store.
func CommitSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, false, false)
}

func insertCanonicalFixtureRecordForTest(ctx context.Context, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect, record eventrecord.Record) (bool, error) {
	var inserted bool
	var err error
	if dialect == authoractivityfixture.DialectPostgres {
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
		if dialect == authoractivityfixture.DialectPostgres {
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

// CommitRevisionedSemanticEventFixtureForTest exercises the canonical event
// and delivery adapters, including their fork revision projection.
func CommitRevisionedSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, false)
}

// CommitSemanticForkFrontierForTest captures all selected-fork fact families
// in the same PostgreSQL fixture transaction.
func CommitSemanticForkFrontierForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	if store, ok := selected.(*PostgresStore); !ok || store == nil {
		return false, fmt.Errorf("semantic fork frontier fixture requires the original PostgreSQL owner, got %T", selected)
	}
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, true)
}

func eventFixtureDialectForTest(selected any) (authoractivityfixture.Dialect, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil && store.pipelinePostgresOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectPostgres, nil
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil && store.pipelineSQLiteOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectSQLite, nil
		}
	}
	return "", fmt.Errorf("event fixture selected store %T is not initialized", selected)
}

func runEventFixtureMutationForTest(ctx context.Context, selected any, write func(context.Context, *mutationprotocol.Attempt) error) error {
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return mutationprotocol.RunSQLite(ctx, store.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func runUnrevisionedEventFixtureTransactionForTest(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "unrevisioned event fixture", write)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func commitSelectedSemanticEventFixtureForTest(ctx context.Context, selectedStore any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, pipelineDisposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return false, fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	if err := settlement.Validate(routes); err != nil {
		return false, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return false, err
	}
	dialect, err := eventFixtureDialectForTest(selectedStore)
	if err != nil {
		return false, err
	}
	postgres := dialect == authoractivityfixture.DialectPostgres
	deliveryAdapter, err := deliveryadapter.NewAdapter(deliveryadapter.Dialect(dialect))
	if err != nil {
		return false, err
	}
	if !revisioned {
		return commitUnrevisionedSemanticEventFixtureForTest(ctx, selectedStore, dialect, record, routes, scope, pipelineDisposition, deliveryAdapter)
	}
	inserted := false
	err = runEventFixtureMutationForTest(ctx, selectedStore, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecordForTest(txctx, attempt, dialect, record)
		if err != nil || !inserted {
			return err
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = persistedEventDeliveryFixtureAuthority(txctx, tx, record.RunID, !postgres)
			return err
		}); err != nil {
			return err
		}
		if _, err := deliveryAdapter.CommitInitial(txctx, attempt, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
			return err
		}
		if err := commitRevisionedEventFixtureFactsForTest(txctx, selectedStore, attempt, record.EventID, scope, pipelineDisposition, time.Now().UTC()); err != nil {
			return err
		}
		if captureForkFrontier {
			if !postgres {
				return fmt.Errorf("semantic fork frontier fixture requires PostgreSQL, got %T", selectedStore)
			}
			for _, family := range []runforkrevision.Family{
				runforkrevision.FamilyEvents,
				runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes,
				runforkrevision.FamilyEventReceipts,
			} {
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

func commitUnrevisionedSemanticEventFixtureForTest(
	ctx context.Context,
	selectedStore any,
	dialect authoractivityfixture.Dialect,
	record eventrecord.Record,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	adapter *deliveryadapter.Adapter,
) (bool, error) {
	inserted := false
	var authority runtimedelivery.ExecutionAuthority
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selectedStore, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
			inserted, err = eventrecordpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		} else {
			inserted, err = eventrecordsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		}
		if err != nil || !inserted {
			return err
		}
		authority, err = persistedEventDeliveryFixtureAuthority(ctx, tx, record.RunID, dialect == authoractivityfixture.DialectSQLite)
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
			if err := insertUnrevisionedDeliveryFixtureForTest(ctx, tx, dialect, adapter, record.EventID, record.RunID, route, authority); err != nil {
				return err
			}
		}
		return commitUnrevisionedEventFixtureFactsForTest(ctx, selectedStore, tx, record.EventID, scope, pipelineDisposition, time.Now().UTC())
	})
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}
	reader, ok := selectedStore.(interface {
		Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
	})
	if !ok {
		return false, fmt.Errorf("unrevisioned semantic fixture store %T cannot read delivery snapshots", selectedStore)
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		obligation, err := runtimedelivery.NewObligation(record.EventID, record.RunID, route, authority)
		if err != nil {
			return false, err
		}
		snapshot, err := reader.Snapshot(ctx, obligation.DeliveryID())
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
			snapshot.ClaimVersion != 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.Authority.Equal(authority) ||
			!bytes.Equal(gotRoute, wantRoute) {
			return false, fmt.Errorf("unrevisioned delivery fixture %s differs from canonical route/authority projection", obligation.DeliveryID())
		}
	}
	return true, nil
}

func insertUnrevisionedDeliveryFixtureForTest(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect, adapter *deliveryadapter.Adapter, eventID, runID string, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) error {
	obligation, err := runtimedelivery.NewObligation(eventID, runID, route, authority)
	if err != nil {
		return err
	}
	_, err = adapter.InsertUnrevisionedFixtureObligationTx(ctx, tx, obligation)
	return err
}

func commitRevisionedEventFixtureFactsForTest(ctx context.Context, selected any, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func commitUnrevisionedEventFixtureFactsForTest(ctx context.Context, selected any, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
