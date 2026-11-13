package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// CommitPersistedEventDeliveryFixtureForTest adds routes to an existing event
// under the original revision-only fixture transaction.
func (s *DeliveryPostgresOwner) CommitPersistedEventDeliveryFixtureForTest(ctx context.Context, eventID, runID string, routes []events.DeliveryRoute) error {
	adapter, err := NewAdapter(DialectPostgres)
	if err != nil {
		return fmt.Errorf("construct postgres delivery fixture adapter: %w", err)
	}
	return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		var authority runtimedelivery.ExecutionAuthority
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = s.FixtureExecutionAuthorityTx(txctx, tx, runID)
			return err
		})
		if err != nil {
			return struct{}{}, err
		}
		_, err = adapter.CommitInitial(txctx, attempt, eventID, runID, routes, authority)
		return struct{}{}, err
	}).Err()
}

func (s *DeliverySQLiteOwner) CommitPersistedEventDeliveryFixtureForTest(ctx context.Context, eventID, runID string, routes []events.DeliveryRoute) error {
	adapter, err := NewAdapter(DialectSQLite)
	if err != nil {
		return fmt.Errorf("construct sqlite delivery fixture adapter: %w", err)
	}
	return mutationprotocol.RunSQLite(ctx, s.backend, "persisted event delivery fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		var authority runtimedelivery.ExecutionAuthority
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = s.FixtureExecutionAuthorityTx(txctx, tx, runID)
			return err
		})
		if err != nil {
			return struct{}{}, err
		}
		_, err = adapter.CommitInitial(txctx, attempt, eventID, runID, routes, authority)
		return struct{}{}, err
	}).Err()
}

func (s *DeliveryPostgresOwner) FixtureExecutionAuthorityTx(ctx context.Context, tx *sql.Tx, runID string) (runtimedelivery.ExecutionAuthority, error) {
	return persistedEventDeliveryFixtureAuthority(ctx, tx, runID, false)
}

func (s *DeliverySQLiteOwner) FixtureExecutionAuthorityTx(ctx context.Context, tx *sql.Tx, runID string) (runtimedelivery.ExecutionAuthority, error) {
	return persistedEventDeliveryFixtureAuthority(ctx, tx, runID, true)
}

func persistedEventDeliveryFixtureAuthority(ctx context.Context, tx *sql.Tx, runID string, sqlite bool) (runtimedelivery.ExecutionAuthority, error) {
	query := `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	if sqlite {
		query = `SELECT bundle_hash FROM runs WHERE run_id=?`
	}
	var bundleHash string
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("load delivery fixture run source artifact: %w", err)
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture source: %w", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "storetest:"+runID, 1)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture authority: %w", err)
	}
	return authority, nil
}
