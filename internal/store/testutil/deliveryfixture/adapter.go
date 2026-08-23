// Package deliveryfixture exposes selected-store delivery mechanics only to
// tests that need a real transaction-backed lifecycle owner. Production code
// must consume the typed deliverylifecycle.Store surface instead.
package deliveryfixture

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type Adapter struct {
	*deliveryadapter.Adapter
	dialect Dialect
}
type Dialect = deliveryadapter.Dialect

const (
	DialectPostgres = deliveryadapter.DialectPostgres
	DialectSQLite   = deliveryadapter.DialectSQLite
)

func NewAdapter(dialect Dialect) (*Adapter, error) {
	owner, err := deliveryadapter.NewAdapter(dialect)
	if err != nil {
		return nil, err
	}
	return &Adapter{Adapter: owner, dialect: dialect}, nil
}

func (a *Adapter) privateDialect() privateauthoractivity.Dialect {
	if a.dialect == DialectPostgres {
		return privateauthoractivity.DialectPostgres
	}
	return privateauthoractivity.DialectSQLite
}

func (a *Adapter) withStory(ctx context.Context, tx *sql.Tx, fn func(*privateauthoractivity.Mutation) error) error {
	story, err := privateauthoractivity.Begin(ctx, tx, a.privateDialect())
	if err != nil {
		return err
	}
	if err := fn(story); err != nil {
		return err
	}
	return story.Finalize(ctx)
}

func (a *Adapter) ClaimExactResult(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute, leaseTTL time.Duration) (result runtimedelivery.ClaimResult, err error) {
	err = a.withStory(ctx, tx, func(story *privateauthoractivity.Mutation) error {
		result, err = a.Adapter.ClaimExactResult(ctx, tx, story, authority, event, route, leaseTTL)
		return err
	})
	return result, err
}

func (a *Adapter) SettleSuccess(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (snapshot runtimedelivery.Snapshot, err error) {
	err = a.withStory(ctx, tx, func(story *privateauthoractivity.Mutation) error {
		snapshot, err = a.Adapter.SettleSuccess(ctx, tx, story, claim, sideEffects, duration, selection)
		return err
	})
	return snapshot, err
}

func (a *Adapter) SettleFailure(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (snapshot runtimedelivery.Snapshot, err error) {
	err = a.withStory(ctx, tx, func(story *privateauthoractivity.Mutation) error {
		snapshot, err = a.Adapter.SettleFailure(ctx, tx, story, claim, settlement)
		return err
	})
	return snapshot, err
}

func (a *Adapter) TerminalizeRun(ctx context.Context, tx *sql.Tx, runID, reason string) (terminalizations []runtimedelivery.Terminalization, err error) {
	err = a.withStory(ctx, tx, func(story *privateauthoractivity.Mutation) error {
		terminalizations, err = a.Adapter.TerminalizeRun(ctx, tx, story, runID, reason)
		return err
	})
	return terminalizations, err
}

func (a *Adapter) CommitInitial(ctx context.Context, tx *sql.Tx, eventID, runID string, routes []events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error) {
	return a.Adapter.CommitInitial(ctx, tx, eventID, runID, routes, authority)
}

func (a *Adapter) ActivateNormalAuthority(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority) error {
	return a.Adapter.ActivateNormalAuthority(ctx, tx, authority)
}

func (a *Adapter) ScanContinuations(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (runtimedelivery.ContinuationPage, error) {
	return a.Adapter.ScanContinuations(ctx, tx, authority, cursor, limit)
}

func (a *Adapter) RenewClaim(ctx context.Context, tx *sql.Tx, claim runtimedelivery.Claim, leaseTTL time.Duration) (runtimedelivery.Snapshot, error) {
	return a.Adapter.RenewClaim(ctx, tx, claim, leaseTTL)
}

func (a *Adapter) ObserveContinuation(ctx context.Context, db *sql.DB, authority runtimedelivery.ExecutionAuthority, deliveryID string) (runtimedelivery.ContinuationObservation, error) {
	return a.Adapter.ObserveContinuation(ctx, db, authority, deliveryID)
}

func (a *Adapter) ObserveContinuationInTransaction(ctx context.Context, tx *sql.Tx, authority runtimedelivery.ExecutionAuthority, deliveryID string) (runtimedelivery.ContinuationObservation, error) {
	return a.Adapter.ObserveContinuation(ctx, tx, authority, deliveryID)
}
