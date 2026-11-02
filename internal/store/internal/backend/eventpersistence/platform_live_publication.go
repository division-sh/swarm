package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	eventpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *EventPostgresOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority delivery.ExecutionAuthority) ([]delivery.DurableHandoffProof, error) {
	if runID == "" && len(routes) != 0 {
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			admitted, _, found, err := eventpostgres.LoadAdmitted(ctx, tx, eventID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("platform delivery publication is absent")
			}
			return requirePlatformDeliveryLandingRuns(ctx, tx, admitted.Event(), routes, true)
		}); err != nil {
			return nil, err
		}
	}
	return s.DeliveryPostgresOwner.CommitInitialDeliveryObligationsTx(ctx, attempt, eventID, runID, routes, authority)
}

func (s *EventSQLiteOwner) CommitInitialDeliveryObligationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, eventID, runID string, routes []events.DeliveryRoute, authority delivery.ExecutionAuthority) ([]delivery.DurableHandoffProof, error) {
	if runID == "" && len(routes) != 0 {
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			admitted, _, found, err := eventsqlite.LoadAdmitted(ctx, tx, eventID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("platform delivery publication is absent")
			}
			return requirePlatformDeliveryLandingRuns(ctx, tx, admitted.Event(), routes, false)
		}); err != nil {
			return nil, err
		}
	}
	return s.DeliverySQLiteOwner.CommitInitialDeliveryObligationsTx(ctx, attempt, eventID, runID, routes, authority)
}

func requirePlatformDeliveryLandingRuns(ctx context.Context, tx *sql.Tx, event events.Event, routes []events.DeliveryRoute, postgres bool) error {
	if !event.IsRunlessPlatformPublication() {
		return fmt.Errorf("run-less executable delivery requires its standalone platform publication")
	}
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		return fmt.Errorf("platform delivery requires its admitted source")
	}
	selected := make(map[string]runlifecycle.LiveRun)
	for _, route := range routes {
		if _, err := events.NewDeliveryEvent(event, route); err != nil {
			return err
		}
		runID, flow := route.Context.RunID, route.Target.Route().FlowID
		if runID == "" || flow == "" {
			return fmt.Errorf("platform delivery requires its exact selected run and flow")
		}
		live := runlifecycle.LiveRun{RunID: runID, Scope: runlifecycle.LiveRunScope{FlowPath: flow}, Source: source}
		if previous, exists := selected[runID]; exists && (previous.Scope != live.Scope || !previous.Source.Matches(live.Source)) {
			return fmt.Errorf("platform delivery selection crosses flow ownership")
		}
		selected[runID] = live
	}
	// All publications acquire the existing run locks in the same order.
	order := make([]string, 0, len(selected))
	for runID := range selected {
		order = append(order, runID)
	}
	slices.Sort(order)
	for _, runID := range order {
		live := selected[runID]
		query := `SELECT run_id,bundle_hash,status FROM runs WHERE flow_path=$1 AND landing_run=TRUE`
		if postgres {
			query += ` FOR UPDATE`
		}
		var selectedID, bundleHash, state string
		if err := tx.QueryRowContext(ctx, query, live.Scope.FlowPath).Scan(&selectedID, &bundleHash, &state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return runtimebus.ErrAPIEventLiveRunChanged
			}
			return err
		}
		if selectedID != live.RunID || bundleHash != live.Source.BundleHash() || (state != "live" && state != "paused") {
			return runtimebus.ErrAPIEventLiveRunChanged
		}
	}
	return nil
}
