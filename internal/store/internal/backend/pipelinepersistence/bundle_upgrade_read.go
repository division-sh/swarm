package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

type bundleUpgradeReadOwner interface {
	ReadActiveRunDeliverySnapshotsTx(context.Context, *sql.Tx, string) ([]deliverylifecycle.Snapshot, error)
	ListDecisionCardsTx(context.Context, *sql.Tx, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error)
}

func (s *PipelinePostgresOwner) ReadBundleUpgradeState(ctx context.Context, runID string, source semanticview.Source) (state runbundle.UpgradeState, err error) {
	if s == nil || s.backend == nil {
		return state, errors.New("PostgreSQL bundle upgrade reader is required")
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		state, err = readBundleUpgradeStateTx(ctx, tx, s, false, runID, source)
		return err
	})
	if err != nil {
		return runbundle.UpgradeState{}, err
	}
	return state, nil
}

func (s *PipelineSQLiteOwner) ReadBundleUpgradeState(ctx context.Context, runID string, source semanticview.Source) (state runbundle.UpgradeState, err error) {
	if s == nil || s.backend == nil {
		return state, errors.New("SQLite bundle upgrade reader is required")
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		state, err = readBundleUpgradeStateTx(ctx, tx, s, true, runID, source)
		return err
	})
	if err != nil {
		return runbundle.UpgradeState{}, err
	}
	return state, nil
}

// The same reader can be called with C's existing native switch transaction;
// no pool read or second transaction is needed at that boundary.
func readBundleUpgradeStateTx(ctx context.Context, tx *sql.Tx, owner bundleUpgradeReadOwner, sqlite bool, runID string, source semanticview.Source) (runbundle.UpgradeState, error) {
	state := runbundle.UpgradeState{RunID: runID}
	if err := requireUpgradeSourceTx(ctx, tx, sqlite, runID, source); err != nil {
		return runbundle.UpgradeState{}, err
	}
	instances, err := listWorkflowInstances(ctx, tx, sqlite, runID)
	if err != nil {
		return runbundle.UpgradeState{}, err
	}
	for _, instance := range instances {
		state.Instances = append(state.Instances, runbundle.UpgradeInstance{
			Route:    flowidentity.RouteForInstancePath(instance.StorageRef),
			EntityID: instance.EntityID, FlowID: entityruntime.ResolveFlowIDForRuntimeInstance(source, runID, instance.StorageRef),
			Status: instance.Status, Stage: instance.CurrentState, Fields: instance.Fields, StateBuckets: instance.StateBuckets,
		})
	}
	state.Deliveries, err = owner.ReadActiveRunDeliverySnapshotsTx(ctx, tx, runID)
	if err != nil {
		return runbundle.UpgradeState{}, err
	}
	for cursor := ""; ; {
		cards, next, err := owner.ListDecisionCardsTx(ctx, tx, decisioncard.ListOptions{RunID: runID, Cursor: cursor, Limit: 200})
		if err != nil {
			return runbundle.UpgradeState{}, err
		}
		for _, card := range cards {
			if card.Status == "pending" {
				state.Cards = append(state.Cards, card)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	for _, status := range []fanoutbarrier.Status{fanoutbarrier.StatusArmed, fanoutbarrier.StatusClosedPending} {
		barriers, err := loadFanOutBarriers(ctx, tx, !sqlite, runID, status, nil, false)
		if err != nil {
			return runbundle.UpgradeState{}, err
		}
		state.Barriers = append(state.Barriers, barriers...)
	}
	state.Events, err = readUpgradeEvents(ctx, tx, sqlite, runID, state.Deliveries)
	if err != nil {
		return runbundle.UpgradeState{}, err
	}
	if err := ctx.Err(); err != nil {
		return runbundle.UpgradeState{}, err
	}
	return state, nil
}

func requireUpgradeSourceTx(ctx context.Context, tx *sql.Tx, sqlite bool, runID string, source semanticview.Source) error {
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return errors.New("bundle upgrade read requires the admitted source")
	}
	identity, err := contracts.BootBundleIdentity(bundle)
	if err != nil {
		return err
	}
	expected, err := correlation.NewSourceArtifactFact(identity.BundleHash)
	if err != nil {
		return err
	}
	load := sourceadmission.LoadPostgresTx
	if sqlite {
		load = sourceadmission.LoadSQLiteTx
	}
	actual, err := load(ctx, tx, runID, true, true)
	if err != nil {
		return err
	}
	if !actual.Matches(expected) {
		return errors.New("bundle upgrade read differs from the run's admitted source")
	}
	return nil
}

func readUpgradeEvents(ctx context.Context, tx *sql.Tx, sqlite bool, runID string, deliveries []deliverylifecycle.Snapshot) ([]events.Event, error) {
	args := diagnosticDirectReplayEventArgs()
	query := `SELECT e.event_id::text FROM events e
		LEFT JOIN event_receipts receipt ON receipt.event_id=e.event_id AND receipt.subscriber_type='platform' AND receipt.subscriber_id='pipeline'
		WHERE e.run_id=$%d::uuid AND receipt.event_id IS NULL AND %s
		UNION SELECT event_id::text FROM decision_card_route_obligations WHERE run_id=$%d::uuid AND status='pending'`
	query = fmt.Sprintf(query, len(args)+1, postgresDiagnosticDirectReplayExclusionSQL("e", 1), len(args)+1)
	args = append(args, runID)
	if sqlite {
		query = `SELECT e.event_id FROM events e
			LEFT JOIN event_receipts receipt ON receipt.event_id=e.event_id AND receipt.subscriber_type='platform' AND receipt.subscriber_id='pipeline'
			WHERE ` + sqliteDiagnosticDirectReplayExclusionSQL("e") + ` AND e.run_id=? AND receipt.event_id IS NULL
			UNION SELECT event_id FROM decision_card_route_obligations WHERE run_id=? AND status='pending'`
		args = append(args, runID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids[id] = true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for _, delivery := range deliveries {
		ids[delivery.EventID] = true
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	var result []events.Event
	for _, id := range ordered {
		var event events.AdmittedEvent
		var found bool
		if sqlite {
			event, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, id)
		} else {
			event, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, id)
		}
		if err != nil || !found {
			return nil, errors.Join(fmt.Errorf("queued upgrade event %s cannot be read", id), err)
		}
		result = append(result, event.Event())
	}
	return result, nil
}
