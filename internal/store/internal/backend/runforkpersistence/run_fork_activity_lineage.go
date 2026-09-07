package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

// The activity owner validates facts; this adapter supplies one transactional
// snapshot and returns exact fresh request IDs, never a platform-name exemption.
func selectedContractActivityLineage(ctx context.Context, tx *sql.Tx, postgres bool, runID string, allowedSourceIDs []string, source semanticview.Source) ([]string, error) {
	reject := func(eventID string, err error) ([]string, error) {
		return nil, runForkReplayResumeError("fork_events_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState,
			fmt.Sprintf("fork activity lineage %s: %v", eventID, err))
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id = $1 ORDER BY created_at, event_id`, runID)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	deliveries := sqliteDeliveryAdapter
	var records []eventrecord.Record
	if postgres {
		deliveries = postgresDeliveryAdapter
		records, err = eventrecordpostgres.LoadMany(ctx, tx, ids)
	} else {
		records, err = eventrecordsqlite.LoadMany(ctx, tx, ids)
	}
	if err != nil {
		return reject("", err)
	}
	eventsByID := make(map[string]events.Event, len(records))
	for _, record := range records {
		admitted, err := record.Decode()
		if err != nil {
			return reject(record.EventID, err)
		}
		eventsByID[record.EventID] = admitted.Event()
	}
	rows, err = tx.QueryContext(ctx, `SELECT CAST(fork_event_id AS TEXT), CAST(source_event_id AS TEXT) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1`, runID)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, id := range allowedSourceIDs {
		allowed[id] = true
	}
	roots := map[string]bool{}
	for rows.Next() {
		var forkID, sourceID string
		if err := rows.Scan(&forkID, &sourceID); err != nil {
			rows.Close()
			return nil, err
		}
		if allowed[sourceID] {
			roots[forkID] = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	requests := map[string]pipeline.ActivityRequestLineage{}
	freshIDs := []string{}
	for _, id := range ids {
		event := eventsByID[id]
		if event.Type() != "platform.activity_requested" {
			continue
		}
		var lineage pipeline.ActivityRequestLineage
		if roots[id] {
			lineage, err = pipeline.SelectedActivityRequestLineage(event)
		} else {
			bundle, _ := semanticview.Bundle(source)
			hash, sourceErr := runtimecontracts.BundleHash(bundle)
			if sourceErr != nil {
				return reject(id, sourceErr)
			}
			var runHash string
			if err := tx.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id = $1`, runID).Scan(&runHash); err != nil {
				return nil, err
			}
			if hash != runHash {
				return reject(id, fmt.Errorf("activity execution source differs from selected run"))
			}
			parent := eventsByID[event.ParentEventID()]
			if parent.ID() == "" {
				return reject(id, fmt.Errorf("activity parent is absent from fork"))
			}
			snapshots, readErr := deliveries.SnapshotsForEvent(ctx, tx, parent.ID())
			if readErr != nil {
				return reject(id, readErr)
			}
			executions := make([]pipeline.ActivityParentExecution, 0, len(snapshots))
			for _, snapshot := range snapshots {
				if snapshot.Status != runtimedelivery.StatusDelivered {
					continue
				}
				selection, readErr := deliveries.HandlerRuleSelection(ctx, tx, snapshot.DeliveryID)
				if readErr != nil {
					return reject(id, readErr)
				}
				executions = append(executions, pipeline.ActivityParentExecution{Delivery: snapshot, RuleSelection: selection})
			}
			activations, readErr := loadRunForkEntityActivations(ctx, tx, runID, event.RoutingSource().Route().EntityID)
			if readErr != nil {
				return reject(id, readErr)
			}
			lineage, err = pipeline.FreshActivityRequestLineage(event, parent, source, executions, activations)
			freshIDs = append(freshIDs, id)
		}
		if err != nil {
			return reject(id, err)
		}
		requests[id] = lineage
	}
	for _, id := range ids {
		event := eventsByID[id]
		subject, diagnostic, err := pipeline.ActivityDiagnosticSubject(event)
		if err != nil {
			return reject(id, err)
		}
		if diagnostic {
			request, ok := requests[subject]
			if !ok {
				return reject(id, fmt.Errorf("activity diagnostic subject is not an admitted request"))
			}
			if err := request.ValidateDiagnostic(event); err != nil {
				return reject(id, err)
			}
		}
		resultType, validResult := false, false
		for _, request := range requests {
			if !request.OwnsResultType(event.Type()) {
				continue
			}
			resultType = true
			if request.ValidateResult(event) == nil {
				validResult = true
				break
			}
		}
		if resultType && !validResult {
			return reject(id, fmt.Errorf("activity result is not owned by an admitted request"))
		}
	}
	return freshIDs, nil
}
