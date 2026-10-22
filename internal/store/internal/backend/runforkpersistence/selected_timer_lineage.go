package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	postgresrecord "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	sqliterecord "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
)

type selectedTimerActivationRead func(context.Context, *sql.Tx, string) (pipeline.WorkflowTimerActivation, bool, error)
type selectedGenericOccurrenceRead func(context.Context, *sql.Tx, bool, string) ([]genericschedule.Activation, error)

func selectedContractTimerLineage(ctx context.Context, tx *sql.Tx, runID string, postgres bool, load selectedTimerActivationRead) ([]string, error) {
	ordinary, err := selectedContractWorkflowTimerLineage(ctx, tx, runID, postgres, load)
	if err != nil {
		return nil, err
	}
	joins, err := selectedContractGenericScheduleLineage(ctx, tx, runID, postgres, storegenericschedule.ReadPublishedOccurrencesTx)
	if err != nil {
		return nil, err
	}
	return append(ordinary, joins...), nil
}

func selectedContractGenericScheduleLineage(ctx context.Context, tx *sql.Tx, runID string, postgres bool, load selectedGenericOccurrenceRead) ([]string, error) {
	activations, err := load(ctx, tx, postgres, runID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(activations))
	seen := make(map[string]struct{}, len(activations))
	for _, activation := range activations {
		if err := activation.Validate(); err != nil {
			return nil, err
		}
		if activation.Command.RunID != runID || activation.Status != genericschedule.StatusFired || activation.CurrentEventID == "" {
			return nil, fmt.Errorf("selected schedule lineage requires an exact accepted child occurrence")
		}
		if _, duplicate := seen[activation.CurrentEventID]; duplicate {
			return nil, fmt.Errorf("selected schedule lineage repeats an accepted occurrence")
		}
		seen[activation.CurrentEventID] = struct{}{}
		var record eventrecord.Record
		var found bool
		if postgres {
			record, found, err = postgresrecord.Load(ctx, tx, activation.CurrentEventID)
		} else {
			record, found, err = sqliterecord.Load(ctx, tx, activation.CurrentEventID)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("selected schedule lineage lacks its exact published event")
		}
		admitted, err := record.Decode()
		if err != nil {
			return nil, err
		}
		if _, err := activation.ValidatePublishedOccurrence(admitted.Event()); err != nil {
			return nil, err
		}
		ids = append(ids, activation.CurrentEventID)
	}
	return ids, nil
}

// The caller already holds exact selected settlement authority. These are
// canonical obligation publications, not input replay or permission to execute.
func selectedContractWorkflowTimerLineage(ctx context.Context, tx *sql.Tx, runID string, postgres bool, load selectedTimerActivationRead) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id, task_id FROM events
		WHERE run_id=$1 AND event_class='runtime_control' AND produced_by_type='platform'
		AND produced_by='runtime.workflow_timer' ORDER BY event_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("read selected timer publication census: %w", err)
	}
	type reference struct{ event, task string }
	var refs []reference
	for rows.Next() {
		var ref reference
		if err := rows.Scan(&ref.event, &ref.task); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(ref.task)
		if !valid || ref.event != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) {
			return nil, fmt.Errorf("selected timer publication has invalid occurrence identity")
		}
		activation, found, err := load(ctx, tx, occurrence.Activation.ActivationID)
		if err != nil {
			return nil, err
		}
		if !found || activation.RunID != runID {
			return nil, fmt.Errorf("selected timer publication has no exact child activation")
		}
		var record eventrecord.Record
		if postgres {
			record, found, err = postgresrecord.Load(ctx, tx, ref.event)
		} else {
			record, found, err = sqliterecord.Load(ctx, tx, ref.event)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("selected timer publication is missing")
		}
		admitted, err := record.Decode()
		if err != nil {
			return nil, err
		}
		event := admitted.Event()
		if event.RunID() != runID || event.AdmissionClass() != events.EventAdmissionRuntimeControl {
			return nil, fmt.Errorf("selected timer publication has foreign event ownership")
		}
		if _, err := activation.ValidatePublishedOccurrence(event); err != nil {
			return nil, err
		}
		ids = append(ids, event.ID())
	}
	return ids, nil
}
