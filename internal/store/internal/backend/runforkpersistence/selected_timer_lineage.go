package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	postgresrecord "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	sqliterecord "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

type selectedTimerActivationRead func(context.Context, *sql.Tx, string) (pipeline.WorkflowTimerActivation, bool, error)

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
