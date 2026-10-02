package genericschedule

import (
	"context"
	"fmt"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// ReadRunClocks uses the same decoder and immutable-hash validation as recovery.
// Unlike recovery, this read never terminalizes malformed rows or arms wakeups.
func ReadRunClocks(ctx context.Context, query queryContext, postgres bool, runID string, runActive bool) ([]runtimegenericschedule.ClockReadback, error) {
	d := sqliteDialect
	statement := activationSelectColumns + ` FROM timers WHERE run_id = ? AND owner_kind = 'instance' AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY owner_agent, schedule_key, timer_id`
	if postgres {
		d = postgresDialect
		statement = activationSelectColumns + ` FROM timers WHERE run_id = $1::uuid AND owner_kind = 'instance' AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY owner_agent, schedule_key, timer_id`
	}
	rows, err := query.QueryContext(ctx, statement, runID)
	if err != nil {
		return nil, fmt.Errorf("read run clocks: %w", err)
	}
	defer rows.Close()
	views := make([]runtimegenericschedule.ClockReadback, 0)
	for rows.Next() {
		activation, err := scanActivationRow(rows, d)
		if err != nil {
			return nil, fmt.Errorf("read run clock activation: %w", err)
		}
		view, err := runtimegenericschedule.ProjectClockReadback(activation, runActive)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, rows.Err()
}
