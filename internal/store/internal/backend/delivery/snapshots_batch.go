package delivery

import (
	"context"
	"fmt"
	"strings"

	. "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

const snapshotEventBatchSize = 128

// SnapshotsForEvents retains complete per-event membership and canonical snapshot
// admission, with one observation time and bounded physical reads. An event with
// no deliveries has an empty slice; event existence belongs to the event adapter.
func (a *Adapter) SnapshotsForEvents(ctx context.Context, q queryer, eventIDs []string) (map[string][]Snapshot, error) {
	ids := make([]string, len(eventIDs))
	out := make(map[string][]Snapshot, len(eventIDs))
	for i, raw := range eventIDs {
		id := strings.TrimSpace(raw)
		parsed, err := uuid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("delivery event snapshots event id: %w", err)
		}
		if a.dialect == DialectPostgres {
			id = parsed.String()
		}
		if _, duplicate := out[id]; duplicate {
			return nil, fmt.Errorf("delivery event snapshots duplicate event %s", id)
		}
		ids[i], out[id] = id, []Snapshot{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	now, err := a.databaseNow(ctx, q)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(ids); start += snapshotEventBatchSize {
		batch := ids[start:min(start+snapshotEventBatchSize, len(ids))]
		args, placeholders := make([]any, len(batch)), make([]string, len(batch))
		for i, id := range batch {
			args[i], placeholders[i] = id, "?"
			if a.dialect == DialectPostgres {
				placeholders[i] = fmt.Sprintf("$%d::uuid", i+1)
			}
		}
		predicate := ` IN (` + strings.Join(placeholders, ", ") + `)`
		// The canonical record join can hide a corrupt run/event relationship.
		// Select direct membership independently, as the scalar adapter does.
		rows, err := q.QueryContext(ctx, `SELECT CAST(delivery_id AS TEXT), CAST(event_id AS TEXT) FROM event_deliveries WHERE event_id`+predicate, args...)
		if err != nil {
			return nil, err
		}
		wanted := make(map[string]string)
		for rows.Next() {
			var id, eventID string
			if err := rows.Scan(&id, &eventID); err != nil {
				rows.Close()
				return nil, err
			}
			if _, duplicate := wanted[id]; duplicate {
				rows.Close()
				return nil, fmt.Errorf("duplicate delivery membership %s", id)
			}
			wanted[id] = eventID
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		rows, err = q.QueryContext(ctx, a.selectRecord()+` WHERE d.event_id`+predicate+` ORDER BY d.created_at, d.delivery_id`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			record, err := a.scanRecord(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			if eventID, found := wanted[record.DeliveryID]; !found || eventID != record.EventID {
				rows.Close()
				return nil, fmt.Errorf("delivery %s duplicated or outside direct membership", record.DeliveryID)
			}
			delete(wanted, record.DeliveryID)
			out[record.EventID] = append(out[record.EventID], snapshotAt(record, now.UTC()))
		}
		readErr = rows.Err()
		closeErr = rows.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for id := range wanted {
			return nil, fmt.Errorf("%w: delivery %s missing from canonical membership", ErrNotFound, id)
		}
	}
	return out, nil
}
