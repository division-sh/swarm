package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

// LoadAdmittedMany preserves request order and rejects duplicate or missing IDs.
// SQLite text identities retain their original spelling.
func LoadAdmittedMany(ctx context.Context, q Queryer, eventIDs []string) ([]eventrecord.AdmittedRecord, error) {
	ordered, err := normalizeIDs(eventIDs)
	if err != nil {
		return nil, err
	}
	out := make([]eventrecord.AdmittedRecord, 0, len(ordered))
	for start := 0; start < len(ordered); start += hydrationBatchSize {
		batch := ordered[start:min(start+hydrationBatchSize, len(ordered))]
		args, placeholders := make([]any, len(batch)), make([]string, len(batch))
		wanted := make(map[string]bool, len(batch))
		for i, id := range batch {
			args[i], placeholders[i], wanted[id] = id, "?", true
		}
		rows, err := q.QueryContext(ctx, selectRecord+` WHERE e.event_id IN (`+strings.Join(placeholders, ", ")+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("load sqlite admitted event batch: %w", err)
		}
		loaded := make(map[string]eventrecord.Record, len(batch))
		for rows.Next() {
			var record eventrecord.Record
			var createdAt any
			if err := rows.Scan(scanTargets(&record, &createdAt)...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan sqlite admitted event batch: %w", err)
			}
			if !wanted[record.EventID] {
				rows.Close()
				return nil, fmt.Errorf("event record %s duplicated or outside requested batch", record.EventID)
			}
			delete(wanted, record.EventID)
			if err := assignCreatedAt(&record, createdAt); err != nil {
				rows.Close()
				return nil, eventrecord.Corrupt(record.EventID, err)
			}
			loaded[record.EventID] = record
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		records := make([]eventrecord.Record, len(batch))
		for i, id := range batch {
			records[i] = loaded[id]
		}
		decoded := eventrecord.DecodeLoadedRecords(records)
		for i, id := range batch {
			record, found := loaded[id]
			if !found {
				return nil, eventrecord.Missing(id)
			}
			if decoded[i].Err != nil {
				return nil, decoded[i].Err
			}
			if err := record.ValidateInheritedFanOutOwner(ctx, q, false); err != nil {
				return nil, err
			}
			out = append(out, decoded[i].AdmittedRecord)
		}
	}
	return out, nil
}
