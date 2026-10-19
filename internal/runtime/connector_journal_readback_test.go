package runtime_test

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func runtimeConnectorActivityRows(ctx context.Context, selected any, runID, tool string) ([]storetest.ActivityAttemptStorageEvidence, error) {
	rows, err := storetest.ReadActivityAttemptStorage(ctx, selected, runID)
	if err != nil {
		return nil, err
	}
	var matches []storetest.ActivityAttemptStorageEvidence
	for _, row := range rows {
		if row.Tool == tool {
			matches = append(matches, row)
		}
	}
	return matches, nil
}

func runtimeConnectorActivityRowsForSource(ctx context.Context, selected any, runID, tool, sourceEventID string) ([]storetest.ActivityAttemptStorageEvidence, error) {
	id, err := uuid.Parse(sourceEventID)
	if err != nil || id == uuid.Nil || id.String() != sourceEventID {
		return nil, fmt.Errorf("connector journal readback requires an exact canonical source event")
	}
	rows, err := runtimeConnectorActivityRows(ctx, selected, runID, tool)
	if err != nil {
		return nil, err
	}
	var matches []storetest.ActivityAttemptStorageEvidence
	for _, row := range rows {
		if row.SourceEventID == sourceEventID {
			matches = append(matches, row)
		}
	}
	return matches, nil
}
