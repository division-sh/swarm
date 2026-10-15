package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func ReadForkReceiverCompanionStorageForTest(ctx context.Context, selected any, runID string, paths ...string) (map[string][]string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness"} {
		var columns, rows []string
		err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			columns, rows, err = pipelinepersistence.ReadForkReceiverCompanionTableStorageTx(ctx, tx, table, runID, paths...)
			return err
		})
		if err != nil {
			return nil, err
		}
		out[table+"/columns"], out[table] = columns, rows
	}
	return out, nil
}
