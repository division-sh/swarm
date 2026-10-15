package runtimepersistence

import (
	"context"
	"database/sql"

	data "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type SourceEvaluationResultWire = data.SourceEvaluationResultWire
type RunCreationRequestEvidenceWire = data.RunCreationRequestEvidenceWire

func ReadResourcePhysicalSnapshotForTest(ctx context.Context, selected any) (map[string][]string, error) {
	state := map[string][]string{}
	for part := range data.ResourceReadOnlyObservationCount() {
		err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			table, values, err := data.ReadResourcePhysicalObservationTx(ctx, tx, part)
			if err == nil && len(values) > 0 {
				state[table] = values
			}
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return state, nil
}

func ReadRunCreationRequestHashStorageForTest(ctx context.Context, selected any, id string) (string, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.ReadRunCreationRequestHashStorageTx)
}

func ReadSourceRequestWireForTest(ctx context.Context, selected any, id string) ([]byte, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.ReadSourceRequestWireTx)
}

func ReadSourceEvaluationResultWireForTest(ctx context.Context, selected any, id string) (SourceEvaluationResultWire, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.ReadSourceEvaluationResultWireTx)
}

func ReadImportProvenanceWireForTest(ctx context.Context, selected any, id string) ([]byte, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.ReadImportProvenanceWireTx)
}

func ReadRunCreationRequestEvidenceWireForTest(ctx context.Context, selected any, id string) (RunCreationRequestEvidenceWire, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.ReadRunCreationRequestEvidenceWireTx)
}

func ReadRunCreationChildContextWireForTest(ctx context.Context, selected any, run, source string) ([]byte, error) {
	if err := validateWorkflowProjectionFaultRun(source); err != nil {
		return nil, err
	}
	return readDeploymentStorageForTest(ctx, selected, run, func(ctx context.Context, tx *sql.Tx, run string) ([]byte, error) {
		return data.ReadRunCreationChildContextWireTx(ctx, tx, run, source)
	})
}

func CountPruneInvocationStorageForTest(ctx context.Context, selected any, id string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, id, data.CountPruneInvocationStorageTx)
}
