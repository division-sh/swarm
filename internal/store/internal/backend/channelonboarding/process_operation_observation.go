package channelonboarding

import (
	"context"
	"database/sql"
	"fmt"
)

type OnboardingOperationDiagnosticStorage struct {
	Phase, FailureCode, FailureMessage, BundleHash, BundleIdentity string
	Inventory, RuntimeInstance, Plan, Target                       string
	Publication, TargetGeneration                                  int64
}

func ReadOnboardingOperationDiagnosticStorageTx(ctx context.Context, tx *sql.Tx, operation string) (OnboardingOperationDiagnosticStorage, error) {
	var out OnboardingOperationDiagnosticStorage
	err := tx.QueryRowContext(ctx, `SELECT phase,failure_code,failure_message,bundle_hash,bundle_identity,
		pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,target_selector,target_generation
		FROM channel_onboarding_operations WHERE operation_id=$1`, operation).Scan(
		&out.Phase, &out.FailureCode, &out.FailureMessage, &out.BundleHash, &out.BundleIdentity,
		&out.Inventory, &out.RuntimeInstance, &out.Publication, &out.Plan, &out.Target, &out.TargetGeneration,
	)
	return out, err
}

type OnboardingPublicationHandoffStorage struct {
	OperationPhase              string
	OperationActivationRevision int64
	OperationRuntimeInstanceID  string
	ActivationRevision          int64
	ActivationRuntimeInstanceID string
	ActivationStatus            string
}

func ReadOnboardingPublicationHandoffStorageTx(ctx context.Context, tx *sql.Tx, operation string) (OnboardingPublicationHandoffStorage, error) {
	var out OnboardingPublicationHandoffStorage
	err := tx.QueryRowContext(ctx, `SELECT operation.phase,operation.activation_revision,operation.runtime_instance_id,
		activation.activation_revision,activation.runtime_instance_id,activation.status
		FROM channel_onboarding_operations operation
		JOIN connected_channel_activations activation ON activation.operation_id=operation.operation_id
		WHERE operation.operation_id=$1 ORDER BY activation.activation_revision DESC LIMIT 1`, operation).Scan(
		&out.OperationPhase, &out.OperationActivationRevision, &out.OperationRuntimeInstanceID,
		&out.ActivationRevision, &out.ActivationRuntimeInstanceID, &out.ActivationStatus,
	)
	return out, err
}

func ReadOnboardingOperationPhysicalSurfaceStorageTx(ctx context.Context, tx *sql.Tx, operation string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT * FROM channel_onboarding_operations WHERE operation_id=$1`, operation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	values, destinations := make([]any, len(columns)), make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.Scan(destinations...); err != nil {
		return nil, err
	}
	record := make(map[string]string, len(columns))
	for index, column := range columns {
		switch value := values[index].(type) {
		case []byte:
			record[column] = string(value)
		default:
			record[column] = fmt.Sprint(value)
		}
	}
	return record, rows.Close()
}
