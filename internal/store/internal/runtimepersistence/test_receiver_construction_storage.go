package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
)

type ReceiverConstructionStorage struct {
	EntityID, Template, EntityType, State string
	EntityTypePresent                     bool
	Revision                              int
	CreatedAt, UpdatedAt                  string
	OrderedClocks                         bool
	ReadinessPresent                      bool
	Phase, PlanHash                       string
	Plan                                  []byte
	FieldRows                             int
	Fields                                []byte
}

// This is the exact physical construction/attachment witness, not a current
// executable-target selector. Fieldless history and absent attachment survive.
func ReadReceiverConstructionStorageForTest(ctx context.Context, selected any, runID, instance, entityID, entityType string) (ReceiverConstructionStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ReceiverConstructionStorage{}, err
	}
	var out ReceiverConstructionStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var storedType sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT entity_id,flow_template,entity_type,current_state,revision,CAST(created_at AS TEXT),CAST(updated_at AS TEXT),updated_at>=created_at FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, instance).
			Scan(&out.EntityID, &out.Template, &storedType, &out.State, &out.Revision, &out.CreatedAt, &out.UpdatedAt, &out.OrderedClocks); err != nil {
			return err
		}
		out.EntityType, out.EntityTypePresent = storedType.String, storedType.Valid
		var rawPlan string
		err := tx.QueryRowContext(ctx, `SELECT phase,plan_hash,CAST(plan AS TEXT) FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`, runID, instance).Scan(&out.Phase, &out.PlanHash, &rawPlan)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		out.ReadinessPresent = err == nil
		out.Plan = []byte(rawPlan)
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, instance).Scan(&out.FieldRows); err != nil {
			return err
		}
		if entityType != "" {
			var raw string
			if err := tx.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance=$2 AND entity_id=$3 AND entity_type=$4`, runID, instance, entityID, entityType).Scan(&raw); err != nil {
				return err
			}
			out.Fields = []byte(raw)
		}
		return nil
	})
	if err != nil {
		return ReceiverConstructionStorage{}, err
	}
	return out, nil
}
