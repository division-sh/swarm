package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// PlanWire is physical JSON text, including PostgreSQL's stored formatting.
// It is deliberately not decoded through the reader under test.
type DescriptorReadinessStampStorage struct {
	GrantID, RequestID sql.NullString
	UpdatedAt          time.Time
	Phase, PlanWire    string
}

func ReadDescriptorReadinessStampStorage(ctx context.Context, tx *sql.Tx, postgres bool, run, path string) (DescriptorReadinessStampStorage, error) {
	query := `SELECT activation_attempt_grant_id, activation_request_id, updated_at, phase, plan FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`
	if postgres {
		query = `SELECT activation_attempt_grant_id::text, activation_request_id::text, updated_at, phase, plan::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
	}
	var out DescriptorReadinessStampStorage
	var at any
	if err := tx.QueryRowContext(ctx, query, run, path).Scan(&out.GrantID, &out.RequestID, &at, &out.Phase, &out.PlanWire); err != nil {
		return DescriptorReadinessStampStorage{}, err
	}
	var found bool
	var err error
	out.UpdatedAt, found, err = sqliteTimeValue(at)
	if err != nil {
		return DescriptorReadinessStampStorage{}, err
	}
	if !found {
		return DescriptorReadinessStampStorage{}, fmt.Errorf("readiness stamp requires an exact stored clock")
	}
	return out, nil
}

func SetDescriptorReadinessMalformedPlan(ctx context.Context, tx *sql.Tx, postgres bool, run, path string, wire json.RawMessage) (int64, error) {
	if !json.Valid(wire) {
		return 0, fmt.Errorf("readiness fault requires JSON storage syntax")
	}
	query := `UPDATE flow_instance_runtime_readiness SET plan=$1 WHERE run_id=$2 AND instance_path=$3`
	if postgres {
		query = `UPDATE flow_instance_runtime_readiness SET plan=$1::jsonb WHERE run_id=$2::uuid AND instance_path=$3`
	}
	return workflowProjectionFaultResult(tx.ExecContext(ctx, query, string(wire), run, path))
}

func SetDescriptorReadinessConcurrentPlan(ctx context.Context, tx *sql.Tx, postgres bool, plan pipeline.DynamicFlowRuntimeReadinessPlan) (int64, error) {
	plan, err := plan.Normalized()
	if err != nil {
		return 0, err
	}
	wire, err := json.Marshal(plan)
	if err != nil {
		return 0, err
	}
	hash, err := plan.Hash()
	if err != nil {
		return 0, err
	}
	query := `UPDATE flow_instance_runtime_readiness SET plan=$1, plan_hash=$2 WHERE run_id=$3 AND instance_path=$4`
	if postgres {
		query = `UPDATE flow_instance_runtime_readiness SET plan=$1::jsonb, plan_hash=$2 WHERE run_id=$3::uuid AND instance_path=$4`
	}
	return workflowProjectionFaultResult(tx.ExecContext(ctx, query, string(wire), hash, plan.RunID, plan.Identity.InstancePath))
}

func RemoveDescriptorReadiness(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`, run, path))
}

func SetDescriptorReadinessCreationClock(ctx context.Context, tx *sql.Tx, run, path string, at time.Time) (int64, error) {
	if at.IsZero() {
		return 0, fmt.Errorf("readiness creation fault requires an exact time")
	}
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instance_runtime_readiness SET creation_event_emitted_at=$1 WHERE run_id=$2 AND instance_path=$3`, at.UTC(), run, path))
}

// This named cut faults the exact later row in the production batch UPDATE.
// The trigger and its cleanup are installed through the selected coordinator.
func InstallDescriptorReadinessBatchUpdateFailure(ctx context.Context, tx *sql.Tx, postgres bool, run, path, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return err
	}
	name := descriptorReadinessTriggerName(id)
	if !postgres {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF plan ON flow_instance_runtime_readiness WHEN NEW.run_id = '%s' AND NEW.instance_path = '%s' BEGIN SELECT RAISE(ABORT, 'injected readiness batch failure'); END`, name, run, quoteDescriptorFaultPath(path)))
		return err
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION %s_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id = '%s'::uuid AND NEW.instance_path = '%s' THEN RAISE EXCEPTION 'injected readiness batch failure'; END IF; RETURN NEW; END $$`, name, run, quoteDescriptorFaultPath(path)))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF plan ON flow_instance_runtime_readiness FOR EACH ROW EXECUTE FUNCTION %s_fn()`, name, name))
	return err
}

func RemoveDescriptorReadinessBatchUpdateFailure(ctx context.Context, tx *sql.Tx, postgres bool, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return err
	}
	name := descriptorReadinessTriggerName(id)
	query := `DROP TRIGGER ` + name
	if postgres {
		query += ` ON flow_instance_runtime_readiness`
	}
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return err
	}
	if postgres {
		_, err := tx.ExecContext(ctx, `DROP FUNCTION `+name+`_fn()`)
		return err
	}
	return nil
}

func descriptorReadinessTriggerName(id string) string {
	return "descriptor_readiness_" + strings.ReplaceAll(uuid.MustParse(id).String(), "-", "")
}

func quoteDescriptorFaultPath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

// Constraint probes deliberately attempt invalid rows; successful insertion is
// returned as success so the caller's rejection assertion detects a regression.
func InsertDescriptorEntityStateWithoutType(ctx context.Context, tx *sql.Tx, run string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state) VALUES ($1, $2, 'review/one', 'active')`, run, uuid.NewString())
	return err
}

func InsertDescriptorEntityStateWithBlankType(ctx context.Context, tx *sql.Tx, run string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1, $2, 'review/two', '   ', 'active')`, run, uuid.NewString())
	return err
}
