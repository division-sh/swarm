package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// DynamicFlowCreationCompletionFault is an opaque installation receipt. Only
// this owner chooses the fixed trigger/function names and rejection predicate.
type DynamicFlowCreationCompletionFault struct {
	name     string
	postgres bool
}

func InstallDynamicFlowCreationCompletionFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, request pipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (DynamicFlowCreationCompletionFault, error) {
	emitted, err := prepareDynamicFlowCreationOccurrenceCommit(ctx, tx, postgres, request)
	if err != nil {
		return DynamicFlowCreationCompletionFault{}, err
	}
	if emitted {
		return DynamicFlowCreationCompletionFault{}, fmt.Errorf("creation completion fault requires an uncommitted occurrence")
	}
	planHash, err := request.Plan.Hash()
	if err != nil {
		return DynamicFlowCreationCompletionFault{}, err
	}
	// The canonical authorizer already locked and validated this row. Capture
	// its actual grant stamp, which may have been restamped on the same attempt.
	var grant string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(activation_attempt_grant_id AS TEXT) FROM flow_instance_runtime_readiness WHERE CAST(run_id AS TEXT)=$1 AND instance_path=$2`, request.RunID, request.InstancePath).Scan(&grant); err != nil {
		return DynamicFlowCreationCompletionFault{}, err
	}
	literal := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	name := "s6_creation_completion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	predicate := `CAST(NEW.run_id AS TEXT)=` + literal(request.RunID) +
		` AND NEW.instance_path=` + literal(request.InstancePath) +
		` AND CAST(NEW.activation_attempt_id AS TEXT)=` + literal(request.Attempt.ID()) +
		` AND CAST(NEW.activation_attempt_grant_id AS TEXT)=` + literal(grant) +
		` AND NEW.plan_hash=` + literal(planHash) +
		` AND NEW.phase='ready' AND NEW.activation_attempt_state='accepted'` +
		` AND OLD.creation_event_emitted_at IS NULL AND NEW.creation_event_emitted_at IS NOT NULL`
	target := func(field string) string { return `json_extract(delivery_target_route,'$.route.` + field + `')` }
	if postgres {
		target = func(field string) string { return `(delivery_target_route->'route'->>` + literal(field) + `)` }
	}
	// Activity Record queued the authored draft before this mark. Its physical
	// INSERT is finalized later and is intentionally unreached on this abort.
	witness := `EXISTS (SELECT 1 FROM events WHERE CAST(event_id AS TEXT)=` + literal(request.Event.ID()) + ` AND CAST(run_id AS TEXT)=` + literal(request.RunID) + `)` +
		` AND EXISTS (SELECT 1 FROM event_deliveries WHERE CAST(event_id AS TEXT)=` + literal(request.Event.ID()) + ` AND CAST(run_id AS TEXT)=` + literal(request.RunID) +
		` AND ` + target("flow_instance") + `=` + literal(request.InstancePath) +
		` AND ` + target("flow_id") + `=` + literal(request.Plan.Identity.TemplateID) +
		` AND ` + target("entity_id") + `=` + literal(request.Plan.Identity.EntityID) + `)`
	statements := []string{
		`CREATE TRIGGER ` + name + ` BEFORE UPDATE OF creation_event_emitted_at ON flow_instance_runtime_readiness WHEN ` + predicate +
			` BEGIN SELECT CASE WHEN ` + witness + ` THEN RAISE(ABORT,'s6_creation_completion_after_native_append') ELSE RAISE(ABORT,'s6_creation_completion_before_native_append') END; END`,
	}
	if postgres {
		body := `BEGIN IF ` + predicate + ` THEN IF ` + witness + ` THEN RAISE EXCEPTION 's6_creation_completion_after_native_append'; ELSE RAISE EXCEPTION 's6_creation_completion_before_native_append'; END IF; END IF; RETURN NEW; END`
		statements = []string{
			`CREATE FUNCTION ` + name + `() RETURNS trigger LANGUAGE plpgsql AS ` + literal(body),
			`CREATE TRIGGER ` + name + ` BEFORE UPDATE OF creation_event_emitted_at ON flow_instance_runtime_readiness FOR EACH ROW EXECUTE FUNCTION ` + name + `()`,
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return DynamicFlowCreationCompletionFault{}, err
		}
	}
	return DynamicFlowCreationCompletionFault{name: name, postgres: postgres}, nil
}

func RemoveDynamicFlowCreationCompletionFailureTx(ctx context.Context, tx *sql.Tx, fault DynamicFlowCreationCompletionFault) error {
	if tx == nil || !strings.HasPrefix(fault.name, "s6_creation_completion_") || len(fault.name) != len("s6_creation_completion_")+32 {
		return fmt.Errorf("creation completion fault removal requires its exact installation receipt")
	}
	statements := []string{`DROP TRIGGER IF EXISTS ` + fault.name}
	if fault.postgres {
		statements = []string{`DROP TRIGGER IF EXISTS ` + fault.name + ` ON flow_instance_runtime_readiness`, `DROP FUNCTION IF EXISTS ` + fault.name + `()`}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
