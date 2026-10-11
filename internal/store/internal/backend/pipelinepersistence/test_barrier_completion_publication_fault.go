package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// This closed fault aborts the actual completion output only after the native
// transaction has changed the paired header/fields and appended authored
// mutations. A premature insert is not faulted: the execution test must prove
// the dead letter rather than mistaking an earlier setup error for this cut.
func SetBarrierCompletionPublicationFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, run, entity string, revision, mutations int, enabled bool) error {
	if tx == nil || revision <= 0 || mutations < 0 {
		return fmt.Errorf("barrier completion fault requires a transaction and existing state")
	}
	for _, value := range []string{run, entity} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("barrier completion fault requires canonical run/entity identities")
		}
	}
	statements := []string{"DROP TRIGGER selected_barrier_completion_output_cut"}
	if postgres {
		statements = []string{"DROP TRIGGER selected_barrier_completion_output_cut ON events", "DROP FUNCTION selected_barrier_completion_output_cut()"}
	}
	if enabled {
		witness := fmt.Sprintf(`EXISTS (SELECT 1 FROM entity_state WHERE run_id='%s' AND entity_id='%s' AND revision=%d)
			AND EXISTS (SELECT 1 FROM flow_instances WHERE run_id='%s' AND entity_id='%s' AND revision=%d)
			AND (SELECT COUNT(*) FROM entity_mutations WHERE run_id='%s' AND entity_id='%s')>%d`,
			run, entity, revision+1, run, entity, revision+1, run, entity, mutations)
		statements = []string{fmt.Sprintf(`CREATE TRIGGER selected_barrier_completion_output_cut AFTER INSERT ON events
			WHEN NEW.run_id='%s' AND NEW.event_name='producer/batch.completed' AND (%s)
			BEGIN SELECT RAISE(ABORT,'selected_barrier_completion_late_output_fault'); END`, run, witness)}
		if postgres {
			statements = []string{fmt.Sprintf(`CREATE FUNCTION selected_barrier_completion_output_cut() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN IF NEW.run_id='%s'::uuid AND NEW.event_name='producer/batch.completed' THEN
					IF %s THEN RAISE EXCEPTION 'selected_barrier_completion_late_output_fault'; END IF;
				END IF; RETURN NEW; END $$`, run, witness),
				"CREATE TRIGGER selected_barrier_completion_output_cut AFTER INSERT ON events FOR EACH ROW EXECUTE FUNCTION selected_barrier_completion_output_cut()"}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
