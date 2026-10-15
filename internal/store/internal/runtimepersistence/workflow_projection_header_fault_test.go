package runtimepersistence

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/lib/pq"
)

func TestWorkflowProjectionDataSchemaRejectsLifecycleShadowsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			if out, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !out.Created || !out.Acknowledged {
				t.Fatalf("actual construction: %+v %v", out, err)
			}
			run, entity := correlation.RunIDFromContext(f.ctx), plan.Instance.EntityID
			owner := flowidentity.RunScopedFlowInstance{RunID: run, Route: plan.Identity.Route()}
			reader := f.store.(pipeline.WorkflowTargetPersistenceReader)
			before, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(entity))
			if err != nil || before.Presence != pipeline.WorkflowTargetPersistenceComplete || before.State.EntityID != entity || before.Lifecycle.State.CurrentState != "active" {
				t.Fatalf("schema witness lacks exact constructed header/data: %+v err=%v", before, err)
			}
			query := `SELECT name FROM pragma_table_info('entity_state')`
			if backend == "postgres" {
				query = `SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='entity_state'`
			}
			rows, err := f.db.QueryContext(f.ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			columns := map[string]bool{}
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(errors.Join(err, rows.Close()))
				}
				if columns[name] {
					t.Fatal(errors.Join(errors.New("schema witness returned a duplicate column"), rows.Close()))
				}
				columns[name] = true
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				t.Fatal(err)
			}
			live := []string{"run_id", "entity_id", "flow_instance", "entity_type", "slug", "name", "fields", "created_at", "updated_at"}
			if len(columns) != len(live) {
				t.Fatalf("fresh data schema has unexpected columns: %+v", columns)
			}
			for _, column := range live {
				if !columns[column] {
					t.Fatalf("schema absence witness lost live data column %s", column)
				}
			}
			for _, column := range []string{"current_state", "gates", "bookkeeping", "accumulator", "revision", "entered_state_at"} {
				t.Run(column, func(t *testing.T) {
					if columns[column] {
						t.Fatalf("fresh data schema still admits lifecycle column %s", column)
					}
					_, err := f.db.ExecContext(f.ctx, `UPDATE entity_state SET `+column+`=NULL WHERE run_id=$1 AND entity_id=$2`, run, entity)
					if backend == "postgres" {
						var native *pq.Error
						if !errors.As(err, &native) || native.Code != "42703" || !strings.Contains(native.Message, `column "`+column+`"`) {
							t.Fatalf("retired %s must fail as that undefined column: %T %v", column, err, err)
						}
					} else {
						var native interface{ Code() int }
						if !errors.As(err, &native) || native.Code() != 1 || !strings.Contains(err.Error(), "no such column: "+column) {
							t.Fatalf("retired %s must fail as that SQLite missing column: %T %v", column, err, err)
						}
					}
					after, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(entity))
					if err != nil || !reflect.DeepEqual(after, before) {
						t.Fatalf("refused %s write changed header/data: before=%+v after=%+v err=%v", column, before, after, err)
					}
				})
			}
			var fields, headers int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&fields); err != nil || fields != 1 {
				t.Fatalf("refused shadow writes changed field membership: count=%d err=%v", fields, err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND entity_id=$2 AND instance_path=$3`, run, entity, plan.Instance.StorageRef).Scan(&headers); err != nil || headers != 1 {
				t.Fatalf("refused shadow writes changed header membership: count=%d err=%v", headers, err)
			}
		})
	}
}
