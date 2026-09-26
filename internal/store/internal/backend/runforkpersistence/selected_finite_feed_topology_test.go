package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedFiniteFeedRecoveryTopologiesUseExactPersistedReadinessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			jsonType := "TEXT"
			if backend == "postgres" {
				jsonType = "JSONB"
			}
			for _, ddl := range []string{
				`CREATE TABLE flow_instances (run_id TEXT NOT NULL,instance_path TEXT NOT NULL,flow_template TEXT NOT NULL,mode TEXT NOT NULL,PRIMARY KEY(run_id,instance_path))`,
				`CREATE TABLE flow_instance_runtime_readiness (run_id TEXT NOT NULL,instance_path TEXT NOT NULL,plan ` + jsonType + ` NOT NULL,PRIMARY KEY(run_id,instance_path))`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx, runID := context.Background(), uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			source, err := correlation.NewSourceArtifactFact(bundle)
			if err != nil {
				t.Fatal(err)
			}
			identity := agentidentitytest.DeclaredForRun(t, runID, "worker", "flow/worker", "flow", "instance", "flow/instance")
			declaration, err := identity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			plan, want, encoded, err := selectedContractWorkflowReadiness(source, selectedContractWorkflowState{
				RunID: runID, EntityID: uuid.NewString(), WorkflowName: "flow", WorkflowVersion: "v1",
				ExecutionMode: executionmode.Mock, Mode: "template", Route: "flow/instance",
				Agents: []runfork.RunForkSelectedContractAgentExpectation{{Plan: declaration, ConfigRevision: strings.Repeat("b", 64)}},
			})
			if err != nil || plan == nil || len(want) != 1 {
				t.Fatalf("construct exact readiness: plan=%+v topologies=%+v err=%v", plan, want, err)
			}
			for _, row := range []struct{ path, template, mode string }{{"root", "root", "static"}, {"flow/instance", "flow", "template"}} {
				if _, err := db.Exec(`INSERT INTO flow_instances VALUES ($1,$2,$3,$4)`, runID, row.path, row.template, row.mode); err != nil {
					t.Fatal(err)
				}
			}
			read := func(valid bool) {
				t.Helper()
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					got, err := selectedFiniteFeedRecoveryTopologiesTx(ctx, tx, runID, bundle)
					if valid {
						if err != nil || !reflect.DeepEqual(got, want) {
							t.Fatalf("recovered topologies=%+v want=%+v err=%v", got, want, err)
						}
					} else if err == nil {
						t.Fatalf("corrupt readiness admitted: %+v", got)
					}
				})
			}
			read(false) // A template without its durable plan cannot be synthesized.
			if _, err := db.Exec(`INSERT INTO flow_instance_runtime_readiness VALUES ($1,'flow/instance',$2)`, runID, string(encoded)); err != nil {
				t.Fatal(err)
			}
			read(true)
			for _, altered := range []struct {
				name   string
				change func(*string, *string)
			}{
				{"wrong-run", func(run, _ *string) { *run = uuid.NewString() }},
				{"wrong-bundle", func(_, hash *string) { *hash = "bundle-v2:sha256:" + strings.Repeat("c", 64) }},
			} {
				t.Run(altered.name, func(t *testing.T) {
					changed := *plan
					altered.change(&changed.RunID, &changed.BundleHash)
					raw, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(`UPDATE flow_instance_runtime_readiness SET plan=$1 WHERE run_id=$2 AND instance_path='flow/instance'`, string(raw), runID); err != nil {
						t.Fatal(err)
					}
					read(false)
					if _, err := db.Exec(`UPDATE flow_instance_runtime_readiness SET plan=$1 WHERE run_id=$2 AND instance_path='flow/instance'`, string(encoded), runID); err != nil {
						t.Fatal(err)
					}
				})
			}
			if _, err := db.Exec(`UPDATE flow_instances SET flow_template='other' WHERE run_id=$1 AND instance_path='flow/instance'`, runID); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`UPDATE flow_instances SET flow_template='flow' WHERE run_id=$1 AND instance_path='flow/instance'`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO flow_instance_runtime_readiness VALUES ($1,'orphan',$2)`, runID, string(encoded)); err != nil {
				t.Fatal(err)
			}
			read(false)
		})
	}
}
