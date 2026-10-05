package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type scopedFlowReadOwner interface {
	ListActiveFlowInstanceDescriptorsForScope(context.Context, string, []string, []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error)
	ListActiveFlowInstanceDescriptorsForKey(context.Context, string, string, string, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error)
	ListSelectedRunTargetOwnersForScope(context.Context, string, []string, string) ([]runtimebus.ActiveTargetDescriptor, error)
}

func TestScopedFlowDescriptorAndTargetReadsBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *sql.DB
			runID := uuid.NewString()
			if postgres {
				db, _ = postgresRouteStatementFixture(t, 0, false)
				runID = postgresStatementRunID
			} else {
				db, _ = sqliteRouteStatementFixture(t, 0)
				runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{
					RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(),
				})
			}
			ctx := context.Background()
			createScopedReadFixture(t, db, postgres, runID)
			var selected scopedFlowReadOwner
			if postgres {
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				selected = &PipelinePostgresOwner{backend: backend}
			} else {
				backend, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				selected = &PipelineSQLiteOwner{backend: backend}
			}

			listFlows := func(templates, paths []string) ([]string, error) {
				rows, err := selected.ListActiveFlowInstanceDescriptorsForScope(ctx, runID, templates, paths)
				if err != nil {
					return nil, err
				}
				descriptors := make([]string, 0, len(rows))
				for _, row := range rows {
					descriptors = append(descriptors, row.FlowInstance)
				}
				return descriptors, nil
			}
			listTargets := func(paths []string, sourceEntityID string) ([]string, error) {
				rows, err := selected.ListSelectedRunTargetOwnersForScope(ctx, runID, paths, sourceEntityID)
				if err != nil {
					return nil, err
				}
				targets := make([]string, 0, len(rows))
				for _, row := range rows {
					targets = append(targets, row.FlowInstance)
				}
				return targets, nil
			}

			for _, tc := range []struct {
				name      string
				templates []string
				paths     []string
				want      []string
			}{
				{"template", []string{"review"}, nil, []string{"review/a", "review/b"}},
				{"path", nil, []string{"other/c"}, []string{"other/c"}},
				{"union", []string{"review"}, []string{"other/c"}, []string{"other/c", "review/a", "review/b"}},
				{"duplicate_input", []string{"review", "review"}, []string{"review/a"}, []string{"review/a", "review/b"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := listFlows(tc.templates, tc.paths)
					if err != nil || !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("scoped flow descriptors = %v, %v; want %v", got, err, tc.want)
					}
				})
			}
			if got, err := listTargets([]string{"root", "review/b"}, ""); err != nil || !reflect.DeepEqual(got, []string{"review/b", "root"}) {
				t.Fatalf("scoped targets = %v, %v", got, err)
			}
			var sourceEntityID string
			if err := db.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance='review/a'`, runID).Scan(&sourceEntityID); err != nil {
				t.Fatal(err)
			}
			if got, err := listTargets(nil, sourceEntityID); err != nil || !reflect.DeepEqual(got, []string{"review/a"}) {
				t.Fatalf("source entity target = %v, %v", got, err)
			}
			if got, err := listTargets([]string{"review/b"}, sourceEntityID); err != nil || !reflect.DeepEqual(got, []string{"review/a", "review/b"}) {
				t.Fatalf("source and connected targets = %v, %v", got, err)
			}
			if got, err := listTargets([]string{"missing"}, uuid.NewString()); err != nil || len(got) != 0 {
				t.Fatalf("absent scoped target = %v, %v", got, err)
			}
			if _, err := listFlows(nil, nil); err == nil || !strings.Contains(err.Error(), "graph-owned scope") {
				t.Fatalf("empty flow scope accepted: %v", err)
			}
			if _, err := listTargets(nil, ""); err == nil || !strings.Contains(err.Error(), "graph-owned paths or source entity") {
				t.Fatalf("empty target scope accepted: %v", err)
			}
			if _, err := listTargets(nil, "not-an-entity"); err == nil || !strings.Contains(err.Error(), "canonical UUID") {
				t.Fatalf("invalid source entity accepted: %v", err)
			}
			if _, err := listFlows(nil, []string{"/review/a"}); err == nil || !strings.Contains(err.Error(), "canonical instance path") {
				t.Fatalf("noncanonical path accepted: %v", err)
			}

			// A bad unrelated readiness plan must not poison an exact scoped read.
			if _, err := db.ExecContext(ctx, `UPDATE flow_instance_runtime_readiness SET plan='{}' WHERE instance_path='other/c'`); err != nil {
				t.Fatal(err)
			}
			if got, err := listFlows([]string{"review"}, nil); err != nil || !reflect.DeepEqual(got, []string{"review/a", "review/b"}) {
				t.Fatalf("unrelated invalid descriptor escaped scope: %v, %v", got, err)
			}
			if _, err := listFlows(nil, []string{"other/c"}); err == nil {
				t.Fatal("selected invalid descriptor was not rejected")
			}
		})
	}
}

func TestKeyedFlowDescriptorCandidatesBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *sql.DB
			runID := uuid.NewString()
			if postgres {
				db, _ = postgresRouteStatementFixture(t, 0, false)
				runID = postgresStatementRunID
			} else {
				db, _ = sqliteRouteStatementFixture(t, 0)
				runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{
					RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(),
				})
			}
			ctx := context.Background()
			createScopedReadFixture(t, db, postgres, runID)
			var selected scopedFlowReadOwner
			if postgres {
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				selected = &PipelinePostgresOwner{backend: backend}
			} else {
				backend, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				selected = &PipelineSQLiteOwner{backend: backend}
			}
			lookup := func(field, value string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
				return selected.ListActiveFlowInstanceDescriptorsForKey(ctx, runID, "review", field, value)
			}
			if rows, err := lookup("entity.startup.id", "alpha"); err != nil || len(rows) != 1 || rows[0].FlowInstance != "review/a" || rows[0].AddressFields["entity.startup.id"] != "/alpha/" {
				t.Fatalf("trimmed literal string key = %v, %v", rows, err)
			}
			if rows, err := lookup("entity.startup.id", " \u00a0/alpha/\u00a0 "); err != nil || len(rows) != 1 || rows[0].FlowInstance != "review/a" {
				t.Fatalf("surrounding whitespace/slashes lost exact candidate: %v, %v", rows, err)
			}
			if rows, err := lookup("entity.startup.id", "/a/lpha/"); err != nil || len(rows) != 0 {
				t.Fatalf("internal slash changed candidate identity: %v, %v", rows, err)
			}
			if rows, err := lookup("entity.startup.id", "unmatched"); err != nil || len(rows) != 0 {
				t.Fatalf("unmatched string key = %v, %v", rows, err)
			}
			for _, tc := range []struct{ field, value, path, projected string }{
				{"entity.count", "1000", "review/a", "1000"},
				{"entity.flag", "true", "review/a", "true"},
			} {
				rows, err := lookup(tc.field, tc.value)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, row := range rows {
					if row.FlowInstance == tc.path && row.AddressFields[tc.field] == tc.projected {
						found = true
					}
				}
				if !found {
					t.Fatalf("numeric/bool candidate %s=%q lost: %v", tc.field, tc.value, rows)
				}
			}

			var bundle string
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&bundle); err != nil {
				t.Fatal(err)
			}
			entityID := uuid.NewString()
			plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
				Identity: flowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: "z", InstancePath: "review/z", EntityID: entityID, HasStoredPath: true},
				RunID:    runID, BundleHash: bundle, WorkflowVersion: "fixture-version", ExecutionMode: "live",
			}).Normalized()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			planHash, err := plan.Hash()
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []struct {
				statement string
				args      []any
			}{
				{`INSERT INTO flow_instances (run_id,instance_path,flow_template,status,mode) VALUES($1,'review/z','review','active','template')`, []any{runID}},
				{`INSERT INTO flow_instance_runtime_readiness (run_id,instance_path,plan,plan_hash,activation_attempt_id) VALUES($1,'review/z',$2,$3,1)`, []any{runID, string(encoded), planHash}},
				{`INSERT INTO entity_state (run_id,flow_instance,entity_id,current_state,fields) VALUES($1,'review/z',$2,'ready',$3)`, []any{runID, entityID, `{"startup.id":"alpha"}`}},
			} {
				if _, err := db.ExecContext(ctx, query.statement, query.args...); err != nil {
					t.Fatal(err)
				}
			}
			if rows, err := lookup("entity.startup.id", "alpha"); err != nil || len(rows) != 2 || rows[0].FlowInstance != "review/a" || rows[1].FlowInstance != "review/z" {
				t.Fatalf("wrong-path same-key duplicate was hidden: %v, %v", rows, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields='null' WHERE flow_instance='review/b'`); err != nil {
				t.Fatal(err)
			}
			if rows, err := lookup("entity.startup.id", "alpha"); err != nil || len(rows) != 3 || rows[1].FlowInstance != "review/b" || len(rows[1].AddressFields) != 0 {
				t.Fatalf("null fields were hidden from canonical matcher: %v, %v", rows, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields='[]' WHERE flow_instance='other/c'`); err != nil {
				t.Fatal(err)
			}
			if rows, err := lookup("entity.startup.id", "alpha"); err != nil || len(rows) != 3 {
				t.Fatalf("unrelated template corruption leaked: %v, %v", rows, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields='[]' WHERE flow_instance='review/b'`); err != nil {
				t.Fatal(err)
			}
			if _, err := lookup("entity.startup.id", "alpha"); err == nil {
				t.Fatal("corrupt selected-template fields were silently filtered")
			}
			if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields=NULL WHERE flow_instance='review/b'`); err != nil {
				t.Fatal(err)
			}
			if _, err := lookup("entity.startup.id", "alpha"); err == nil {
				t.Fatal("missing selected-template fields were silently filtered")
			}
			if !postgres {
				if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields='{' WHERE flow_instance='review/b'`); err != nil {
					t.Fatal(err)
				}
				if _, err := lookup("entity.startup.id", "alpha"); err == nil {
					t.Fatal("malformed sqlite selected-template fields were silently filtered")
				}
			}
			if _, err := lookup("startup.id", "alpha"); err == nil || !strings.Contains(err.Error(), "entity.<literal field>") {
				t.Fatalf("noncanonical key field admitted: %v", err)
			}
		})
	}
}

func createScopedReadFixture(t *testing.T, db *sql.DB, postgres bool, runID string) {
	t.Helper()
	queries := []string{
		`CREATE TABLE flow_instances (run_id TEXT, instance_path TEXT, flow_template TEXT, status TEXT, mode TEXT, terminated_at TIMESTAMP, entity_id TEXT, current_state TEXT)`,
		`CREATE TABLE flow_instance_runtime_readiness (run_id TEXT, instance_path TEXT, plan TEXT, plan_hash TEXT NOT NULL, activation_attempt_id INTEGER NOT NULL, phase TEXT NOT NULL DEFAULT 'planned', activation_attempt_state TEXT NOT NULL DEFAULT 'planned')`,
		`CREATE TABLE entity_state (run_id TEXT, flow_instance TEXT, entity_id TEXT, current_state TEXT, fields TEXT)`,
	}
	if postgres {
		queries = []string{
			`CREATE TABLE flow_instances (run_id UUID, instance_path TEXT, flow_template TEXT, status TEXT, mode TEXT, terminated_at TIMESTAMPTZ, entity_id UUID, current_state TEXT)`,
			`CREATE TABLE flow_instance_runtime_readiness (run_id UUID, instance_path TEXT, plan JSONB, plan_hash TEXT NOT NULL, activation_attempt_id BIGINT NOT NULL, phase TEXT NOT NULL DEFAULT 'planned', activation_attempt_state TEXT NOT NULL DEFAULT 'planned')`,
			`CREATE TABLE entity_state (run_id UUID, flow_instance TEXT, entity_id UUID, current_state TEXT, fields JSONB)`,
		}
	}
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	var bundle string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&bundle); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ path, template, instance string }{
		{"review/a", "review", "a"},
		{"review/b", "review", "b"},
		{"other/c", "other", "c"},
	} {
		entityID := uuid.NewString()
		plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: flowidentity.Instance{TemplateID: item.template, ScopeKey: item.template, InstanceID: item.instance, InstancePath: item.path, EntityID: entityID, HasStoredPath: true},
			RunID:    runID, BundleHash: bundle, WorkflowVersion: "fixture-version", ExecutionMode: "live",
		}).Normalized()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		planHash, err := plan.Hash()
		if err != nil {
			t.Fatal(err)
		}
		fields := `{"startup.id":"other","count":3,"flag":false}`
		switch item.path {
		case "review/a":
			fields = `{"startup.id":"\u00a0/alpha/\u00a0","count":1e3,"flag":true}`
		case "review/b":
			fields = `{"startup.id":"beta","count":2.0,"flag":false}`
		}
		for _, query := range []struct {
			statement string
			args      []any
		}{
			{`INSERT INTO flow_instances (run_id,instance_path,flow_template,status,mode,entity_id,current_state) VALUES($1,$2,$3,'active','template',$4,'ready')`, []any{runID, item.path, item.template, entityID}},
			{`INSERT INTO flow_instance_runtime_readiness (run_id,instance_path,plan,plan_hash,activation_attempt_id) VALUES($1,$2,$3,$4,1)`, []any{runID, item.path, string(encoded), planHash}},
			{`INSERT INTO entity_state (run_id,flow_instance,entity_id,current_state,fields) VALUES($1,$2,$3,'ready',$4)`, []any{runID, item.path, entityID, fields}},
		} {
			if _, err := db.Exec(query.statement, query.args...); err != nil {
				t.Fatal(err)
			}
		}
	}
	rootEntityID := uuid.NewString()
	if _, err := db.Exec(`INSERT INTO flow_instances (run_id,instance_path,flow_template,status,mode,entity_id,current_state) VALUES($1,'root','root','active','static',$2,'ready')`, runID, rootEntityID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO entity_state (run_id,flow_instance,entity_id,current_state,fields) VALUES($1,'root',$2,'ready','{}')`, runID, rootEntityID); err != nil {
		t.Fatal(err)
	}
}
