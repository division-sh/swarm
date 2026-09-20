package pipelinepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

// Frozen raw queries are the pre-preparation oracle; predicates and order are
// intentionally identical, including nullable joined readiness/entity facts.
const descriptorFixedReadFlowsBefore = `
		SELECT fi.run_id, fi.instance_path, fi.flow_template, readiness.plan,
		       run.bundle_hash, es.fields
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.run_id = fi.run_id AND readiness.instance_path = fi.instance_path
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es
		  ON es.run_id = fi.run_id
		 AND es.flow_instance = fi.instance_path
		 AND es.entity_id = json_extract(readiness.plan, '$.identity.EntityID')
		WHERE fi.run_id = ?
		  AND fi.status = 'active' AND fi.mode = 'template'
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state owned
			  WHERE owned.run_id = fi.run_id
			    AND owned.flow_instance = fi.instance_path
		  )
		ORDER BY fi.run_id, fi.instance_path ASC
	`

const descriptorFixedReadTargetsBefore = `
		SELECT es.entity_id, es.flow_instance, es.current_state,
 CASE WHEN fi.instance_path IS NULL THEN 'active' ELSE fi.status END, fi.terminated_at IS NOT NULL
		FROM entity_state es
 LEFT JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance
		JOIN runs run ON run.run_id = es.run_id
		WHERE es.run_id = ?
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		ORDER BY es.flow_instance ASC, es.entity_id ASC
	`

func TestDescriptorFixedReadsFreshFactsAndCanonicalErrors(t *testing.T) {
	db, _ := sqliteRouteStatementFixture(t, 0)
	b, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	owner := &PipelineSQLiteOwner{backend: b}
	ctx := context.Background()
	runID := uuid.NewString()
	runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin()})
	for _, ddl := range []string{
		`CREATE TABLE flow_instances (run_id TEXT, instance_path TEXT, flow_template TEXT, status TEXT, mode TEXT, terminated_at TIMESTAMP)`,
		`CREATE TABLE flow_instance_runtime_readiness (run_id TEXT, instance_path TEXT, plan TEXT)`,
		`CREATE TABLE entity_state (run_id TEXT, flow_instance TEXT, entity_id TEXT, current_state TEXT, fields TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	var bundle string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=?`, runID).Scan(&bundle); err != nil {
		t.Fatal(err)
	}
	entityID := uuid.NewString()
	plan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: flowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: "one", InstancePath: "review/one", EntityID: entityID, HasStoredPath: true},
		RunID:    runID, BundleHash: bundle, WorkflowVersion: "fixture-version", ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	compare := func(flowFailure, targetFailure bool) {
		t.Helper()
		rows, rawErr := db.QueryContext(ctx, descriptorFixedReadFlowsBefore, runID)
		var raw any
		if rawErr == nil {
			raw, rawErr = scanExactActiveFlowInstanceDescriptors(rows, "sqlite active flow instance descriptor")
		} else {
			rawErr = fmt.Errorf("list sqlite active flow instance descriptors: %w", rawErr)
		}
		got, gotErr := owner.ListActiveFlowInstanceDescriptors(ctx, " "+runID+" ")
		if (gotErr != nil) != flowFailure || fmt.Sprint(rawErr) != fmt.Sprint(gotErr) || reflect.TypeOf(rawErr) != reflect.TypeOf(gotErr) || rawErr == nil && !reflect.DeepEqual(raw, got) {
			t.Fatalf("flows raw=%v/%v prepared=%v/%v want failure=%v", raw, rawErr, got, gotErr, flowFailure)
		}
		rows, rawErr = db.QueryContext(ctx, descriptorFixedReadTargetsBefore, runID)
		if rawErr == nil {
			raw, rawErr = scanSelectedRunTargetOwners(rows, "sqlite selected-run target owner")
		} else {
			rawErr = fmt.Errorf("list sqlite selected-run target owners: %w", rawErr)
		}
		targets, targetErr := owner.ListSelectedRunTargetOwners(ctx, " "+runID+" ")
		if (targetErr != nil) != targetFailure || fmt.Sprint(rawErr) != fmt.Sprint(targetErr) || reflect.TypeOf(rawErr) != reflect.TypeOf(targetErr) || rawErr == nil && !reflect.DeepEqual(raw, targets) {
			t.Fatalf("targets raw=%v/%v prepared=%v/%v want failure=%v", raw, rawErr, targets, targetErr, targetFailure)
		}
	}
	compare(false, false)
	for _, tc := range []struct {
		query                      string
		args                       []any
		flowFailure, targetFailure bool
	}{
		{`INSERT INTO entity_state VALUES ('run','review/one',?,'ready','{"address":"first"}')`, []any{entityID}, false, false},
		{`INSERT INTO flow_instances VALUES ('run','review/one','review','active','template',NULL)`, nil, true, false},
		{`INSERT INTO flow_instance_runtime_readiness VALUES ('run','review/one',?)`, []any{string(planRaw)}, false, false},
		{`UPDATE entity_state SET fields='{"address":"second"}', current_state='done'`, nil, false, false},
		{`UPDATE entity_state SET fields='[]'`, nil, true, false},
		{`UPDATE entity_state SET fields='{}'`, nil, false, false},
		{`UPDATE flow_instance_runtime_readiness SET plan='{}'`, nil, true, false},
		{`UPDATE flow_instance_runtime_readiness SET plan=?`, []any{string(planRaw)}, false, false},
		{`UPDATE entity_state SET entity_id=''`, nil, true, true},
		{`UPDATE entity_state SET entity_id=?`, []any{entityID}, false, false},
		{`INSERT INTO entity_state VALUES ('foreign','foreign/path','','ready','[]')`, nil, false, false},
		{`ALTER TABLE entity_state RENAME COLUMN fields TO missing_fields`, nil, true, false},
		{`ALTER TABLE entity_state RENAME COLUMN missing_fields TO fields`, nil, false, false},
		{`DROP TABLE flow_instance_runtime_readiness`, nil, true, false},
		{`CREATE TABLE flow_instance_runtime_readiness (run_id TEXT, instance_path TEXT, plan TEXT)`, nil, true, false},
		{`INSERT INTO flow_instance_runtime_readiness VALUES ('run','review/one',?)`, []any{string(planRaw)}, false, false},
		{`DELETE FROM entity_state WHERE run_id='run'`, nil, false, false},
	} {
		if _, err := db.ExecContext(ctx, strings.ReplaceAll(tc.query, "'run'", "'"+runID+"'"), tc.args...); err != nil {
			t.Fatal(err)
		}
		compare(tc.flowFailure, tc.targetFailure)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ListActiveFlowInstanceDescriptors(ctx, "run"); err == nil {
		t.Fatal("flow read reopened closed backend")
	}
	if _, err := owner.ListSelectedRunTargetOwners(ctx, "run"); err == nil {
		t.Fatal("target read reopened closed backend")
	}
	// Existing preconditions remain earlier than SQL/closed-pool errors.
	if _, err := owner.ListActiveFlowInstanceDescriptors(ctx, ""); err == nil || err.Error() != "active flow instance descriptors require exact run_id" {
		t.Fatalf("flow precondition: %v", err)
	}
	if _, err := owner.ListSelectedRunTargetOwners(ctx, ""); err == nil || err.Error() != "selected-run target owners require exact run_id" {
		t.Fatalf("target precondition: %v", err)
	}
}
