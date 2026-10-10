package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
)

type flowRouteTestExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func seedFlowRouteHeaderFixture(t *testing.T, ctx context.Context, exec flowRouteTestExecutor, runID, path, flow, mode, status, entityID, entityType string) {
	t.Helper()
	at := time.Now().UTC()
	seedWorkflowHeaderProjectionFixture(t, ctx, exec, runID, entityID, path, flow, entityType, "active", "{}", at)
	payload, err := runtimepipeline.WorkflowInstanceHeaderPayloadForRoute(runtimeflowidentity.RouteForInstancePath(path), "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var terminatedAt any
	if status == "terminated" {
		terminatedAt = at
	}
	if _, err := exec.ExecContext(ctx, `UPDATE flow_instances SET mode=$1, status=$2, terminated_at=$3, config=$4 WHERE run_id=$5 AND instance_path=$6`, mode, status, terminatedAt, string(config), runID, path); err != nil {
		t.Fatalf("seed flow-route header controls: %v", err)
	}
}

func ensureFlowInstanceRouteTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS flow_instances (
			run_id UUID NOT NULL,
			instance_path TEXT NOT NULL,
			flow_template TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT 'template',
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL DEFAULT 'active',
			terminated_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (run_id, instance_path)
		)
	`); err != nil {
		t.Fatalf("ensure flow_instances table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS entity_state (
			entity_id UUID PRIMARY KEY,
			run_id UUID NOT NULL,
			flow_instance TEXT NOT NULL,
			entity_type TEXT NOT NULL DEFAULT '',
			current_state TEXT NOT NULL DEFAULT '',
			fields JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("ensure entity_state table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS routing_rules (
			rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_pattern TEXT NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			flow_instance TEXT,
			source_flow TEXT,
			is_wildcard BOOLEAN NOT NULL DEFAULT FALSE,
			is_materialized BOOLEAN NOT NULL DEFAULT FALSE,
			materialized_from UUID,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("ensure routing_rules table: %v", err)
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	const foreignRunID = "44444444-4444-4444-8444-444444444444"
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: foreignRunID})

	seedFlowRouteHeaderFixture(t, ctx, db, runID, "component-scaffold/active", "component-scaffold", "template", "active", entityID, "component")
	seedFlowRouteHeaderFixture(t, ctx, db, runID, "component-scaffold/terminated", "component-scaffold", "template", "terminated", runtimeflowidentity.EntityID("component-scaffold/terminated"), "component")
	seedFlowRouteHeaderFixture(t, ctx, db, runID, "service-owner", "service-owner", "static", "active", runtimeflowidentity.EntityID("service-owner"), "component")
	seedFlowRouteHeaderFixture(t, ctx, db, foreignRunID, "component-scaffold/active", "component-scaffold", "template", "active", "33333333-3333-4333-8333-333333333333", "component")
	readinessOwner, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: "component-scaffold", ScopeKey: "component-scaffold", InstanceID: "active",
			InstancePath: "component-scaffold/active", EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: authorActivityTestBundleHash,
		WorkflowVersion: "1.0.0", ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness plan: %v", err)
	}
	readinessPlan, err := json.Marshal(readinessOwner)
	if err != nil {
		t.Fatalf("marshal readiness plan: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/active', $2::jsonb, $3, NOW(), NOW())
	`, runID, readinessPlan, readinessPlanFixtureHash(t, string(readinessPlan))); err != nil {
		t.Fatalf("seed flow-instance readiness: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			($2::uuid, $1::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"v-active","weight":1.1234567}'::jsonb, NOW(), NOW()),
			('33333333-3333-4333-8333-333333333333', $3::uuid, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}'::jsonb, NOW() + INTERVAL '1 minute', NOW() + INTERVAL '1 minute')
	`, runID, entityID, foreignRunID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx, runID)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("descriptors = %#v, want exactly active template descriptor", descriptors)
	}
	got := descriptors[0]
	if got.FlowInstance != "component-scaffold/active" {
		t.Fatalf("FlowInstance = %q, want component-scaffold/active", got.FlowInstance)
	}
	if got.InstanceID != "active" {
		t.Fatalf("InstanceID = %q, want active", got.InstanceID)
	}
	if got.EntityID != entityID {
		t.Fatalf("EntityID = %q, want exact readiness entity id", got.EntityID)
	}
	if got.FlowTemplate != "component-scaffold" {
		t.Fatalf("FlowTemplate = %q, want component-scaffold", got.FlowTemplate)
	}
	if got.BundleHash != authorActivityTestBundleHash ||
		got.WorkflowVersion != "1.0.0" {
		t.Fatalf("semantic source = %#v, want exact run bundle and workflow version", got)
	}
	if got.AddressFields["entity.vertical_id"] != "v-active" {
		t.Fatalf("AddressFields[entity.vertical_id] = %q, want v-active", got.AddressFields["entity.vertical_id"])
	}
	if got.AddressFields["entity.weight"] != "1.1234567" {
		t.Fatalf("AddressFields[entity.weight] = %q, want 1.1234567", got.AddressFields["entity.weight"])
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsRejectsUnscopedCensus(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)

	if descriptors, err := pg.ListActiveFlowInstanceDescriptors(ctx, ""); err == nil || !strings.Contains(err.Error(), "exact run_id") {
		t.Fatalf("unscoped descriptor census: descriptors=%#v err=%v, want exact run scope rejection", descriptors, err)
	}
}

func TestPostgresStoreListActiveFlowInstanceDescriptorsDoesNotReadAmbientTransaction(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ensureFlowInstanceRouteTables(t, ctx, db)
	requireDefaultSourceArtifactForTest(t, ctx, pg)
	requireRunFixtureForTest(t, ctx, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	seedFlowRouteHeaderFixture(t, ctx, tx, runID, "component-scaffold/uncommitted", "component-scaffold", "template", "active", runtimeflowidentity.EntityID("component-scaffold/uncommitted"), "component")
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at)
		VALUES ($1::uuid, 'component-scaffold/uncommitted', '{"workflow_version":"1.0.0"}'::jsonb, $2, NOW(), NOW())
	`, runID, readinessPlanFixtureHash(t, `{"workflow_version":"1.0.0"}`)); err != nil {
		t.Fatalf("seed readiness in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ('22222222-2222-4222-8222-222222222222'::uuid, $1::uuid, 'component-scaffold/uncommitted', 'component', 'ready', '{}'::jsonb, NOW(), NOW())
	`, runID); err != nil {
		t.Fatalf("seed entity state in tx: %v", err)
	}

	descriptors, err := pg.ListActiveFlowInstanceDescriptors(runtimepipelinefixture.WithSQLTx(ctx, tx), runID)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("descriptors = %#v, want no ambient uncommitted rows", descriptors)
	}
}
