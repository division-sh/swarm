package runtimepersistence_test

import (
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("test context is missing bundle source fact")
	}
	bundleHash := source.BundleHash()
	const activeEntityID = "22222222-2222-4222-8222-222222222222"
	activeReadiness := exactFlowInstanceDescriptorReadinessJSON(t, runID, bundleHash, "1.0.0", "component-scaffold", "component-scaffold/active", activeEntityID)

	if _, err := storetest.Database(sqliteStore).ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('component-scaffold/active', 'component-scaffold', 'template', '{}', 'active', CURRENT_TIMESTAMP),
			('component-scaffold/terminated', 'component-scaffold', 'template', '{}', 'terminated', CURRENT_TIMESTAMP),
			('service-owner', 'service-owner', 'static', '{}', 'active', CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	storetest.RequireRun(t, ctx, sqliteStore, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
	storetest.RequireRun(t, ctx, sqliteStore, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),
		RunID: "44444444-4444-4444-8444-444444444444",
	})
	if _, err := storetest.Database(sqliteStore).ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES (?, 'component-scaffold/active', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, runID, activeReadiness); err != nil {
		t.Fatalf("seed flow-instance readiness: %v", err)
	}
	if _, err := storetest.Database(sqliteStore).ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			(?, ?, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"v-active","weight":1.1234567}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
			('33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444', 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}', datetime('now', '+1 minute'), datetime('now', '+1 minute'))
	`, activeEntityID, runID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	descriptors, err := sqliteStore.ListActiveFlowInstanceDescriptors(ctx)
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
	if got.EntityID != activeEntityID {
		t.Fatalf("EntityID = %q, want readiness-owned entity id", got.EntityID)
	}
	if got.FlowTemplate != "component-scaffold" {
		t.Fatalf("FlowTemplate = %q, want component-scaffold", got.FlowTemplate)
	}
	if got.BundleHash != source.BundleHash() || got.WorkflowVersion != "1.0.0" {
		t.Fatalf("semantic source = %#v, want exact run bundle and workflow version", got)
	}
	if got.AddressFields["entity.vertical_id"] != "v-active" {
		t.Fatalf("AddressFields[entity.vertical_id] = %q, want v-active", got.AddressFields["entity.vertical_id"])
	}
	if got.AddressFields["entity.weight"] != "1.1234567" {
		t.Fatalf("AddressFields[entity.weight] = %q, want 1.1234567", got.AddressFields["entity.weight"])
	}
}

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsAllowsUnscopedEmptyCensus(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

	descriptors, err := sqliteStore.ListActiveFlowInstanceDescriptors(ctx)
	if err == nil || !strings.Contains(err.Error(), "run_id is required") || len(descriptors) != 0 {
		t.Fatalf("unscoped descriptor census: descriptors=%#v err=%v, want fail-closed run scope", descriptors, err)
	}
}

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsIgnoresAmbientPipelineTransaction(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	storetest.RequireRun(t, ctx, sqliteStore, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("test context is missing bundle source fact")
	}
	bundleHash := source.BundleHash()
	const uncommittedEntityID = "22222222-2222-4222-8222-222222222222"
	uncommittedReadiness := exactFlowInstanceDescriptorReadinessJSON(t, runID, bundleHash, "1.0.0", "component-scaffold", "component-scaffold/uncommitted", uncommittedEntityID)

	tx, err := storetest.Database(sqliteStore).BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('component-scaffold/uncommitted', 'component-scaffold', 'template', '{}', 'active', CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("seed flow_instances in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, created_at, updated_at)
		VALUES (?, 'component-scaffold/uncommitted', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, runID, uncommittedReadiness); err != nil {
		t.Fatalf("seed readiness in tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES (?, ?, 'component-scaffold/uncommitted', 'component', 'ready', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, uncommittedEntityID, runID); err != nil {
		t.Fatalf("seed entity state in tx: %v", err)
	}

	descriptors, err := sqliteStore.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("descriptors = %#v, want ambient uncommitted flow instance hidden", descriptors)
	}
}
