package store_test

import (
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('component-scaffold/active', 'component-scaffold', 'template', '{}', 'active', CURRENT_TIMESTAMP),
			('component-scaffold/terminated', 'component-scaffold', 'template', '{}', 'terminated', CURRENT_TIMESTAMP),
			('service-owner', 'service-owner', 'static', '{}', 'active', CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES (?, 'running'), ('44444444-4444-4444-8444-444444444444', 'running')
	`, runID); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES
			('22222222-2222-4222-8222-222222222222', ?, 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"v-active","weight":1.1234567}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
			('33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444', 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}', datetime('now', '+1 minute'), datetime('now', '+1 minute'))
	`, runID); err != nil {
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
	if got.EntityID != runtimeflowidentity.EntityID("component-scaffold/active") {
		t.Fatalf("EntityID = %q, want derived flow instance entity id", got.EntityID)
	}
	if got.FlowTemplate != "component-scaffold" {
		t.Fatalf("FlowTemplate = %q, want component-scaffold", got.FlowTemplate)
	}
	if got.AddressFields["entity.vertical_id"] != "v-active" {
		t.Fatalf("AddressFields[entity.vertical_id] = %q, want v-active", got.AddressFields["entity.vertical_id"])
	}
	if got.AddressFields["entity.weight"] != "1.1234567" {
		t.Fatalf("AddressFields[entity.weight] = %q, want 1.1234567", got.AddressFields["entity.weight"])
	}
}

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsOmitsAddressFieldsWithoutRunScope(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('component-scaffold/active', 'component-scaffold', 'template', '{}', 'active', CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ('44444444-4444-4444-8444-444444444444', 'running')
	`); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO entity_state (entity_id, run_id, flow_instance, entity_type, current_state, fields, created_at, updated_at)
		VALUES ('33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444', 'component-scaffold/active', 'component', 'ready', '{"vertical_id":"wrong-run"}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	descriptors, err := sqliteStore.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("descriptors = %#v, want exactly active template descriptor", descriptors)
	}
	if len(descriptors[0].AddressFields) != 0 {
		t.Fatalf("AddressFields = %#v, want no run-scoped descriptor evidence without run_id", descriptors[0].AddressFields)
	}
}

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsReadsPipelineTransaction(t *testing.T) {
	ctx := testAuthorActivityContext()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

	tx, err := sqliteStore.DB.BeginTx(ctx, nil)
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

	descriptors, err := sqliteStore.ListActiveFlowInstanceDescriptors(runtimepipeline.WithPipelineSQLTxContext(ctx, tx))
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	if len(descriptors) != 1 || descriptors[0].FlowInstance != "component-scaffold/uncommitted" {
		t.Fatalf("descriptors = %#v, want uncommitted tx flow instance", descriptors)
	}
}
