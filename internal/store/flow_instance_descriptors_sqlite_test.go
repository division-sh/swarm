package store_test

import (
	"context"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsFiltersToActiveTemplates(t *testing.T) {
	ctx := context.Background()
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
}

func TestSQLiteRuntimeStoreListActiveFlowInstanceDescriptorsReadsPipelineTransaction(t *testing.T) {
	ctx := context.Background()
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
