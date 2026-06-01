package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestExecuteNodeContractHandlerSelectEntityUpdatesTargetOwnedEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		}.WithEntityID("22222222-2222-2222-2222-222222222222"),
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := FlowInstanceEntityID(instance.StorageRef); got != budgetEntityID {
		t.Fatalf("selected entity storage identity = %q, want %q", got, budgetEntityID)
	}
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectEntityReplayUsesSameTargetEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	for _, amount := range []int{42, 99} {
		result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
			Event: events.Event{
				Type:    events.EventType("opco.spend_recorded"),
				Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": amount}),
			}.WithEntityID("22222222-2222-2222-2222-222222222222"),
			State: WorkflowState{},
		}, false)
		if err != nil {
			t.Fatalf("executeNodeContractHandler amount %d: %v", amount, err)
		}
		if !result.Handled {
			t.Fatalf("expected selected handler to run for amount %d", amount)
		}
		assertEntityStateRowCount(t, db, 1)
	}

	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(99) && got != 99 {
		t.Fatalf("spent_usd after replay = %#v, want 99", got)
	}
}

func TestExecuteNodeContractHandlerSelectEntityMatchesTypedStatusField(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudgetWithMetadata(t, pc.workflowStore, ctx, source, "budget-1", map[string]any{
		"vertical_id":      "vertical-1",
		"status":           "pending",
		"business_status":  "approved",
		"spent_usd":        0,
		"domain_status_id": "status-field-regression",
	}, map[string]any{"status": "waiting"})

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", runtimecontracts.SystemNodeEventHandler{
		SelectEntity: &runtimecontracts.SelectEntitySpec{
			By: map[string]string{
				"vertical_id": "payload.vertical_id",
				"status":      "payload.status",
			},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{
				{
					Field:   "vertical_id",
					Ref:     "payload.vertical_id",
					RefPath: runtimecontracts.RefExpression("payload.vertical_id").RefPath,
				},
				{
					Field:   "status",
					Ref:     "payload.status",
					RefPath: runtimecontracts.RefExpression("payload.status").RefPath,
				},
			},
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "amount_usd",
				TargetField: "spent_usd",
			}},
		},
	}, workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "status": "pending", "amount_usd": 42}),
		}.WithEntityID("22222222-2222-2222-2222-222222222222"),
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["status"])); got != "pending" {
		t.Fatalf("typed status metadata = %q, want pending", got)
	}
	assertEntityStateField(t, db, budgetEntityID, "status", "pending")
	assertFlowInstanceControlConfig(t, db, instance.StorageRef, "status", "waiting")
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityCreatesTargetOwnedEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected-or-created handler to run")
	}

	assertEntityStateRowCount(t, db, 1)
	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Metadata["vertical_id"]; got != "vertical-1" {
		t.Fatalf("vertical_id = %#v, want vertical-1", got)
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := strings.TrimSpace(asString(instance.Metadata["entity_type"])); got != "opco_budget" {
		t.Fatalf("entity_type metadata = %q, want opco_budget", got)
	}
	assertEntityStateEntityType(t, db, FlowInstanceEntityID(instance.StorageRef), "opco_budget")
}

func TestRepairContractEntityTypesUsesBundleAvailabilityAndDoesNotPromoteLegacyFingerprint(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	resolvableID := uuid.NewString()
	unresolvedID := uuid.NewString()
	oldVersionID := uuid.NewString()
	legacyRunID := "88888888-8888-8888-8888-888888888888"
	legacyBundleID := uuid.NewString()
	ephemeralRunID := "99999999-9999-9999-9999-999999999999"
	ephemeralBundleID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2, bundle_source = 'persisted', bundle_fingerprint = NULL
		WHERE run_id = $1::uuid
	`, testPipelineRunID, persistedHash); err != nil {
		t.Fatalf("seed current run bundle source: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: runtime-test', '{}'::jsonb)
	`, persistedHash); err != nil {
		t.Fatalf("seed current bundle row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint)
		VALUES ($1::uuid, 'running', 'legacy', 'sha256:old')
	`, legacyRunID); err != nil {
		t.Fatalf("seed legacy bundle run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
		VALUES ($1::uuid, 'running', 'bundle-v1:sha256:9999999999999999999999999999999999999999999999999999999999999999', 'ephemeral')
	`, ephemeralRunID); err != nil {
		t.Fatalf("seed ephemeral bundle run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES
			('treasury/legacy', 'treasury', 'template', '{"workflow_version":"1.0.0"}'::jsonb, 'active', now()),
			('treasury/legacy-old-version', 'treasury', 'template', '{"workflow_version":"0.9.0"}'::jsonb, 'active', now()),
			('treasury/legacy-bundle', 'treasury', 'template', '{"workflow_version":"1.0.0"}'::jsonb, 'active', now()),
			('treasury/ephemeral-bundle', 'treasury', 'template', '{"workflow_version":"1.0.0"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow_instances: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES
			($1::uuid, $2::uuid, 'treasury/legacy', 'default', 'active',
			 '{}'::jsonb, '{"entity_type":"not-authority","vertical_id":"vertical-1"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($1::uuid, $3::uuid, 'unknown/legacy', 'default', 'active',
			 '{}'::jsonb, '{"entity_type":"opco_budget"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($1::uuid, $4::uuid, 'treasury/legacy-old-version', 'default', 'active',
			 '{}'::jsonb, '{"entity_type":"opco_budget"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($6::uuid, $5::uuid, 'treasury/legacy-bundle', 'default', 'active',
			 '{}'::jsonb, '{"entity_type":"opco_budget"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($7::uuid, $8::uuid, 'treasury/ephemeral-bundle', 'default', 'active',
			 '{}'::jsonb, '{"entity_type":"opco_budget"}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, testPipelineRunID, resolvableID, unresolvedID, oldVersionID, legacyBundleID, legacyRunID, ephemeralRunID, ephemeralBundleID); err != nil {
		t.Fatalf("seed default entity_state rows: %v", err)
	}

	repaired, err := pc.RepairContractEntityTypes(ctx)
	if err != nil {
		t.Fatalf("RepairContractEntityTypes: %v", err)
	}
	if repaired != 0 {
		t.Fatalf("repaired rows = %d, want 0 without canonical current bundle hash owner", repaired)
	}
	assertEntityStateEntityType(t, db, resolvableID, "default")
	assertEntityStateEntityType(t, db, unresolvedID, "default")
	assertEntityStateEntityType(t, db, oldVersionID, "default")
	assertEntityStateEntityTypeForRun(t, db, legacyRunID, legacyBundleID, "default")
	assertEntityStateEntityTypeForRun(t, db, ephemeralRunID, ephemeralBundleID, "default")
}

func TestRepairContractEntityTypesBlocksPersistedMissingBundleRow(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	entityID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = 'bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
		    bundle_source = $2,
		    bundle_fingerprint = NULL
		WHERE run_id = $1::uuid
	`, testPipelineRunID, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("seed persisted-missing run bundle source: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('treasury/missing-bundle', 'treasury', 'template', '{"workflow_version":"1.0.0"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'treasury/missing-bundle', 'default', 'active',
			'{}'::jsonb, '{"entity_type":"opco_budget"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, testPipelineRunID, entityID); err != nil {
		t.Fatalf("seed default entity_state row: %v", err)
	}

	repaired, err := pc.RepairContractEntityTypes(ctx)
	if err == nil {
		t.Fatal("RepairContractEntityTypes error = nil, want persisted-missing bundle block")
	}
	if repaired != 0 {
		t.Fatalf("repaired rows = %d, want 0", repaired)
	}
	got := err.Error()
	for _, want := range []string{"contract entity type repair blocked by run bundle availability", "BUNDLE_DATA_INTEGRITY_ERROR", "persisted_missing_bundle_row"} {
		if !strings.Contains(got, want) {
			t.Fatalf("RepairContractEntityTypes error = %q, want %q", got, want)
		}
	}
	assertEntityStateEntityType(t, db, entityID, "default")
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityReplayUsesSameDeclaredKey(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)

	for _, amount := range []int{42, 99} {
		result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateEntitySpendHandler(), workflowTriggerContext{
			Event: events.Event{
				Type:    events.EventType("opco.spend_recorded"),
				Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": amount}),
			},
			State: WorkflowState{},
		}, false)
		if err != nil {
			t.Fatalf("executeNodeContractHandler amount %d: %v", amount, err)
		}
		if !result.Handled {
			t.Fatalf("expected selected-or-created handler to run for amount %d", amount)
		}
		assertEntityStateRowCount(t, db, 1)
	}

	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Metadata["spent_usd"]; got != float64(99) && got != 99 {
		t.Fatalf("spent_usd after replay = %#v, want 99", got)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFailsClosedOnAmbiguousMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-1", "vertical-1", 0)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-2", "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_or_create_entity_ambiguous") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_or_create_entity_ambiguous", err)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFailsClosedOnDeterministicIDConflict(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	instanceID, err := selectOrCreateEntityInstanceID(source, "treasury", map[string]any{"vertical_id": "vertical-1"})
	if err != nil {
		t.Fatalf("selectOrCreateEntityInstanceID: %v", err)
	}
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      identity.EntityID,
		StorageRef:      identity.InstancePath,
		WorkflowName:    "treasury",
		WorkflowVersion: "1.0.0",
		CurrentState:    "active",
		Metadata: map[string]any{
			"flow_path":   identity.InstancePath,
			"instance_id": identity.InstanceID,
			"vertical_id": "other-key",
			"storage_ref": identity.InstancePath,
			"entity_type": "opco_budget",
		},
	}); err != nil {
		t.Fatalf("seed conflicting entity: %v", err)
	}

	_, err = pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_or_create_entity_conflict") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_or_create_entity_conflict", err)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityConcurrentDuplicateCreatesOneEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateEntitySpendHandler(), workflowTriggerContext{
				Event: events.Event{
					Type:    events.EventType("opco.spend_recorded"),
					Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
				},
				State: WorkflowState{},
			}, false)
			if err != nil {
				errs <- err
				return
			}
			if !result.Handled {
				errs <- fmt.Errorf("handler was not handled")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent select_or_create handler: %v", err)
		}
	}
	assertEntityStateRowCount(t, db, 1)
}

func TestBackgroundWorkflowNodeSelectOrCreateEntityDuplicateSameEventIsReceiptIdempotent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectOrCreateEntityTestCoordinator(t, db)
	pc.eventReceiptsCapability = eventReceiptsCapabilityStub{enabled: true}.resolve
	ctx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedSelectEntitySpendEvent(t, db, ctx, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42})
	runner := newSelectEntityBackgroundNode(t, pc, pc.SemanticSource(), db)
	runner.SetRetryPolicyForTest(1, func(int) time.Duration { return 0 })

	runner.ProcessEventForTest(ctx, evt)
	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd after first delivery = %#v, want 42", got)
	}

	if err := pc.workflowStore.Mutate(ctx, FlowInstanceEntityID(instance.StorageRef), func(instance *WorkflowInstance) {
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		instance.Metadata["spent_usd"] = 7
	}); err != nil {
		t.Fatalf("workflowStore.Mutate between duplicate deliveries: %v", err)
	}

	runner.ProcessEventForTest(ctx, evt)
	instance = loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Metadata["spent_usd"]; got != float64(7) && got != 7 {
		t.Fatalf("spent_usd after duplicate same-event delivery = %#v, want unchanged 7", got)
	}
	assertSelectEntityReceiptRow(t, db, evt.ID, "treasury-orchestrator")
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFeedsEntityIDToArtifactRepoCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	pc.artifactRoot = t.TempDir()
	ctx := testPipelineCoordinatorRunContext(t, pc)
	sourceEventID := "33333333-3333-3333-3333-333333333333"
	payload := map[string]any{"artifact_key": "case-1", "request_id": "44444444-4444-4444-4444-444444444444", "namespace": "tenant-alpha", "partition_key": "project-42", "display_slug": "Demo Artifact", "mvp_yaml": "name: Demo\n"}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'source/case-1', 'entity', $4::jsonb, 'test', 'node', to_timestamp(1700000000))
	`, sourceEventID, "spec_repo.commit_requested", "22222222-2222-2222-2222-222222222222", string(mustJSON(payload))); err != nil {
		t.Fatalf("seed artifact event: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectOrCreateArtifactRepoCommitHandler(), workflowTriggerContext{
		Event: events.Event{
			ID:        sourceEventID,
			Type:      events.EventType("spec_repo.commit_requested"),
			RunID:     testPipelineRunID,
			Payload:   mustJSON(payload),
			CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
		},
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected artifact_repo_commit handler to run")
	}

	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "case-1")
	entityID := FlowInstanceEntityID(instance.StorageRef)
	if got := strings.TrimSpace(asString(instance.Metadata["repo_url"])); got != "swarm-artifact://repos/"+entityID {
		t.Fatalf("repo_url = %q, want repo url derived from entity id %q", got, entityID)
	}
	if ref := strings.TrimSpace(asString(instance.Metadata["current_ref"])); len(ref) != 40 {
		t.Fatalf("current_ref length = %d ref=%q", len(ref), ref)
	}
}

func TestBackgroundWorkflowNodeSelectEntityDuplicateSameEventIsReceiptIdempotent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	pc.eventReceiptsCapability = eventReceiptsCapabilityStub{enabled: true}.resolve
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)
	evt := seedSelectEntitySpendEvent(t, db, ctx, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42})
	runner := newSelectEntityBackgroundNode(t, pc, source, db)
	runner.SetRetryPolicyForTest(1, func(int) time.Duration { return 0 })

	runner.ProcessEventForTest(ctx, evt)
	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load after first delivery: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist after first delivery")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd after first delivery = %#v, want 42", got)
	}

	if err := pc.workflowStore.Mutate(ctx, budgetEntityID, func(instance *WorkflowInstance) {
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		instance.Metadata["spent_usd"] = 7
	}); err != nil {
		t.Fatalf("workflowStore.Mutate between duplicate deliveries: %v", err)
	}

	runner.ProcessEventForTest(ctx, evt)
	instance, ok, err = pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load after duplicate delivery: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist after duplicate delivery")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(7) && got != 7 {
		t.Fatalf("spent_usd after duplicate same-event delivery = %#v, want unchanged 7", got)
	}
	assertSelectEntityReceiptRow(t, db, evt.ID, "treasury-orchestrator")
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectEntityIgnoresTerminalAndTerminatedMatches(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	activeBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-active", "vertical-1", 0, "active")
	terminalBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-archived", "vertical-1", 10, "archived")
	terminatedBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-terminated", "vertical-1", 20, "active")
	terminated, ok, err := pc.workflowStore.Load(ctx, terminatedBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminated: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	if err := pc.workflowStore.MarkTerminated(ctx, terminated.StorageRef, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	active, ok, err := pc.workflowStore.Load(ctx, activeBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load active: %v", err)
	}
	if !ok {
		t.Fatal("expected active budget entity to exist")
	}
	if got := active.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("active spent_usd = %#v, want 42", got)
	}
	terminal, ok, err := pc.workflowStore.Load(ctx, terminalBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminal: %v", err)
	}
	if !ok {
		t.Fatal("expected terminal budget entity to exist")
	}
	if got := terminal.Metadata["spent_usd"]; got != float64(10) && got != 10 {
		t.Fatalf("terminal spent_usd = %#v, want unchanged 10", got)
	}
	reloadedTerminated, ok, err := pc.workflowStore.Load(ctx, terminatedBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminated after select: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	if got := reloadedTerminated.Metadata["spent_usd"]; got != float64(20) && got != 20 {
		t.Fatalf("terminated spent_usd = %#v, want unchanged 20", got)
	}
	assertEntityStateRowCount(t, db, 3)
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnNoMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "missing", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_entity_no_match", err)
	}
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnMissingPayloadRef(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "missing required payload ref") {
		t.Fatalf("executeNodeContractHandler error = %v, want missing payload ref", err)
	}
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnAmbiguousMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-1", "vertical-1", 0)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-2", "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_entity_ambiguous") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_entity_ambiguous", err)
	}
}

func newSelectEntityTestCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorWithNodes(t, db, `
treasury-orchestrator:
  id: treasury-orchestrator
  execution_type: system_node
  subscribes_to: [opco.spend_recorded]
  event_handlers:
    opco.spend_recorded:
      select_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`)
}

func newSelectOrCreateEntityTestCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorWithNodes(t, db, `
treasury-orchestrator:
  id: treasury-orchestrator
  execution_type: system_node
  subscribes_to: [opco.spend_recorded]
  event_handlers:
    opco.spend_recorded:
      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`)
}

func newSelectEntityTestCoordinatorWithNodes(t *testing.T, db *sql.DB, treasuryNodes string) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: treasury
    flow: treasury
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/treasury/schema.yaml": `
name: treasury
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [opco.spend_recorded]
`,
		"flows/treasury/events.yaml": `
opco.spend_recorded:
  vertical_id: string
  amount_usd: number
`,
		"flows/treasury/entities.yaml": `
opco_budget:
  vertical_id:
    type: text
  spent_usd:
    type: number
    initial: 0
  repo_url:
    type: text
  current_ref:
    type: text
  file_manifest:
    type: text
  status:
    type: text
  failure_reason:
    type: text
  last_request_id:
    type: text
  last_source_event_id:
    type: text
`,
		"flows/treasury/nodes.yaml": treasuryNodes,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		db:             db,
		workflowStore:  NewWorkflowInstanceStore(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("treasury", []WorkflowStage{
				{Name: "active"},
			}, nil),
		},
	}
	return pc, source
}

func newSelectEntityBackgroundNode(t *testing.T, pc *PipelineCoordinator, source semanticview.Source, db *sql.DB) *backgroundWorkflowNode {
	t.Helper()
	contract, ok := source.NodeEntries()["treasury-orchestrator"]
	if !ok {
		t.Fatal("expected treasury-orchestrator node contract")
	}
	executor := NewNode(contract, pc.SemanticSource(), newCoordinatorHandlerExecutionEngine(pc, "treasury-orchestrator"), nil)
	if executor == nil {
		t.Fatal("expected treasury-orchestrator node executor")
	}
	runner := newBackgroundWorkflowNodeWithRetryBase(executor, &recordingPipelineBus{}, db, pc.eventReceiptsCapability, 0)
	if runner == nil {
		t.Fatal("expected treasury-orchestrator background runner")
	}
	return runner
}

func selectEntitySpendHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectEntity: &runtimecontracts.SelectEntitySpec{
			By: map[string]string{"vertical_id": "payload.vertical_id"},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{{
				Field:   "vertical_id",
				Ref:     "payload.vertical_id",
				RefPath: runtimecontracts.RefExpression("payload.vertical_id").RefPath,
			}},
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "amount_usd",
				TargetField: "spent_usd",
			}},
		},
	}
}

func selectOrCreateEntitySpendHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
			By: map[string]string{"vertical_id": "payload.vertical_id"},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{{
				Field:   "vertical_id",
				Ref:     "payload.vertical_id",
				RefPath: runtimecontracts.RefExpression("payload.vertical_id").RefPath,
			}},
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "amount_usd",
				TargetField: "spent_usd",
			}},
		},
	}
}

func selectOrCreateArtifactRepoCommitHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
			By: map[string]string{"vertical_id": "payload.artifact_key"},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{{
				Field:   "vertical_id",
				Ref:     "payload.artifact_key",
				RefPath: runtimecontracts.RefExpression("payload.artifact_key").RefPath,
			}},
		},
		Action: runtimecontracts.ActionSpec{
			ID: "artifact_repo_commit",
			ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
				Provider:     "local_git",
				RepoID:       runtimecontracts.RefExpression("entity.entity_id"),
				Namespace:    runtimecontracts.RefExpression("payload.namespace"),
				PartitionKey: runtimecontracts.RefExpression("payload.partition_key"),
				DisplaySlug:  runtimecontracts.RefExpression("payload.display_slug"),
				RequestID:    runtimecontracts.RefExpression("payload.request_id"),
				Author:       runtimecontracts.LiteralExpression("artifact-writer"),
				Provenance: map[string]runtimecontracts.ExpressionValue{
					"artifact_type": runtimecontracts.LiteralExpression("fixture"),
				},
				AllowedPaths: []string{"specs/mvp.yaml"},
				Files: []runtimecontracts.ArtifactRepoFileSpec{{
					Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
					Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
					ContentType: "yaml",
					Schema: runtimecontracts.ArtifactRepoSchemaSpec{
						Type:           "object",
						RequiredFields: []string{"name"},
					},
					MaxBytes: 4096,
				}},
				Output: runtimecontracts.ArtifactRepoOutputSpec{
					RepoURL:           "repo_url",
					CurrentRef:        "current_ref",
					FileManifest:      "file_manifest",
					Status:            "status",
					FailureReason:     "failure_reason",
					LastRequestID:     "last_request_id",
					LastSourceEventID: "last_source_event_id",
				},
				Limits: runtimecontracts.ArtifactRepoLimitsSpec{
					MaxYAMLBytes: 4096,
					MaxRepoBytes: 1048576,
				},
				FailureEvent: "artifact_repo.commit_failed",
			},
		},
	}
}

func loadSelectOrCreateBudgetByKey(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, verticalID string) WorkflowInstance {
	t.Helper()
	instanceID, err := selectOrCreateEntityInstanceID(source, "treasury", map[string]any{"vertical_id": verticalID})
	if err != nil {
		t.Fatalf("selectOrCreateEntityInstanceID: %v", err)
	}
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	instance, ok, err := store.Load(ctx, identity.EntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatalf("expected select_or_create entity %s to exist", identity.EntityID)
	}
	return instance
}

func seedSelectEntitySpendEvent(t *testing.T, db *sql.DB, ctx context.Context, payload map[string]any) events.Event {
	t.Helper()
	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spend_recorded"),
		SourceAgent: "opco",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'opco/vertical-1', 'entity', $4::jsonb, 'opco', 'node', now())
	`, evt.ID, string(evt.Type), entityID, string(evt.Payload)); err != nil {
		t.Fatalf("seed select_entity spend event: %v", err)
	}
	return evt
}

func seedSelectEntityBudget(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithInstance(t, store, ctx, source, "budget-1", verticalID, spent)
}

func seedSelectEntityBudgetWithInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithState(t, store, ctx, source, instanceID, verticalID, spent, "active")
}

func seedSelectEntityBudgetWithState(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any, currentState string) string {
	t.Helper()
	return seedSelectEntityBudgetWithMetadataAndState(t, store, ctx, source, instanceID, map[string]any{
		"vertical_id": verticalID,
		"spent_usd":   spent,
	}, nil, currentState)
}

func seedSelectEntityBudgetWithMetadata(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID string, metadata map[string]any, config map[string]any) string {
	t.Helper()
	return seedSelectEntityBudgetWithMetadataAndState(t, store, ctx, source, instanceID, metadata, config, "active")
}

func seedSelectEntityBudgetWithMetadataAndState(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID string, metadata map[string]any, config map[string]any, currentState string) string {
	t.Helper()
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	metadata = cloneStringAnyMap(metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	config = cloneStringAnyMap(config)
	metadata["flow_path"] = identity.InstancePath
	metadata["instance_id"] = identity.InstanceID
	metadata["storage_ref"] = identity.InstancePath
	metadata["entity_type"] = "opco_budget"
	instance := WorkflowInstance{
		InstanceID:      identity.EntityID,
		StorageRef:      identity.InstancePath,
		WorkflowName:    "treasury",
		WorkflowVersion: "1.0.0",
		CurrentState:    strings.TrimSpace(currentState),
		Config:          config,
		Metadata:        metadata,
	}
	if err := store.Upsert(ctx, instance); err != nil {
		t.Fatalf("seed budget entity: %v", err)
	}
	return identity.EntityID
}

func assertSelectEntityReceiptRow(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND idempotency_key = $2 || ':' || $1
	`, eventID, nodeID).Scan(&count); err != nil {
		t.Fatalf("count select_entity event_receipts: %v", err)
	}
	if count != 1 {
		t.Fatalf("select_entity event_receipts rows = %d, want 1", count)
	}
}

func assertEntityStateField(t *testing.T, db *sql.DB, entityID, field string, want any) {
	t.Helper()
	var gotRaw []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT fields -> $3
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, testPipelineRunID, entityID, field).Scan(&gotRaw); err != nil {
		t.Fatalf("load entity_state fields for %s: %v", entityID, err)
	}
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted entity_state field %s: %v", field, err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("entity_state.fields[%q] = %s, want %s", field, gotRaw, wantRaw)
	}
}

func assertFlowInstanceControlConfig(t *testing.T, db *sql.DB, storageRef, field string, want any) {
	t.Helper()
	var gotRaw []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT config -> $2
		FROM flow_instances
		WHERE instance_id = $1
	`, storageRef, field).Scan(&gotRaw); err != nil {
		t.Fatalf("load flow_instances config for %s: %v", storageRef, err)
	}
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted flow instance config %s: %v", field, err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("flow_instances.config[%q] = %s, want %s", field, gotRaw, wantRaw)
	}
}

func assertEntityStateRowCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM entity_state`).Scan(&got); err != nil {
		t.Fatalf("count entity_state: %v", err)
	}
	if got != want {
		t.Fatalf("entity_state row count = %d, want %d", got, want)
	}
}

func assertEntityStateEntityType(t *testing.T, db *sql.DB, entityID, want string) {
	t.Helper()
	assertEntityStateEntityTypeForRun(t, db, testPipelineRunID, entityID, want)
}

func assertEntityStateEntityTypeForRun(t *testing.T, db *sql.DB, runID, entityID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(entity_type, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entityID).Scan(&got); err != nil {
		t.Fatalf("load entity_state entity_type for %s: %v", entityID, err)
	}
	if got != want {
		t.Fatalf("entity_state.entity_type = %q, want %q", got, want)
	}
}
