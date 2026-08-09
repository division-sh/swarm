package runtimepersistence

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var runInsertPattern = regexp.MustCompile(`(?is)INSERT\s+INTO\s+runs\s*\(([^)]*)\)`)

func TestRepositoryRunCreationHasCanonicalOwnersOnly(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	wantOwners := map[string]bool{
		"internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go": false,
		"internal/testutil/runlifecyclefixture/fixture.go":                               false,
	}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/storetest/event.go" {
			return nil
		}
		source := readRepositoryFile(t, path)
		if !runInsertPattern.MatchString(source) {
			return nil
		}
		if _, ok := wantOwners[rel]; !ok {
			t.Errorf("%s can create runs outside the canonical run lifecycle owner", rel)
			return nil
		}
		wantOwners[rel] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk production run writers: %v", err)
	}
	for owner, found := range wantOwners {
		if !found {
			t.Errorf("canonical run lifecycle owner %s no longer contains its run insert", owner)
		}
	}
}

func TestRepositoryRunInsertFixturesHaveExplicitCanonicalIdentity(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	for _, rel := range bundleIdentityFixtureLedger {
		path := filepath.Join(root, filepath.FromSlash(rel))
		switch rel {
		case "internal/store/run_bundle_fingerprint_test.go":
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("retired compatibility fixture %s still exists", rel)
			}
		default:
			if _, err := os.Stat(path); err != nil {
				t.Errorf("fixture ledger row %s is missing: %v", rel, err)
			}
		}
	}

	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go" {
			return nil
		}
		source := readRepositoryFile(t, path)
		for _, match := range runInsertPattern.FindAllStringSubmatch(source, -1) {
			columns := normalizedSQLColumns(match[1])
			if rel == "internal/store/internal/runtimepersistence/schema_compatibility_bootstrap_test.go" {
				if columns["bundle_hash"] != columns["bundle_source"] {
					t.Errorf("%s contains a partially specified old-store run insert", rel)
				}
				continue
			}
			if !columns["bundle_hash"] || !columns["bundle_source"] {
				t.Errorf("%s contains a run fixture insert without explicit bundle_hash and bundle_source", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk run fixture inserts: %v", err)
	}
}

func TestRepositoryContainsNoLegacyBundleIdentityInterpreter(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	prohibited := []string{
		"bundle_" + "ref",
		"Bundle" + "Ref",
		"Bundle" + "Fingerprint",
		"bundle_" + "fingerprint",
		"BundleSource" + "Legacy",
		"bundle-" + "fingerprint",
		"legacy_bundle_" + "fingerprint",
		"UNSUPPORTED_BUNDLE_" + "REF",
	}
	var files []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk production sources: %v", err)
	}
	files = append(files, filepath.Join(root, "platform-spec.yaml"), filepath.Join(root, "openrpc.json"))
	sort.Strings(files)
	for _, path := range files {
		source := readRepositoryFile(t, path)
		for _, retired := range prohibited {
			if strings.Contains(source, retired) {
				t.Errorf("%s retains retired bundle identity interpreter %q", repositoryRelativePath(t, root, path), retired)
			}
		}
	}
}

func TestPlatformSpecResumeRecoveryRejectsLegacyBundleSourceStates(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	var document map[string]any
	if err := yaml.Unmarshal([]byte(readRepositoryFile(t, filepath.Join(root, "platform-spec.yaml"))), &document); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	multiBundle, ok := document["multi_bundle_persistence"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing multi_bundle_persistence")
	}
	resumeRecovery, ok := multiBundle["resume_and_recovery"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing multi_bundle_persistence.resume_and_recovery")
	}
	var legacyPaths []string
	collectLegacyBundleSourceStatePaths(resumeRecovery, "multi_bundle_persistence.resume_and_recovery", &legacyPaths)
	if len(legacyPaths) != 0 {
		t.Fatalf("resume/recovery retains legacy bundle source states: %s", strings.Join(legacyPaths, ", "))
	}
}

func TestRepositoryBundleSourceOwnershipHandoffsRequireExactOpaqueFacts(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	for _, check := range []struct {
		path       string
		required   []string
		prohibited []string
	}{
		{
			path: "internal/store/internal/backend/mutationlog/adapter.go",
			required: []string{
				"runLifecycle.RequireActiveRunSource",
				"runFact.Matches(contextFact)",
			},
			prohibited: []string{
				"func requireBundleSourceAvailable",
				"SELECT EXISTS (SELECT 1 FROM bundles",
			},
		},
		{
			path: "internal/runtime/bus/eventbus.go",
			required: []string{
				"durable event bus requires an immutable bundle source fact",
				"cleanupCtx, err := eb.admitBundleSourceFact(context.Background())",
				"cleanupCtx, err := p.bus.admitBundleSourceFact(p.lifecycleCtx)",
				"cleanupCtx, err := eb.admitBundleSourceFact(context.WithoutCancel(handle.lifecycleCtx))",
				"operation.publicationClaim.Release(cleanupCtx)",
			},
			prohibited: []string{
				"retireAndWait(context.Background()",
				"NewRoute(context.Background()",
			},
		},
		{
			path: "internal/runtime/bus/eventbus_publish.go",
			required: []string{
				"func (eb *EventBus) admitBundleSourceFact(ctx context.Context) (context.Context, error)",
				"!hasOwnedFact && !eb.ephemeral",
				"!sourceFact.Matches(contextFact)",
				"func (eb *EventBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error)",
				"func (eb *EventBus) admitPreparedPublish(ctx context.Context, prepared PreparedPublish) (context.Context, error)",
				"prepared.publicationClaim.bus != eb",
				"eb.admitBundleSourceFact(ctx)",
				"eb.admitBundleSourceFact(dispatchCtx)",
				"dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)",
				"func (eb *EventBus) beginRuntimeWork(ctx context.Context) (context.Context, *worklifetime.Lease, error)",
				"admittedCtx, err := eb.admitBundleSourceFact(ctx)",
				"lease, err := owner.Begin(admittedCtx)",
				"return bindWorkContext(admittedCtx, lease, owner), lease, nil",
			},
			prohibited: []string{
				"func (eb *EventBus) WithBundleSourceFact",
			},
		},
		{
			path: "internal/runtime/bus/outbox.go",
			required: []string{
				"func (d engineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error",
				"ctx, lease, err := d.bus.beginRuntimeWork(ctx)",
				"func (d engineDispatcher) dispatchPendingOutboxOperation(ctx context.Context, fallback runtimeengine.EmitIntent)",
				"ctx, err = d.bus.admitBundleSourceFact(ctx)",
				"func (d engineDispatcher) dispatchAndRecord(ctx context.Context, intent runtimeengine.EmitIntent, publicationClaim *pipelinePublicationClaim)",
				"func (eb *EventBus) clearPendingOutboxOperation(ctx context.Context, eventID string) error",
			},
		},
		{
			path: "internal/runtime/bus/pipeline_publication_claim.go",
			required: []string{
				"ctx, err = eb.admitBundleSourceFact(ctx)",
				"ctx, err = c.bus.admitBundleSourceFact(ctx)",
			},
		},
		{
			path: "internal/runtime/bus/sweeper.go",
			required: []string{
				"func (eb *EventBus) StartOutboxSweeper(ctx context.Context, cfg OutboxSweeperConfig) error",
				"ctx, lease, err := eb.beginRuntimeWork(ctx)",
				"func (eb *EventBus) SweepPipelineObligations(ctx context.Context, limit int)",
				"func (eb *EventBus) sweepPipelineObligations(ctx context.Context, request runtimepipelineobligation.ScanRequest, limit int)",
				"func (eb *EventBus) ReleaseRunQueue(ctx context.Context, runID string, limit int)",
				"func (eb *EventBus) closePipelineScanLocked(ctx context.Context, request runtimepipelineobligation.ScanRequest) error",
				"ctx, err = eb.admitBundleSourceFact(ctx)",
			},
		},
		{
			path: "internal/runtime/runforkexecution/runtime_container.go",
			required: []string{
				"BundleSourceFact:            req.LoadedSource.BundleSourceFact",
			},
		},
		{
			path: "internal/runtime/manager/types.go",
			required: []string{
				"AdmitBundleSourceFact(context.Context) (context.Context, error)",
			},
		},
		{
			path: "internal/runtime/manager/runtime.go",
			required: []string{
				"ctx, err = am.bus.AdmitBundleSourceFact(ctx)",
			},
			prohibited: []string{
				"type bundleSourceFactContextOwner interface",
				"am.bus.(bundleSourceFactContextOwner)",
			},
		},
		{
			path: "internal/runtime/manager/agent_manager.go",
			required: []string{
				"!current.Matches(fact)",
			},
			prohibited: []string{
				"current.BundleHash() != fact.BundleHash()",
			},
		},
		{
			path: "platform-spec.yaml",
			required: []string{
				"Every durable operation admits that fact before work-occurrence admission or local/store mutation",
				"internal subscriptions, and agent/internal route generations remain bound to that exact fact",
			},
		},
		{
			path: "internal/apiv1/operator_runtime_context.go",
			required: []string{
				"DecodeBundleSourceFact(availability.BundleHash, availability.BundleSource.String())",
				"!fact.Matches(runFact)",
			},
		},
		{
			path: "internal/apiv1/operator_bundle_admission.go",
			required: []string{
				"!publisherFact.Matches(runFact)",
				"!publisherFact.Matches(currentFact)",
				"!current.Matches(fact)",
			},
		},
	} {
		source := readRepositoryFile(t, filepath.Join(root, filepath.FromSlash(check.path)))
		for _, required := range check.required {
			if !strings.Contains(source, required) {
				t.Errorf("%s no longer consumes exact opaque bundle source ownership through %q", check.path, required)
			}
		}
		for _, prohibited := range check.prohibited {
			if strings.Contains(source, prohibited) {
				t.Errorf("%s restored non-authoritative bundle source handoff %q", check.path, prohibited)
			}
		}
	}
}

func TestRepositoryEventBusSourceOperationLedgerIsExhaustive(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	const (
		operationMutation      = "mutation"
		operationRetained      = "retained capability"
		operationAdmittedChild = "already-admitted child"
		operationPureRead      = "pure read"
	)
	ledger := map[string]string{
		"AbandonPreparedPublish":                     operationMutation,
		"AbandonInboundDeliveryPlan":                 operationMutation,
		"AddFlowInstanceRoute":                       operationAdmittedChild,
		"AddFlowInstanceRouteContext":                operationMutation,
		"AdmitBundleSourceFact":                      operationPureRead,
		"BeginPipelineParentTransition":              operationMutation,
		"CheckDirectRoutes":                          operationPureRead,
		"CheckPublishRecipientPlan":                  operationPureRead,
		"CommitFlowInstanceActivation":               operationMutation,
		"DispatchPreparedPublish":                    operationMutation,
		"DispatchPreparedPublishAndWait":             operationMutation,
		"DispatchPreparedPublishAsync":               operationMutation,
		"DispatchDeliveryContinuation":               operationMutation,
		"EngineDispatcher":                           operationRetained,
		"ApplyInboundDeliveryCommit":                 operationMutation,
		"CommitDynamicFlowRuntimeCreationOccurrence": operationMutation,
		"FinalizeEnginePublications":                 operationMutation,
		"FenceAgentRoute":                            operationMutation,
		"FinalizeSelectedReceiverAdmission":          operationRetained,
		"HasFlowInstanceRoute":                       operationPureRead,
		"ListFlowInstanceRoutes":                     operationPureRead,
		"LogRuntime":                                 operationMutation,
		"MarkDeliveryInProgress":                     operationMutation,
		"OutboxSweeperActive":                        operationPureRead,
		"PinRoutingDescriptors":                      operationPureRead,
		"PipelineObligationOwner":                    operationRetained,
		"PipelineWorkPresence":                       operationPureRead,
		"PrepareAgentRoute":                          operationRetained,
		"PrepareEnginePublications":                  operationMutation,
		"PrepareInboundDeliveryBatch":                operationMutation,
		"PrepareSelectedForkPublish":                 operationMutation,
		"Publish":                                    operationMutation,
		"PublishAcknowledged":                        operationMutation,
		"PublishAndWait":                             operationMutation,
		"PublishDirect":                              operationMutation,
		"PublishDirectRoutes":                        operationMutation,
		"RecoverPersistedPipeline":                   operationMutation,
		"RegisterRuntimeActiveAgentDescriptor":       operationRetained,
		"ReleaseRunQueue":                            operationMutation,
		"ReleaseEnginePublications":                  operationMutation,
		"ReleaseRuntimeIngressQueue":                 operationMutation,
		"RemoveAgentRoute":                           operationMutation,
		"RemoveFlowInstanceRoute":                    operationAdmittedChild,
		"RemoveFlowInstanceRouteContext":             operationMutation,
		"ReplaceAgentRoute":                          operationAdmittedChild,
		"ResetInMemoryState":                         operationMutation,
		"ResolveSubscribedRecipients":                operationPureRead,
		"PublishPersistedFlowInstanceRoute":          operationMutation,
		"RetirePublishedFlowInstanceRoute":           operationMutation,
		"RetireCommittedFlowInstanceRoute":           operationAdmittedChild,
		"RouteTable":                                 operationRetained,
		"RunLifecycleCandidateOwner":                 operationRetained,
		"SetDeliveryAuthority":                       operationRetained,
		"SetDeliveryContinuationOwner":               operationRetained,
		"SetInterceptors":                            operationRetained,
		"SetLoggerHook":                              operationRetained,
		"SetProviderOutputAuthorizationVerifier":     operationRetained,
		"SetRunDispatchGate":                         operationRetained,
		"SetRuntimeIngressDispatchGate":              operationRetained,
		"SetStandingRunWorkOwner":                    operationRetained,
		"StartOutboxSweeper":                         operationRetained,
		"StageFlowInstanceRouteContext":              operationMutation,
		"Store":                                      operationRetained,
		"SubscribeInternal":                          operationRetained,
		"DeliveryAuthority":                          operationRetained,
		"DeliveryContinuationOwner":                  operationRetained,
		"AcquireDeliveryContinuation":                operationRetained,
		"AcceptCommittedDeliveryHandoffs":            operationRetained,
		"RetainDeliveryContinuation":                 operationRetained,
		"ReleaseDeliveryContinuation":                operationRetained,
		"SignalDeliveryContinuations":                operationRetained,
		"SweepPipelineObligations":                   operationMutation,
		"WaitForOutboxSweeper":                       operationMutation,
		"WaitForQuiescence":                          operationMutation,
		"VerifyFlowInstanceRoute":                    operationPureRead,
	}
	found := map[string]bool{}
	methodPattern := regexp.MustCompile(`func \([A-Za-z_][A-Za-z0-9_]* \*EventBus\) ([A-Z][A-Za-z0-9_]*)\(`)
	err := filepath.WalkDir(filepath.Join(root, "internal", "runtime", "bus"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		for _, match := range methodPattern.FindAllStringSubmatch(readRepositoryFile(t, path), -1) {
			method := match[1]
			category, ok := ledger[method]
			if !ok {
				t.Errorf("%s is an unclassified exported EventBus operation", method)
				continue
			}
			switch category {
			case operationMutation, operationRetained, operationAdmittedChild, operationPureRead:
			default:
				t.Errorf("%s has unknown EventBus operation category %q", method, category)
			}
			found[method] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk EventBus exported operations: %v", err)
	}
	for method := range ledger {
		if !found[method] {
			t.Errorf("classified EventBus operation %s no longer exists", method)
		}
	}

	for _, operationRoot := range []struct {
		category string
		path     string
		owner    string
		guard    string
	}{
		{operationMutation, "internal/runtime/bus/eventbus_publish.go", "beginRuntimeWork", "admitBundleSourceFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "dispatchPendingOutboxOperation", "admitBundleSourceFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "dispatchAndRecord", "admitBundleSourceFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "clearPendingOutboxOperation", "admitBundleSourceFact(ctx)"},
		{operationMutation, "internal/runtime/bus/pipeline_publication_claim.go", "claimPipelinePublication", "admitBundleSourceFact(ctx)"},
		{operationRetained, "internal/runtime/bus/eventbus.go", "completeInternalSubscription", "admitBundleSourceFact(context.WithoutCancel(handle.lifecycleCtx))"},
		{operationMutation, "internal/runtime/bus/sweeper.go", "closePipelineScanLocked", "admitBundleSourceFact(ctx)"},
	} {
		source := readRepositoryFile(t, filepath.Join(root, filepath.FromSlash(operationRoot.path)))
		if !strings.Contains(source, "func ") || !strings.Contains(source, operationRoot.owner) || !strings.Contains(source, operationRoot.guard) {
			t.Errorf(
				"%s store-facing %s root %s no longer consumes %q",
				operationRoot.category, operationRoot.path, operationRoot.owner, operationRoot.guard,
			)
		}
	}
}

func collectLegacyBundleSourceStatePaths(value any, path string, out *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if strings.Contains(strings.ToLower(key), "legacy") {
				*out = append(*out, childPath)
			}
			collectLegacyBundleSourceStatePaths(child, childPath, out)
		}
	case []any:
		for index, child := range typed {
			collectLegacyBundleSourceStatePaths(child, path+"["+strconv.Itoa(index)+"]", out)
		}
	case string:
		if strings.Contains(strings.ToLower(typed), "legacy") {
			*out = append(*out, path)
		}
	}
}

func repositoryRootForBundleIdentityTest(t *testing.T) string {
	t.Helper()
	return repoRootForRuntimeWriterGuard(t)
}

func repositoryRelativePath(t *testing.T, root, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("relative path for %s: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

func readRepositoryFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func normalizedSQLColumns(raw string) map[string]bool {
	columns := map[string]bool{}
	for _, column := range strings.Split(raw, ",") {
		columns[strings.ToLower(strings.TrimSpace(column))] = true
	}
	return columns
}

var bundleIdentityFixtureLedger = []string{
	"internal/apiv1/agent_diagnose_delivery_pagination_parity_test.go",
	"internal/apiv1/operator_entity_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/operator_human_task_ack_loss_test.go",
	"internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go",
	"internal/apiv1/sqlite_agent_usage_supported_surface_test.go",
	"internal/apiv1/sqlite_observability_supported_surface_test.go",
	"internal/apiv1/sqlite_operator_read_supported_surface_test.go",
	"internal/cliapp/entities_test.go",
	"internal/dashboard/server/observability_sql_test.go",
	"internal/dashboard/server/server_test.go",
	"internal/digest/source_test.go",
	"internal/runtime/budget_recovery_parity_test.go",
	"internal/runtime/bus/event_identity_dispatch_surface_test.go",
	"internal/runtime/bus/eventbus_publish_test.go",
	"internal/runtime/cataloge2e/assertions_test.go",
	"internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go",
	"internal/runtime/conformance/persisted_surfaces_test.go",
	"internal/runtime/conformance/reply_resolution_conformance_test.go",
	"internal/runtime/inbound_postgres_test.go",
	"internal/runtime/manager/flow_activation_test.go",
	"internal/runtime/mutationlog/mutationlog_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/pipeline/activity_boring_proof_test.go",
	"internal/runtime/pipeline/activity_engine_test.go",
	"internal/runtime/pipeline/create_entity_exact_once_test.go",
	"internal/runtime/pipeline/forked_source_claimants_test.go",
	"internal/runtime/pipeline/handler_engine_transaction_test.go",
	"internal/runtime/pipeline/human_task_expiry_transaction_test.go",
	"internal/runtime/pipeline/run_scoped_test_helpers_test.go",
	"internal/runtime/pipeline/select_entity_test.go",
	"internal/runtime/pipeline/workflow_gate_lifecycle_test.go",
	"internal/runtime/pipeline/workflow_gate_recovery_external_test.go",
	"internal/runtime/pipeline/workflow_instance_store_run_scope_test.go",
	"internal/runtime/pipeline/workflow_instance_store_sqlite_test.go",
	"internal/runtime/pipeline/workflow_join_lifecycle_test.go",
	"internal/runtime/runforkadmission/revision_source_route_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/template_instance_delivery_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/provider_trigger_smoke_helpers_test.go",
	"internal/serveapp/run_fork_runtime_test.go",
	"internal/store/internal/runtimepersistence/agent_directive_operations_test.go",
	"internal/store/internal/runtimepersistence/agent_directive_run_target_test.go",
	"internal/store/internal/runtimepersistence/agent_lifecycle_effects_test.go",
	"internal/store/internal/runtimepersistence/agent_lifecycle_read_surface_test.go",
	"internal/store/internal/runtimepersistence/agent_lifecycle_subordinate_test.go",
	"internal/store/internal/runtimepersistence/author_activity_receipt_parity_test.go",
	"internal/store/internal/runtimepersistence/budget_spend_test.go",
	"internal/store/internal/runtimepersistence/bundle_delete_test.go",
	"internal/store/internal/runtimepersistence/completion_settlement_test.go",
	"internal/store/internal/runtimepersistence/decision_cards_test.go",
	"internal/store/internal/runtimepersistence/destructive_reset_cleanup_test.go",
	"internal/store/internal/runtimepersistence/destructive_reset_directive_integration_test.go",
	"internal/store/internal/runtimepersistence/diagnostic_direct_replay_test.go",
	"internal/store/internal/runtimepersistence/directive_acknowledgment_test.go",
	"internal/store/internal/runtimepersistence/entity_state_run_scope_test.go",
	"internal/store/internal/runtimepersistence/event_admission_persistence_test.go",
	"internal/store/internal/runtimepersistence/flow_instance_descriptors_sqlite_test.go",
	"internal/store/internal/runtimepersistence/flow_instance_routes_test.go",
	"internal/store/internal/runtimepersistence/operator_agent_conversation_read_surface_test.go",
	"internal/store/internal/runtimepersistence/operator_conversation_projection_test.go",
	"internal/store/internal/runtimepersistence/operator_entity_read_surface_test.go",
	"internal/store/internal/runtimepersistence/operator_observability_read_surface_test.go",
	"internal/store/internal/runtimepersistence/postgres_helpers_test.go",
	"internal/store/internal/runtimepersistence/postgres_smoke_test.go",
	"internal/store/internal/runtimepersistence/postgres_store_additional_test.go",
	"internal/store/internal/runtimepersistence/preservation_cleanup_test.go",
	"internal/store/run_bundle_fingerprint_test.go",
	"internal/store/internal/runtimepersistence/run_completion_test.go",
	"internal/store/internal/runtimepersistence/run_control_test.go",
	"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go",
	"internal/store/internal/runtimepersistence/run_fork_gate_activation_test.go",
	"internal/store/internal/runtimepersistence/run_fork_materializer_test.go",
	"internal/store/internal/runtimepersistence/run_fork_planner_test.go",
	"internal/store/internal/runtimepersistence/run_fork_revision_conformance_test.go",
	"internal/store/internal/runtimepersistence/run_fork_selected_contract_route_recovery_test.go",
	"internal/store/internal/runtimepersistence/run_fork_source_freeze_test.go",
	"internal/store/internal/runtimepersistence/runtime_effects_neutral_schema_test.go",
	"internal/store/internal/runtimepersistence/runtime_log_persistence_test.go",
	"internal/store/internal/runtimepersistence/runtime_mutation_test.go",
	"internal/store/internal/runtimepersistence/schema_compatibility_bootstrap_test.go",
	"internal/store/internal/runtimepersistence/selected_contract_runtime_execution_test.go",
	"internal/store/internal/runtimepersistence/sqlite_run_api_read_surface_test.go",
	"internal/store/internal/runtimepersistence/sqlite_run_completion_test.go",
	"internal/store/internal/runtimepersistence/sqlite_run_trace_parity_test.go",
	"internal/store/internal/runtimepersistence/sqlite_runtime_test.go",
	"internal/store/storetest/event.go",
}
