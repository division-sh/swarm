package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"

	"gopkg.in/yaml.v3"
)

var runInsertPattern = regexp.MustCompile(`(?is)INSERT\s+INTO\s+runs\s*\(([^)]*)\)`)

func TestRepositoryRunCreationHasCanonicalOwnersOnly(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	wantOwners := map[string]bool{
		"internal/store/internal/backend/runlifecycle/run_lifecycle_mutation.go": false,
		"internal/store/internal/backend/runlifecycle/test_snapshot_fault.go":    false,
		"internal/testutil/runlifecyclefixture/fixture.go":                       false,
	}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
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

	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation.go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		violations, err := runFixtureIdentityViolations(rel, file)
		if err != nil {
			return err
		}
		for _, violation := range violations {
			t.Errorf("%s: %s", rel, violation)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk run fixture inserts: %v", err)
	}
}

func runFixtureIdentityViolations(path string, file *ast.File) ([]string, error) {
	minimal, err := classifyBackendMinimalRunLiterals(path, file)
	if err != nil {
		return nil, err
	}
	oracles := classifyRunFixtureIdentityOracleLiterals(path, file)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || minimal[literal.Pos()] || oracles[literal.Pos()] {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		for _, match := range runInsertPattern.FindAllStringSubmatch(value, -1) {
			columns := normalizedSQLColumns(match[1])
			if !columns["bundle_hash"] {
				violations = append(violations, "run fixture insert lacks explicit bundle_hash")
			}
			if columns["bundle_source"] {
				violations = append(violations, "retired bundle_source identity")
			}
		}
		return true
	})
	return violations, nil
}

// Guard counterexamples describe SQL without executing it. The exemption is
// declaration-local, and disappears if the declaration acquires a SQL caller.
func classifyRunFixtureIdentityOracleLiterals(path string, file *ast.File) map[token.Pos]bool {
	approved := map[token.Pos]bool{}
	if path != "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go" &&
		path != "internal/store/internal/runtimepersistence/run_bundle_identity_repository_test.go" {
		return approved
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "TestRepositoryRunFixtureIdentityGuardRejectsNewWriters":
			if path != "internal/store/internal/runtimepersistence/run_bundle_identity_repository_test.go" {
				continue
			}
		case "classifyBackendMinimalRunLiterals", "classifyA2CollectionMinimalRunLiterals",
			"TestA2CollectionMinimalRunFixtureClassificationIsExact", "classifyReceiverHistoryMinimalRunLiterals",
			"TestReceiverHistoryMinimalRunFixtureClassificationIsExact", "allowedSemanticRunFixtureLiteral":
			if path != "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go" {
				continue
			}
		default:
			continue
		}
		literals := map[token.Pos]bool{}
		nonExecuting := true
		ast.Inspect(fn, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				switch selector.Sel.Name {
				case "Exec", "ExecContext", "Query", "QueryContext", "QueryRow", "QueryRowContext",
					"Prepare", "PrepareContext", "Begin", "BeginTx", "WithSQL":
					nonExecuting = false
				}
			}
			if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				literals[literal.Pos()] = true
			}
			return true
		})
		if nonExecuting {
			for pos := range literals {
				approved[pos] = true
			}
		}
	}
	return approved
}

func TestRepositoryRunFixtureIdentityGuardRejectsNewWriters(t *testing.T) {
	const oraclePath = "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go"
	const source = "package fixture\nfunc allowedSemanticRunFixtureLiteral() { use(`INSERT INTO runs (run_id) VALUES ($1)`) }"
	const projectionPath = "internal/store/internal/backend/mutationprotocol/event_counts_test.go"
	const projection = "package fixture\nvar ddl = `CREATE TABLE runs (run_id UUID PRIMARY KEY, event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0))`\nvar insert = `INSERT INTO runs (run_id) VALUES ($1)`"
	for _, tc := range []struct {
		name, path, source string
		want               int
	}{
		{"oracle", oraclePath, source, 0},
		{"wrong-owner", "internal/runtime/other_test.go", source, 1},
		{"sql-in-oracle", oraclePath, strings.Replace(source, "use(", "db.Exec(", 1), 1},
		{"additional-writer", oraclePath, source + "\nfunc other() { db.Exec(`INSERT INTO runs (run_id) VALUES ($1)`) }", 1},
		{"minimal-projection", projectionPath, projection, 0},
		{"extra-projection-writer", projectionPath, projection + "\nvar extra = `INSERT INTO runs (run_id, status) VALUES ($1, 'running')`", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, err := runFixtureIdentityViolations(tc.path, file)
			if err != nil || len(violations) != tc.want {
				t.Fatalf("violations=%v, want %d, err=%v", violations, tc.want, err)
			}
		})
	}

}

func TestRepositoryContainsNoLegacyBundleIdentityInterpreter(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	prohibited := []string{
		"bundle_" + "ref",
		"Bundle" + "Ref",
		"Bundle" + "Fingerprint",
		"Bundle" + "SourceFact",
		"bundle_" + "fingerprint",
		"bundle_" + "source",
		"SourceArtifact" + "Legacy",
		"bundle-" + "fingerprint",
		"legacy_bundle_" + "fingerprint",
		"UNSUPPORTED_BUNDLE_" + "REF",
	}
	var files []string
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
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

func TestPlatformSpecSourceArtifactRecoveryRejectsLegacySourceArtifactStates(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	var document map[string]any
	if err := yaml.Unmarshal([]byte(readRepositoryFile(t, filepath.Join(root, "platform-spec.yaml"))), &document); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	sourceModel, ok := document["filesystem_source_model"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing filesystem_source_model")
	}
	durableLifecycle, ok := sourceModel["durable_lifecycle"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing filesystem_source_model.durable_lifecycle")
	}
	var legacyPaths []string
	collectLegacySourceArtifactStatePaths(durableLifecycle, "filesystem_source_model.durable_lifecycle", &legacyPaths)
	if len(legacyPaths) != 0 {
		t.Fatalf("resume/recovery retains legacy bundle source states: %s", strings.Join(legacyPaths, ", "))
	}
}

func TestRepositorySourceArtifactOwnershipHandoffsRequireExactOpaqueFacts(t *testing.T) {
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
				"func requireSourceArtifactAvailable",
				"SELECT EXISTS (SELECT 1 FROM bundles",
			},
		},
		{
			path: "internal/runtime/bus/eventbus.go",
			required: []string{
				"durable event bus requires an immutable bundle source fact",
				"cleanupCtx, err := eb.admitSourceArtifactFact(context.Background())",
				"cleanupCtx, err := p.bus.admitSourceArtifactFact(p.lifecycleCtx)",
				"cleanupCtx, err := eb.admitSourceArtifactFact(context.WithoutCancel(handle.lifecycleCtx))",
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
				"func (eb *EventBus) admitSourceArtifactFact(ctx context.Context) (context.Context, error)",
				"!hasOwnedFact && !eb.ephemeral",
				"!sourceFact.Matches(contextFact)",
				"func (eb *EventBus) AdmitSourceArtifactFact(ctx context.Context) (context.Context, error)",
				"func (eb *EventBus) admitPreparedPublish(ctx context.Context, prepared PreparedPublish) (context.Context, error)",
				"prepared.publicationClaim.bus != eb",
				"eb.admitSourceArtifactFact(ctx)",
				"eb.admitSourceArtifactFact(dispatchCtx)",
				"dispatchCtx, err := eb.admitPreparedPublish(ctx, prepared)",
				"func (eb *EventBus) beginRuntimeWork(ctx context.Context) (context.Context, *worklifetime.Lease, error)",
				"admittedCtx, err := eb.admitSourceArtifactFact(ctx)",
				"lease, err := owner.Begin(admittedCtx)",
				"return bindWorkContext(admittedCtx, lease, owner), lease, nil",
			},
			prohibited: []string{
				"func (eb *EventBus) WithSourceArtifactFact",
			},
		},
		{
			path: "internal/runtime/bus/outbox.go",
			required: []string{
				"func (d engineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) (err error)",
				"ctx, lease, err := d.bus.beginRuntimeWork(ctx)",
				"func (d engineDispatcher) dispatchOnePostCommit(ctx context.Context, intent runtimeengine.EmitIntent) (err error)",
				"func (d engineDispatcher) dispatchPendingOutboxOperation(ctx context.Context, fallback runtimeengine.EmitIntent)",
				"ctx, err = d.bus.admitSourceArtifactFact(ctx)",
				"func (d engineDispatcher) dispatchAndRecord(ctx context.Context, intent runtimeengine.EmitIntent, publicationClaim *pipelinePublicationClaim)",
				"func (eb *EventBus) clearPendingOutboxOperation(ctx context.Context, eventID string) error",
			},
		},
		{
			path: "internal/runtime/bus/pipeline_publication_claim.go",
			required: []string{
				"ctx, err = eb.admitSourceArtifactFact(ctx)",
				"ctx, err = c.bus.admitSourceArtifactFact(ctx)",
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
				"ctx, err = eb.admitSourceArtifactFact(ctx)",
			},
		},
		{
			path: "internal/runtime/runforkexecution/runtime_container.go",
			required: []string{
				"SourceArtifactFact:          req.LoadedSource.SourceArtifactFact",
			},
		},
		{
			path: "internal/runtime/manager/types.go",
			required: []string{
				"AdmitSourceArtifactFact(context.Context) (context.Context, error)",
			},
		},
		{
			path: "internal/runtime/manager/runtime.go",
			required: []string{
				"ctx, err = am.bus.AdmitSourceArtifactFact(ctx)",
			},
			prohibited: []string{
				"type sourceArtifactFactContextOwner interface",
				"am.bus.(sourceArtifactFactContextOwner)",
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
				"Local durable run/serve persists or reconciles the exact logical blob before publishing a runtime or run that references its hash",
				"Internal standing recovery, replay, reset and fork decode the selected-store blob and compile only the reconstructed artifact",
			},
		},
		{
			path: "internal/apiv1/operator_runtime_context.go",
			required: []string{
				"DecodeSourceArtifactFact(availability.BundleHash)",
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

const (
	operationMutation      = "mutation"
	operationRetained      = "retained capability"
	operationAdmittedChild = "already-admitted child"
	operationPureRead      = "pure read"
)

func eventBusSourceOperationLedger() map[string]string {
	return map[string]string{
		"AbandonPreparedPublish":                     operationMutation,
		"AbandonInboundDeliveryPlan":                 operationMutation,
		"AdmitSourceArtifactFact":                    operationPureRead,
		"BeginPipelineParentTransition":              operationMutation,
		"BeginRunStop":                               operationMutation,
		"CheckAPIEventPublishRecipientPlan":          operationPureRead,
		"CheckDirectRoutes":                          operationPureRead,
		"CheckPublishRecipientPlan":                  operationPureRead,
		"CommitFlowInstanceActivation":               operationMutation,
		"DispatchPreparedPublish":                    operationMutation,
		"DispatchPreparedPublishAndWait":             operationMutation,
		"DispatchPreparedPublishAsync":               operationMutation,
		"DispatchDeliveryContinuation":               operationMutation,
		"DispatchFanOutPublications":                 operationMutation,
		"DispatchTurnTimeoutReaction":                operationMutation,
		"EngineDispatcher":                           operationRetained,
		"ApplyInboundDeliveryCommit":                 operationMutation,
		"CommitDynamicFlowRuntimeCreationOccurrence": operationMutation,
		"FinalizeEnginePublications":                 operationMutation,
		"FinalizeFanOutPublications":                 operationMutation,
		"FenceAgentRoute":                            operationMutation,
		"FinalizeSelectedReceiverAdmission":          operationRetained,
		"HasFlowInstanceRoute":                       operationPureRead,
		"ListFlowInstanceRoutes":                     operationPureRead,
		"LookupAPIEventPublication":                  operationPureRead,
		"LogRuntime":                                 operationMutation,
		// This projects an exact already-admitted durable transition, including retained
		// history. Current consumer source admission cannot retag its producer identity.
		"ProjectLifecycleDiagnostic":        operationAdmittedChild,
		"MarkDeliveryInProgress":            operationMutation,
		"OutboxSweeperActive":               operationPureRead,
		"PinRoutingDescriptors":             operationPureRead,
		"PipelineObligationOwner":           operationRetained,
		"PipelineWorkPresence":              operationPureRead,
		"PrepareAgentRoute":                 operationRetained,
		"PrepareEnginePublications":         operationMutation,
		"PrepareEngineMutationPublications": operationMutation,
		"PrepareFanOutPublication":          operationMutation,
		"PrepareFanOutPublications":         operationMutation,
		"PrepareInboundDeliveryBatch":       operationMutation,
		// Admission-only projection and elected-receipt validation do not commit
		// construction or publication; their existing owners retain authority.
		"PrepareInboundEvidence":                      operationPureRead,
		"ReconcileInboundConstruction":                operationPureRead,
		"PrepareSelectedForkPublish":                  operationMutation,
		"PrepareTurnTimeoutReaction":                  operationMutation,
		"PrepareRecoveredTurnTimeoutReaction":         operationMutation,
		"PreflightRunQueue":                           operationPureRead,
		"PreflightRuntimeIngressQueue":                operationPureRead,
		"Publish":                                     operationMutation,
		"PublishAcknowledged":                         operationMutation,
		"PublishAPIEventAcknowledged":                 operationMutation,
		"PublishAPIEventWithRunCreationAcknowledged":  operationMutation,
		"PublishAndWait":                              operationMutation,
		"PublishDirect":                               operationMutation,
		"PublishDirectRoutes":                         operationMutation,
		"PublishEmit":                                 operationMutation,
		"RecoverPersistedPipeline":                    operationMutation,
		"RecoverSelectedRunPipelineToExhaustion":      operationMutation,
		"RegisterRuntimeActiveAgentDescriptor":        operationRetained,
		"ReleaseRunQueue":                             operationMutation,
		"ReleaseEnginePublications":                   operationMutation,
		"ReleaseRuntimeIngressQueue":                  operationMutation,
		"ReleaseTurnTimeoutReaction":                  operationMutation,
		"RemoveAgentRoute":                            operationMutation,
		"ReplaceAgentRoute":                           operationAdmittedChild,
		"ResetInMemoryState":                          operationMutation,
		"ResolveSubscribedRecipients":                 operationPureRead,
		"PublishPersistedFlowInstanceRouteForAttempt": operationMutation,
		"RetireFlowInstanceRouteForAttempt":           operationAdmittedChild,
		"RetireCommittedFlowInstanceRoute":            operationAdmittedChild,
		"RouteTable":                                  operationRetained,
		"RunLifecycleCandidateOwner":                  operationRetained,
		"SealFanOutPublications":                      operationMutation,
		"SetDeliveryAuthority":                        operationRetained,
		"SetDeliveryContinuationOwner":                operationRetained,
		"SetCommittedAgentReadinessFinalizer":         operationRetained,
		"SetInterceptors":                             operationRetained,
		"SetLoggerHook":                               operationRetained,
		"SetProviderOutputAuthorizationVerifier":      operationRetained,
		"SetRunDispatchGate":                          operationRetained,
		"SetRuntimeIngressDispatchGate":               operationRetained,
		"SetStandingRunWorkOwner":                     operationRetained,
		"StartOutboxSweeper":                          operationRetained,
		"StageFlowInstanceRouteContext":               operationMutation,
		"StartDeploymentRunAcknowledged":              operationMutation,
		"Store":                                       operationRetained,
		"SubscribeInternal":                           operationRetained,
		"DeliveryAuthority":                           operationRetained,
		"DeliveryContinuationOwner":                   operationRetained,
		"AcquireDeliveryContinuation":                 operationRetained,
		"AcceptCommittedDeliveryHandoffs":             operationRetained,
		"RetainDeliveryContinuation":                  operationRetained,
		"ReleaseDeliveryContinuation":                 operationRetained,
		"SignalDeliveryContinuations":                 operationRetained,
		"SweepPipelineObligations":                    operationMutation,
		"WaitForOutboxSweeper":                        operationMutation,
		"WaitForQuiescence":                           operationMutation,
		"VerifyFlowInstanceRoute":                     operationPureRead,
		"SetupScenarioEntities":                       operationMutation,
	}
}

func TestRepositoryEventBusSourceOperationLedgerIsExhaustive(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	sources := map[string]string{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "runtime", "bus"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		sources[filepath.Base(path)] = readRepositoryFile(t, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk EventBus exported operations: %v", err)
	}
	if err := validateEventBusSourceOperationCensus(sources, eventBusSourceOperationLedger()); err != nil {
		t.Error(err)
	}
	if err := validateEventBusGroupSourceConsumers(sources); err != nil {
		t.Error(err)
	}

	for _, operationRoot := range []struct {
		category string
		path     string
		owner    string
		guard    string
	}{
		{operationMutation, "internal/runtime/bus/eventbus_publish.go", "beginRuntimeWork", "admitSourceArtifactFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "dispatchPendingOutboxOperation", "admitSourceArtifactFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "dispatchAndRecord", "admitSourceArtifactFact(ctx)"},
		{operationMutation, "internal/runtime/bus/outbox.go", "clearPendingOutboxOperation", "admitSourceArtifactFact(ctx)"},
		{operationMutation, "internal/runtime/bus/pipeline_publication_claim.go", "claimPipelinePublication", "admitSourceArtifactFact(ctx)"},
		{operationRetained, "internal/runtime/bus/eventbus.go", "completeInternalSubscription", "admitSourceArtifactFact(context.WithoutCancel(handle.lifecycleCtx))"},
		{operationMutation, "internal/runtime/bus/sweeper.go", "closePipelineScanLocked", "admitSourceArtifactFact(ctx)"},
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

func collectLegacySourceArtifactStatePaths(value any, path string, out *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if strings.Contains(strings.ToLower(key), "legacy") {
				*out = append(*out, childPath)
			}
			collectLegacySourceArtifactStatePaths(child, childPath, out)
		}
	case []any:
		for index, child := range typed {
			collectLegacySourceArtifactStatePaths(child, path+"["+strconv.Itoa(index)+"]", out)
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
	"internal/store/internal/runtimepersistence/author_activity_receipt_parity_test.go",
	"internal/store/internal/runtimepersistence/budget_spend_test.go",
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
