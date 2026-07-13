package effects

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type primitiveOwner string

const (
	ownerManagedAgent        primitiveOwner = "managed_agent_attempt"
	ownerRuntimeDependency   primitiveOwner = "runtime_dependency"
	ownerPipelineActivity    primitiveOwner = "pipeline_activity"
	ownerNotification        primitiveOwner = "notification"
	ownerOperatorInfra       primitiveOwner = "operator_infrastructure"
	ownerCredentialLifecycle primitiveOwner = "credential_lifecycle"
	ownerDiagnostic          primitiveOwner = "diagnostic"
	ownerComputeSandbox      primitiveOwner = "compute_sandbox"
	ownerBuildTest           primitiveOwner = "build_test_infrastructure"
)

// sourcePrimitiveOwners is an exact source-derived ledger. Keys include the
// enclosing function and per-function ordinal so adding, moving, or removing a
// launch/write primitive makes this test fail until ownership is reclassified.
var sourcePrimitiveOwners = map[string]primitiveOwner{
	"internal/runtime/contracts/bundle_build.go:BuildBundleMaterialization:filesystem_write:1":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleInputs:filesystem_write:1":                               ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleSourceInputs:filesystem_write:1":                         ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:writeDeterministicJSONFile:filesystem_write:1":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:materializeBundleCatalogRuntimeFiles:filesystem_write:1": ownerRuntimeDependency,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:1":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:2":                                            ownerCredentialLifecycle,
	"internal/runtime/engine/helpers.go:executionConditionEnv:http_do:1":                                                  ownerPipelineActivity,
	"internal/runtime/llm/api_runtime.go:sendRequest:http_do:1":                                                           ownerManagedAgent,
	"internal/runtime/llm/cli_runtime_process.go:runStreamingPrepared:process_launch:1":                                   ownerManagedAgent,
	"internal/runtime/llm/cli_runtime_process.go:runWithPreparedInput:process_launch:1":                                   ownerManagedAgent,
	"internal/runtime/llm/cli_runtime_startup_probe.go:runUntilCLIStartupInit:process_launch:1":                           ownerRuntimeDependency,
	"internal/runtime/llm/cli_tool_result_relay.go:runWorkspaceCommand:process_launch:1":                                  ownerManagedAgent,
	"internal/runtime/llm/openai_compatible_runtime.go:sendRequest:http_do:1":                                             ownerManagedAgent,
	"internal/runtime/llm/openai_responses_runtime.go:sendRequest:http_do:1":                                              ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:exchange:http_do:1":                                                     ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:exchangeGitHubAppInstallation:http_do:1":                                ownerManagedAgent,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:1":                                         ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:2":                                         ownerCredentialLifecycle,
	"internal/runtime/mcp/client.go:callHTTPServerWithCredentialKeyResolver:http_do:1":                                    ownerManagedAgent,
	"internal/runtime/mcp/client.go:newStdioRPCClient:process_launch:1":                                                   ownerRuntimeDependency,
	"internal/runtime/mcp/client.go:Call:stdio_write:1":                                                                   ownerManagedAgent,
	"internal/runtime/pipeline/activity_engine.go:executePreparedActivityHTTPTool:http_do:1":                              ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:runArtifactGit:process_launch:1":                                          ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoWritableDirectory:filesystem_write:1":                 ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:writeArtifactRepoFiles:filesystem_write:1":                                ownerPipelineActivity,
	"internal/runtime/pipeline/generic_test_module.go:init:http_do:1":                                                     ownerBuildTest,
	"internal/runtime/pythonmodule/runtime.go:materializedArtifactDir:http_do:1":                                          ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:runHarness:filesystem_write:1":                                              ownerComputeSandbox,
	"internal/runtime/runtime.go:startSystemNodesAndWaitForSubscriptions:http_do:1":                                       ownerRuntimeDependency,
	"internal/runtime/runtime_claude_startup.go:startupCallMCP:http_do:1":                                                 ownerRuntimeDependency,
	"internal/runtime/sessions/heartbeat.go:StartLeaseHeartbeatWithErrorHandler:http_do:1":                                ownerRuntimeDependency,
	"internal/runtime/shutdown_admission.go:BeginContext:http_do:1":                                                       ownerRuntimeDependency,
	"internal/runtime/testfixtures/notifyallchildren/fixture.go:copyTree:filesystem_write:1":                              ownerBuildTest,
	"internal/runtime/testfixtures/notifyallchildren/fixture.go:copyTree:filesystem_write:2":                              ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:AddOverlayFile:filesystem_write:1":                         ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:AddOverlayFile:filesystem_write:2":                         ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:SetOverlayFile:filesystem_write:1":                         ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:SetOverlayFile:filesystem_write:2":                         ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:applyClosedReplacement:filesystem_write:1":                 ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:copyTree:filesystem_write:1":                               ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:copyTree:filesystem_write:2":                               ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:writeClosedNegativeFile:filesystem_write:1":                ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:writeClosedNegativeFile:filesystem_write:2":                ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:writeFixtureFile:filesystem_write:1":                       ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:writeFixtureFile:filesystem_write:2":                       ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/fixture.go:writeYAMLDocument:filesystem_write:1":                      ownerBuildTest,
	"internal/runtime/testfixtures/canonicalrouting/specialized_variants.go:removeInheritedScenarios:filesystem_write:1":  ownerBuildTest,
	"internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:writeFile:filesystem_write:1":                     ownerBuildTest,
	"internal/runtime/tools/executor_http.go:execHTTPRequestOnce:http_do:1":                                               ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:doNormalizedSearch:http_do:1":                                              ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:1":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:2":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1":                                      ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:1":                             ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:2":                             ownerManagedAgent,
	"internal/runtime/workflowexpr/data_expression.go:dataExpressionEnvForContext:http_do:1":                              ownerPipelineActivity,
	"internal/runtime/workflowexpr/data_expression.go:dataExpressionEnvForContext:http_do:2":                              ownerPipelineActivity,
	"internal/runtime/workspace/manager.go:RunDocker:process_launch:1":                                                    ownerRuntimeDependency,
	"internal/runtime/contracts/bundle_build.go:BuildBundleMaterialization:filesystem_write:2":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:BuildBundleMaterialization:filesystem_write:3":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:BuildBundleMaterialization:filesystem_write:4":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleInputs:filesystem_write:2":                               ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:materializeBundleSourceInputs:filesystem_write:2":                         ownerOperatorInfra,
	"internal/runtime/contracts/bundle_build.go:writeDeterministicJSONFile:filesystem_write:2":                            ownerOperatorInfra,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:Cleanup:filesystem_write:1":                              ownerRuntimeDependency,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:LoadBundleCatalogRuntimeSource:filesystem_write:1":       ownerRuntimeDependency,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:LoadBundleCatalogRuntimeSource:filesystem_write:2":       ownerRuntimeDependency,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:LoadBundleCatalogRuntimeSource:filesystem_write:3":       ownerRuntimeDependency,
	"internal/runtime/contracts/bundle_catalog_runtime_loader.go:materializeBundleCatalogRuntimeFiles:filesystem_write:2": ownerRuntimeDependency,
	"internal/runtime/credentials/file_lock_unix.go:lockCredentialFile:filesystem_write:1":                                ownerCredentialLifecycle,
	"internal/runtime/credentials/file_lock_windows.go:lockCredentialFile:filesystem_write:1":                             ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:3":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:4":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:5":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:saveLocked:filesystem_write:6":                                            ownerCredentialLifecycle,
	"internal/runtime/credentials/file_store.go:withWriteLockLocked:filesystem_write:1":                                   ownerCredentialLifecycle,
	"internal/runtime/llm/monitor_sink.go:OpenTurn:filesystem_write:1":                                                    ownerDiagnostic,
	"internal/runtime/llm/monitor_sink.go:OpenTurn:filesystem_write:2":                                                    ownerDiagnostic,
	"internal/runtime/managedcredentials/file_lock_unix.go:lockManagedCredentialFile:filesystem_write:1":                  ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/file_lock_windows.go:lockManagedCredentialFile:filesystem_write:1":               ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:withWriteLockLocked:filesystem_write:1":                                 ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:3":                                         ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:4":                                         ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:5":                                         ownerCredentialLifecycle,
	"internal/runtime/managedcredentials/store.go:writeLocked:filesystem_write:6":                                         ownerCredentialLifecycle,
	"internal/runtime/pipeline/artifact_repo.go:ensureArtifactRepoInitialized:filesystem_write:1":                         ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoRootWritable:filesystem_write:1":                      ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoRootWritable:filesystem_write:2":                      ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoWritableDirectory:filesystem_write:2":                 ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:validateArtifactRepoWritableDirectory:filesystem_write:3":                 ownerPipelineActivity,
	"internal/runtime/pipeline/artifact_repo.go:writeArtifactRepoFiles:filesystem_write:2":                                ownerPipelineActivity,
	"internal/runtime/pythonmodule/runtime.go:extractArtifact:filesystem_write:1":                                         ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:extractArtifact:filesystem_write:2":                                         ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:extractArtifact:filesystem_write:3":                                         ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:extractArtifact:filesystem_write:4":                                         ownerComputeSandbox,
	"internal/runtime/pythonmodule/runtime.go:runHarness:filesystem_write:2":                                              ownerComputeSandbox,
	"internal/runtime/testfixtures/notifyallchildren/fixture.go:replaceFile:filesystem_write:1":                           ownerBuildTest,
	"internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:writeFile:filesystem_write:2":                     ownerBuildTest,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:3":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:4":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:5":                                ownerManagedAgent,
	"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:6":                                ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:3":                             ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:4":                             ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:5":                             ownerManagedAgent,
	"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:6":                             ownerManagedAgent,
	"internal/runtime/workspace/host_manager.go:EnsurePrereqs:filesystem_write:1":                                         ownerRuntimeDependency,
	"internal/runtime/workspace/host_manager.go:ensureHostWorkspaceDir:filesystem_write:1":                                ownerRuntimeDependency,
}

func TestDirectPrimitiveOwnershipManifestIsTotal(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	actual, err := collectDirectPrimitives(root)
	if err != nil {
		t.Fatalf("collect direct primitives: %v", err)
	}
	missing := make([]string, 0)
	stale := make([]string, 0)
	for key := range actual {
		if _, ok := sourcePrimitiveOwners[key]; !ok {
			missing = append(missing, key)
		}
	}
	for key := range sourcePrimitiveOwners {
		if _, ok := actual[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) != 0 || len(stale) != 0 {
		t.Fatalf("direct primitive ownership manifest drift\nmissing:\n%s\nstale:\n%s", strings.Join(missing, "\n"), strings.Join(stale, "\n"))
	}
}

func TestManagedEffectRegistrationsAreCompleteAndLive(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	seen := map[string]struct{}{}
	primitiveAdapters := map[string][]string{}
	for _, registration := range Registrations() {
		if registration.Kind == "" || registration.Class == "" || strings.TrimSpace(registration.Adapter) == "" ||
			strings.TrimSpace(registration.Transport) == "" || strings.TrimSpace(registration.LaunchSite) == "" ||
			strings.TrimSpace(registration.LaunchObserved) == "" || strings.TrimSpace(registration.OutcomeMapping) == "" ||
			strings.TrimSpace(registration.CanonicalEvidence) == "" || strings.TrimSpace(registration.SettlementRecovery) == "" ||
			strings.TrimSpace(registration.Proof) == "" || len(registration.PrimitiveKeys) == 0 ||
			registration.PrelaunchFailure == "" || registration.PostlaunchFailure == "" {
			t.Fatalf("incomplete effect registration: %#v", registration)
		}
		if _, ok := seen[registration.Adapter]; ok {
			t.Fatalf("duplicate effect adapter registration %q", registration.Adapter)
		}
		seen[registration.Adapter] = struct{}{}
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(registration.LaunchSite)))
		if err != nil {
			t.Fatalf("read launch site for %s: %v", registration.Adapter, err)
		}
		if !strings.Contains(string(raw), fmt.Sprintf("\"%s\"", registration.Adapter)) {
			t.Fatalf("launch site %s does not consume adapter %s", registration.LaunchSite, registration.Adapter)
		}
		for _, primitiveKey := range registration.PrimitiveKeys {
			primitiveAdapters[primitiveKey] = append(primitiveAdapters[primitiveKey], registration.Adapter)
		}
	}
	for primitiveKey, owner := range sourcePrimitiveOwners {
		if owner != ownerManagedAgent {
			continue
		}
		if len(primitiveAdapters[primitiveKey]) == 0 {
			t.Errorf("managed primitive %s has no adapter contract", primitiveKey)
		}
	}
	for primitiveKey, adapters := range primitiveAdapters {
		if sourcePrimitiveOwners[primitiveKey] != ownerManagedAgent {
			t.Errorf("adapter contract %s -> %v does not name a live managed primitive", primitiveKey, adapters)
			continue
		}
		requiresCompletionHeartbeat := false
		for _, adapter := range adapters {
			registration, ok := RegistrationFor(adapter)
			if ok && registration.Kind == KindProviderTurn {
				requiresCompletionHeartbeat = true
				break
			}
		}
		if err := verifyManagedPrimitiveOrdering(root, primitiveKey, requiresCompletionHeartbeat); err != nil {
			t.Errorf("managed primitive contract %s -> %v: %v", primitiveKey, adapters, err)
		}
	}
}

func verifyManagedPrimitiveOrdering(root, primitiveKey string, requiresCompletionHeartbeat bool) error {
	parts := strings.Split(primitiveKey, ":")
	if len(parts) != 4 {
		return fmt.Errorf("invalid primitive key")
	}
	ordinal := 0
	if _, err := fmt.Sscanf(parts[3], "%d", &ordinal); err != nil || ordinal <= 0 {
		return fmt.Errorf("invalid primitive ordinal %q", parts[3])
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(parts[0])), nil, 0)
	if err != nil {
		return err
	}
	matchedFunction := false
	var lastErr error
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != parts[1] || fn.Body == nil {
			continue
		}
		matchedFunction = true
		commandVars := commandVariables(fn.Body)
		fileVars := fileVariables(fn.Body)
		typedAttempt := hasRuntimeEffectsHandleParameter(fn)
		var beginPos, heartbeatPos, launchPos, primitivePos token.Pos
		primitiveCount := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if (isEffectsCall(call, "Begin") || isEffectsCall(call, "BeginCompletion")) && beginPos == token.NoPos {
				beginPos = call.Pos()
			}
			if (isLocalCall(call, "startCompletionAttemptHeartbeat") || isLocalCall(call, "requireCompletionAttemptHeartbeat") || isMethodCall(call, "Heartbeat")) && heartbeatPos == token.NoPos {
				heartbeatPos = call.Pos()
			}
			if isMethodCall(call, "MarkLaunched") && launchPos == token.NoPos {
				launchPos = call.Pos()
			}
			if directPrimitive(call, commandVars, fileVars) == parts[2] {
				primitiveCount++
				if primitiveCount == ordinal {
					primitivePos = call.Pos()
				}
			}
			return true
		})
		if (!typedAttempt && beginPos == token.NoPos) || launchPos == token.NoPos || primitivePos == token.NoPos {
			lastErr = fmt.Errorf("missing Begin or typed attempt/MarkLaunched/primitive binding")
			continue
		}
		if requiresCompletionHeartbeat && (heartbeatPos == token.NoPos || !(heartbeatPos < launchPos)) {
			lastErr = fmt.Errorf("completion primitive requires heartbeat binding before MarkLaunched")
			continue
		}
		if !(launchPos < primitivePos) || (!typedAttempt && !(beginPos < launchPos)) {
			lastErr = fmt.Errorf("required order (Begin or typed attempt) < MarkLaunched < primitive is not satisfied")
			continue
		}
		return nil
	}
	if matchedFunction {
		return lastErr
	}
	return fmt.Errorf("function %s not found", parts[1])
}

func hasRuntimeEffectsHandleParameter(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Type == nil || fn.Type.Params == nil {
		return false
	}
	for _, field := range fn.Type.Params.List {
		pointer, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		selector, ok := pointer.X.(*ast.SelectorExpr)
		if ok && selectorRoot(selector.X) == "runtimeeffects" && selector.Sel.Name == "Handle" {
			return true
		}
	}
	return false
}

func isEffectsCall(call *ast.CallExpr, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selectorRoot(selector.X) == "runtimeeffects" && selector.Sel.Name == name
}

func isMethodCall(call *ast.CallExpr, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == name
}

func isLocalCall(call *ast.CallExpr, name string) bool {
	identifier, ok := call.Fun.(*ast.Ident)
	return ok && identifier.Name == name
}

func collectDirectPrimitives(root string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	base := filepath.Join(root, "internal", "runtime")
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			counts := map[string]int{}
			commandVars := commandVariables(fn.Body)
			fileVars := fileVariables(fn.Body)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				primitive := directPrimitive(call, commandVars, fileVars)
				if primitive == "" {
					return true
				}
				counts[primitive]++
				key := fmt.Sprintf("%s:%s:%s:%d", filepath.ToSlash(rel), fn.Name.Name, primitive, counts[primitive])
				out[key] = struct{}{}
				return true
			})
		}
		return nil
	})
	return out, err
}

func commandVariables(body *ast.BlockStmt) map[string]struct{} {
	variables := map[string]struct{}{}
	ast.Inspect(body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for idx, rhs := range assign.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || !isExecCommandCall(call) || idx >= len(assign.Lhs) {
				continue
			}
			if ident, ok := assign.Lhs[idx].(*ast.Ident); ok {
				variables[ident.Name] = struct{}{}
			}
		}
		return true
	})
	return variables
}

func isExecCommandCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selectorRoot(selector.X) == "exec" && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext")
}

func fileVariables(body *ast.BlockStmt) map[string]struct{} {
	variables := map[string]struct{}{}
	ast.Inspect(body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, rhs := range assign.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || !isOSFileCreationCall(call) {
				continue
			}
			if len(assign.Lhs) > 0 {
				if ident, ok := assign.Lhs[0].(*ast.Ident); ok {
					variables[ident.Name] = struct{}{}
				}
			}
		}
		return true
	})
	return variables
}

func isOSFileCreationCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selectorRoot(selector.X) != "os" {
		return false
	}
	switch selector.Sel.Name {
	case "Create", "CreateTemp", "OpenFile":
		return true
	default:
		return false
	}
}

func directPrimitive(call *ast.CallExpr, commandVars, fileVars map[string]struct{}) string {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	root := selectorRoot(selector.X)
	switch {
	case selector.Sel.Name == "Do":
		return "http_do"
	case commandExecutionCall(selector, commandVars):
		return "process_launch"
	case root == "os" && osMutation(selector.Sel.Name):
		return "filesystem_write"
	case fileMutationCall(selector, fileVars):
		return "filesystem_write"
	case selector.Sel.Name == "Write" && strings.Contains(strings.ToLower(selectorPath(selector.X)), "stdin"):
		return "stdio_write"
	default:
		return ""
	}
}

func osMutation(name string) bool {
	switch name {
	case "WriteFile", "Create", "CreateTemp", "OpenFile", "Rename", "Mkdir", "MkdirAll", "Remove", "RemoveAll", "Chmod", "Chtimes", "Symlink", "Link", "Truncate":
		return true
	default:
		return false
	}
}

func fileMutationCall(selector *ast.SelectorExpr, fileVars map[string]struct{}) bool {
	switch selector.Sel.Name {
	case "Write", "WriteAt", "Sync", "Chmod", "Truncate":
	default:
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = fileVars[ident.Name]
	return ok
}

func commandExecutionCall(selector *ast.SelectorExpr, commandVars map[string]struct{}) bool {
	switch selector.Sel.Name {
	case "Start", "Run", "Output", "CombinedOutput":
	default:
		return false
	}
	if ident, ok := selector.X.(*ast.Ident); ok {
		_, tracked := commandVars[ident.Name]
		return tracked || ident.Name == "cmd"
	}
	call, ok := selector.X.(*ast.CallExpr)
	return ok && isExecCommandCall(call)
}

func selectorPath(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		prefix := selectorPath(value.X)
		if prefix == "" {
			return value.Sel.Name
		}
		return prefix + "." + value.Sel.Name
	case *ast.ParenExpr:
		return selectorPath(value.X)
	default:
		return ""
	}
}

func selectorRoot(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return selectorRoot(value.X)
	case *ast.ParenExpr:
		return selectorRoot(value.X)
	default:
		return ""
	}
}
