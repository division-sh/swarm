package worklifetime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

type asyncSiteClass string

const (
	asyncSiteCanonicalOwner  asyncSiteClass = "canonical_typed_owner"
	asyncSiteSynchronousJoin asyncSiteClass = "synchronously_joined_different_concept"
)

type asyncSiteLedgerEntry struct {
	count     int
	class     asyncSiteClass
	rationale string
}

// This inventory records review classifications, not executable ownership proof.
// Keys are launch kind, repository-relative file, and receiver/function; count
// preserves multiplicity without coupling identity to source formatting. Replacing
// a launch within a function can preserve the count: behavioral tests and review
// must still establish cancellation, joining, and exactly-once settlement.
var productionAsyncSiteLedger = map[string]asyncSiteLedgerEntry{
	"after_func|internal/apiv1/handler.go|webSocketSession.run":                                                     {1, asyncSiteCanonicalOwner, "session lease cancellation closes the socket and the owned subscription joins before release"},
	"after_func|internal/runtime/channelactivation/owner.go|Owner.AcquirePresentationContext":                       {1, asyncSiteSynchronousJoin, "presentation acquisition stops the cancellation wakeup bridge before returning with an exact publication lease or error"},
	"after_func|internal/runtime/channelactivation/owner.go|Owner.ReplaceContext":                                   {1, asyncSiteSynchronousJoin, "replacement stops the cancellation wakeup bridge before returning after the predecessor lease fence"},
	"after_func|internal/runtime/core/eventreceiver/execution.go|NewContext":                                        {1, asyncSiteCanonicalOwner, "the closed receiver context binds cancellation to its explicit lifetime input and cleanup releases the bridge"},
	"after_func|internal/runtime/core/worklifetime/worklifetime.go|gate.beginClass":                                 {1, asyncSiteCanonicalOwner, "the canonical gate binds lease cancellation to its typed occurrence"},
	"after_func|internal/runtime/manager/lifecycle_coordinator.go|agentLifecycleCoordinator.acquireExecutionLocked": {1, asyncSiteCanonicalOwner, "the exact agent generation cancels and joins the acquired execution lease"},
	"after_func|internal/runtime/manager/runtime.go|AgentManager.launchExecutionLoop":                               {1, asyncSiteCanonicalOwner, "the exact agent-generation cancellation is bridged into the pre-admitted Manager lease and joined with that loop"},
	"after_func|internal/runtime/runforkexecution/operation_lifetime.go|beginSelectedContractOperation":             {1, asyncSiteCanonicalOwner, "process lease cancellation reaches preparation; Finish stops or joins the cancellation bridge before settling the process lease"},
	"go|cmd/swarm-test/main.go|run":                                                                                   {2, asyncSiteSynchronousJoin, "the test harness joins process-tree signal forwarding after command completion; the test harness stops and joins its queue/child signal relay before return"},
	"go|internal/apiv1/handler.go|webSocketSession.run":                                                               {1, asyncSiteCanonicalOwner, "the process-owned websocket session settles after the writer goroutine exits"},
	"go|internal/apiv1/operator_conversation_fork.go|executeConversationForkChatWithHeartbeat":                        {1, asyncSiteCanonicalOwner, "the operation heartbeat has a typed lease and is canceled and joined by the enclosing call"},
	"go|internal/apiv1/subscriptions.go|ownedSubscriptionWork.Start":                                                  {1, asyncSiteCanonicalOwner, "owned subscription work settles its process lease after the polling loop exits"},
	"go|internal/cliapp/events.go|subscribeEvents":                                                                    {1, asyncSiteSynchronousJoin, "the CLI subscription closes and joins its read loop"},
	"go|internal/cliapp/logs.go|subscribeRuntimeLogs":                                                                 {1, asyncSiteSynchronousJoin, "the CLI log subscription closes and joins its read loop"},
	"go|internal/cliapp/run_command.go|closeRunTraceWebSocketOnContext":                                               {1, asyncSiteSynchronousJoin, "the trace cancellation closer is stopped and joined when subscription acquisition returns"},
	"go|internal/cliapp/run_command.go|startForegroundRunTraceObserver":                                               {1, asyncSiteSynchronousJoin, "the foreground trace observer is cancelled and joined before command teardown"},
	"go|internal/cliapp/run_command.go|startForegroundRunTraceRenderer":                                               {2, asyncSiteSynchronousJoin, "the run-start rendering controller joins its bounded stdout trace worker before teardown; the run-start rendering controller joins its reserved stderr detach-fact worker before teardown"},
	"go|internal/cliapp/run_command.go|startLocalRunServe":                                                            {1, asyncSiteSynchronousJoin, "the local serve command reports terminal completion through a joined result channel"},
	"go|internal/cliapp/run_command.go|subscribeRunTrace":                                                             {1, asyncSiteSynchronousJoin, "the CLI run trace subscription closes and joins its read loop"},
	"go|internal/runtime/bus/eventbus_publish.go|EventBus.DispatchPreparedPublishAsync":                               {1, asyncSiteCanonicalOwner, "prepared async dispatch acquires its occurrence lease before launch and settles exactly once"},
	"go|internal/runtime/bus/sweeper.go|EventBus.StartOutboxSweeper":                                                  {1, asyncSiteCanonicalOwner, "the outbox sweeper owns a runtime lease and exposes an exact completion channel"},
	"go|internal/runtime/deliverycontinuation/coordinator.go|Coordinator.Start":                                       {1, asyncSiteCanonicalOwner, "the normal-generation coordinator acquires standing ownership before launch and joins its exact loop"},
	"go|internal/runtime/deliverylifecycle/heartbeat.go|startClaimHeartbeat":                                          {1, asyncSiteCanonicalOwner, "the delivery claim heartbeat acquires its runtime occurrence before launch and exposes an exact stop-and-wait owner"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.startRecovery":                                            {1, asyncSiteSynchronousJoin, "generic schedule recovery is canceled by lifecycle shutdown and joined by Lifecycle.Stop"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.startTerminalRetirementRecovery":                          {1, asyncSiteSynchronousJoin, "exact terminal wakeup retirement recovery is canceled by lifecycle shutdown and joined by Lifecycle.Stop"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.Stop":                                                     {1, asyncSiteSynchronousJoin, "the lifecycle stop wait adapter is joined before scheduler and claim shutdown"},
	"go|internal/runtime/llm/cli_runtime_process.go|ClaudeCLIRuntime.runStreamingPrepared":                            {2, asyncSiteSynchronousJoin, "stdout collection is joined before the subprocess call returns; stderr collection is joined before the subprocess call returns"},
	"go|internal/runtime/llm/cli_runtime_startup_probe.go|ClaudeCLIRuntime.runUntilCLIStartupInit":                    {2, asyncSiteSynchronousJoin, "startup stdout collection is joined before probe completion; startup stderr collection is joined before probe completion"},
	"go|internal/runtime/llm/completion_authority.go|startCompletionAttemptHeartbeatWithTiming":                       {1, asyncSiteCanonicalOwner, "the completion heartbeat owns an explicit stop-and-wait handle"},
	"go|internal/runtime/llm/session_watchdog.go|newSessionWatchdogMonitorWriter":                                     {1, asyncSiteSynchronousJoin, "the watchdog writer is closed and joined by its owning monitor"},
	"go|internal/runtime/manager/runtime.go|AgentManager.executePreparedDirectiveOperation":                           {1, asyncSiteCanonicalOwner, "the directive heartbeat owns a lease and is canceled and joined by the directive operation"},
	"go|internal/runtime/manager/runtime.go|AgentManager.launchExecutionLoop":                                         {1, asyncSiteCanonicalOwner, "the pre-admitted Manager generation and exact agent generation jointly own and joins the execution loop"},
	"go|internal/runtime/manager/runtime.go|AgentManager.resetRuntimeState":                                           {1, asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor joins the fenced Manager generation before reset cleanup"},
	"go|internal/runtime/manager/runtime.go|AgentManager.Run":                                                         {1, asyncSiteCanonicalOwner, "the retry loop acquires the Manager run occurrence before launch and publishes settlement"},
	"go|internal/runtime/manager/runtime.go|AgentManager.ShutdownWithOptions":                                         {1, asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor joins the fenced Manager generation and settles before shutdown returns"},
	"go|internal/runtime/manager/runtime.go|AgentManager.startShutdownWatcher":                                        {1, asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor completes the retained transition after all accepted Manager work settles"},
	"go|internal/runtime/manager/terminal_retirement.go|AgentManager.launchTerminalFlowCompletion":                    {1, asyncSiteCanonicalOwner, "the typed terminal completion retains a pre-admitted finite Manager lease with the exact standing/fork companion; it joins only predecessor routes/loops and is itself joined by the reserved shutdown executor"},
	"go|internal/runtime/manager/flow_runtime_readiness.go|AgentManager.reconcileDeclaredDynamicFlowRuntimeReadiness": {1, asyncSiteCanonicalOwner, "the exact keyed readiness attempt owns a pre-admitted Manager lease; retirement releases dependent callers before joining predecessor execution, and shutdown joins the attempt"},
	"go|internal/runtime/mcp/client.go|newStdioRPCClient":                                                             {1, asyncSiteSynchronousJoin, "the stdio client read loop is canceled and joined by client close"},
	"go|internal/runtime/mcp/client.go|stdioRPCClient.Call":                                                           {1, asyncSiteSynchronousJoin, "request cancellation notification is bounded by the call completion context"},
	"go|internal/runtime/pipeline/scheduler.go|Scheduler.startTask":                                                   {1, asyncSiteCanonicalOwner, "the scheduler task owns one inert wakeup occurrence plus exact parkable projection, linearized fire state, and done channel"},
	"go|internal/runtime/pipeline/workflow_timer_owner.go|WorkflowTimerLifecycle.startRecovery":                       {1, asyncSiteCanonicalOwner, "timer recovery acquires its runtime occurrence before launch and settles on return"},
	"go|internal/runtime/publicingress/exposure.go|Controller.Start":                                                  {2, asyncSiteSynchronousJoin, "the exposure controller shuts down the ingress-only HTTP server before Stop returns; the exposure controller cancels and joins its supervisor through the exact done channel"},
	"go|internal/runtime/publicingress/exposure.go|execManagedLauncher.Launch":                                        {1, asyncSiteSynchronousJoin, "the managed process wait loop closes the exact process completion channel consumed by controller shutdown"},
	"go|internal/runtime/pythonmodule/runtime.go|newInterpreterModuleForContext":                                      {1, asyncSiteSynchronousJoin, "the interpreter stderr collector is joined when module startup completes"},
	"go|internal/runtime/pythonmodule/runtime.go|runHarness":                                                          {1, asyncSiteSynchronousJoin, "the harness execution result is joined before the call returns"},
	"go|internal/runtime/runforkexecution/agent_runtime_materialization.go|serveSelectedContractGateway":              {1, asyncSiteCanonicalOwner, "the gateway receives an admitted process-preparation or execution-occurrence lease; Close joins handlers and Serve before settling it"},
	"go|internal/runtime/runforkexecution/runtime_container.go|selectedContractForkLocalRuntimeContainer.Publish":     {1, asyncSiteCanonicalOwner, "selected-fork publication acquires a local occurrence lease before launch and joins it"},
	"go|internal/runtime/runlifecycle/executor.go|Executor.installReserved":                                           {1, asyncSiteCanonicalOwner, "the run-lifecycle executor owns each pre-admitted completion candidate until the exact lease settles"},
	"go|internal/runtime/runtime.go|Runtime.releaseAutonomousStartupProducers":                                        {1, asyncSiteCanonicalOwner, "pipeline background nodes acquire runtime leases after completed topology and settle on exit"},
	"go|internal/runtime/runtime.go|Runtime.startSystemNodesAndWaitForSubscriptions":                                  {1, asyncSiteCanonicalOwner, "system-node runners acquire standing/runtime ownership and publish readiness before return"},
	"go|internal/runtime/sessions/heartbeat.go|StartLeaseHeartbeatWithErrorHandler":                                   {1, asyncSiteCanonicalOwner, "the session heartbeat exposes a stop-and-wait owner and cannot outlive it"},
	"go|internal/runtime/startupownership/process_capability.go|processCapability.startPossessionMonitor":             {1, asyncSiteCanonicalOwner, "the retained process capability owns and joins its exact selected-store possession monitor"},
	"go|internal/serveapp/main.go|Run":                                                                                {2, asyncSiteCanonicalOwner, "the API listener is admitted under served-process ownership and joined at shutdown; the MCP listener is admitted under served-process ownership and joined at shutdown"},
	"go|internal/serveapp/main.go|startServeOwnershipWatch":                                                           {1, asyncSiteCanonicalOwner, "the served-process root joins the exact process-capability terminal watcher before store release"},
	"go|internal/serveapp/public_ingress.go|startServePublicIngressRenewal":                                           {1, asyncSiteCanonicalOwner, "the public-ingress renewal loop owns one served-process lease and settles it on cancellation"},
	"go|internal/serveapp/run_stalled_monitor.go|startServeRunStalledEscalation":                                      {1, asyncSiteCanonicalOwner, "the stalled-run monitor has a process lease and exact stop-and-wait completion"},
	"go|internal/serveapp/serve_author_activity.go|newServeAuthorActivityFollower":                                    {1, asyncSiteCanonicalOwner, "the author-activity follower has a process lease and exact close/join path"},
}

var retiredLifetimeOwners = []string{
	"inFlightPublishes",
	"inFlightEventIDs",
	"runtimeQuiescenceStableChecks",
	"PendingAgentRouteDeliveries",
	"PendingAgentDeliveries",
	"dispatchCommittedPublishAsync",
}

func TestProductionAsyncSitesAreClassified(t *testing.T) {
	found := map[string][]int{}
	walkProductionWorkLifetimeFiles(t, func(path, relative string) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", relative, err)
		}
		collectAsyncSites(fset, file, relative, found)
	})
	if err := compareAsyncSiteLedger(found, productionAsyncSiteLedger); err != nil {
		t.Fatal(err)
	}
}

func TestProductionWorkLifetimeBoundaries(t *testing.T) {
	walkProductionWorkLifetimeFiles(t, func(path, relative string) {
		checkProductionWorkLifetimeFile(t, path, relative)
	})
}

// Preserve the existing scan scope. This is not a whole-program async analysis:
// store and other unlisted directories, test files, package-level initializers,
// indirect callbacks, and timers are outside the async-site inventory.
func walkProductionWorkLifetimeFiles(t *testing.T, visit func(path, relative string)) {
	t.Helper()
	repoRoot := workLifetimeRepositoryRoot(t)
	for _, rootName := range []string{"cmd", "internal/runtime", "internal/serveapp", "internal/apiv1", "internal/cliapp"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			visit(path, relative)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
}

func TestAsyncSiteInventoryRejectsUnclassifiedGoroutine(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", `package fixture
func launch() { go func() {}() }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]int{}
	collectAsyncSites(fset, file, "fixture.go", found)
	if err := compareAsyncSiteLedger(found, nil); err == nil || !strings.Contains(err.Error(), "unclassified async sites") {
		t.Fatalf("unclassified goroutine inventory error = %v, want unclassified async site", err)
	}
}

func checkProductionWorkLifetimeFile(t *testing.T, path, relative string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", relative, err)
	}
	eventAliases := importAliases(file, "github.com/division-sh/swarm/internal/events")
	workAliases := importAliases(file, "github.com/division-sh/swarm/internal/runtime/core/worklifetime")

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", relative, err)
	}
	for _, retired := range retiredLifetimeOwners {
		if strings.Contains(string(raw), retired) {
			t.Fatalf("%s retains retired process-local lifetime owner %q", relative, retired)
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.BlockStmt:
			for index := 0; index+1 < len(value.List); index++ {
				settle, settleOK := value.List[index].(*ast.DeferStmt)
				signal, signalOK := value.List[index+1].(*ast.DeferStmt)
				if settleOK && signalOK && deferredCallContainsDone(settle) && deferredCallIsClose(signal) {
					t.Fatalf("%s:%d defers work settlement before completion signaling; defer execution would signal first", relative, fset.Position(settle.Pos()).Line)
				}
			}
		case *ast.ChanType:
			if isImportedType(value.Value, eventAliases, "Event") {
				t.Fatalf("%s:%d uses raw events.Event as an asynchronous carrier", relative, fset.Position(value.Pos()).Line)
			}
		case *ast.CallExpr:
			selector, ok := value.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewProcess" || !isPackageIdent(selector.X, workAliases) {
				return true
			}
			if relative != "internal/serveapp/main.go" {
				t.Fatalf("%s:%d creates a private process work owner outside the serve root", relative, fset.Position(value.Pos()).Line)
			}
		}
		return true
	})
}

func collectAsyncSites(fset *token.FileSet, file *ast.File, relative string, found map[string][]int) {
	contextAliases := importAliases(file, "context")
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		functionName := declaredFunctionName(function)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.GoStmt:
				key := asyncSiteKey("go", relative, functionName)
				found[key] = append(found[key], fset.Position(value.Go).Line)
			case *ast.CallExpr:
				name, packageName := calledFunctionName(value.Fun)
				line := fset.Position(value.Pos()).Line
				switch {
				case name == "AfterFunc" && packageName != "" && containsAlias(contextAliases, packageName):
					key := asyncSiteKey("after_func", relative, functionName)
					found[key] = append(found[key], line)
				case isOwnerActionRegistration(name):
					key := asyncSiteKey("owner_action", relative, functionName)
					found[key] = append(found[key], line)
				}
			}
			return true
		})
	}
}

func asyncSiteKey(kind, relative, function string) string {
	return fmt.Sprintf("%s|%s|%s", kind, relative, function)
}

func declaredFunctionName(function *ast.FuncDecl) string {
	name := function.Name.Name
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return name
	}
	receiver := function.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	switch generic := receiver.(type) {
	case *ast.IndexExpr:
		receiver = generic.X
	case *ast.IndexListExpr:
		receiver = generic.X
	}
	if identifier, ok := receiver.(*ast.Ident); ok {
		return identifier.Name + "." + name
	}
	return name
}

func calledFunctionName(expression ast.Expr) (string, string) {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name, ""
	case *ast.SelectorExpr:
		identifier, _ := value.X.(*ast.Ident)
		if identifier == nil {
			return value.Sel.Name, ""
		}
		return value.Sel.Name, identifier.Name
	default:
		return "", ""
	}
}

func containsAlias(aliases map[string]struct{}, alias string) bool {
	_, ok := aliases[alias]
	return ok
}

func isOwnerActionRegistration(name string) bool {
	switch name {
	case "QueuePipelinePostCommitAction", "queuePipelinePostCommitAction",
		"QueuePipelineRollbackAction", "queuePipelineRollbackAction":
		return true
	default:
		return false
	}
}

func compareAsyncSiteLedger(found map[string][]int, ledger map[string]asyncSiteLedgerEntry) error {
	unclassified := make([]string, 0)
	missing := make([]string, 0)
	invalid := make([]string, 0)
	counts := make([]string, 0)
	for key, lines := range found {
		entry, ok := ledger[key]
		if !ok {
			unclassified = append(unclassified, fmt.Sprintf("%s: found %d (lines %v)", key, len(lines), lines))
			continue
		}
		if len(lines) != entry.count {
			counts = append(counts, fmt.Sprintf("%s: expected %d, found %d (lines %v)", key, entry.count, len(lines), lines))
		}
	}
	for key, entry := range ledger {
		if entry.count <= 0 || (entry.class != asyncSiteCanonicalOwner && entry.class != asyncSiteSynchronousJoin) || strings.TrimSpace(entry.rationale) == "" {
			invalid = append(invalid, key)
		}
		if _, ok := found[key]; !ok {
			missing = append(missing, fmt.Sprintf("%s: expected %d, found 0", key, entry.count))
		}
	}
	sort.Strings(unclassified)
	sort.Strings(missing)
	sort.Strings(invalid)
	sort.Strings(counts)
	if len(unclassified) == 0 && len(missing) == 0 && len(invalid) == 0 && len(counts) == 0 {
		return nil
	}
	return fmt.Errorf("async-site inventory mismatch\nunclassified async sites:\n%s\nmissing ledger sites:\n%s\nchanged launch counts:\n%s\ninvalid ledger entries (require positive count, known class, and rationale):\n%s",
		strings.Join(unclassified, "\n"), strings.Join(missing, "\n"), strings.Join(counts, "\n"), strings.Join(invalid, "\n"))
}

func deferredCallContainsDone(statement *ast.DeferStmt) bool {
	found := false
	ast.Inspect(statement.Call, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Done" {
			found = true
			return false
		}
		return true
	})
	return found
}

func deferredCallIsClose(statement *ast.DeferStmt) bool {
	identifier, ok := statement.Call.Fun.(*ast.Ident)
	return ok && identifier.Name == "close"
}

func importAliases(file *ast.File, importPath string) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, imported := range file.Imports {
		if strings.Trim(imported.Path.Value, `"`) != importPath {
			continue
		}
		name := filepath.Base(importPath)
		if imported.Name != nil {
			name = imported.Name.Name
		}
		if name != "_" && name != "." {
			aliases[name] = struct{}{}
		}
	}
	return aliases
}

func isImportedType(expr ast.Expr, aliases map[string]struct{}, name string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == name && isPackageIdent(selector.X, aliases)
}

func isPackageIdent(expr ast.Expr, aliases map[string]struct{}) bool {
	identifier, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = aliases[identifier.Name]
	return ok
}

func workLifetimeRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve work lifetime source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
