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
	class asyncSiteClass
	proof string
}

var productionAsyncSiteLedger = map[string]asyncSiteLedgerEntry{
	"after_func|internal/apiv1/handler.go|webSocketSession.run|445":                                                      {asyncSiteCanonicalOwner, "session lease cancellation closes the socket and the owned subscription joins before release"},
	"after_func|internal/runtime/core/worklifetime/worklifetime.go|gate.beginClass|106":                                  {asyncSiteCanonicalOwner, "the canonical gate binds lease cancellation to its typed occurrence"},
	"after_func|internal/runtime/core/eventreceiver/execution.go|NewContext|221":                                         {asyncSiteCanonicalOwner, "the closed receiver context binds cancellation to its explicit lifetime input and cleanup releases the bridge"},
	"after_func|internal/runtime/manager/lifecycle_coordinator.go|agentLifecycleCoordinator.acquireExecutionLocked|1334": {asyncSiteCanonicalOwner, "the exact agent generation cancels and joins the acquired execution lease"},
	"after_func|internal/runtime/manager/runtime.go|AgentManager.launchExecutionLoop|1742":                               {asyncSiteCanonicalOwner, "the exact agent-generation cancellation is bridged into the pre-admitted Manager lease and joined with that loop"},
	"go|cmd/swarm-test/main.go|run|219":                                                                                  {asyncSiteSynchronousJoin, "the test harness joins process-tree signal forwarding after command completion"},
	"go|cmd/swarm-test/main.go|run|76":                                                                                   {asyncSiteSynchronousJoin, "the test harness stops and joins its queue/child signal relay before return"},
	"go|internal/apiv1/handler.go|webSocketSession.run|454":                                                              {asyncSiteCanonicalOwner, "the process-owned websocket session settles after the writer goroutine exits"},
	"go|internal/apiv1/operator_conversation_fork.go|executeConversationForkChatWithHeartbeat|276":                       {asyncSiteCanonicalOwner, "the operation heartbeat has a typed lease and is canceled and joined by the enclosing call"},
	"go|internal/apiv1/subscriptions.go|ownedSubscriptionWork.Start|99":                                                  {asyncSiteCanonicalOwner, "owned subscription work settles its process lease after the polling loop exits"},
	"go|internal/builder/handler_ws.go|wsClient.handleSubscribe|95":                                                      {asyncSiteSynchronousJoin, "the websocket client owns and joins its subscription pump on close"},
	"go|internal/builder/runs_control.go|runHub.startRun|123":                                                            {asyncSiteCanonicalOwner, "run execution is admitted under the served-process occurrence and settles before teardown"},
	"go|internal/cliapp/events.go|subscribeEvents|1036":                                                                  {asyncSiteSynchronousJoin, "the CLI subscription closes and joins its read loop"},
	"go|internal/cliapp/logs.go|subscribeRuntimeLogs|386":                                                                {asyncSiteSynchronousJoin, "the CLI log subscription closes and joins its read loop"},
	"go|internal/cliapp/run_command.go|closeRunTraceWebSocketOnContext|1113":                                             {asyncSiteSynchronousJoin, "the trace cancellation closer is stopped and joined when subscription acquisition returns"},
	"go|internal/cliapp/run_command.go|startForegroundRunTraceObserver|832":                                              {asyncSiteSynchronousJoin, "the foreground trace observer is cancelled and joined before command teardown"},
	"go|internal/cliapp/run_command.go|startForegroundRunTraceRenderer|790":                                              {asyncSiteSynchronousJoin, "the run-start rendering controller joins its bounded stdout trace worker before teardown"},
	"go|internal/cliapp/run_command.go|startForegroundRunTraceRenderer|797":                                              {asyncSiteSynchronousJoin, "the run-start rendering controller joins its reserved stderr detach-fact worker before teardown"},
	"go|internal/cliapp/run_command.go|startLocalRunServe|511":                                                           {asyncSiteSynchronousJoin, "the local serve command reports terminal completion through a joined result channel"},
	"go|internal/cliapp/run_command.go|subscribeRunTrace|1035":                                                           {asyncSiteSynchronousJoin, "the CLI run trace subscription closes and joins its read loop"},
	"go|internal/runtime/bus/eventbus_publish.go|EventBus.DispatchPreparedPublishAsync|1228":                             {asyncSiteCanonicalOwner, "prepared async dispatch acquires its occurrence lease before launch and settles exactly once"},
	"go|internal/runtime/deliverycontinuation/coordinator.go|Coordinator.Start|133":                                      {asyncSiteCanonicalOwner, "the normal-generation coordinator acquires standing ownership before launch and joins its exact loop"},
	"go|internal/runtime/bus/sweeper.go|EventBus.StartOutboxSweeper|88":                                                  {asyncSiteCanonicalOwner, "the outbox sweeper owns a runtime lease and exposes an exact completion channel"},
	"go|internal/runtime/deliverylifecycle/heartbeat.go|startClaimHeartbeat|115":                                         {asyncSiteCanonicalOwner, "the delivery claim heartbeat acquires its runtime occurrence before launch and exposes an exact stop-and-wait owner"},
	"go|internal/runtime/llm/cli_runtime_process.go|ClaudeCLIRuntime.runStreamingPrepared|155":                           {asyncSiteSynchronousJoin, "stdout collection is joined before the subprocess call returns"},
	"go|internal/runtime/llm/cli_runtime_process.go|ClaudeCLIRuntime.runStreamingPrepared|156":                           {asyncSiteSynchronousJoin, "stderr collection is joined before the subprocess call returns"},
	"go|internal/runtime/llm/cli_runtime_startup_probe.go|ClaudeCLIRuntime.runUntilCLIStartupInit|161":                   {asyncSiteSynchronousJoin, "startup stdout collection is joined before probe completion"},
	"go|internal/runtime/llm/cli_runtime_startup_probe.go|ClaudeCLIRuntime.runUntilCLIStartupInit|162":                   {asyncSiteSynchronousJoin, "startup stderr collection is joined before probe completion"},
	"go|internal/runtime/llm/completion_authority.go|startCompletionAttemptHeartbeatWithTiming|326":                      {asyncSiteCanonicalOwner, "the completion heartbeat owns an explicit stop-and-wait handle"},
	"go|internal/runtime/llm/session_watchdog.go|newSessionWatchdogMonitorWriter|97":                                     {asyncSiteSynchronousJoin, "the watchdog writer is closed and joined by its owning monitor"},
	"go|internal/runtime/manager/runtime.go|AgentManager.Run|869":                                                        {asyncSiteCanonicalOwner, "the retry loop acquires the Manager run occurrence before launch and publishes settlement"},
	"go|internal/runtime/manager/runtime.go|AgentManager.ShutdownWithOptions|151":                                        {asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor joins the fenced Manager generation and settles before shutdown returns"},
	"go|internal/runtime/manager/runtime.go|AgentManager.executePreparedDirectiveOperation|655":                          {asyncSiteCanonicalOwner, "the directive heartbeat owns a lease and is canceled and joined by the directive operation"},
	"go|internal/runtime/manager/runtime.go|AgentManager.launchExecutionLoop|1740":                                       {asyncSiteCanonicalOwner, "the pre-admitted Manager generation and exact agent generation jointly own and join the execution loop"},
	"go|internal/runtime/manager/runtime.go|AgentManager.resetRuntimeState|1318":                                         {asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor joins the fenced Manager generation before reset cleanup"},
	"go|internal/runtime/manager/runtime.go|AgentManager.startShutdownWatcher|920":                                       {asyncSiteCanonicalOwner, "the pre-reserved outer-runtime executor completes the retained transition after all accepted Manager work settles"},
	"go|internal/runtime/mcp/client.go|newStdioRPCClient|437":                                                            {asyncSiteSynchronousJoin, "the stdio client read loop is canceled and joined by client close"},
	"go|internal/runtime/mcp/client.go|stdioRPCClient.Call|497":                                                          {asyncSiteSynchronousJoin, "request cancellation notification is bounded by the call completion context"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.startRecovery|481":                                           {asyncSiteSynchronousJoin, "generic schedule recovery is canceled by lifecycle shutdown and joined by Lifecycle.Stop"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.startTerminalRetirementRecovery|535":                         {asyncSiteSynchronousJoin, "exact terminal wakeup retirement recovery is canceled by lifecycle shutdown and joined by Lifecycle.Stop"},
	"go|internal/runtime/genericschedule/owner.go|Lifecycle.Stop|572":                                                    {asyncSiteSynchronousJoin, "the lifecycle stop wait adapter is joined before scheduler and claim shutdown"},
	"go|internal/runtime/pipeline/scheduler.go|Scheduler.startTask|964":                                                  {asyncSiteCanonicalOwner, "the scheduler task owns one inert wakeup occurrence plus exact parkable projection, linearized fire state, and done channel"},
	"go|internal/runtime/pipeline/workflow_timer_owner.go|WorkflowTimerLifecycle.startRecovery|1138":                     {asyncSiteCanonicalOwner, "timer recovery acquires its runtime occurrence before launch and settles on return"},
	"go|internal/runtime/pythonmodule/runtime.go|newInterpreterModuleForContext|305":                                     {asyncSiteSynchronousJoin, "the interpreter stderr collector is joined when module startup completes"},
	"go|internal/runtime/pythonmodule/runtime.go|runHarness|209":                                                         {asyncSiteSynchronousJoin, "the harness execution result is joined before the call returns"},
	"go|internal/runtime/publicingress/exposure.go|Controller.Start|172":                                                 {asyncSiteSynchronousJoin, "the exposure controller shuts down the ingress-only HTTP server before Stop returns"},
	"go|internal/runtime/publicingress/exposure.go|Controller.Start|180":                                                 {asyncSiteSynchronousJoin, "the exposure controller cancels and joins its supervisor through the exact done channel"},
	"go|internal/runtime/publicingress/exposure.go|execManagedLauncher.Launch|549":                                       {asyncSiteSynchronousJoin, "the managed process wait loop closes the exact process completion channel consumed by controller shutdown"},
	"go|internal/runtime/runlifecycle/executor.go|Executor.installReserved|258":                                          {asyncSiteCanonicalOwner, "the run-lifecycle executor owns each pre-admitted completion candidate until the exact lease settles"},
	"go|internal/runtime/runforkexecution/agent_runtime_materialization.go|startSelectedContractAgentRuntimeGateway|553": {asyncSiteCanonicalOwner, "the selected-fork gateway is admitted under and joined by its selected-fork occurrence"},
	"go|internal/runtime/runforkexecution/runtime_container.go|selectedContractForkLocalRuntimeContainer.Publish|347":    {asyncSiteCanonicalOwner, "selected-fork publication acquires a local occurrence lease before launch and joins it"},
	"go|internal/runtime/runtime.go|Runtime.Start|1529":                                                                  {asyncSiteCanonicalOwner, "pipeline background nodes acquire runtime leases before launch and settle on exit"},
	"go|internal/runtime/runtime.go|Runtime.startSystemNodesAndWaitForSubscriptions|1874":                                {asyncSiteCanonicalOwner, "system-node runners acquire standing/runtime ownership and publish readiness before return"},
	"go|internal/runtime/startupownership/process_capability.go|processCapability.startPossessionMonitor|576":            {asyncSiteCanonicalOwner, "the retained process capability owns and joins its exact selected-store possession monitor"},
	"go|internal/runtime/sessions/heartbeat.go|StartLeaseHeartbeatWithErrorHandler|51":                                   {asyncSiteCanonicalOwner, "the session heartbeat exposes a stop-and-wait owner and cannot outlive it"},
	"go|internal/serveapp/main.go|Run|1544":                                                                              {asyncSiteCanonicalOwner, "the API listener is admitted under served-process ownership and joined at shutdown"},
	"go|internal/serveapp/main.go|Run|1548":                                                                              {asyncSiteCanonicalOwner, "the MCP listener is admitted under served-process ownership and joined at shutdown"},
	"go|internal/serveapp/main.go|startServeOwnershipWatch|1745":                                                         {asyncSiteCanonicalOwner, "the served-process root joins the exact process-capability terminal watcher before store release"},
	"go|internal/serveapp/public_ingress.go|startServePublicIngressRenewal|114":                                          {asyncSiteCanonicalOwner, "the public-ingress renewal loop owns one served-process lease and settles it on cancellation"},
	"go|internal/serveapp/run_stalled_monitor.go|startServeRunStalledEscalation|45":                                      {asyncSiteCanonicalOwner, "the stalled-run monitor has a process lease and exact stop-and-wait completion"},
	"go|internal/serveapp/serve_author_activity.go|newServeAuthorActivityFollower|56":                                    {asyncSiteCanonicalOwner, "the author-activity follower has a process lease and exact close/join path"},
}

var retiredLifetimeOwners = []string{
	"inFlightPublishes",
	"inFlightEventIDs",
	"runtimeQuiescenceStableChecks",
	"PendingAgentRouteDeliveries",
	"PendingAgentDeliveries",
	"dispatchCommittedPublishAsync",
}

func TestProductionAsyncWorkUsesCanonicalTypedOwners(t *testing.T) {
	repoRoot := workLifetimeRepositoryRoot(t)
	found := map[string]struct{}{}
	for _, rootName := range []string{"cmd", "internal/runtime", "internal/serveapp", "internal/apiv1", "internal/builder", "internal/cliapp"} {
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
			checkProductionWorkLifetimeFile(t, path, relative, found)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
	if err := compareAsyncSiteLedger(found, productionAsyncSiteLedger); err != nil {
		t.Fatal(err)
	}
}

func TestProductionAsyncWorkLedgerRejectsUnownedGoroutine(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", `package fixture
func launch() { go func() {}() }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]struct{}{}
	collectAsyncSites(fset, file, "fixture.go", found)
	if err := compareAsyncSiteLedger(found, nil); err == nil || !strings.Contains(err.Error(), "unclassified async sites") {
		t.Fatalf("unowned goroutine ledger error = %v, want unclassified async site", err)
	}
}

func checkProductionWorkLifetimeFile(t *testing.T, path, relative string, found map[string]struct{}) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", relative, err)
	}
	eventAliases := importAliases(file, "github.com/division-sh/swarm/internal/events")
	workAliases := importAliases(file, "github.com/division-sh/swarm/internal/runtime/core/worklifetime")
	collectAsyncSites(fset, file, relative, found)

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

func collectAsyncSites(fset *token.FileSet, file *ast.File, relative string, found map[string]struct{}) {
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
				found[asyncSiteKey("go", relative, functionName, fset.Position(value.Go).Line)] = struct{}{}
			case *ast.CallExpr:
				name, packageName := calledFunctionName(value.Fun)
				line := fset.Position(value.Pos()).Line
				switch {
				case name == "AfterFunc" && packageName != "" && containsAlias(contextAliases, packageName):
					found[asyncSiteKey("after_func", relative, functionName, line)] = struct{}{}
				case isOwnerActionRegistration(name):
					found[asyncSiteKey("owner_action", relative, functionName, line)] = struct{}{}
				}
			}
			return true
		})
	}
}

func asyncSiteKey(kind, relative, function string, line int) string {
	return fmt.Sprintf("%s|%s|%s|%d", kind, relative, function, line)
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

func compareAsyncSiteLedger(found map[string]struct{}, ledger map[string]asyncSiteLedgerEntry) error {
	unclassified := make([]string, 0)
	missing := make([]string, 0)
	invalid := make([]string, 0)
	for key := range found {
		entry, ok := ledger[key]
		if !ok {
			unclassified = append(unclassified, key)
			continue
		}
		if (entry.class != asyncSiteCanonicalOwner && entry.class != asyncSiteSynchronousJoin) || strings.TrimSpace(entry.proof) == "" {
			invalid = append(invalid, key)
		}
	}
	for key := range ledger {
		if _, ok := found[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(missing)
	sort.Strings(invalid)
	if len(unclassified) == 0 && len(missing) == 0 && len(invalid) == 0 {
		return nil
	}
	return fmt.Errorf("async ownership ledger mismatch\nunclassified async sites:\n%s\nmissing ledger sites:\n%s\ninvalid ledger entries:\n%s",
		strings.Join(unclassified, "\n"), strings.Join(missing, "\n"), strings.Join(invalid, "\n"))
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
