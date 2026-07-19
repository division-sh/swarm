package store

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

type runtimeWriterPrimitiveKind string

const (
	primitiveBegin    runtimeWriterPrimitiveKind = "transaction-open"
	primitiveWrite    runtimeWriterPrimitiveKind = "sql-write"
	primitiveRead     runtimeWriterPrimitiveKind = "sql-read"
	primitiveBoundary runtimeWriterPrimitiveKind = "canonical-boundary"
)

type runtimeWriterClassification string

const (
	classConsumesCanonical runtimeWriterClassification = "already consumes canonical boundary"
	classActiveTxHelper    runtimeWriterClassification = "valid active-transaction helper under canonical boundary"
	classDifferentConcept  runtimeWriterClassification = "different semantic concept"
	classSplitLegacy       runtimeWriterClassification = "split / old non-authoritative path"
)

type runtimeWriterCallSite struct {
	Path                          string
	Line                          int
	Function                      string
	Receiver                      string
	ReceiverVariable              string
	Primitive                     string
	Kind                          runtimeWriterPrimitiveKind
	CallReceiver                  string
	CallsRuntimeMutation          bool
	CallsEventTransaction         bool
	CallsPipelineTransaction      bool
	UsesPipelineTxFromContext     bool
	InRuntimeMutationCallback     bool
	InEventTransactionCallback    bool
	InPipelineTransactionCallback bool
	UsesBoundaryCallbackTx        bool
	UsesFunctionTxParam           bool
	UsesReceiverDBArgument        bool
	FunctionContainsWrite         bool
	FunctionContainsBegin         bool
	FunctionContainsRead          bool
	FunctionContainsBoundary      bool
	FunctionSourceDescription     string
}

type runtimeWriterAuditRow struct {
	Site           runtimeWriterCallSite
	Classification runtimeWriterClassification
	Reason         string
}

type runtimeWriterRule struct {
	name           string
	path           *regexp.Regexp
	function       *regexp.Regexp
	receiver       *regexp.Regexp
	kinds          map[runtimeWriterPrimitiveKind]bool
	classification runtimeWriterClassification
	reason         string
}

type runtimeWriterBoundaryCallbackScope struct {
	start       token.Pos
	end         token.Pos
	runtime     bool
	event       bool
	pipeline    bool
	txParamName map[string]bool
}

func TestSelectedSQLiteRuntimeWriterBoundaryAudit(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	sites, err := collectRuntimeWriterCallSites(root)
	if err != nil {
		t.Fatalf("collect runtime writer call sites: %v", err)
	}
	if len(sites) == 0 {
		t.Fatal("expected production SQL/transaction call sites")
	}

	rows, failures := classifyRuntimeWriterCallSites(sites)
	if len(failures) > 0 {
		t.Fatalf("unclassified or invalid selected-runtime writer seams:\n%s", strings.Join(failures, "\n"))
	}

	byClass := map[runtimeWriterClassification]int{}
	for _, row := range rows {
		byClass[row.Classification]++
	}
	for _, required := range []runtimeWriterClassification{
		classConsumesCanonical,
		classActiveTxHelper,
		classDifferentConcept,
	} {
		if byClass[required] == 0 {
			t.Fatalf("runtime writer audit did not classify any seam as %q", required)
		}
	}
	t.Logf("runtime writer boundary audit classified %d call sites: %s", len(rows), runtimeWriterAuditSummary(byClass))
}

func TestRuntimeWriterBoundaryGuardRejectsSelectedSQLiteBypassFixture(t *testing.T) {
	const src = `package store

import (
	"context"
	"database/sql"
)

type SQLiteRuntimeStore struct {
	DB *sql.DB
}

func (s *SQLiteRuntimeStore) BadBypass(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, "INSERT INTO events(execution_mode, event_id) VALUES ('live', ?)", "evt")
	return err
}
`
	sites, err := collectRuntimeWriterCallSitesFromSource("internal/store/fixture_bypass.go", src)
	if err != nil {
		t.Fatalf("collect fixture call sites: %v", err)
	}
	_, failures := classifyRuntimeWriterCallSites(sites)
	if len(failures) == 0 {
		t.Fatal("expected selected SQLite direct write bypass fixture to fail classification")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "BadBypass") {
		t.Fatalf("expected failure to name BadBypass, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestRuntimeWriterBoundaryGuardRejectsSameFunctionSQLiteBypassFixture(t *testing.T) {
	const src = `package store

import (
	"context"
	"database/sql"
)

type SQLiteRuntimeStore struct {
	DB *sql.DB
}

func (store *SQLiteRuntimeStore) BadMixedBypass(ctx context.Context) error {
	if err := store.runRuntimeMutation(ctx, "valid write", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, "INSERT INTO events(execution_mode, event_id) VALUES ('live', ?)", "inside")
		return err
	}); err != nil {
		return err
	}
	_, err := store.DB.ExecContext(ctx, "INSERT INTO events(execution_mode, event_id) VALUES ('live', ?)", "outside")
	return err
}
`
	sites, err := collectRuntimeWriterCallSitesFromSource("internal/store/fixture_mixed_bypass.go", src)
	if err != nil {
		t.Fatalf("collect fixture call sites: %v", err)
	}
	_, failures := classifyRuntimeWriterCallSites(sites)
	if len(failures) == 0 {
		t.Fatal("expected selected SQLite same-function direct write bypass fixture to fail classification")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "BadMixedBypass") {
		t.Fatalf("expected failure to name BadMixedBypass, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestRuntimeWriterBoundaryGuardRejectsPipelineSQLiteDBHelperBypassFixture(t *testing.T) {
	const src = `package pipeline

import (
	"context"
	"database/sql"
)

type WorkflowInstanceStore struct {
	db *sql.DB
}

func (s *WorkflowInstanceStore) BadPipelineBypass(ctx context.Context) error {
	_, err := dbExecContext(ctx, s.db, "UPDATE flow_instances SET status = 'terminated'")
	return err
}
`
	sites, err := collectRuntimeWriterCallSitesFromSource("internal/runtime/pipeline/workflow_instance_store_sqlite.go", src)
	if err != nil {
		t.Fatalf("collect fixture call sites: %v", err)
	}
	_, failures := classifyRuntimeWriterCallSites(sites)
	if len(failures) == 0 {
		t.Fatal("expected selected SQLite pipeline helper-mediated write bypass fixture to fail classification")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "BadPipelineBypass") {
		t.Fatalf("expected failure to name BadPipelineBypass, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestRuntimeWriterBoundaryGuardRejectsPipelineSQLiteRawDBHelperFixture(t *testing.T) {
	const src = `package pipeline

import (
	"context"
	"database/sql"
)

func badSQLitePipelineHelper(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "UPDATE flow_instances SET status = 'terminated'")
	return err
}
`
	sites, err := collectRuntimeWriterCallSitesFromSource("internal/runtime/pipeline/workflow_instance_store_sqlite.go", src)
	if err != nil {
		t.Fatalf("collect fixture call sites: %v", err)
	}
	_, failures := classifyRuntimeWriterCallSites(sites)
	if len(failures) == 0 {
		t.Fatal("expected receiverless SQLite pipeline raw DB helper bypass fixture to fail classification")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "badSQLitePipelineHelper") {
		t.Fatalf("expected failure to name badSQLitePipelineHelper, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestSelectedSQLiteRuntimeConstructionConsumesMutationBoundary(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	mainData, err := os.ReadFile(filepath.Join(root, "internal", "serveapp", "main.go"))
	if err != nil {
		t.Fatalf("read internal/serveapp/main.go: %v", err)
	}
	mainText := string(mainData)
	if !strings.Contains(mainText, "NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)") {
		t.Fatal("serve runtime SQLite store construction must wire WorkflowInstanceStore through SQLiteRuntimeStore.RunRuntimeMutation")
	}
	if strings.Contains(mainText, "NewSQLiteWorkflowInstanceStore(sqliteStore.DB)") {
		t.Fatal("serve runtime SQLite store construction uses legacy WorkflowInstanceStore without the runtime mutation boundary")
	}
	sqliteBlock := runtimeWriterSnippetAfter(mainText, "case storebackend.BackendSQLite:", "default:")
	if strings.Contains(sqliteBlock, "RuntimeSQLDB:") {
		t.Fatal("SQLite runtime construction must not expose raw runtime SQLDB to runtime.Stores")
	}

	facadeData, err := os.ReadFile(filepath.Join(root, "internal", "serveapp", "store_facade.go"))
	if err != nil {
		t.Fatalf("read internal/serveapp/store_facade.go: %v", err)
	}
	facadeText := string(facadeData)
	if !strings.Contains(facadeText, "SQLDB:               s.RuntimeSQLDB,") {
		t.Fatal("selected runtime facade must use RuntimeSQLDB, leaving runtime.Stores.SQLDB nil for SQLite")
	}

	publishData, err := os.ReadFile(filepath.Join(root, "internal", "runtime", "bus", "eventbus_publish.go"))
	if err != nil {
		t.Fatalf("read internal/runtime/bus/eventbus_publish.go: %v", err)
	}
	publishText := string(publishData)
	if !strings.Contains(publishText, ".CommitPublish(") {
		t.Fatal("event publish must consume the closed CommitPublish operation")
	}
	if strings.Contains(publishText, "EventMutation") || strings.Contains(publishText, "RunEventMutation") {
		t.Fatal("event publish retains the removed generic event-mutation callback")
	}
	if strings.Contains(publishText, "PublishTx") {
		t.Fatal("event publish must not expose a producer-facing PublishTx raw transaction hook")
	}

	operatorMailboxData, err := os.ReadFile(filepath.Join(root, "internal", "apiv1", "operator_mailbox.go"))
	if err != nil {
		t.Fatalf("read internal/apiv1/operator_mailbox.go: %v", err)
	}
	operatorMailboxText := string(operatorMailboxData)
	for _, forbidden := range []string{"TransactionalEventPublisher", "ApprovalEventTx", "*sql.Tx"} {
		if strings.Contains(operatorMailboxText, forbidden) {
			t.Fatalf("operator mailbox decision publish must consume typed event mutation API, found %s", forbidden)
		}
	}
	if !strings.Contains(operatorMailboxText, "PublishInMutation") {
		t.Fatal("operator mailbox decision publish must consume EventBus.PublishInMutation")
	}

	pipelineData, err := os.ReadFile(filepath.Join(root, "internal", "runtime", "pipeline", "workflow_instance_store.go"))
	if err != nil {
		t.Fatalf("read internal/runtime/pipeline/workflow_instance_store.go: %v", err)
	}
	pipelineText := string(pipelineData)
	for _, forbidden := range []string{
		"runInSQLitePipelineTransaction",
		"sqlitePipelineBusyError",
		"sqlitePipelineTransactionRetry",
		"lockSQLitePipelineOperation",
	} {
		if strings.Contains(pipelineText, forbidden) {
			t.Fatalf("SQLite pipeline store must not own busy/retry policy through %s", forbidden)
		}
	}
	if !strings.Contains(pipelineText, "errSQLiteWorkflowInstanceStoreRuntimeMutationRunnerRequired") {
		t.Fatal("SQLite pipeline writes must fail closed when no RuntimeMutationRunner is injected")
	}
}

func collectRuntimeWriterCallSites(root string) ([]runtimeWriterCallSite, error) {
	var out []runtimeWriterCallSite
	for _, relRoot := range []string{"cmd", "internal"} {
		base := filepath.Join(root, relRoot)
		if _, err := os.Stat(base); err != nil {
			return nil, err
		}
		err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch filepath.Base(path) {
				case "testdata":
					return filepath.SkipDir
				}
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				if rel == filepath.Join("internal", "testutil") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			sites, parseErr := collectRuntimeWriterCallSitesFromFile(path, filepath.ToSlash(rel))
			if parseErr != nil {
				return parseErr
			}
			out = append(out, sites...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sortRuntimeWriterCallSites(out)
	return out, nil
}

func collectRuntimeWriterCallSitesFromFile(path, rel string) ([]runtimeWriterCallSite, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return collectRuntimeWriterCallSitesFromSource(rel, string(data))
}

func collectRuntimeWriterCallSitesFromSource(path, src string) ([]runtimeWriterCallSite, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var out []runtimeWriterCallSite
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		info := runtimeWriterFunctionInfo{
			path:             filepath.ToSlash(path),
			function:         fn.Name.Name,
			receiver:         runtimeWriterReceiverName(fn),
			receiverVariable: runtimeWriterReceiverVariableName(fn),
			txParamNames:     runtimeWriterFunctionTxParamNames(fn),
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			primitive, kind, ok := runtimeWriterPrimitive(call)
			if !ok {
				return true
			}
			switch primitive {
			case "RunRuntimeMutation", "RunRuntimeMutationContext", "runAuthorActivityMutation", "runDecisionCardMutation":
				info.callsRuntimeMutation = true
			case "runRuntimeMutation":
				info.callsRuntimeMutation = true
			case "runEventTransaction", "RunEventPublication":
				info.callsEventTransaction = true
			case "RunPipelineMutation", "runInPipelineTransaction":
				info.callsPipelineTransaction = true
			case "PipelineSQLTxFromContext", "sqlTxFromContext":
				info.usesPipelineTxFromContext = true
			}
			switch kind {
			case primitiveWrite:
				info.containsWrite = true
			case primitiveBegin:
				info.containsBegin = true
			case primitiveRead:
				info.containsRead = true
			case primitiveBoundary:
				info.containsBoundary = true
			}
			return true
		})
		scopes := collectRuntimeWriterBoundaryCallbackScopes(fn.Body)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			primitive, kind, ok := runtimeWriterPrimitive(call)
			if !ok {
				return true
			}
			callReceiver := runtimeWriterCallReceiver(call.Fun)
			scope, inScope := innermostRuntimeWriterBoundaryCallbackScope(scopes, call.Pos())
			pos := fset.Position(call.Pos())
			out = append(out, runtimeWriterCallSite{
				Path:                          info.path,
				Line:                          pos.Line,
				Function:                      info.function,
				Receiver:                      info.receiver,
				ReceiverVariable:              info.receiverVariable,
				Primitive:                     primitive,
				Kind:                          kind,
				CallReceiver:                  callReceiver,
				CallsRuntimeMutation:          info.callsRuntimeMutation,
				CallsEventTransaction:         info.callsEventTransaction,
				CallsPipelineTransaction:      info.callsPipelineTransaction,
				UsesPipelineTxFromContext:     info.usesPipelineTxFromContext,
				InRuntimeMutationCallback:     inScope && scope.runtime,
				InEventTransactionCallback:    inScope && scope.event,
				InPipelineTransactionCallback: inScope && scope.pipeline,
				UsesBoundaryCallbackTx:        inScope && scope.txParamName[callReceiver],
				UsesFunctionTxParam:           info.txParamNames[callReceiver],
				UsesReceiverDBArgument:        runtimeWriterCallUsesReceiverDBArgument(call, info.receiverVariable),
				FunctionContainsWrite:         info.containsWrite,
				FunctionContainsBegin:         info.containsBegin,
				FunctionContainsRead:          info.containsRead,
				FunctionContainsBoundary:      info.containsBoundary,
				FunctionSourceDescription:     fmt.Sprintf("%s.%s", info.receiver, info.function),
			})
			return true
		})
	}
	sortRuntimeWriterCallSites(out)
	return out, nil
}

type runtimeWriterFunctionInfo struct {
	path                      string
	function                  string
	receiver                  string
	receiverVariable          string
	callsRuntimeMutation      bool
	callsEventTransaction     bool
	callsPipelineTransaction  bool
	usesPipelineTxFromContext bool
	txParamNames              map[string]bool
	containsWrite             bool
	containsBegin             bool
	containsRead              bool
	containsBoundary          bool
}

func runtimeWriterPrimitive(call *ast.CallExpr) (string, runtimeWriterPrimitiveKind, bool) {
	name := runtimeWriterCallName(call.Fun)
	switch name {
	case "Begin", "BeginTx", "BeginTxx":
		return name, primitiveBegin, true
	case "Exec", "ExecContext":
		return name, primitiveWrite, true
	case "dbExecContext":
		return name, primitiveWrite, true
	case "Query", "QueryContext", "QueryRow", "QueryRowContext", "Prepare", "PrepareContext":
		return name, primitiveRead, true
	case "RunPipelineMutation", "runInPipelineTransaction", "RunRuntimeMutation", "RunRuntimeMutationContext", "runRuntimeMutation", "runAuthorActivityMutation", "runDecisionCardMutation", "runEventTransaction", "RunEventPublication", "PipelineSQLTxFromContext", "sqlTxFromContext":
		return name, primitiveBoundary, true
	default:
		return "", "", false
	}
}

func collectRuntimeWriterBoundaryCallbackScopes(body ast.Node) []runtimeWriterBoundaryCallbackScope {
	var scopes []runtimeWriterBoundaryCallbackScope
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		primitive, _, ok := runtimeWriterPrimitive(call)
		if !ok {
			return true
		}
		scope := runtimeWriterBoundaryCallbackScope{}
		switch primitive {
		case "RunRuntimeMutation", "RunRuntimeMutationContext", "runRuntimeMutation", "runAuthorActivityMutation", "runDecisionCardMutation":
			scope.runtime = true
		case "runEventTransaction", "RunEventPublication":
			scope.event = true
		case "RunPipelineMutation", "runInPipelineTransaction":
			scope.pipeline = true
		default:
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.FuncLit)
			if !ok || lit.Body == nil {
				continue
			}
			next := scope
			next.start = lit.Body.Pos()
			next.end = lit.Body.End()
			next.txParamName = runtimeWriterCallbackTxParamNames(lit)
			scopes = append(scopes, next)
		}
		return true
	})
	return scopes
}

func innermostRuntimeWriterBoundaryCallbackScope(scopes []runtimeWriterBoundaryCallbackScope, pos token.Pos) (runtimeWriterBoundaryCallbackScope, bool) {
	var out runtimeWriterBoundaryCallbackScope
	found := false
	for _, scope := range scopes {
		if pos < scope.start || pos > scope.end {
			continue
		}
		if !found || (scope.end-scope.start) < (out.end-out.start) {
			out = scope
			found = true
		}
	}
	return out, found
}

func runtimeWriterCallbackTxParamNames(lit *ast.FuncLit) map[string]bool {
	if lit == nil || lit.Type == nil || lit.Type.Params == nil {
		return map[string]bool{}
	}
	return runtimeWriterTxParamNames(lit.Type.Params)
}

func runtimeWriterFunctionTxParamNames(fn *ast.FuncDecl) map[string]bool {
	if fn == nil || fn.Type == nil || fn.Type.Params == nil {
		return map[string]bool{}
	}
	return runtimeWriterTxParamNames(fn.Type.Params)
}

func runtimeWriterTxParamNames(params *ast.FieldList) map[string]bool {
	out := map[string]bool{}
	if params == nil {
		return out
	}
	for _, field := range params.List {
		if !runtimeWriterExprLooksSQLTx(field.Type) {
			continue
		}
		for _, name := range field.Names {
			if name == nil || name.Name == "" || name.Name == "_" {
				continue
			}
			out[name.Name] = true
		}
	}
	return out
}

func runtimeWriterExprLooksSQLTx(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return runtimeWriterExprLooksSQLTx(e.X)
	case *ast.SelectorExpr:
		return e.Sel.Name == "Tx"
	case *ast.Ident:
		return e.Name == "Tx"
	case *ast.ParenExpr:
		return runtimeWriterExprLooksSQLTx(e.X)
	default:
		return false
	}
}

func runtimeWriterCallName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	default:
		return ""
	}
}

func runtimeWriterCallReceiver(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return runtimeWriterRootIdent(sel.X)
}

func runtimeWriterCallUsesReceiverDBArgument(call *ast.CallExpr, receiverVariable string) bool {
	receiverVariable = strings.TrimSpace(receiverVariable)
	if call == nil || receiverVariable == "" {
		return false
	}
	for _, arg := range call.Args {
		sel, ok := arg.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "db" && sel.Sel.Name != "DB" {
			continue
		}
		if runtimeWriterRootIdent(sel.X) == receiverVariable {
			return true
		}
	}
	return false
}

func runtimeWriterRootIdent(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return runtimeWriterRootIdent(e.X)
	case *ast.ParenExpr:
		return runtimeWriterRootIdent(e.X)
	case *ast.StarExpr:
		return runtimeWriterRootIdent(e.X)
	default:
		return ""
	}
}

func classifyRuntimeWriterCallSites(sites []runtimeWriterCallSite) ([]runtimeWriterAuditRow, []string) {
	rows := make([]runtimeWriterAuditRow, 0, len(sites))
	failures := []string{}
	for _, site := range sites {
		classification, reason, ok := classifyRuntimeWriterCallSite(site)
		if !ok {
			failures = append(failures, fmt.Sprintf("- %s:%d %s %s.%s %s is not classified", site.Path, site.Line, site.Kind, site.Receiver, site.Function, site.Primitive))
			continue
		}
		rows = append(rows, runtimeWriterAuditRow{
			Site:           site,
			Classification: classification,
			Reason:         reason,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i].Site, rows[j].Site
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		if a.Function != b.Function {
			return a.Function < b.Function
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Primitive < b.Primitive
	})
	return rows, failures
}

func classifyRuntimeWriterCallSite(site runtimeWriterCallSite) (runtimeWriterClassification, string, bool) {
	if strings.HasPrefix(site.Path, "internal/runtime/authoractivity/") {
		return classConsumesCanonical, "canonical author activity transaction, persistence, and read owner", true
	}
	if site.Kind == primitiveBoundary {
		switch site.Primitive {
		case "RunRuntimeMutation", "RunRuntimeMutationContext", "runRuntimeMutation", "runAuthorActivityMutation", "runDecisionCardMutation", "runEventTransaction", "RunEventPublication":
			return classConsumesCanonical, "canonical runtime mutation/event transaction boundary", true
		case "RunPipelineMutation", "runInPipelineTransaction":
			return classConsumesCanonical, "pipeline mutation owner delegates production SQLite writes to RunRuntimeMutationContext", true
		case "PipelineSQLTxFromContext", "sqlTxFromContext":
			return classActiveTxHelper, "active transaction context probe; not a writer by itself", true
		}
	}

	if site.Receiver == "SQLiteRuntimeStore" {
		return classifySQLiteRuntimeStoreCallSite(site)
	}
	if site.Receiver == "WorkflowInstanceStore" {
		if classification, reason, ok := classifyWorkflowInstanceStoreCallSite(site); ok {
			return classification, reason, true
		}
		if workflowInstanceStoreSQLiteBypassCandidate(site) {
			return "", "", false
		}
	}
	if classification, reason, ok := classifyWorkflowInstanceStoreSpecCallSite(site); ok {
		return classification, reason, true
	}
	if sqlitePipelineTxHelper(site) {
		return classActiveTxHelper, "SQLite pipeline helper uses caller-provided *sql.Tx instead of raw *sql.DB", true
	}
	if sqliteActiveTxHelper(site) {
		return classActiveTxHelper, "helper writes only through caller-provided canonical transaction", true
	}
	if site.Receiver == "PostgresStore" || strings.Contains(site.Path, "postgres") || strings.Contains(site.Function, "Postgres") {
		return classDifferentConcept, "Postgres implementation keeps normal transaction semantics and does not consume SQLite serialization", true
	}
	for _, rule := range runtimeWriterRules() {
		if rule.matches(site) {
			return rule.classification, rule.reason, true
		}
	}
	if site.Kind == primitiveRead && !site.FunctionContainsWrite && !site.FunctionContainsBegin {
		return classDifferentConcept, "read-only projection/query surface; not selected SQLite mutation authority", true
	}
	return "", "", false
}

func classifyWorkflowInstanceStoreCallSite(site runtimeWriterCallSite) (runtimeWriterClassification, string, bool) {
	if site.Kind == primitiveRead && site.InPipelineTransactionCallback && site.UsesBoundaryCallbackTx {
		return classConsumesCanonical, "WorkflowInstanceStore selected read executes through the callback transaction", true
	}
	if site.Kind != primitiveWrite && site.Kind != primitiveBegin {
		return "", "", false
	}
	if (site.InPipelineTransactionCallback || site.InRuntimeMutationCallback) && site.UsesBoundaryCallbackTx {
		return classConsumesCanonical, "WorkflowInstanceStore selected writer executes through the callback transaction", true
	}
	if sqlitePipelineTxHelper(site) {
		return classActiveTxHelper, "SQLite WorkflowInstanceStore helper uses caller-provided *sql.Tx", true
	}
	if classification, reason, ok := classifyWorkflowInstanceStoreSpecCallSite(site); ok {
		return classification, reason, true
	}
	if site.Primitive == "dbExecContext" && site.UsesReceiverDBArgument {
		if site.InPipelineTransactionCallback || site.InRuntimeMutationCallback {
			return classConsumesCanonical, "WorkflowInstanceStore helper-mediated write executes inside canonical transaction context", true
		}
		return "", "", false
	}
	if site.Path == "internal/runtime/pipeline/workflow_instance_store_sqlite.go" &&
		site.ReceiverVariable != "" &&
		site.CallReceiver == site.ReceiverVariable {
		return "", "", false
	}
	return "", "", false
}

func classifyWorkflowInstanceStoreSpecCallSite(site runtimeWriterCallSite) (runtimeWriterClassification, string, bool) {
	if site.Path != "internal/runtime/pipeline/workflow_instance_store.go" {
		return "", "", false
	}
	switch site.Function {
	case "runInPipelineTransactionOnce":
		if site.Kind == primitiveBegin {
			return classDifferentConcept, "Postgres workflow pipeline transaction owner; SQLite requires the selected runtime mutation boundary before this path", true
		}
	case "workflowInstanceStoreTx":
		if site.Kind == primitiveBegin {
			return classDifferentConcept, "Postgres workflow instance helper opens a local transaction; SQLite writes consume the selected runtime mutation boundary", true
		}
	case "MarkTerminated", "markWorkflowInstanceTerminatedSpecTx", "upsertSpec", "createSpec":
		if site.CallReceiver == "tx" {
			return classDifferentConcept, "named Postgres/spec workflow instance writer uses a local transaction after SQLite has delegated to the canonical path", true
		}
	case "workflowInstanceCreateTargetExists", "lockWorkflowInstanceMutation",
		"insertWorkflowCreateEntityInitialValueMutations", "loadTrackedEntityStateProjection":
		if site.UsesFunctionTxParam {
			return classDifferentConcept, "named Postgres/spec helper uses caller-provided *sql.Tx", true
		}
	}
	return "", "", false
}

func workflowInstanceStoreSQLiteBypassCandidate(site runtimeWriterCallSite) bool {
	if site.Kind != primitiveWrite && site.Kind != primitiveBegin {
		return false
	}
	if site.Primitive == "dbExecContext" && site.UsesReceiverDBArgument {
		return true
	}
	return site.Path == "internal/runtime/pipeline/workflow_instance_store_sqlite.go" &&
		site.ReceiverVariable != "" &&
		site.CallReceiver == site.ReceiverVariable
}

func classifySQLiteRuntimeStoreCallSite(site runtimeWriterCallSite) (runtimeWriterClassification, string, bool) {
	if site.Path == "internal/store/agent_directive_operations.go" {
		switch site.Function {
		case "AdmitDirectiveExecution", "RecordDirectiveExecuted":
			return classConsumesCanonical, "SQLite directive transition executes through transitionSQLiteDirectiveOperation and its canonical runRuntimeMutation boundary", true
		}
	}
	switch site.Function {
	case "RunRuntimeMutation", "RunRuntimeMutationContext", "runEventTransaction", "RunEventPublication", "runRuntimeMutation", "runAuthorActivityMutation", "runDecisionCardMutation", "runRuntimeMutationOnce", "runRuntimeMutationOnceLocked":
		return classConsumesCanonical, "canonical SQLite runtime mutation owner", true
	case "CompleteDecisionRouteObligation", "QuarantineDecisionRouteObligation":
		return classConsumesCanonical, "decision-card obligation completion consumes the serialized decision-card mutation owner", true
	}
	if site.Kind == primitiveWrite || site.Kind == primitiveBegin {
		if (site.InRuntimeMutationCallback || site.InEventTransactionCallback) && site.UsesBoundaryCallbackTx {
			return classConsumesCanonical, "SQLiteRuntimeStore selected writer executes through the callback transaction", true
		}
		if sqliteActiveTxHelper(site) {
			return classActiveTxHelper, "SQLite helper writes only through caller-provided canonical transaction", true
		}
		return "", "", false
	}
	if site.Kind == primitiveRead {
		if ((site.InRuntimeMutationCallback || site.InEventTransactionCallback) && site.UsesBoundaryCallbackTx) || sqliteActiveTxHelper(site) {
			return classActiveTxHelper, "read/query inside canonical SQLite mutation helper", true
		}
		return classDifferentConcept, "SQLiteRuntimeStore read/capability surface; not mutation authority", true
	}
	return "", "", false
}

func sqlitePipelineTxHelper(site runtimeWriterCallSite) bool {
	if site.Kind == primitiveBegin || site.Kind == primitiveBoundary {
		return false
	}
	if !site.UsesFunctionTxParam {
		return false
	}
	switch site.Path {
	case "internal/runtime/pipeline/workflow_instance_store_sqlite.go",
		"internal/runtime/pipeline/standing_service_store.go",
		"internal/runtime/pipeline/system_node_receipt_store.go":
		return true
	default:
		return false
	}
}

func sqliteActiveTxHelper(site runtimeWriterCallSite) bool {
	if site.Receiver == "" && strings.HasPrefix(site.Path, "internal/store/") && site.UsesFunctionTxParam && site.Kind != primitiveBegin {
		return true
	}
	if site.Receiver == "SQLiteRuntimeStore" && site.ReceiverVariable != "" && site.CallReceiver == site.ReceiverVariable {
		return false
	}
	if site.Receiver == "" && strings.HasPrefix(site.Path, "internal/store/") && strings.Contains(site.Function, "Tx") && site.Kind != primitiveBegin {
		return true
	}
	if site.Receiver == "SQLiteRuntimeStore" {
		if strings.Contains(site.Function, "Tx") {
			return true
		}
		switch site.Function {
		case "sqliteLockAgentDeliveryTx", "sqliteEnsureActiveRunRow", "sqliteEnsureRunRow", "sqliteRequireRunRowPresent", "purgeExpiredSQLiteAPIIdempotency", "sqliteStoreAPIIdempotency",
			"appendSQLiteMailboxV1DecisionEventTx", "normalizeSQLiteMailboxV1DecisionReplayResult",
			"loadSQLiteMailboxV1RowTx", "sqliteMarkRunTerminalTx", "sqliteNormalRunCompletionRunReadyTx",
			"sqliteNormalRunCompletionSessionLeasesSettledTx", "ensureSQLiteTaskConversationAuditRowTx":
			return true
		}
	}
	switch site.Function {
	case "sqliteEnsureActiveRunRow", "sqliteEnsureRunRow", "sqliteRequireRunRowPresent", "purgeExpiredSQLiteAPIIdempotency", "sqliteStoreAPIIdempotency",
		"sqliteSyncRunCounts", "sqlitePauseRunControl", "sqliteContinueRunControl", "sqliteStopRunControl",
		"insertSQLiteEntityStateDiff":
		return true
	}
	return strings.HasPrefix(site.Path, "internal/store/sqlite_") && strings.Contains(site.Function, "Tx")
}

func runtimeWriterRules() []runtimeWriterRule {
	return []runtimeWriterRule{
		{
			name:           "sealed event publication test transaction",
			path:           rx(`^internal/runtime/bus/bustest/publish\.go$`),
			function:       rx(`^BeginPreparedPublish$`),
			kinds:          kinds(primitiveBegin),
			classification: classDifferentConcept,
			reason:         "test-only implementation of the sealed CommitPublish transaction; it opens no database transaction",
		},
		{
			name:           "canonical semantic event fixture transaction",
			path:           rx(`^internal/store/storetest/event\.go$`),
			function:       rx(`^commitSemanticEventWithInitialFacts$`),
			kinds:          kinds(primitiveBegin),
			classification: classActiveTxHelper,
			reason:         "test-only semantic event fixture commits an admitted record and its declared initial facts atomically",
		},
		{
			name:           "declared event corruption fixture",
			path:           rx(`^internal/store/testsql/event\.go$`),
			function:       rx(`^(CorruptEventStore|RejectEventStoreCorruption|RejectEventStoreCorruptionCategory)$`),
			kinds:          kinds(primitiveWrite),
			classification: classDifferentConcept,
			reason:         "test-only fail-closed corruption owner requires an explicit invariant and reason",
		},
		{
			name:           "private sqlite admitted event adapter",
			path:           rx(`^internal/store/internal/eventrecord/sqlite/adapter\.go$`),
			function:       rx(`^(Insert|Load|LoadMany)$`),
			kinds:          allPrimitiveKinds(),
			classification: classActiveTxHelper,
			reason:         "private event-record adapter executes only for the named admitted-event storage operations",
		},
		{
			name:           "private postgres admitted event adapter",
			path:           rx(`^internal/store/internal/eventrecord/postgres/adapter\.go$`),
			function:       rx(`^(Insert|Load|LoadMany|DeleteSelectedForkRunEvents)$`),
			kinds:          allPrimitiveKinds(),
			classification: classActiveTxHelper,
			reason:         "private event-record adapter executes only for the named admitted-event storage operations",
		},
		{
			name:           "workflow timer lifecycle transaction helpers",
			path:           rx(`^internal/runtime/pipeline/workflow_timer_store\.go$`),
			function:       rx(`^(insertWorkflowTimerActivation|cancelWorkflowTimerActivation|completeWorkflowTimerOccurrence)$`),
			kinds:          kinds(primitiveWrite),
			classification: classActiveTxHelper,
			reason:         "canonical workflow timer row transitions execute only through the selected WorkflowTimerLifecycle pipeline transaction",
		},
		{
			name:           "decision card transactional helpers",
			path:           rx(`^internal/store/decision_cards\.go$`),
			function:       rx(`^(insertDecisionCard|decideDecisionCard|deferDecisionCard|beginDecisionCardInput|updateDecisionCardDraftStatus|transitionDecisionCardDrafts|supersedeDecisionCardsForStage|supersedeDecisionCardsForRun|supersedeRunGateActivations)$`),
			kinds:          kinds(primitiveRead, primitiveWrite),
			classification: classActiveTxHelper,
			reason:         "decision-card helpers write only through a selected-store or workflow pipeline transaction supplied by their canonical mutation owner",
		},
		{
			name:           "decision card obligation transactional helpers",
			path:           rx(`^internal/store/decision_card_route_obligations\.go$`),
			function:       rx(`^(insertDecisionRouteObligation|deferDecisionRouteObligation)$`),
			kinds:          kinds(primitiveWrite),
			classification: classActiveTxHelper,
			reason:         "decision-card route obligations write only inside their selected-store mutation owner",
		},
		{
			name:           "decision card obligation selected-store owners",
			path:           rx(`^internal/store/decision_card_route_obligations\.go$`),
			receiver:       rx(`^SQLiteRuntimeStore$`),
			kinds:          kinds(primitiveWrite),
			classification: classConsumesCanonical,
			reason:         "SQLite decision-card obligation mutations consume the canonical serialized decision-card mutation boundary",
		},
		{
			name:           "decision card obligation readers",
			path:           rx(`^internal/store/decision_card_route_obligations\.go$`),
			kinds:          kinds(primitiveRead),
			classification: classDifferentConcept,
			reason:         "decision-card obligation due/pending scans are read-only recovery surfaces",
		},
		{
			name:           "sqlite lifecycle subordinate transaction helper",
			path:           rx(`^internal/store/agent_lifecycle\.go$`),
			function:       rx(`^applySQLiteLifecycleSubordinate$`),
			kinds:          allPrimitiveKinds(),
			classification: classActiveTxHelper,
			reason:         "subordinate session mutation consumes the lifecycle transition's active SQLite transaction",
		},
		{
			name:           "managed external effect attempt admission",
			path:           rx(`^internal/runtime/(llm/(api_runtime|openai_compatible_runtime|openai_responses_runtime|cli_runtime|cli_runtime_process|cli_tool_result_relay)|managedcredentials/store|mcp/client|tools/(executor_http|executor_native|tool_result_relay))\.go$`),
			function:       rx(`.*`),
			kinds:          kinds(primitiveBegin),
			classification: classDifferentConcept,
			reason:         "runtimeeffects.Begin is the durable managed-attempt admission owner, not a raw SQLite transaction open",
		},
		{
			name:           "sqlite schema bootstrap",
			path:           rx(`^internal/store/(sqlite_schema|sqlite_schema_bootstrap|platformschema/platformschema)\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "schema/bootstrap owns dialect DDL and PRAGMA behavior, split from selected runtime mutation writers",
		},
		{
			name:           "api idempotency postgres helper",
			path:           rx(`^internal/store/api_idempotency\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres API idempotency advisory-lock helper; SQLite owner is SQLiteRuntimeStore.WithAPIIdempotency",
		},
		{
			name:           "postgres advisory lock helper",
			path:           rx(`^internal/store/shared_store_claims\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres advisory-lock helper; not selected SQLite mutation authority",
		},
		{
			name:           "run lifecycle postgres helper",
			path:           rx(`^internal/store/runlifecycle/.*\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "run lifecycle helper operates on Postgres raw DB owner, not selected SQLite runtime store",
		},
		{
			name:           "run bundle postgres helper",
			path:           rx(`^internal/store/runbundle/.*\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "run bundle catalog/availability is a Postgres or split feature capability, not selected SQLite mutation authority",
		},
		{
			name:           "bundle and reset split features",
			path:           rx(`^internal/store/(bundle_|destructive_reset|preservation_cleanup|conversation_fork|run_fork).*\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "bundle/reset/fork feature family remains split or Postgres-owned unless independently promoted",
		},
		{
			name:           "runtime postgres sessions",
			path:           rx(`^internal/runtime/sessions/postgres\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres session registry implementation; SQLite session rows are owned by SQLiteRuntimeStore",
		},
		{
			name:           "pipeline postgres raw sql helpers",
			path:           rx(`^internal/runtime/pipeline/(node_system_runner|workflow_entity_type_repair|coordinator|engine_adapter|runtime_support|workflow_transitions)\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "raw-SQL pipeline helper is Postgres/raw runtime SQL path; SQLite runtime facade leaves SQLDB nil and uses typed stores",
		},
		{
			name:           "runtime db tx helper",
			path:           rx(`^internal/runtime/dbtx\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "generic DB/tx helper is not selected SQLite runtime writer authority",
		},
		{
			name:           "runtime diagnostics adjacent raw sql",
			path:           rx(`^internal/runtime/(deadletters/record|mutationlog/mutationlog)\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "diagnostic/dead-letter raw SQL helper is adjacent and split from selected SQLite mutation boundary",
		},
		{
			name:           "postgres run-fork revision owner",
			path:           rx(`^internal/runtime/runforkrevision/.*\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres fixed-revision capture is the run-fork history owner; SQLite run-fork mutation remains typed unsupported",
		},
		{
			name:           "workspace and digest stores",
			path:           rx(`^internal/(runtime/workspace|digest)/.*\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "workspace/digest persistence is not selected runtime SQLite mutation authority",
		},
		{
			name:           "agent directives read helper",
			path:           rx(`^internal/store/agent_directive_run_target\.go$`),
			kinds:          kinds(primitiveRead),
			classification: classDifferentConcept,
			reason:         "agent directive helper is read-only",
		},
		{
			name:           "agent directive postgres operation owner",
			path:           rx(`^internal/store/agent_directive_operations\.go$`),
			receiver:       rx(`^PostgresStore$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres directive operation transactions are separate from the selected SQLite runtime mutation boundary",
		},
		{
			name:           "agent directive sqlite operation owner",
			path:           rx(`^internal/store/agent_directive_operations\.go$`),
			receiver:       rx(`^SQLiteRuntimeStore$`),
			kinds:          allPrimitiveKinds(),
			classification: classConsumesCanonical,
			reason:         "SQLite directive operation transitions consume SQLiteRuntimeStore.runRuntimeMutation and never hold a transaction across BoardStep",
		},
		{
			name:           "read surface files",
			path:           rx(`^internal/store/.*(read_surface|read|list|debug|observability|usage).*\.go$`),
			kinds:          kinds(primitiveRead),
			classification: classDifferentConcept,
			reason:         "read/projection surface; not selected SQLite mutation authority",
		},
		{
			name:           "operator conversation read surface",
			path:           rx(`^internal/store/operator_agent_conversation_read_surface\.go$`),
			kinds:          kinds(primitiveRead),
			classification: classDifferentConcept,
			reason:         "operator conversation read surface; not selected SQLite mutation authority",
		},
		{
			name:           "mailbox legacy postgres store",
			path:           rx(`^internal/store/mailbox\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "legacy/Postgres mailbox store implementation; SQLite mailbox rows are owned by SQLiteRuntimeStore",
		},
		{
			name:           "postgres selected store files",
			path:           rx(`^internal/store/(agent_store|events|event_receipt_store|inbound|llm_store|mailbox_v1|run_completion|run_control|runtime_ingress_state|schedule_store|tool_persistence|active_run_quiescence)\.go$`),
			receiver:       rx(`^PostgresStore$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "Postgres selected store implementation; SQLite counterpart is guarded separately",
		},
	}
}

func (r runtimeWriterRule) matches(site runtimeWriterCallSite) bool {
	if r.path != nil && !r.path.MatchString(site.Path) {
		return false
	}
	if r.function != nil && !r.function.MatchString(site.Function) {
		return false
	}
	if r.receiver != nil && !r.receiver.MatchString(site.Receiver) {
		return false
	}
	if len(r.kinds) > 0 && !r.kinds[site.Kind] {
		return false
	}
	return true
}

func runtimeWriterReceiverName(fn *ast.FuncDecl) string {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	switch expr := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if ident, ok := expr.X.(*ast.Ident); ok {
			return ident.Name
		}
	case *ast.Ident:
		return expr.Name
	}
	return ""
}

func runtimeWriterReceiverVariableName(fn *ast.FuncDecl) string {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	if len(fn.Recv.List[0].Names) == 0 || fn.Recv.List[0].Names[0] == nil {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

func repoRootForRuntimeWriterGuard(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func runtimeWriterSnippetAfter(text, start, end string) string {
	startIdx := strings.Index(text, start)
	if startIdx < 0 {
		return ""
	}
	rest := text[startIdx+len(start):]
	if endIdx := strings.Index(rest, end); endIdx >= 0 {
		return rest[:endIdx]
	}
	return rest
}

func runtimeWriterAuditSummary(counts map[runtimeWriterClassification]int) string {
	parts := make([]string, 0, len(counts))
	for class, count := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", class, count))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func sortRuntimeWriterCallSites(sites []runtimeWriterCallSite) {
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].Path != sites[j].Path {
			return sites[i].Path < sites[j].Path
		}
		if sites[i].Line != sites[j].Line {
			return sites[i].Line < sites[j].Line
		}
		if sites[i].Function != sites[j].Function {
			return sites[i].Function < sites[j].Function
		}
		return sites[i].Primitive < sites[j].Primitive
	})
}

func rx(pattern string) *regexp.Regexp {
	return regexp.MustCompile(pattern)
}

func kinds(values ...runtimeWriterPrimitiveKind) map[runtimeWriterPrimitiveKind]bool {
	out := map[runtimeWriterPrimitiveKind]bool{}
	for _, value := range values {
		out[value] = true
	}
	return out
}

func allPrimitiveKinds() map[runtimeWriterPrimitiveKind]bool {
	return kinds(primitiveBegin, primitiveWrite, primitiveRead, primitiveBoundary)
}
