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
	Path                      string
	Line                      int
	Function                  string
	Receiver                  string
	Primitive                 string
	Kind                      runtimeWriterPrimitiveKind
	CallsRuntimeMutation      bool
	CallsEventTransaction     bool
	CallsPipelineTransaction  bool
	UsesPipelineTxFromContext bool
	FunctionContainsWrite     bool
	FunctionContainsBegin     bool
	FunctionContainsRead      bool
	FunctionContainsBoundary  bool
	FunctionSourceDescription string
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
		classSplitLegacy,
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
	_, err := s.DB.ExecContext(ctx, "INSERT INTO events(event_id) VALUES (?)", "evt")
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

func TestSelectedSQLiteRuntimeConstructionConsumesMutationBoundary(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	mainData, err := os.ReadFile(filepath.Join(root, "cmd", "swarm", "main.go"))
	if err != nil {
		t.Fatalf("read cmd/swarm/main.go: %v", err)
	}
	mainText := string(mainData)
	if !strings.Contains(mainText, "NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)") {
		t.Fatal("cmd/swarm SQLite store construction must wire WorkflowInstanceStore through SQLiteRuntimeStore.RunRuntimeMutation")
	}
	if strings.Contains(mainText, "NewSQLiteWorkflowInstanceStore(sqliteStore.DB)") {
		t.Fatal("cmd/swarm SQLite store construction uses legacy WorkflowInstanceStore without the runtime mutation boundary")
	}
	sqliteBlock := runtimeWriterSnippetAfter(mainText, "case storebackend.BackendSQLite:", "default:")
	if strings.Contains(sqliteBlock, "RuntimeSQLDB:") {
		t.Fatal("SQLite runtime construction must not expose raw runtime SQLDB to runtime.Stores")
	}

	facadeData, err := os.ReadFile(filepath.Join(root, "cmd", "swarm", "store_facade.go"))
	if err != nil {
		t.Fatalf("read cmd/swarm/store_facade.go: %v", err)
	}
	facadeText := string(facadeData)
	if !strings.Contains(facadeText, "SQLDB:               s.RuntimeSQLDB,") {
		t.Fatal("selected runtime facade must use RuntimeSQLDB, leaving runtime.Stores.SQLDB nil for SQLite")
	}

	publishData, err := os.ReadFile(filepath.Join(root, "internal", "runtime", "bus", "eventbus_publish.go"))
	if err != nil {
		t.Fatalf("read internal/runtime/bus/eventbus_publish.go: %v", err)
	}
	if !strings.Contains(string(publishData), ".RunEventTransaction(ctx,") {
		t.Fatal("event publish must consume EventTransactionRunner.RunEventTransaction when available")
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
			path:     filepath.ToSlash(path),
			function: fn.Name.Name,
			receiver: runtimeWriterReceiverName(fn),
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
			case "RunRuntimeMutation":
				info.callsRuntimeMutation = true
			case "runRuntimeMutation":
				info.callsRuntimeMutation = true
			case "RunEventTransaction":
				info.callsEventTransaction = true
			case "RunInPipelineTransaction":
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
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			primitive, kind, ok := runtimeWriterPrimitive(call)
			if !ok {
				return true
			}
			pos := fset.Position(call.Pos())
			out = append(out, runtimeWriterCallSite{
				Path:                      info.path,
				Line:                      pos.Line,
				Function:                  info.function,
				Receiver:                  info.receiver,
				Primitive:                 primitive,
				Kind:                      kind,
				CallsRuntimeMutation:      info.callsRuntimeMutation,
				CallsEventTransaction:     info.callsEventTransaction,
				CallsPipelineTransaction:  info.callsPipelineTransaction,
				UsesPipelineTxFromContext: info.usesPipelineTxFromContext,
				FunctionContainsWrite:     info.containsWrite,
				FunctionContainsBegin:     info.containsBegin,
				FunctionContainsRead:      info.containsRead,
				FunctionContainsBoundary:  info.containsBoundary,
				FunctionSourceDescription: fmt.Sprintf("%s.%s", info.receiver, info.function),
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
	callsRuntimeMutation      bool
	callsEventTransaction     bool
	callsPipelineTransaction  bool
	usesPipelineTxFromContext bool
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
	case "Query", "QueryContext", "QueryRow", "QueryRowContext", "Prepare", "PrepareContext":
		return name, primitiveRead, true
	case "RunInPipelineTransaction", "RunRuntimeMutation", "runRuntimeMutation", "RunEventTransaction", "PipelineSQLTxFromContext", "sqlTxFromContext":
		return name, primitiveBoundary, true
	default:
		return "", "", false
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
	if site.Kind == primitiveBoundary {
		switch site.Primitive {
		case "RunRuntimeMutation", "runRuntimeMutation", "RunEventTransaction":
			return classConsumesCanonical, "canonical runtime mutation/event transaction boundary", true
		case "RunInPipelineTransaction":
			return classConsumesCanonical, "pipeline transaction owner delegates production SQLite writes to RunRuntimeMutation", true
		case "PipelineSQLTxFromContext", "sqlTxFromContext":
			return classActiveTxHelper, "active transaction context probe; not a writer by itself", true
		}
	}

	if site.Receiver == "SQLiteRuntimeStore" {
		return classifySQLiteRuntimeStoreCallSite(site)
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

func classifySQLiteRuntimeStoreCallSite(site runtimeWriterCallSite) (runtimeWriterClassification, string, bool) {
	switch site.Function {
	case "RunRuntimeMutation", "RunEventTransaction", "runRuntimeMutation", "runRuntimeMutationOnce", "runRuntimeMutationOnceLocked":
		return classConsumesCanonical, "canonical SQLite runtime mutation owner", true
	case "BeginEventTx":
		if site.Kind == primitiveBegin {
			return classSplitLegacy, "legacy TransactionalEventStore fallback; production SQLite publish uses RunEventTransaction", true
		}
	}
	if site.Kind == primitiveWrite || site.Kind == primitiveBegin {
		if site.CallsRuntimeMutation || site.CallsEventTransaction {
			return classConsumesCanonical, "SQLiteRuntimeStore selected writer enters RunRuntimeMutation before executing SQL", true
		}
		if sqliteActiveTxHelper(site) {
			return classActiveTxHelper, "SQLite helper writes only through caller-provided canonical transaction", true
		}
		return "", "", false
	}
	if site.Kind == primitiveRead {
		if site.CallsRuntimeMutation || sqliteActiveTxHelper(site) {
			return classActiveTxHelper, "read/query inside canonical SQLite mutation helper", true
		}
		return classDifferentConcept, "SQLiteRuntimeStore read/capability surface; not mutation authority", true
	}
	return "", "", false
}

func sqliteActiveTxHelper(site runtimeWriterCallSite) bool {
	if site.Receiver == "" && strings.HasPrefix(site.Path, "internal/store/") && strings.Contains(site.Function, "Tx") && site.Kind != primitiveBegin {
		return true
	}
	if site.Receiver == "SQLiteRuntimeStore" {
		if strings.Contains(site.Function, "Tx") {
			return true
		}
		switch site.Function {
		case "sqliteLockAgentDeliveryTx", "sqliteEnsureRunRow", "purgeExpiredSQLiteAPIIdempotency", "sqliteStoreAPIIdempotency",
			"appendSQLiteMailboxV1ApprovalEventTx", "normalizeSQLiteMailboxV1DecisionReplayResult",
			"loadSQLiteMailboxV1RowTx", "sqliteMarkRunTerminalTx", "sqliteNormalRunCompletionRunReadyTx",
			"sqliteNormalRunCompletionSessionLeasesSettledTx", "ensureSQLiteTaskConversationAuditRowTx":
			return true
		}
	}
	switch site.Function {
	case "sqliteEnsureRunRow", "purgeExpiredSQLiteAPIIdempotency", "sqliteStoreAPIIdempotency",
		"sqliteSyncRunCounts", "sqlitePauseRunControl", "sqliteContinueRunControl", "sqliteStopRunControl",
		"insertSQLiteEntityStateDiff":
		return true
	}
	return strings.HasPrefix(site.Path, "internal/store/sqlite_") && strings.Contains(site.Function, "Tx")
}

func runtimeWriterRules() []runtimeWriterRule {
	return []runtimeWriterRule{
		{
			name:           "sqlite schema bootstrap",
			path:           rx(`^internal/store/(sqlite_schema|platformschema/platformschema)\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classDifferentConcept,
			reason:         "schema/bootstrap owns dialect DDL and PRAGMA behavior, split from selected runtime mutation writers",
		},
		{
			name:           "schema capability reads",
			path:           rx(`^internal/store/schema_capabilities\.go$`),
			kinds:          kinds(primitiveRead),
			classification: classDifferentConcept,
			reason:         "schema capability reader; not mutation authority",
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
			name:           "pipeline sqlite active tx helpers",
			path:           rx(`^internal/runtime/pipeline/(workflow_instance_store_sqlite|system_node_receipt_store)\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classActiveTxHelper,
			reason:         "SQLite pipeline helper executes inside WorkflowInstanceStore.RunInPipelineTransaction",
		},
		{
			name:           "pipeline transaction owner and old fallback",
			path:           rx(`^internal/runtime/pipeline/workflow_instance_store\.go$`),
			kinds:          allPrimitiveKinds(),
			classification: classSplitLegacy,
			reason:         "WorkflowInstanceStore owns Postgres/spec txs and legacy SQLite fallback; production SQLite construction injects RunRuntimeMutation",
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
