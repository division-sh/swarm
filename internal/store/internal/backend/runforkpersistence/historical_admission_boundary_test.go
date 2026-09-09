package runforkpersistence

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"golang.org/x/tools/go/packages"
)

const historicalBoundaryModule = "github.com/division-sh/swarm/internal/"
const historicalBoundaryOwner = "store/internal/backend/runforkpersistence::"
const historicalBoundaryWriter = "store/internal/backend/runforkrevision::"

type historicalBoundaryFinding struct {
	Scope string
	Kind  string
	Site  string
}

type historicalBoundaryAllowance struct {
	Count  int
	Reason string
}

// Exceptions name compiler-resolved functions and exact operations, not files,
// receiver variable spellings, or every method on an otherwise trusted type.
func historicalBoundaryAllowances() map[string]historicalBoundaryAllowance {
	allowed := map[string]historicalBoundaryAllowance{
		historicalBoundaryOwner + "admitRunForkTerminalBarrierHistory/reference:runtime/runfork::NewTerminalBarrierHistory":                              {1, "only the complete fixed-revision barrier relation may mint terminal-history admission"},
		historicalBoundaryOwner + "loadRunForkAdmissionEvidenceFromRevision/reference:" + historicalBoundaryOwner + "admitRunForkTerminalBarrierHistory": {1, "all fixed-revision admission consumes the terminal relation"},
		historicalBoundaryOwner + "resolveRunForkRevisionPoint/ledger_sql":                                                                               {1, "shared event-point read; contextual admission precedes cursor construction"},
		historicalBoundaryOwner + "loadRunForkRevisionSnapshot/ledger_sql":                                                                               {1, "rank before filtering tombstones, admit surviving present facts"},
		historicalBoundaryOwner + "collectRunForkSourceAdvancedFacts/ledger_sql":                                                                         {1, "post-R family inventory, not payload decoding"},
		historicalBoundaryOwner + "ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision/ledger_sql":                                              {1, "post-R marker existence, not historical payload admission"},
		historicalBoundaryOwner + "ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling/ledger_sql":                                          {1, "current coupling revision safety, not historical payload admission"},
		historicalBoundaryWriter + "postgresAdapter.latestFacts/ledger_sql":                                                                              {1, "canonical latest equality owner"},
		historicalBoundaryWriter + "sqliteAdapter.latestFacts/ledger_sql":                                                                                {1, "canonical latest equality owner"},
		historicalBoundaryWriter + "postgresAdapter.insertFact/ledger_sql":                                                                               {1, "canonical ledger writer"},
		historicalBoundaryWriter + "sqliteAdapter.insertFact/ledger_sql":                                                                                 {1, "canonical ledger writer"},
		historicalBoundaryOwner + "appendRunForkHistoricalFact/raw_decode":                                                                               {2, "embedded owning-run check and contextual family decoding closure"},
		historicalBoundaryOwner + "appendRunForkHistoricalFact/reference:" + historicalBoundaryWriter + "FactKey":                                        {1, "historical admission consumes the writer's exact key relation"},
		historicalBoundaryWriter + "loadCanonicalProjection/reference:" + historicalBoundaryWriter + "FactKey":                                           {1, "canonical writer consumes that same key relation"},
		historicalBoundaryOwner + "appendRunForkHistoricalFact/reference:runtime/deliverylifecycle::DecodeHistoricalSnapshot":                            {1, "typed historical delivery decoding under contextual admission"},
		historicalBoundaryOwner + "resolveRunForkRevisionPoint/reference:" + historicalBoundaryOwner + "appendRunForkHistoricalFact":                     {1, "event cursor uses the same contextual relation"},
		historicalBoundaryOwner + "loadRunForkRevisionSnapshot/reference:" + historicalBoundaryOwner + "appendRunForkHistoricalFact":                     {1, "all present snapshot families use contextual admission"},
	}
	for _, caller := range []string{
		"resolveSQLiteRunForkRevisionPoint", "lockRunForkSourceRevisionFrontier", "lockSQLiteRunForkSourceRevisionFrontier",
		"RunForkPostgresOwner.EnsureRunForkNoPostForkCommittedReplayScopeMarkers", "RunForkSQLiteOwner.EnsureRunForkNoPostForkCommittedReplayScopeMarkers",
		"RunForkPostgresOwner.PlanRunFork", "RunForkPostgresOwner.LoadRunForkSelectedContractSourceEvents",
		"postgresRunForkSelectedContractActivationPort", "postgresRunForkSelectedContractMaterializationPort",
	} {
		allowed[historicalBoundaryOwner+caller+"/reference:"+historicalBoundaryOwner+"resolveRunForkRevisionPoint"] = historicalBoundaryAllowance{1, "exact shared contextual event-point consumer"}
	}
	for _, caller := range []string{
		"RunForkSQLiteOwner.PlanRunFork", "RunForkSQLiteOwner.ActivateRunFork", "RunForkSQLiteOwner.LoadRunForkSelectedContractSourceEvents",
		"sqliteRunForkSelectedContractActivationPort", "sqliteRunForkSelectedContractMaterializationPort",
	} {
		allowed[historicalBoundaryOwner+caller+"/reference:"+historicalBoundaryOwner+"resolveSQLiteRunForkRevisionPoint"] = historicalBoundaryAllowance{1, "thin SQLite adapter consumes the shared contextual event point"}
	}
	for edge, allowance := range inheritedFanOutBoundaryAllowances() {
		allowed[edge] = allowance
	}
	for edge, allowance := range receiverMaterializationBoundaryAllowances() {
		allowed[edge] = allowance
	}
	return allowed
}

// H19 is a bounded compiler guard, not a semantic SQL verifier or whole-program
// taint analysis. It detects typed historical decoding (including aliases and
// embedded records), raw ledger SQL at exact function scopes, and required owner
// edges. Dynamic SQL assembled from unrelated runtime fragments, unsafe/reflection,
// opaque cross-function erasure and same-count replacement of admitted operations
// remain behavioral-test obligations, not claims made by this test.
func TestRunForkHistoricalAdmissionConsumers(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	patterns, err := historicalBoundaryPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := packages.Load(&packages.Config{
		Dir: root, Tests: false,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(loaded) != 0 || len(loaded) != len(patterns) {
		t.Fatal("historical boundary requires successful compiler resolution of every discovered production package")
	}
	imports := historicalBoundaryImports{}
	packages.Visit(loaded, func(pkg *packages.Package) bool {
		if pkg.Types != nil {
			imports[pkg.PkgPath] = pkg.Types
		}
		return true
	}, nil)
	var findings []historicalBoundaryFinding
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			findings = append(findings, historicalBoundaryCollect(pkg.Types, pkg.TypesInfo, pkg.Fset, file)...)
		}
	}
	t.Run("production", func(t *testing.T) {
		problems := historicalBoundaryProblems(findings, historicalBoundaryAllowances(), true)
		owner := imports[historicalBoundaryModule+"store/internal/backend/runforkpersistence"]
		if err := historicalBoundarySignature(owner); err != nil {
			problems = append(problems, err.Error())
		}
		if len(problems) != 0 {
			sort.Strings(problems)
			t.Fatalf("historical admission bypass or stale owner census:\n%s", strings.Join(problems, "\n"))
		}
	})
	t.Run("closed_family_registry", func(t *testing.T) {
		want := []string{"agent_conversation_audits", "agent_sessions", "agent_turns", "committed_replay_scopes", "dead_letters", "entity_metadata", "entity_mutations", "event_deliveries", "event_receipts", "events", "fan_out_obligations", "reply_contexts", "timers"}
		var got []string
		for _, family := range runforkrevision.AllFamilies() {
			got = append(got, string(family))
			if !runforkrevision.ValidFamily(family) {
				t.Fatalf("registry listed invalid family %q", family)
			}
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) || runforkrevision.ValidFamily("unregistered") {
			t.Fatalf("historical registry = %v, want exact thirteen families %v", got, want)
		}
	})
	t.Run("hostile_compiler_resolved_consumers", func(t *testing.T) {
		historicalBoundaryHostile(t, imports)
	})
}

// Discover consumers before compiler loading so a newly introduced reader in a
// different production package cannot hide outside a fixed package allowlist.
func historicalBoundaryPackages(root string) ([]string, error) {
	dirs := map[string]bool{"./internal/events": true, "./internal/store/internal/backend/runforkpersistence": true, "./internal/store/internal/backend/runforkrevision": true}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" || entry.Name() == "vendor" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(raw, []byte("run_fork_fact_revisions")) && !bytes.Contains(raw, []byte("runforkpersistence")) && !bytes.Contains(raw, []byte("runforkrevision")) && !bytes.Contains(raw, []byte("DecodeHistoricalSnapshot")) && !bytes.Contains(raw, []byte("/runtime/runfork\"")) && !bytes.Contains(raw, []byte("/events\"")) && !bytes.Contains(raw, []byte("fanoutobligation")) && !bytes.Contains(raw, []byte("fanoutorigin")) && !bytes.Contains(raw, []byte("eventrecord")) && !bytes.Contains(raw, []byte("/runtime/bus\"")) {
			return nil
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		dirs["./"+filepath.ToSlash(relative)] = true
		return nil
	})
	var patterns []string
	for dir := range dirs {
		patterns = append(patterns, dir)
	}
	sort.Strings(patterns)
	return patterns, err
}

func historicalBoundaryFunction(fn *types.Func) string {
	if fn == nil || fn.Pkg() == nil {
		return ""
	}
	name := strings.TrimPrefix(fn.Pkg().Path(), historicalBoundaryModule) + "::"
	if receiver := fn.Type().(*types.Signature).Recv(); receiver != nil {
		typ := types.Unalias(receiver.Type())
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(pointer.Elem())
		}
		named, ok := typ.(*types.Named)
		if !ok {
			return "" // An anonymous interface method is not a named owner.
		}
		name += named.Obj().Name() + "."
	}
	return name + fn.Name()
}

func historicalBoundaryCollect(pkg *types.Package, info *types.Info, fset *token.FileSet, file *ast.File) []historicalBoundaryFinding {
	var findings []historicalBoundaryFinding
	for _, declaration := range file.Decls {
		scope := strings.TrimPrefix(pkg.Path(), historicalBoundaryModule) + "::<package>"
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			resolved, ok := info.Defs[fn.Name].(*types.Func)
			if !ok {
				panic("historical guard function was not compiler-resolved")
			}
			scope = historicalBoundaryFunction(resolved)
		}
		add := func(node ast.Node, kind string) {
			findings = append(findings, historicalBoundaryFinding{scope, kind, fset.Position(node.Pos()).String()})
		}
		ledgerReader := false
		ast.Inspect(declaration, func(node ast.Node) bool {
			expr, ok := node.(ast.Expr)
			if !ok {
				return true
			}
			value := info.Types[expr].Value
			if value == nil || value.Kind() != constant.String {
				return true
			}
			sql := strings.ToLower(constant.StringVal(value))
			if strings.Contains(sql, "run_fork_fact_revisions") && (strings.Contains(sql, "select") || strings.Contains(sql, "insert") || strings.Contains(sql, "update") || strings.Contains(sql, "delete")) {
				ledgerReader = true
				add(expr, "ledger_sql")
				return false // Count a folded constant expression once, not its fragments.
			}
			return true
		})
		aliases := historicalBoundaryFunctionAliases(declaration, info)
		factAliases := historicalBoundaryFactAliases(declaration, info)
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if n.Name.Name == "AppendRunForkRevisionFact" || n.Name.Name == "appendRunForkRevisionFact" {
					add(n, "contextless_append_declaration")
				}
			case *ast.Ident:
				fn, ok := info.Uses[n].(*types.Func)
				if !ok {
					break
				}
				callee := historicalBoundaryFunction(fn)
				if inheritedFanOutBoundaryReference(callee) || receiverMaterializationBoundaryReference(callee) {
					add(n, "reference:"+callee)
				}
				switch callee {
				case historicalBoundaryWriter + "FactKey", historicalBoundaryOwner + "appendRunForkHistoricalFact", "runtime/deliverylifecycle::DecodeHistoricalSnapshot",
					"runtime/runfork::NewTerminalBarrierHistory", historicalBoundaryOwner + "admitRunForkTerminalBarrierHistory",
					historicalBoundaryOwner + "resolveRunForkRevisionPoint", historicalBoundaryOwner + "resolveSQLiteRunForkRevisionPoint":
					add(n, "reference:"+callee)
				case historicalBoundaryOwner + "AppendRunForkRevisionFact", historicalBoundaryOwner + "appendRunForkRevisionFact":
					add(n, "contextless_append_reference")
				}
			case *ast.CallExpr:
				fn := historicalBoundaryCalled(n.Fun, info, aliases)
				if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != "encoding/json" {
					break
				}
				var target ast.Expr
				if fn.Name() == "Unmarshal" && len(n.Args) == 2 {
					target = n.Args[1]
				}
				if fn.Name() == "Decode" && len(n.Args) == 1 {
					target = n.Args[0]
				}
				if target != nil && (historicalBoundaryFactExpression(target, info, factAliases) || ledgerReader || scope == historicalBoundaryOwner+"appendRunForkHistoricalFact") {
					add(n, "raw_decode")
				}
			}
			return true
		})
	}
	return findings
}

func historicalBoundaryFactExpression(expr ast.Expr, info *types.Info, aliases map[types.Object]bool) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if expr, ok := node.(ast.Expr); ok && historicalBoundaryContainsFact(info.TypeOf(expr), map[types.Type]bool{}) {
			found = true
		}
		if id, ok := node.(*ast.Ident); ok && aliases[info.ObjectOf(id)] {
			found = true
		}
		return !found
	})
	return found
}

func historicalBoundaryFactAliases(declaration ast.Decl, info *types.Info) map[types.Object]bool {
	aliases := map[types.Object]bool{}
	for changed := true; changed; {
		changed = false
		bind := func(left ast.Expr, right ast.Expr) {
			id, ok := left.(*ast.Ident)
			if !ok || id.Name == "_" {
				return
			}
			object := info.ObjectOf(id)
			if !aliases[object] && historicalBoundaryFactExpression(right, info, aliases) {
				aliases[object], changed = true, true
			}
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for i, left := range n.Lhs {
					if i < len(n.Rhs) {
						bind(left, n.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				for i, left := range n.Names {
					if i < len(n.Values) {
						bind(left, n.Values[i])
					}
				}
			}
			return true
		})
	}
	return aliases
}

func historicalBoundaryContainsFact(typ types.Type, seen map[types.Type]bool) bool {
	if typ == nil || seen[typ] {
		return false
	}
	seen[typ] = true
	switch typ := types.Unalias(typ).(type) {
	case *types.Named:
		if object := typ.Obj(); object.Pkg() != nil && object.Pkg().Path() == historicalBoundaryModule+"store/internal/backend/runforkpersistence" {
			switch object.Name() {
			case "runForkRevisionSnapshot", "runForkRevisionedFact", "runForkHistoricalFactContext",
				"runForkRevisionEvent", "runForkRevisionEntityMutation", "runForkRevisionEntityMetadata",
				"runForkRevisionDelivery", "runForkRevisionCommittedReplayScope", "runForkRevisionReceipt",
				"runForkRevisionDeadLetter", "runForkRevisionTimer", "runForkRevisionSession",
				"runForkRevisionTurn", "runForkRevisionConversationAudit", "runForkRevisionReplyContext", "runForkRevisionFanOutFact":
				return true
			}
		}
		return historicalBoundaryContainsFact(typ.Underlying(), seen)
	case *types.Pointer:
		return historicalBoundaryContainsFact(typ.Elem(), seen)
	case *types.Array:
		return historicalBoundaryContainsFact(typ.Elem(), seen)
	case *types.Slice:
		return historicalBoundaryContainsFact(typ.Elem(), seen)
	case *types.Map:
		return historicalBoundaryContainsFact(typ.Key(), seen) || historicalBoundaryContainsFact(typ.Elem(), seen)
	case *types.Struct:
		for index := 0; index < typ.NumFields(); index++ {
			if historicalBoundaryContainsFact(typ.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Signature:
		for _, tuple := range []*types.Tuple{typ.Params(), typ.Results()} {
			for i := 0; i < tuple.Len(); i++ {
				if historicalBoundaryContainsFact(tuple.At(i).Type(), seen) {
					return true
				}
			}
		}
	}
	return false
}

func historicalBoundaryCalled(expr ast.Expr, info *types.Info, aliases map[types.Object]*types.Func) *types.Func {
	var object types.Object
	switch expr := expr.(type) {
	case *ast.ParenExpr:
		return historicalBoundaryCalled(expr.X, info, aliases)
	case *ast.Ident:
		object = info.ObjectOf(expr)
	case *ast.SelectorExpr:
		object = info.Uses[expr.Sel]
	}
	if fn, ok := object.(*types.Func); ok {
		return fn
	}
	return aliases[object]
}

func historicalBoundaryFunctionAliases(declaration ast.Decl, info *types.Info) map[types.Object]*types.Func {
	aliases := map[types.Object]*types.Func{}
	for changed := true; changed; {
		changed = false
		bind := func(left ast.Expr, right ast.Expr) {
			id, ok := left.(*ast.Ident)
			if !ok || id.Name == "_" {
				return
			}
			object := info.ObjectOf(id)
			if fn := historicalBoundaryCalled(right, info, aliases); fn != nil && aliases[object] == nil {
				aliases[object] = fn
				changed = true
			}
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if i < len(n.Rhs) {
						bind(lhs, n.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) {
						bind(name, n.Values[i])
					}
				}
			}
			return true
		})
	}
	return aliases
}

func historicalBoundarySignature(pkg *types.Package) error {
	if pkg == nil {
		return fmt.Errorf("historical owner package missing")
	}
	for _, name := range pkg.Scope().Names() {
		object := pkg.Scope().Lookup(name)
		if object.Exported() && historicalBoundaryContainsFact(object.Type(), map[types.Type]bool{}) {
			return fmt.Errorf("historical snapshot/fact representation must stay private: %s", name)
		}
		if name == "AppendRunForkHistoricalFact" || name == "AppendRunForkRevisionFact" || name == "LoadRunForkPendingWorkFromRevision" || name == "LoadRunForkSourceFactsFromRevision" {
			return fmt.Errorf("retired public historical bypass survives: %s", name)
		}
	}
	fn, ok := pkg.Scope().Lookup("appendRunForkHistoricalFact").(*types.Func)
	if !ok {
		return fmt.Errorf("private contextual appendRunForkHistoricalFact is required; no contextless compatibility helper")
	}
	signature := fn.Type().(*types.Signature)
	if signature.Params().Len() != 3 {
		return fmt.Errorf("appendRunForkHistoricalFact must take snapshot, historical context, raw fact")
	}
	context := types.Unalias(signature.Params().At(1).Type())
	named, ok := context.(*types.Named)
	if !ok || named.Obj().Pkg() != pkg || named.Obj().Name() != "runForkHistoricalFactContext" {
		return fmt.Errorf("appendRunForkHistoricalFact must consume the private value context")
	}
	return nil
}

func historicalBoundaryProblems(findings []historicalBoundaryFinding, allowances map[string]historicalBoundaryAllowance, requireAll bool) []string {
	counts := map[string]int{}
	var problems []string
	for _, finding := range findings {
		key := finding.Scope + "/" + finding.Kind
		counts[key]++
		if _, allowed := allowances[key]; !allowed {
			problems = append(problems, key+" at "+finding.Site)
		}
	}
	for key, allowance := range allowances {
		if allowance.Reason == "" || counts[key] > allowance.Count || requireAll && counts[key] != allowance.Count {
			problems = append(problems, fmt.Sprintf("%s: observed %d, audited %d (%s)", key, counts[key], allowance.Count, allowance.Reason))
		}
	}
	sort.Strings(problems)
	return problems
}

type historicalBoundaryImports map[string]*types.Package

func (imports historicalBoundaryImports) Import(path string) (*types.Package, error) {
	if pkg := imports[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("fixture dependency %q was not compiler-loaded", path)
}

func historicalBoundaryHostile(t *testing.T, imports historicalBoundaryImports) {
	t.Helper()
	const source = `package runforkpersistence
import (
    codec "encoding/json"
    "bytes"
    delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
    history "github.com/division-sh/swarm/internal/runtime/runfork"
    events "github.com/division-sh/swarm/internal/events"
    fanout "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
    bus "github.com/division-sh/swarm/internal/runtime/bus"
)
type unexpectedReader struct{}
// Private package-local stand-in: no exported historical compatibility seam.
type runForkRevisionEvent struct { EventID string }
type eventAlias = runForkRevisionEvent
func (arbitrary *unexpectedReader) mintTerminalHistory() {
    alias := history.NewTerminalBarrierHistory
    _ = alias
}
func (arbitrary *unexpectedReader) mintOrigin() {
    mint := events.NewInheritedFanOutOrigin
    alias := mint
    _ = alias
    prepare := fanout.PrepareOrdinalEmission
    _ = prepare
}
func (arbitrary *unexpectedReader) admitOrigin(other bus.PublicationCommand) error {
    return other.ValidateFanOut()
}
func (arbitrary *unexpectedReader) mintReceiverPlan() {
    mint := events.AdmitReceiverMaterializationPlan
    alias := mint
    _ = alias
}
func (arbitrary *unexpectedReader) decode(raw []byte) error {
    var fact eventAlias
    unmarshal := codec.Unmarshal
    alias := unmarshal
    return alias(raw, &fact)
}
func (arbitrary *unexpectedReader) appendRunForkHistoricalFact(raw []byte) error {
    var wrapper struct { runForkRevisionEvent }
    return codec.NewDecoder(bytes.NewReader(raw)).Decode(&wrapper)
}
func (arbitrary *unexpectedReader) erased(raw []byte) error {
    var fact runForkRevisionEvent
    var target any = &fact
    return codec.Unmarshal(raw, target)
}
func (arbitrary *unexpectedReader) delivery(raw []byte) error {
    _, err := delivery.DecodeHistoricalSnapshot(raw)
    return err
}
func (arbitrary *unexpectedReader) cursor() string {
    return "SELECT MIN(revision) FROM " + "run_fork_fact_revisions WHERE fact_key = $1"
}
// Even the exact approved function cannot add an unadmitted decode.
func loadRunForkRevisionSnapshot(raw []byte) error {
    query := "SELECT fact FROM run_fork_fact_revisions"
    _ = query
    var erased map[string]any
    return codec.Unmarshal(raw, &erased)
}
// Positive: the contextual owner has one raw JSON decoding operation.
func appendRunForkHistoricalFact(raw []byte) error {
    var wrapper struct { runForkRevisionEvent }
    return codec.Unmarshal(raw, &wrapper)
}
func ordinaryBusiness(raw []byte) error {
    var business struct { EventID string }
    return codec.Unmarshal(raw, &business)
}
`
	fset := token.NewFileSet()
	// Deliberately use the real approved filename, without writing an overlay.
	file, err := parser.ParseFile(fset, "run_fork_revision_snapshot.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	pkg, err := (&types.Config{Importer: imports}).Check(historicalBoundaryModule+"store/internal/backend/runforkpersistence", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("hostile fixture must type-check: %v", err)
	}
	findings := historicalBoundaryCollect(pkg, info, fset, file)
	got := historicalBoundaryProblems(findings, historicalBoundaryAllowances(), false)
	want := []string{
		historicalBoundaryOwner + "unexpectedReader.mintReceiverPlan/reference:events::AdmitReceiverMaterializationPlan",
		historicalBoundaryOwner + "unexpectedReader.mintOrigin/reference:events::NewInheritedFanOutOrigin",
		historicalBoundaryOwner + "unexpectedReader.mintOrigin/reference:runtime/fanoutobligation::PrepareOrdinalEmission",
		historicalBoundaryOwner + "unexpectedReader.admitOrigin/reference:runtime/bus::PublicationCommand.ValidateFanOut",
		historicalBoundaryOwner + "unexpectedReader.mintTerminalHistory/reference:runtime/runfork::NewTerminalBarrierHistory",
		historicalBoundaryOwner + "unexpectedReader.decode/raw_decode",
		historicalBoundaryOwner + "unexpectedReader.appendRunForkHistoricalFact/raw_decode",
		historicalBoundaryOwner + "unexpectedReader.erased/raw_decode",
		historicalBoundaryOwner + "unexpectedReader.delivery/reference:runtime/deliverylifecycle::DecodeHistoricalSnapshot",
		historicalBoundaryOwner + "unexpectedReader.cursor/ledger_sql",
		historicalBoundaryOwner + "loadRunForkRevisionSnapshot/raw_decode",
	}
	if len(got) != len(want) {
		t.Fatalf("hostile findings = %v, want exactly %v", got, want)
	}
	for _, expected := range want {
		matched := false
		for _, actual := range got {
			matched = matched || strings.HasPrefix(actual, expected+" at ")
		}
		if !matched {
			t.Errorf("missing hostile finding %s in %v", expected, got)
		}
	}
	// Prove an extra decode in the owner itself exceeds the approved call count.
	duplicate := historicalBoundaryFinding{historicalBoundaryOwner + "appendRunForkHistoricalFact", "raw_decode", "same approved function:extra decode"}
	if got := historicalBoundaryProblems([]historicalBoundaryFinding{duplicate, duplicate, duplicate}, historicalBoundaryAllowances(), false); len(got) != 1 || !strings.Contains(got[0], "observed 3, audited 2") {
		t.Fatalf("same-owner duplicate decoding must fail: %v", got)
	}
}
