// fixture-codemod migrates finite, audited fixture patterns. It is not a SQL
// translator: a match requires the original selected owner and an exact oracle.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/tools/go/packages"
)

const conformancePackage = "github.com/division-sh/swarm/internal/runtime/conformance"
const storetestPackage = "github.com/division-sh/swarm/internal/store/storetest"
const serveappPackage = "github.com/division-sh/swarm/internal/serveapp"
const externalRuntimePackage = "github.com/division-sh/swarm/internal/runtime_test"

type change struct {
	File        string `json:"file"`
	Function    string `json:"function"`
	Line        int    `json:"line"`
	Field       string `json:"field"`
	start, end  int
	replacement string
}

func main() {
	write := flag.Bool("write", false, "apply exact matched rewrites (default: report only)")
	census := flag.String("classify", "", "classify an existing authority-census JSON file without changing source")
	propagate := flag.Bool("propagate-served-diagnostics", false, "propagate original-owner parameters through the finite served diagnostic helper family")
	only := flag.String("only", "", "comma-separated audited rewrite families to apply")
	flag.Parse()
	if *propagate {
		if *census != "" || flag.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "propagation takes no package or census")
			os.Exit(2)
		}
		path := "internal/serveapp/main_runtime_test.go"
		source, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		updated, count, err := propagateServedDiagnosticOwner(source)
		if err != nil {
			panic(err)
		}
		if *write && count != 0 {
			if err := os.WriteFile(path, updated, 0644); err != nil {
				panic(err)
			}
		}
		if err := json.NewEncoder(os.Stdout).Encode(struct {
			Helpers int  `json:"helpers"`
			Write   bool `json:"write"`
		}{count, *write}); err != nil {
			panic(err)
		}
		return
	}
	if *census != "" {
		if *write || flag.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "-classify cannot be combined with a rewrite")
			os.Exit(2)
		}
		if err := classify(*census); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if flag.NArg() != 1 || (flag.Arg(0) != "./internal/runtime/conformance" && flag.Arg(0) != "./internal/serveapp" && flag.Arg(0) != "./internal/runtime") {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/fixture-codemod [-write] ./internal/{runtime,runtime/conformance,serveapp}")
		os.Exit(2)
	}
	started := time.Now()
	selected := map[string]bool{}
	for _, family := range strings.Split(*only, ",") {
		if family != "" {
			selected[family] = true
		}
	}
	changes, err := migrateSelected(flag.Arg(0), *write, selected)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result := struct {
		Write          bool     `json:"write"`
		ElapsedSeconds float64  `json:"elapsed_seconds"`
		Changes        []change `json:"changes"`
	}{*write, time.Since(started).Seconds(), changes}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}

func classify(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var findings []struct{ Kind, Member string }
	if err := json.Unmarshal(data, &findings); err != nil {
		return err
	}
	counts := map[string]int{}
	for _, finding := range findings {
		if finding.Kind != "raw-operation" {
			continue
		}
		member := finding.Member
		if index := strings.LastIndex(member, "#"); index >= 0 {
			if _, err := strconv.Atoi(member[index+1:]); err == nil {
				member = member[:index]
			}
		}
		counts[patternClass(member)]++
	}
	return json.NewEncoder(os.Stdout).Encode(counts)
}

func patternClass(member string) string {
	if strings.HasPrefix(member, "call:database/sql.") {
		method := member[strings.LastIndex(member, ".")+1:]
		switch method {
		case "Scan", "Next", "Err", "Columns":
			return "cursor-plumbing"
		case "Close":
			if strings.Contains(member, "sql.Rows)") {
				return "cursor-plumbing"
			}
			return "open-and-connection-lifetime"
		case "QueryRow", "QueryRowContext", "Query", "QueryContext":
			return "sql-read-endpoints"
		case "Exec", "ExecContext", "RowsAffected":
			return "sql-write-endpoints"
		case "Begin", "BeginTx", "Commit", "Rollback", "PrepareContext":
			return "transaction-protocol"
		case "Open", "OpenDB", "Ping", "PingContext", "Conn", "Stats", "SetMaxOpenConns", "SetMaxIdleConns", "Driver":
			return "open-and-connection-lifetime"
		default:
			return "unclassified-sql"
		}
	}
	switch member {
	case "call:github.com/division-sh/swarm/internal/store/storetest.DatabaseForTest",
		"call:github.com/division-sh/swarm/internal/store/storetest.Database",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.(*github.com/division-sh/swarm/internal/runtime/pipeline.workflowInstanceStore).testDB":
		return "generic-raw-getter-calls"
	case "call:github.com/division-sh/swarm/internal/testutil.StartPostgres",
		"call:github.com/division-sh/swarm/internal/store/storetest.AdmitPostgresRuntimeStore",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newPostgresWorkflowInstanceStoreForTest",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newSQLiteWorkflowInstanceStoreTestDB",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newSQLiteWorkflowInstanceStoreForTest",
		"call:github.com/division-sh/swarm/internal/serveapp.selectedRuntimeStoreForTest":
		return "raw-setup-helper-calls"
	default:
		return "other-raw-bearing-shared-helper-calls"
	}
}

func migrateSelected(pattern string, write bool, selected map[string]bool) ([]change, error) {
	root, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	pkgs, err := packages.Load(&packages.Config{
		Tests: true,
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, pattern)
	if err != nil {
		return nil, err
	}
	if packages.PrintErrors(pkgs) != 0 {
		return nil, fmt.Errorf("package type checking failed; no files changed")
	}
	changes := []change{}
	seen := map[string]bool{}
	packagePath := conformancePackage
	if pattern == "./internal/serveapp" {
		packagePath = serveappPackage
	}
	if pattern == "./internal/runtime" {
		packagePath = externalRuntimePackage
	}
	for _, pkg := range pkgs {
		if pkg.Types == nil || pkg.Types.Path() != packagePath {
			continue
		}
		unusedDatabaseFamily := ignoredConformanceDatabaseFamily(pkg.TypesInfo, pkg.Syntax)
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if seen[filename] || !strings.HasSuffix(filename, "_test.go") {
				continue
			}
			seen[filename] = true
			alias := storetestAlias(file)
			original, err := os.ReadFile(filename)
			if err != nil {
				return nil, err
			}
			var edits []change
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				if packagePath == conformancePackage {
					if replacement, field, matched := rewriteSelectedConformanceOwner(pkg.Fset, pkg.TypesInfo, fn, selected, unusedDatabaseFamily); matched {
						position := pkg.Fset.Position(fn.Pos())
						relative, err := filepath.Rel(root, filename)
						if err != nil {
							return nil, err
						}
						edits = append(edits, change{File: relative, Function: fn.Name.Name, Line: position.Line, Field: field, start: position.Offset, end: pkg.Fset.Position(fn.End()).Offset, replacement: replacement})
						continue
					}
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					expr := func(node ast.Node) string {
						var out bytes.Buffer
						if err := format.Node(&out, pkg.Fset, node); err != nil {
							panic(err)
						}
						return out.String()
					}
					field, replacement := "", ""
					callee, _ := call.Fun.(*ast.Ident)
					var calledFunction *types.Func
					if callee != nil {
						calledFunction, _ = pkg.TypesInfo.Uses[callee].(*types.Func)
					}
					if unusedDatabaseFamily[calledFunction] && ignoredConformanceDatabaseCall(pkg.TypesInfo, call) {
						args := []string{}
						for i, arg := range call.Args {
							if i != 2 {
								args = append(args, expr(arg))
							}
						}
						if call.Ellipsis.IsValid() {
							args[len(args)-1] += "..."
						}
						field, replacement = "UnusedConformanceDatabase", fmt.Sprintf("%s(%s)", expr(call.Fun), strings.Join(args, ", "))
					} else if owner, ok := matchConformanceMutationColumnOwner(pkg.TypesInfo, fn, call); ok {
						field, replacement = "CanonicalMutationColumnOwner", fmt.Sprintf("requireMutationSurface(%s, %s)", expr(call.Args[0]), owner)
					} else if index, ok := matchNotifyExecutionCaller(pkg.TypesInfo, call); ok {
						args := []string{}
						for i, arg := range call.Args {
							if i != index {
								args = append(args, expr(arg))
							}
						}
						if call.Ellipsis.IsValid() {
							args[len(args)-1] += "..."
						}
						field, replacement = "NotifyExecutionReadOwner", fmt.Sprintf("%s(%s)", expr(call.Fun), strings.Join(args, ", "))
					} else if helper, ok := matchNotifyObserverOwner(pkg.TypesInfo, call); ok {
						args := []string{expr(call.Args[0]), expr(call.Args[1]), expr(call.Args[2])}
						for _, arg := range call.Args[4:] {
							args = append(args, expr(arg))
						}
						field, replacement = helper, fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
					} else if helper, ok := matchNotifyRuntimeOwner(pkg.TypesInfo, call); ok {
						args := []string{expr(call.Args[0]), expr(call.Args[1])}
						for _, arg := range call.Args[3:] {
							args = append(args, expr(arg))
						}
						if call.Ellipsis.IsValid() {
							args[len(args)-1] += "..."
						}
						field, replacement = helper, fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
					} else if owner, ok := matchRuntimeNodeDeliveryCount(pkg.TypesInfo, call); ok {
						field = "RuntimeNodeDeliveryCount"
						replacement = fmt.Sprintf("assertRuntimeNodeDeliveryCount(%s, %s, %s, %s, %s, %s)", expr(call.Args[0]), expr(call.Args[1]), owner, expr(call.Args[5]), expr(call.Args[6]), expr(call.Args[4]))
					} else if owner, helper, ok := matchChannelDispositionOwner(pkg.TypesInfo, call); ok {
						args := []string{expr(call.Args[0]), owner}
						for _, arg := range call.Args[2:] {
							args = append(args, expr(arg))
						}
						if call.Ellipsis.IsValid() {
							args[len(args)-1] += "..."
						}
						field, replacement = helper, fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
					} else if matchSemanticNumericOwner(pkg.TypesInfo, call) {
						field, replacement = "semanticNumericOutput", fmt.Sprintf("semanticNumericOutput(%s, %s, %s, %s)", expr(call.Args[0]), expr(call.Args[1]), expr(call.Args[3]), expr(call.Args[4]))
					} else if helper, ok := matchServedEntityStateOwner(pkg.TypesInfo, call); ok {
						args := []string{expr(call.Args[0])}
						for _, arg := range call.Args[2:] {
							args = append(args, expr(arg))
						}
						field, replacement = helper, fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
					} else if owner, helper, index, replace, ok := matchServedDatabaseOwner(pkg.TypesInfo, call); ok {
						original, test := expr(owner), expr(call.Args[0])
						selected := fmt.Sprintf("forkReceiverSelectedFixtureOwner(%s, %s.Backend, %s.Postgres, %s.SQLite)", test, original, original, original)
						args := []string{}
						for i, arg := range call.Args {
							if i == index && replace {
								args = append(args, selected)
							} else {
								args = append(args, expr(arg))
								if i == index {
									args = append(args, selected)
								}
							}
						}
						if call.Ellipsis.IsValid() {
							args[len(args)-1] += "..."
						}
						field, replacement = helper, fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
					} else if owner, helper, ok := matchServedOwnerProjection(pkg.TypesInfo, call); ok {
						original := expr(owner)
						test := expr(call.Args[0])
						if helper == "selectedMailboxFixtureStore" {
							test = "t"
						}
						selected := fmt.Sprintf("forkReceiverSelectedFixtureOwner(%s, %s.Backend, %s.Postgres, %s.SQLite)", test, original, original, original)
						switch helper {
						case "readLifecycleLoop", "requireA2PortfolioEntity", "requireA2PortfolioEvents", "requireA2PortfolioEvent", "requireA2PortfolioEmission", "issue2394SurfaceCLI", "issue2394ReporterRPC", "lifecycleDecisionParamsForCard", "proveIssue2394SurfaceEvents":
							selected = original + ".Endpoint"
						case "a2ReadJoinPublicObligation", "a2ReadJoinPublicEvent", "a2WaitJoinPublicRun", "a2RequireJoinPublicClients", "a2JoinPublicCLI", "a2RequireJoinPublicTrace":
							selected = original + ".Endpoint"
						case "a2RequireJoinPublicGraph":
							selected = original + ".BundleHash"
						}
						field, replacement = "SelectedOwner", selected
						if helper != "forkReceiverSelectedFixtureOwner" && helper != "selectedMailboxFixtureStore" {
							field = helper
							args := []string{expr(call.Args[0]), selected}
							if helper == "servedControlProofAuthorActivityContext" {
								args = []string{expr(call.Args[0]), original + ".Runtime", original + ".BundleHash"}
							}
							if helper == "requireReceiverPublicReadback" {
								args = []string{expr(call.Args[0]), original + ".Endpoint", selected}
							}
							if helper == "lifecycleGateDecisionParams" || helper == "waitLifecycleGateCard" || helper == "awaitIssue2394SurfaceDiagnosis" {
								args = []string{expr(call.Args[0]), original + ".Endpoint", selected, original + ".Backend"}
							}
							if helper == "proveIssue2394SurfacePages" || helper == "proveIssue2394SurfaceDiagnostics" || helper == "proveIssue2394SurfaceRefusals" {
								args = []string{expr(call.Args[0]), original + ".Endpoint", original + ".BundleHash"}
							}
							if helper == "requireSelectedForkDurablePublicReads" || helper == "requireSelectedForkDeclaredAgentReads" {
								args = []string{expr(call.Args[0]), original + ".Endpoint", original + ".BundleHash", selected}
							}
							if helper == "installMailboxCompletionFaultWitness" {
								args = []string{expr(call.Args[0]), original + ".Backend", selected}
							}
							if helper == "waitForkReceiverSourceCompletion" || helper == "waitPublicationSiteCompletion" || helper == "mailboxCompletionRunEffects" {
								args = []string{expr(call.Args[0]), selected, original + ".Backend"}
							}
							if helper == "requireLifecycleFlowEntity" {
								args = []string{expr(call.Args[0]), selected, original + ".Backend"}
							}
							for _, arg := range call.Args[2:] {
								args = append(args, expr(arg))
							}
							if call.Ellipsis.IsValid() {
								args[len(args)-1] += "..."
							}
							replacement = fmt.Sprintf("%s(%s)", helper, strings.Join(args, ", "))
						}
					} else if matchedField, receiver, query, destination, ok := match(pkg.TypesInfo, call); ok && alias != "" {
						field = matchedField
						replacement = fmt.Sprintf("func() error { evidence, err := %s.ReadSelectedForkHistoryCardinality(%s, %s.selected, %s); if err != nil { return err }; %s = evidence.%s; return nil }()", alias, expr(query.Args[0]), expr(receiver), expr(query.Args[2]), expr(destination.X), field)
					} else {
						return true
					}
					if len(selected) != 0 && !selected[field] {
						return true
					}
					pos := pkg.Fset.Position(call.Pos())
					relative, err := filepath.Rel(root, filename)
					if err != nil {
						panic(err)
					}
					edits = append(edits, change{File: relative, Function: fn.Name.Name, Line: pos.Line, Field: field, start: pos.Offset, end: pkg.Fset.Position(call.End()).Offset, replacement: replacement})
					return false
				})
			}
			changes = append(changes, edits...)
			if !write || len(edits) == 0 {
				continue
			}
			sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
			updated := original
			for _, edit := range edits {
				updated = append(append(append([]byte{}, updated[:edit.start]...), []byte(edit.replacement)...), updated[edit.end:]...)
			}
			updated, err = format.Source(updated)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(filename, updated, 0o644); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].File == changes[j].File {
			return changes[i].Line < changes[j].Line
		}
		return changes[i].File < changes[j].File
	})
	return changes, nil
}

func rewriteSelectedConformanceOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, selected map[string]bool, unusedDatabaseFamily map[*types.Func]bool) (string, string, bool) {
	if len(selected) == 0 || selected["ConformanceMutationProjection"] {
		if replacement, matched := rewriteConformanceMutationProjectionOwner(fset, info, fn); matched {
			return replacement, "ConformanceMutationProjection", true
		}
	}
	if len(selected) == 0 || selected["UnusedConformanceDatabase"] {
		if replacement, matched := rewriteIgnoredConformanceDatabase(fset, info, fn, unusedDatabaseFamily); matched {
			return replacement, "UnusedConformanceDatabase", true
		}
	}
	if len(selected) == 0 || selected["CanonicalStorageColumns"] {
		if replacement, matched := rewriteConformanceColumnOwner(fset, info, fn); matched {
			return replacement, "CanonicalStorageColumns", true
		}
	}
	return "", "", false
}

func matchNotifyObserverOwner(info *types.Info, call *ast.CallExpr) (string, bool) {
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", false
	}
	fn, ok := info.Uses[name].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != conformancePackage {
		return "", false
	}
	count := 0
	switch fn.Name() {
	case "dumpNotifyAllChildrenRuntimeState":
		count = 4
	case "loadNotifyAllChildrenItemEvents":
		count = 6
	case "assertNotifyAllChildrenMetadata":
		count = 7
	default:
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Variadic() || sig.Params().Len() != count || len(call.Args) != count || call.Ellipsis.IsValid() || !pureFixtureReference(call.Args[3]) {
		return "", false
	}
	if types.TypeString(sig.Params().At(3).Type(), func(p *types.Package) string { return p.Path() }) != "*database/sql.DB" {
		return "", false
	}
	return fn.Name(), true
}

func matchNotifyRuntimeOwner(info *types.Info, call *ast.CallExpr) (string, bool) {
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", false
	}
	fn, ok := info.Uses[name].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != conformancePackage || fn.Name() != "newNotifyAllChildrenRuntime" {
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Params().Len() != 6 || !sig.Variadic() || len(call.Args) < 5 {
		return "", false
	}
	if types.TypeString(sig.Params().At(2).Type(), func(p *types.Package) string { return p.Path() }) != "*database/sql.DB" || !pureFixtureReference(call.Args[2]) {
		return "", false
	}
	return fn.Name(), true
}

func matchRuntimeNodeDeliveryCount(info *types.Info, call *ast.CallExpr) (string, bool) {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) != 7 || call.Ellipsis.IsValid() || !pureFixtureReference(call.Args[2]) {
		return "", false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != externalRuntimePackage || fn.Name() != "assertRuntimeDBCount" {
		return "", false
	}
	signature, ok := fn.Type().(*types.Signature)
	if !ok || !signature.Variadic() || signature.Params().Len() != 6 {
		return "", false
	}
	db, ok := signature.Params().At(2).Type().(*types.Pointer)
	if !ok {
		return "", false
	}
	dbName, ok := db.Elem().(*types.Named)
	if !ok || dbName.Obj().Pkg() == nil || dbName.Obj().Pkg().Path() != "database/sql" || dbName.Obj().Name() != "DB" {
		return "", false
	}
	query, ok := call.Args[3].(*ast.BasicLit)
	if !ok || query.Kind != token.STRING {
		return "", false
	}
	sqlText, err := strconv.Unquote(query.Value)
	if err != nil || strings.Join(strings.Fields(sqlText), " ") != "SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2" {
		return "", false
	}
	// The rewritten call moves want after event/recipient. Only an integer
	// literal may move: expressions with effects retain their original order.
	want, ok := call.Args[4].(*ast.BasicLit)
	if !ok || want.Kind != token.INT {
		return "", false
	}
	var nearest *types.Scope
	for _, scope := range info.Scopes {
		if scope.Contains(call.Pos()) && (nearest == nil || nearest.Pos() <= scope.Pos() && scope.End() <= nearest.End()) {
			nearest = scope
		}
	}
	if nearest == nil {
		return "", false
	}
	_, owner := nearest.LookupParent("pg", call.Pos())
	if owner == nil {
		return "", false
	}
	pointer, ok := owner.Type().(*types.Pointer)
	if !ok {
		return "", false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "github.com/division-sh/swarm/internal/store/internal/runtimepersistence" || named.Obj().Name() != "PostgresStore" {
		return "", false
	}
	return "pg", true
}

// The audited channel family carries h and its observer DB together. Resolve
// that exact harness in lexical scope; never manufacture an owner from the DB.
func matchChannelDispositionOwner(info *types.Info, call *ast.CallExpr) (string, string, bool) {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) < 2 || !pureFixtureReference(call.Args[1]) {
		return "", "", false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != serveappPackage || (fn.Name() != "waitObjectChannelDisposition" && fn.Name() != "proveObjectRejectedControl" && fn.Name() != "waitChannelAnchorCard" && fn.Name() != "waitChannelAnchorReceipt" && fn.Name() != "waitChannelAnchorDecision" && fn.Name() != "waitNativeIntentCount" && fn.Name() != "waitChannelDeliverySendsSettled" && fn.Name() != "selectedChannelDraftState") {
		return "", "", false
	}
	signature, ok := fn.Type().(*types.Signature)
	if !ok || (fn.Name() == "waitObjectChannelDisposition" && signature.Params().Len() != 5) || (fn.Name() == "proveObjectRejectedControl" && signature.Params().Len() != 6) || (fn.Name() == "waitChannelAnchorCard" && (signature.Params().Len() != 4 || len(call.Args) != 4 || call.Ellipsis.IsValid())) {
		return "", "", false
	}
	if (fn.Name() == "waitChannelAnchorReceipt" || fn.Name() == "waitChannelAnchorDecision" || fn.Name() == "selectedChannelDraftState") && (signature.Params().Len() != 3 || len(call.Args) != 3 || call.Ellipsis.IsValid()) {
		return "", "", false
	}
	if fn.Name() == "waitNativeIntentCount" && (signature.Params().Len() != 4 || len(call.Args) != 4 || call.Ellipsis.IsValid()) {
		return "", "", false
	}
	if fn.Name() == "waitChannelDeliverySendsSettled" && (signature.Params().Len() != 2 || len(call.Args) != 2 || call.Ellipsis.IsValid()) {
		return "", "", false
	}
	pointer, ok := signature.Params().At(1).Type().(*types.Pointer)
	if !ok {
		return "", "", false
	}
	named, ok := pointer.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "database/sql" || named.Obj().Name() != "DB" {
		return "", "", false
	}
	if fn.Name() == "waitObjectChannelDisposition" {
		literal, ok := call.Args[2].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return "", "", false
		}
		table, err := strconv.Unquote(literal.Value)
		if err != nil || (table != "operator_channel_action_intents" && table != "operator_channel_text_intents") {
			return "", "", false
		}
	}
	var nearest *types.Scope
	for _, scope := range info.Scopes {
		if scope.Contains(call.Pos()) && (nearest == nil || nearest.Pos() <= scope.Pos() && scope.End() <= nearest.End()) {
			nearest = scope
		}
	}
	if nearest == nil {
		return "", "", false
	}
	_, owner := nearest.LookupParent("h", call.Pos())
	if owner == nil {
		return "", "", false
	}
	pointer, ok = owner.Type().(*types.Pointer)
	if !ok {
		return "", "", false
	}
	named, ok = pointer.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != serveappPackage || named.Obj().Name() != "channelOnboardingE2EHarness" {
		return "", "", false
	}
	return "h", fn.Name(), true
}

// This family already carries its audited owner. Drop only the obsolete pure
// DB argument; do not infer ownership or discard an effectful expression.
func matchServedEntityStateOwner(info *types.Info, call *ast.CallExpr) (string, bool) {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) < 3 || call.Ellipsis.IsValid() || !pureFixtureReference(call.Args[1]) {
		return "", false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != serveappPackage {
		return "", false
	}
	var tail []string
	switch fn.Name() {
	case "requireServedEventPublishEntityState":
		tail = []string{"backend", "runID", "entityID", "wantState"}
	case "waitServedEventPublishDeliveryStatusCountForRun":
		tail = []string{"backend", "runID", "eventID", "subscriberType", "subscriberID", "status", "want"}
	case "waitServedEventPublishReceiptOutcomeCount":
		tail = []string{"backend", "eventID", "subscriberType", "subscriberID", "outcome", "want"}
	case "waitForServedEventPublishNodeDeliveryLifecycleForNode":
		tail = []string{"backend", "runID", "eventID", "nodeID", "probe"}
	case "waitForServedEventPublishNodeDeliveryLifecycle":
		tail = []string{"backend", "runID", "eventID", "probe"}
	case "requireServedEventPublishPreHandlerProof":
		tail = []string{"backend", "proofs", "runID", "eventID", "nodeID"}
	default:
		return "", false
	}
	signature, ok := fn.Type().(*types.Signature)
	if !ok || signature.Params().Len() != len(tail)+3 || len(call.Args) != len(tail)+3 {
		return "", false
	}
	params := signature.Params()
	db, ok := params.At(1).Type().(*types.Pointer)
	if !ok {
		return "", false
	}
	named, ok := db.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "database/sql" || named.Obj().Name() != "DB" || params.At(1).Name() != "db" || params.At(2).Name() != "selected" {
		return "", false
	}
	selected, ok := params.At(2).Type().Underlying().(*types.Interface)
	if !ok || !selected.Empty() {
		return "", false
	}
	for i, name := range tail {
		param := params.At(i + 3)
		if param.Name() != name {
			return "", false
		}
		if name == "probe" {
			pointer, ok := param.Type().(*types.Pointer)
			if !ok {
				return "", false
			}
			named, ok := pointer.Elem().(*types.Named)
			if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest" || named.Obj().Name() != "Probe" {
				return "", false
			}
			continue
		}
		if name == "proofs" {
			channel, ok := param.Type().(*types.Chan)
			if !ok || channel.Dir() != types.RecvOnly {
				return "", false
			}
			named, ok := channel.Elem().(*types.Named)
			if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != serveappPackage || named.Obj().Name() != "servedEventPublishPreHandlerProof" {
				return "", false
			}
			continue
		}
		kind := types.String
		if name == "want" {
			kind = types.Int
		}
		if !types.Identical(param.Type(), types.Typ[kind]) {
			return "", false
		}
	}
	return fn.Name(), true
}

func matchSemanticNumericOwner(info *types.Info, call *ast.CallExpr) bool {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) != 5 || call.Ellipsis.IsValid() || !pureFixtureReference(call.Args[2]) {
		return false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != serveappPackage || fn.Name() != "semanticNumericOutput" {
		return false
	}
	signature, ok := fn.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 5 {
		return false
	}
	params := signature.Params()
	db, ok := params.At(2).Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := db.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "database/sql" || named.Obj().Name() != "DB" || params.At(2).Name() != "db" || params.At(3).Name() != "selected" || params.At(1).Name() != "endpoint" || params.At(4).Name() != "run" {
		return false
	}
	selected, ok := params.At(3).Type().Underlying().(*types.Interface)
	return ok && selected.Empty() && types.Identical(params.At(1).Type(), types.Typ[types.String]) && types.Identical(params.At(4).Type(), types.Typ[types.String])
}

// Only the original typed runtime's DB field establishes this pairing. Bare
// handles, lookalike fixtures and effectful receivers need an explicit audit.
func servedDiagnosticHelperShape(name string) (int, bool, bool) {
	index, replace := 1, false
	switch name {
	case "waitServedRunDeliveryQuiescence", "servedEventPublishDebugSummary", "servedEventPublishDebugSummaryForEvent", "requireServedControlAPIIdempotencyRows", "servedEventPublishAPIIdempotencyCount", "servedEventPublishDeliveryStatusCount", "waitServedEventPublishDeliveryStatusCount", "requireNoServedDeliveryStatusDuring", "servedEventNameCount", "requireServedEventNameCount", "issue2394SurfaceSnapshot", "readIssue2394StopSnapshot", "requireNoServedReceiptOutcomeDuring":
		replace = true
	case "requireServedRunStatusWithDebug", "requireServedParitySettlementPostconditions", "requireServedParitySettlementPostconditionsWithDebug":
		index, replace = 2, true
	case "assertIssue2394StopUnchanged":
		index, replace = 2, true
	case "runServedDynamicAutoEmitProof", "runServedEventPublishFollowUpProof", "runServedEventPublishTargetRouteProof", "runServedCreateCarryProjectionProof":
		index = 2
	case "waitServedJoinSourceTimer", "requireServedEventPublishEntityState", "requireServedStoppedPendingDelivery", "servedJoinTarget", "requireServedEventPublishCommittedReplayScope", "waitServedEventPublishReceiptOutcomeCount", "waitServedDeliveryOutcomeCount", "waitServedEventPublishEventID":
	default:
		return 0, false, false
	}
	return index, replace, true
}

func matchServedDatabaseOwner(info *types.Info, call *ast.CallExpr) (ast.Expr, string, int, bool, bool) {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, "", 0, false, false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != serveappPackage || len(call.Args) < 2 || !pureFixtureReference(call.Args[0]) {
		return nil, "", 0, false, false
	}
	index, replace, ok := servedDiagnosticHelperShape(fn.Name())
	if !ok {
		return nil, "", 0, false, false
	}
	if len(call.Args) <= index {
		return nil, "", 0, false, false
	}
	if signature, ok := fn.Type().(*types.Signature); ok && !replace && signature.Params().Len() > index+1 && signature.Params().At(index+1).Name() == "selected" {
		return nil, "", 0, false, false
	}
	db, ok := call.Args[index].(*ast.SelectorExpr)
	if !ok || db.Sel.Name != "DB" || !pureFixtureReference(db.X) {
		return nil, "", 0, false, false
	}
	fixture, ok := info.TypeOf(db.X).(*types.Named)
	if !ok || fixture.Obj().Pkg() == nil || fixture.Obj().Pkg().Path() != serveappPackage || fixture.Obj().Name() != "servedControlProofRuntime" {
		return nil, "", 0, false, false
	}
	return db.X, fn.Name(), index, replace, true
}

// This second pass only propagates an explicit owner through named helper
// parameters; it never derives an owner from a DB, filename or nearby variable.
func propagateServedDiagnosticOwner(source []byte) ([]byte, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main_runtime_test.go", source, parser.ParseComments)
	if err != nil {
		return nil, 0, err
	}
	if file.Name.Name != "serveapp" {
		return nil, 0, fmt.Errorf("expected serveapp helper source")
	}
	count := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Body == nil {
			continue
		}
		_, replace, ok := servedDiagnosticHelperShape(fn.Name.Name)
		if !ok {
			continue
		}
		alreadyCarriesOwner := false
		for _, field := range fn.Type.Params.List {
			for _, name := range field.Names {
				if name.Name == "selected" {
					alreadyCarriesOwner = true
				}
			}
		}
		if alreadyCarriesOwner {
			continue
		}
		for i, field := range fn.Type.Params.List {
			pointer, ok := field.Type.(*ast.StarExpr)
			if !ok || len(field.Names) != 1 || field.Names[0].Name != "db" {
				continue
			}
			typeName, ok := pointer.X.(*ast.SelectorExpr)
			if !ok || typeName.Sel.Name != "DB" {
				continue
			}
			alias, ok := typeName.X.(*ast.Ident)
			if !ok || alias.Name != "sql" {
				continue
			}
			selected := &ast.Field{Names: []*ast.Ident{ast.NewIdent("selected")}, Type: ast.NewIdent("any")}
			if replace {
				fn.Type.Params.List[i] = selected
			} else {
				fields := append([]*ast.Field{}, fn.Type.Params.List[:i+1]...)
				fields = append(fields, selected)
				fn.Type.Params.List = append(fields, fn.Type.Params.List[i+1:]...)
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if name.Obj != nil && name.Obj.Kind != ast.Fun {
					return true
				}
				index, childReplace, ok := servedDiagnosticHelperShape(name.Name)
				if !ok || len(call.Args) <= index {
					return true
				}
				db, ok := call.Args[index].(*ast.Ident)
				if !ok || db.Name != "db" {
					return true
				}
				if childReplace {
					call.Args[index] = ast.NewIdent("selected")
				} else {
					args := append([]ast.Expr{}, call.Args[:index+1]...)
					args = append(args, ast.NewIdent("selected"))
					call.Args = append(args, call.Args[index+1:]...)
				}
				return true
			})
			count++
			break
		}
	}
	var output bytes.Buffer
	if count == 0 {
		return source, 0, nil
	}
	if err := format.Node(&output, fset, file); err != nil {
		return nil, 0, err
	}
	return output.Bytes(), count, nil
}

func matchServedOwnerProjection(info *types.Info, call *ast.CallExpr) (ast.Expr, string, bool) {
	identifier, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, "", false
	}
	fn, ok := info.Uses[identifier].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != serveappPackage {
		return nil, "", false
	}
	fixtureIndex := 1
	if fn.Name() == "selectedMailboxFixtureStore" {
		if len(call.Args) != 1 || !hasLexicalTestingT(info, call.Pos()) {
			return nil, "", false
		}
		fixtureIndex = 0
	} else if len(call.Args) < 2 || !pureFixtureReference(call.Args[0]) {
		return nil, "", false
	}
	switch fn.Name() {
	case "forkReceiverSelectedFixtureOwner", "snapshotForkReceiverApplication",
		"readForkReceiverRows", "readForkReceiverEventMutations", "readForkReceiverEntityHistory",
		"readForkReceiverCompanions", "readServedForkDeliveryLifecycleEvidence", "readServedForkRecipientSourceDomain":
	case "lifecycleStoredSnapshot", "readLifecycleTransitionHistory", "readLifecycleStoredLoop", "readLifecycleLoop":
	case "requireLifecycleEventCount", "readLifecycleTemplateGate":
	case "servedControlProofAuthorActivityContext":
	case "receiverIngressApplicationSnapshot", "semanticNumericDomainCounts":
	case "readServedForkDeliveryEvidence":
	case "readForkReceiverProducerEvidence":
	case "requireA2PortfolioEntity", "requireA2PortfolioEvents", "requireA2PortfolioEvent", "requireA2PortfolioEmission":
	case "requireA2PortfolioJoin":
	case "requireReceiverPublicReadback":
	case "requireSelectedForkDurablePublicReads", "requireSelectedForkDeclaredAgentReads":
	case "a2ReadJoinPublicObligation", "a2RequireJoinPublicGraph", "a2ReadJoinPublicEvent", "a2WaitJoinPublicRun", "a2RequireJoinPublicClients", "a2JoinPublicCLI", "a2RequireJoinPublicTrace":
	case "selectedMailboxFixtureStore":
	case "installMailboxCompletionFaultWitness":
	case "waitForkReceiverSourceCompletion", "waitPublicationSiteCompletion":
	case "mailboxCompletionRunEffects":
	case "gateCompletionRead":
	case "lifecycleGateDecisionParams", "lifecycleDecisionParamsForCard", "waitLifecycleGateCard":
	case "awaitIssue2394SurfaceDiagnosis", "proveIssue2394SurfacePages", "proveIssue2394SurfaceEvents", "proveIssue2394SurfaceDiagnostics", "proveIssue2394SurfaceRefusals":
	case "readForkReceiverNoticeDomain":
	case "requireLifecycleFlowEntity":
	case "requirePendingInputStateCount":
	case "issue2394SurfaceCLI", "issue2394ReporterRPC":
	default:
		return nil, "", false
	}
	fixture, ok := info.TypeOf(call.Args[fixtureIndex]).(*types.Named)
	if !ok || fixture.Obj().Pkg() == nil || fixture.Obj().Pkg().Path() != serveappPackage || fixture.Obj().Name() != "servedControlProofRuntime" || !pureFixtureReference(call.Args[fixtureIndex]) {
		return nil, "", false
	}
	return call.Args[fixtureIndex], fn.Name(), true
}

func hasLexicalTestingT(info *types.Info, pos token.Pos) bool {
	var nearest *types.Scope
	for _, scope := range info.Scopes {
		if scope.Contains(pos) && (nearest == nil || nearest.Pos() <= scope.Pos() && scope.End() <= nearest.End()) {
			nearest = scope
		}
	}
	if nearest == nil {
		return false
	}
	_, object := nearest.LookupParent("t", pos)
	if object == nil {
		return false
	}
	pointer, ok := object.Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := pointer.Elem().(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "testing" && named.Obj().Name() == "T"
}

func pureFixtureReference(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.Ident:
		return true
	case *ast.SelectorExpr:
		return pureFixtureReference(value.X)
	default:
		return false
	}
}

func storetestAlias(file *ast.File) string {
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != storetestPackage {
			continue
		}
		if imp.Name == nil {
			return "storetest"
		}
		if imp.Name.Name != "." && imp.Name.Name != "_" {
			return imp.Name.Name
		}
	}
	return ""
}

func historyField(sql string) string {
	switch strings.Join(strings.Fields(strings.ToLower(sql)), " ") {
	case "select count(*) from events where run_id=$1":
		return "Events"
	case "select count(*) from event_deliveries where run_id=$1":
		return "Deliveries"
	case "select count(*) from entity_state where run_id=$1":
		return "EntityState"
	default:
		return ""
	}
}

func match(info *types.Info, call *ast.CallExpr) (string, ast.Expr, *ast.CallExpr, *ast.UnaryExpr, bool) {
	scan, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !method(info, scan, "database/sql", "Row", "Scan") || len(call.Args) != 1 {
		return "", nil, nil, nil, false
	}
	query, ok := scan.X.(*ast.CallExpr)
	if !ok || len(query.Args) != 3 {
		return "", nil, nil, nil, false
	}
	queryMethod, ok := query.Fun.(*ast.SelectorExpr)
	if !ok || !method(info, queryMethod, "database/sql", "DB", "QueryRowContext") {
		return "", nil, nil, nil, false
	}
	db, ok := queryMethod.X.(*ast.SelectorExpr)
	if !ok || db.Sel.Name != "db" {
		return "", nil, nil, nil, false
	}
	receiver, ok := db.X.(*ast.Ident)
	if !ok {
		return "", nil, nil, nil, false
	}
	ptr, ok := info.TypeOf(receiver).(*types.Pointer)
	if !ok {
		return "", nil, nil, nil, false
	}
	named, ok := ptr.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != conformancePackage || named.Obj().Name() != "deploymentResourceFixture" {
		return "", nil, nil, nil, false
	}
	literal, ok := query.Args[1].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", nil, nil, nil, false
	}
	sql, err := strconv.Unquote(literal.Value)
	if err != nil {
		return "", nil, nil, nil, false
	}
	field := historyField(sql)
	if field == "" {
		return "", nil, nil, nil, false
	}
	destination, ok := call.Args[0].(*ast.UnaryExpr)
	if !ok || destination.Op != token.AND || !types.Identical(info.TypeOf(destination.X), types.Typ[types.Int]) {
		return "", nil, nil, nil, false
	}
	// The destination must remain a simple local assignment, not a reevaluated
	// pointer expression with side effects or aliasing.
	if _, ok := destination.X.(*ast.Ident); !ok {
		return "", nil, nil, nil, false
	}
	return field, receiver, query, destination, true
}

func method(info *types.Info, selector *ast.SelectorExpr, pkg, receiver, name string) bool {
	selection := info.Selections[selector]
	if selection == nil || selection.Kind() != types.MethodVal {
		return false
	}
	fn, ok := selection.Obj().(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != pkg || fn.Name() != name {
		return false
	}
	sig := fn.Type().(*types.Signature)
	ptr, ok := sig.Recv().Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	return ok && named.Obj().Name() == receiver
}
