package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

const readbackPredecessor = "19de2c2e1fc57bcf4b9264c738db22de51742195"
const readbackManifest = "tools/fixture-codemod/pipeline-observations/family145-receiver-publication-readback-preimages.json"

var readbackHelpers = map[string]bool{
	"requireForkReceiverDelivery": true, "requireForkReceiverBusinessMutation": true,
	"requireForkReceiverEffect": true, "requireForkReceiverExecution": true,
	"requireSupplementalForkAcquisitionSettlement": true,
	"requirePublicationSiteReadback":               true, "waitPublicationSiteFanOut": true,
	"requireConnectionPolicyRoutes": true, "requireMixedConnectionReadback": true,
	"requireNumericMockForkOutput":        true,
	"requireActivityPublicationReadback":  true,
	"requireForkLoopStateEffect":          true,
	"requireServedReceiverAgentCollision": true,
}

func migrateReadback(write, capture bool) {
	if capture {
		captureReadback(write)
		return
	}
	root, err := os.Getwd()
	must(err)
	pins, err := selectedreadrole.ReadReadbackPins(root)
	must(err)
	outputs := map[string][]byte{}
	for _, pin := range pins {
		raw := outputs[pin.File]
		if raw == nil {
			raw, err = os.ReadFile(filepath.Join(root, pin.File))
			must(err)
		}
		outputs[pin.File], err = processReadCandidate(pin, raw)
		must(err)
	}
	for path, raw := range outputs {
		outputs[path], err = imports.Process(filepath.Join(root, path), raw, nil)
		must(err)
	}
	validateCandidate(root, outputs)
	fmt.Printf("readback replay declarations=%d files=%d sha256=%s\n", len(pins), len(outputs), selectedreadrole.ReadbackDigest)
	if write {
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
	}
}

func captureReadback(write bool) {
	root, err := os.Getwd()
	must(err)
	originals, err := collectSignedSources(root, readbackPredecessor)
	must(err)
	prepared := map[string][]byte{}
	for path := range originals {
		prepared[path], err = os.ReadFile(filepath.Join(root, path))
		must(err)
	}
	load(root, originals)
	prepared = removeReadbackNumericArgument(root, prepared)
	pruneReadbackNumericCapture(prepared)
	pkgs := load(root, prepared)
	roles, uses := closedReadbackRoles(root, pkgs, prepared)
	outputs, _ := servedRoleCandidatesAt(prepared, roles, uses, nil, func(path string, pins []runphysical.Pin) ([]byte, error) {
		return destinationSource(path, prepared[path], pins)
	})
	for path, raw := range prepared {
		if string(raw) != string(originals[path]) && outputs[path] == nil {
			outputs[path] = raw
		}
	}
	for path, raw := range outputs {
		outputs[path], err = imports.Process(filepath.Join(root, path), raw, nil)
		must(err)
	}
	pins := readRoleFinalPins(originals, outputs)
	for _, pin := range pins {
		if !readbackDeclarationAllowed(pin.File, pin.Function) {
			panic("readback candidate widened its declared output set: " + pin.File + "/" + pin.Function)
		}
		fmt.Printf("readback-source\t%s\t%s\n", pin.File, pin.Function)
	}
	validateCandidate(root, outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("readback declarations=%d roles=%d calls=%d files=%d sha256=%x\n", len(pins), len(roles), len(uses), len(outputs), sha256.Sum256(raw))
	if write {
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(readbackManifest, raw, 0644))
	}
}

func closedReadbackRoles(root string, pkgs []*packages.Package, source map[string][]byte) (map[string]servedRole, map[string]string) {
	blocked := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && !readbackHelpers[fn.Name.Name] {
					blocked[fn.Name.Name] = true
				}
			}
		}
	}
	for {
		roles := discoverServedRoles(pkgs, blocked)
		uses := typedRoleCalls(root, pkgs, roles)
		untyped := untypedServedRoleReferences(source, roles, uses)
		if len(untyped) == 0 {
			return roles, uses
		}
		for name, path := range untyped {
			fmt.Printf("readback-held-indirect-carrier\t%s\t%s\n", name, path)
			blocked[name] = true
		}
	}
}

func removeReadbackNumericArgument(root string, source map[string][]byte) map[string][]byte {
	edits := map[string]map[int]servedsettlement.Splice{}
	for _, pkg := range load(root, source) {
		for _, file := range pkg.Syntax {
			collectReadbackNumericEdits(root, pkg, file, edits)
		}
	}
	for path, unique := range edits {
		var ordered []servedsettlement.Splice
		for _, edit := range unique {
			ordered = append(ordered, edit)
		}
		raw, err := servedsettlement.Apply(source[path], ordered)
		must(err)
		source[path], err = imports.Process(filepath.Join(root, path), raw, nil)
		must(err)
	}
	return source
}

func collectReadbackNumericEdits(root string, pkg *packages.Package, file *ast.File, edits map[string]map[int]servedsettlement.Splice) {
	add := func(start, end token.Pos) {
		position := pkg.Fset.Position(start)
		path, err := filepath.Rel(root, position.Filename)
		must(err)
		if edits[path] == nil {
			edits[path] = map[int]servedsettlement.Splice{}
		}
		edits[path][position.Offset] = servedsettlement.Splice{Start: position.Offset, End: pkg.Fset.Position(end).Offset}
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok {
			readbackNumericDeclaration(pkg.TypesInfo, fn, add)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok {
			readbackNumericCall(pkg.TypesInfo, call, add)
		}
		return true
	})
}

func readbackNumericDeclaration(info *types.Info, fn *ast.FuncDecl, add func(token.Pos, token.Pos)) {
	if fn.Recv != nil || fn.Name.Name != "semanticNumericOutput" || len(fn.Type.Params.List) == 4 {
		return
	}
	if len(fn.Type.Params.List) != 5 {
		panic("numeric readback argument inventory changed")
	}
	field := fn.Type.Params.List[2]
	if len(field.Names) != 1 || field.Names[0].Name != "db" || requestkey.Node(field.Type) != "*sql.DB" {
		panic("numeric readback argument inventory changed")
	}
	object := info.Defs[field.Names[0]]
	for id, use := range info.Uses {
		if use == object && id.Pos() >= fn.Body.Pos() && id.End() <= fn.Body.End() {
			panic("numeric raw argument still supplies authority")
		}
	}
	add(fn.Type.Params.List[2].Pos(), fn.Type.Params.List[3].Pos())
}

func readbackNumericCall(info *types.Info, call *ast.CallExpr, add func(token.Pos, token.Pos)) {
	name, ok := call.Fun.(*ast.Ident)
	if !ok || name.Name != "semanticNumericOutput" {
		return
	}
	fn, ok := info.Uses[name].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/division-sh/swarm/internal/serveapp" {
		panic("numeric readback call is shadowed or foreign")
	}
	if len(call.Args) == 4 {
		return
	}
	if len(call.Args) != 5 || call.Ellipsis.IsValid() || !readbackDiscardableDB(info, call.Args[2]) {
		panic("numeric readback argument would discard evaluation")
	}
	add(call.Args[2].Pos(), call.Args[3].Pos())
}

func readbackDiscardableDB(info *types.Info, argument ast.Expr) bool {
	if types.TypeString(info.TypeOf(argument), nil) != "*database/sql.DB" {
		return false
	}
	if _, ok := argument.(*ast.Ident); ok {
		return true
	}
	field, ok := argument.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "DB" || !isServedRoleType(info.TypeOf(field.X)) {
		return false
	}
	_, ok = field.X.(*ast.Ident)
	return ok
}

func pruneReadbackNumericCapture(source map[string][]byte) {
	const path = "internal/serveapp/semantic_numeric_ingress_test.go"
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source[path], parser.AllErrors)
	must(err)
	fn, err := uniqueDestination(file, "TestServedSemanticNumericIdempotencyReplay")
	must(err)
	var edits []servedsettlement.Splice
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		after, selected := readbackNumericCaptureStatement(node)
		if selected {
			edits = append(edits, servedsettlement.Splice{Start: set.Position(node.Pos()).Offset, End: set.Position(node.End()).Offset, After: after})
			return false
		}
		return true
	})
	if len(edits) != 0 && len(edits) != 3 {
		panic("numeric incidental capture is not its complete three-statement cut")
	}
	source[path], err = servedsettlement.Apply(source[path], edits)
	must(err)
}

func readbackNumericCaptureStatement(node ast.Node) (string, bool) {
	stmt, ok := node.(ast.Stmt)
	if !ok {
		return "", false
	}
	text := requestkey.Node(stmt)
	if text == "var db *sql.DB" {
		return "", true
	}
	const capture = "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })"
	expected, err := requestkey.Normalize("func proof() { " + capture + " }")
	must(err)
	actual, err := requestkey.Normalize("func proof() { " + text + " }")
	if err == nil && actual == expected {
		return "", true
	}
	assignment, ok := stmt.(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 3 || len(assignment.Rhs) != 1 || requestkey.Node(assignment.Lhs[0]) != "_" || requestkey.Node(assignment.Lhs[1]) != "db" || requestkey.Node(assignment.Lhs[2]) != "_" {
		return "", false
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok || requestkey.Node(call.Fun) != "installServeRuntimeEmptyPostgresTestStores" {
		return "", false
	}
	return requestkey.Node(call), true
}

func readbackDeclarationAllowed(path, function string) bool {
	for _, name := range readbackDeclarations[path] {
		if function == name {
			return true
		}
	}
	return false
}

var readbackDeclarations = map[string][]string{
	"internal/serveapp/publication_activity_test.go":                  {"TestServedGeneratedActivityPublicationBothStores", "requireActivityPublicationReadback"},
	"internal/serveapp/fork_loop_generation_notice_test.go":           {"TestServedForkLoopGenerationStateEffectBothStores", "requireForkLoopStateEffect"},
	"internal/serveapp/fork_loop_accumulator_test.go":                 {"TestServedForkAccumulatorRetainedGenerationBothStores"},
	"internal/serveapp/receiver_agent_collision_test.go":              {"TestServedReceiverAgentBusinessNamespaceBothStores", "requireServedReceiverAgentCollision"},
	"internal/serveapp/connection_edge_policy_test.go":                {"TestEdgeOwnedConnectionPoliciesSharingInputBothStores", "TestEdgeOwnedMixedConnectionProjectionsBothStores", "requireMixedConnectionReadback", "requireConnectionPolicyRoutes"},
	"internal/serveapp/fork_historical_context_test.go":               {"TestRunForkHistoricalIdentityPublicExecutionBothStores"},
	"internal/serveapp/fork_receiver_acquisition_effect_only_test.go": {"requireSupplementalForkAcquisitionSettlement", "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores"},
	"internal/serveapp/fork_receiver_execution_effect_test.go":        {"requireForkReceiverExecution"},
	"internal/serveapp/fork_receiver_ownership_test.go":               {"requireForkReceiverDelivery", "requireForkReceiverEffect", "runForkReceiverOwnershipJourney", "requireForkReceiverBusinessMutation", "TestSelectedForkIndependentReceiverBusinessMutationBothStores", "TestSelectedForkReceiverGeometryPostRevisionPolicyRefusalBothStores"},
	"internal/serveapp/fork_receiver_post_revision_refusal_test.go":   {"TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores", "requireForkReceiverPostRevisionPolicyRefusal"},
	"internal/serveapp/publication_sites_test.go":                     {"TestServedPublicationDirectRestartBothStores", "proveServedPublicationSites", "waitPublicationSiteFanOut", "requirePublicationSiteReadback"},
	"internal/serveapp/semantic_numeric_activity_test.go":             {"proveActivitySemanticResultExecutionParity", "requireNumericMockForkOutput"},
	"internal/serveapp/semantic_numeric_gate_test.go":                 {"TestWorkflowGateSemanticNumericOutcomeExecutionParity"},
	"internal/serveapp/semantic_numeric_ingress_test.go":              {"TestServedSemanticNumericIdempotencyReplay", "TestServedSemanticNumericEventSubscription", "semanticNumericOutput"},
	"internal/serveapp/semantic_numeric_schedule_test.go":             {"TestGenericScheduleSemanticPayloadExecutionParity"},
	"internal/serveapp/semantic_numeric_scenario_test.go":             {"startSemanticNumericRuntime"},
}
