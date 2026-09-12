package runforkexecution

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const recipientBoundaryModule = "github.com/division-sh/swarm/internal/"

type recipientBoundaryFinding struct {
	Scope string
	Kind  string
	Site  string
}

type recipientBoundaryAllowance struct {
	Count  int
	Reason string
}

// Like the existing handler-rule and receiver ownership guards, exceptions are
// compiler-resolved functions plus operation counts, never files or receiver names.
func recipientBoundaryAllowances() map[string]recipientBoundaryAllowance {
	return map[string]recipientBoundaryAllowance{
		"runtime/bus::validateRoutedNodeDeliveryAuthority/reduced_recipient_map_key":                                     {2, "existing route-intent target/recipient corroboration; selected evidence is independently checked using canonical Equal"},
		"runtime/runforkadmission::AdmitContractFrontier/evidence_field:Recipient":                                       {1, "node diagnostics projected only from the narrowed admitted input frontier"},
		"runtime/runforkadmission::completedInputRecipient/evidence_field:Recipient":                                     {2, "historical completed-slot exclusion, never creation of selected authority"},
		"runtime/runforkadmission::completedInputRecipient/evidence_field:Path":                                          {1, "exact historical flow-instance completion correspondence"},
		"runtime/runforkadmission::completedInputRecipient/evidence_field:AgentPlan":                                     {1, "complete runless agent identity after verifying the source run"},
		"runtime/runforkexecution::prepareSelectedFork/evidence_field:Recipient":                                         {1, "choose node or agent schema owner for selected input validation"},
		"runtime/runforkexecution::prepareSelectedFork/evidence_field:AgentPlan":                                         {1, "schema validation through the exact admitted agent owner"},
		"runtime/runfork::EqualSelectedContractRecipientPlanning/raw_aggregate_equality":                                 {1, "metadata-only comparison after canonical keys are compared and cloned recipient fields cleared"},
		"runtime/runfork::EqualSelectedContractRouteTopology/raw_aggregate_equality":                                     {1, "metadata-only comparison after canonical keys are compared and both cloned topology evidence fields cleared"},
		"runtime/runfork::EqualSelectedContractFrontierEvents/raw_aggregate_equality":                                    {1, "metadata-only comparison after canonical keys are compared and cloned frontier recipient fields cleared"},
		"store/internal/backend/runforkpersistence::decodeRunForkSelectedContractRouteRecoveryModels/raw_authority_json": {2, "strict typed topology/planning decoding after verifying each payload integrity hash; semantic comparison uses the shared owner"},
		"runtime/runforkexecution::selectedContractWorkflowProjection.BindRecipient/evidence_field:Path":                 {3, "bind root execution copy through admitted root coordinate and source-event membership; no producer-state transfer"},
		"runtime/runforkexecution::selectedContractNodeDeliveryRoutes/evidence_field:Recipient":                          {2, "typed node materialization, not recipient-set identity"},
		"runtime/runforkexecution::selectedContractNodeDeliveryRoutes/evidence_field:Path":                               {1, "exact node target blueprint"},
		"runtime/runfork::RunForkSelectedContractRecipientPlanning.SelectedAgentPlans/evidence_field:Recipient":          {1, "shared admitted agent subset classification"},
		"runtime/runfork::RunForkSelectedContractRecipientPlanning.SelectedAgentPlans/evidence_field:AgentPlan":          {1, "shared full canonical runless agent plan selection"},
		"runtime/runforkreadiness::Project/evidence_field:Recipient":                                                     {2, "node/agent workflow-state projection"},
		"runtime/runforkreadiness::Project/evidence_field:Path":                                                          {1, "exact workflow-state route projection"},
		"runtime/runforkreadiness::selectedContractTemplateAgentWorkflowState/evidence_field:Path":                       {1, "existing selected template workflow-state owner"},
		"runtime/runforkreadiness::selectedContractTemplateAgentWorkflowState/evidence_field:AgentPlan":                  {1, "full plan correspondence, not a reduced recipient key"},
		"runtime/runforkexecution::selectedContractDynamicTopologyEvidence/evidence_field:Path":                          {2, "nonmutating dynamic topology corroboration"},
	}
}

// This is a bounded compiler guard, not whole-program taint analysis. It fences
// typed diagnostics, Evidence projection sites, typed map keys, and intraprocedural
// JSON erasure in the active build configuration. Reflection, unsafe, arbitrary
// cross-function/opaque erasure, and same-count repurposing of approved field
// reads remain outside its proof; the R01-R28 behavioral matrix is still required.
func TestSelectedForkRecipientAuthorityConsumers(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	patterns := []string{
		"./internal/runtime/bus",
		"./internal/runtime/runforkreadiness",
		"./internal/runtime/runfork",
		"./internal/runtime/runforkadmission",
		"./internal/runtime/runforkexecution",
		"./internal/runtime/manager",
		"./internal/store/internal/backend/runforkpersistence",
		"./internal/store/internal/runtimepersistence",
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
		t.Fatal("recipient authority guard requires successful compiler resolution of every audited package")
	}
	imports := recipientBoundaryImports{}
	packages.Visit(loaded, func(pkg *packages.Package) bool {
		if pkg.Types != nil {
			imports[pkg.PkgPath] = pkg.Types
		}
		return true
	}, nil)
	diagnostic := recipientBoundaryNamed(t, imports, "runtime/bus", "PublishDiagnosticRecipient")
	evidence := recipientBoundaryNamed(t, imports, "runtime/core/forkrecipient", "Evidence")
	var findings []recipientBoundaryFinding
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			findings = append(findings, recipientBoundaryCollect(pkg.Types, pkg.TypesInfo, pkg.Fset, file, diagnostic, evidence)...)
		}
	}
	t.Run("production", func(t *testing.T) {
		if problems := recipientBoundaryProblems(findings, recipientBoundaryAllowances(), true); len(problems) != 0 {
			t.Fatalf("selected recipient authority escaped its canonical owners:\n%s", strings.Join(problems, "\n"))
		}
	})
	t.Run("hostile_compiler_resolved_consumers", func(t *testing.T) {
		recipientBoundaryHostile(t, imports, diagnostic, evidence)
	})
}

type recipientBoundaryImports map[string]*types.Package

func (i recipientBoundaryImports) Import(path string) (*types.Package, error) {
	if pkg := i[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("guard fixture dependency %q was not compiler-loaded", path)
}

func recipientBoundaryNamed(t *testing.T, imports recipientBoundaryImports, path, name string) *types.Named {
	t.Helper()
	pkg := imports[recipientBoundaryModule+path]
	if pkg == nil || pkg.Scope().Lookup(name) == nil {
		t.Fatalf("missing canonical type %s.%s", path, name)
	}
	named, ok := types.Unalias(pkg.Scope().Lookup(name).Type()).(*types.Named)
	if !ok {
		t.Fatalf("canonical type %s.%s is not named", path, name)
	}
	return named
}

func recipientBoundaryScope(pkg *types.Package, info *types.Info, declaration ast.Decl) string {
	scope := strings.TrimPrefix(pkg.Path(), recipientBoundaryModule) + "::"
	fn, ok := declaration.(*ast.FuncDecl)
	if !ok {
		return scope + "<package>"
	}
	resolved, ok := info.Defs[fn.Name].(*types.Func)
	if !ok {
		panic("compiler did not resolve an audited function")
	}
	if receiver := resolved.Type().(*types.Signature).Recv(); receiver != nil {
		typ := types.Unalias(receiver.Type())
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(pointer.Elem())
		}
		scope += typ.(*types.Named).Obj().Name() + "."
	}
	return scope + resolved.Name()
}

func recipientBoundaryCollect(pkg *types.Package, info *types.Info, fset *token.FileSet, file *ast.File, diagnostic, evidence *types.Named) []recipientBoundaryFinding {
	var findings []recipientBoundaryFinding
	diagnosticFields := recipientBoundaryFields(diagnostic)
	evidenceFields := recipientBoundaryFields(evidence)
	for _, declaration := range file.Decls {
		// Bus owns ordinary live-delivery maps and diagnostic production too.
		// Extend the selected-evidence guard to every compiler-identified bus
		// consumer without treating those unrelated families as fork evidence.
		if pkg.Path() == recipientBoundaryModule+"runtime/bus" {
			consumesEvidence := false
			ast.Inspect(declaration, func(node ast.Node) bool {
				if expr, ok := node.(ast.Expr); ok && recipientBoundaryContains(info.TypeOf(expr), evidence) {
					consumesEvidence = true
				}
				return !consumesEvidence
			})
			if !consumesEvidence {
				continue
			}
		}
		scope := recipientBoundaryScope(pkg, info, declaration)
		add := func(node ast.Node, kind string) {
			findings = append(findings, recipientBoundaryFinding{scope, kind, fset.Position(node.Pos()).String()})
		}
		tainted := recipientBoundaryTaint(declaration, info, evidence)
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				if recipientBoundaryDiagnosticValue(info.TypeOf(n), diagnostic) {
					add(n, "diagnostic_projection")
				}
				if selection := info.Selections[n]; selection != nil {
					if diagnosticFields[selection.Obj()] {
						add(n, "diagnostic_field")
					}
					if evidenceFields[selection.Obj()] {
						add(n, "evidence_field:"+selection.Obj().Name())
					}
					if fn, ok := selection.Obj().(*types.Func); ok && fn.Pkg() != nil &&
						fn.Pkg().Path() == recipientBoundaryModule+"runtime/core/forkrecipient" && fn.Name() == "RouteSourceCode" {
						add(n, "diagnostic_method")
					}
				}
			case *ast.Ident:
				// Whole diagnostic records/collections cannot bypass field detection
				// through equality, aliases, range, helper calls, or interface boxing.
				if object, ok := info.Uses[n].(*types.Var); ok && !object.IsField() && recipientBoundaryDiagnosticValue(object.Type(), diagnostic) {
					add(n, "diagnostic_value")
				}
			case *ast.CompositeLit:
				if recipientBoundarySame(info.TypeOf(n), evidence) && len(n.Elts) != 0 {
					add(n, "raw_evidence_construction")
				}
				if recipientBoundaryDiagnosticValue(info.TypeOf(n), diagnostic) {
					add(n, "diagnostic_construction")
				}
			case *ast.TypeSpec:
				if !n.Assign.IsValid() {
					if obj := info.Defs[n.Name]; obj != nil && types.Identical(obj.Type().Underlying(), evidence.Underlying()) {
						add(n, "replacement_evidence_type")
					}
				}
			case *ast.MapType:
				if recipientBoundaryReducedKey(info.TypeOf(n.Key), evidence) {
					add(n, "reduced_recipient_map_key")
				}
			case *ast.BinaryExpr:
				if n.Op == token.EQL || n.Op == token.NEQ {
					if recipientBoundarySame(info.TypeOf(n.X), evidence) || recipientBoundarySame(info.TypeOf(n.Y), evidence) {
						add(n, "raw_evidence_equality")
					}
				}
			case *ast.CallExpr:
				if recipientBoundaryReflectEquality(n, info, evidence) {
					add(n, "raw_aggregate_equality")
				}
				if recipientBoundaryJSONErase(n, info, evidence, tainted) {
					add(n, "raw_authority_json")
				}
			}
			return true
		})
	}
	return findings
}

func recipientBoundaryReflectEquality(call *ast.CallExpr, info *types.Info, evidence *types.Named) bool {
	var object types.Object
	switch function := call.Fun.(type) {
	case *ast.SelectorExpr:
		object = info.Uses[function.Sel]
	case *ast.Ident:
		object = info.Uses[function]
	}
	function, ok := object.(*types.Func)
	if !ok || function.Pkg() == nil || function.Pkg().Path() != "reflect" || function.Name() != "DeepEqual" {
		return false
	}
	for _, argument := range call.Args {
		found := false
		// Inspect explicit interface conversions too, but do not infer authority
		// through arbitrary pre-erased interfaces or cross-function aliases.
		ast.Inspect(argument, func(node ast.Node) bool {
			if expression, ok := node.(ast.Expr); ok && recipientBoundaryContains(info.TypeOf(expression), evidence) {
				found = true
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func recipientBoundaryFields(named *types.Named) map[types.Object]bool {
	fields := map[types.Object]bool{}
	structure := named.Underlying().(*types.Struct)
	for i := 0; i < structure.NumFields(); i++ {
		fields[structure.Field(i)] = true
	}
	return fields
}

func recipientBoundarySame(typ types.Type, named *types.Named) bool {
	return typ != nil && types.Identical(types.Unalias(typ), named)
}

// A service or publication envelope containing diagnostics is not itself a
// diagnostic read. Extracting its diagnostic member is checked separately.
func recipientBoundaryDiagnosticValue(typ types.Type, diagnostic *types.Named) bool {
	if typ == nil {
		return false
	}
	if recipientBoundarySame(typ, diagnostic) {
		return true
	}
	switch typ := types.Unalias(typ).(type) {
	case *types.Pointer:
		return recipientBoundaryDiagnosticValue(typ.Elem(), diagnostic)
	case *types.Slice:
		return recipientBoundaryDiagnosticValue(typ.Elem(), diagnostic)
	case *types.Array:
		return recipientBoundaryDiagnosticValue(typ.Elem(), diagnostic)
	case *types.Map:
		return recipientBoundaryDiagnosticValue(typ.Key(), diagnostic) || recipientBoundaryDiagnosticValue(typ.Elem(), diagnostic)
	}
	return false
}

func recipientBoundaryContains(typ types.Type, named *types.Named) bool {
	seen := map[types.Type]bool{}
	var visit func(types.Type) bool
	visit = func(typ types.Type) bool {
		if typ == nil || seen[typ] {
			return false
		}
		seen[typ] = true
		if recipientBoundarySame(typ, named) {
			return true
		}
		switch typ := types.Unalias(typ).(type) {
		case *types.Named:
			return visit(typ.Underlying())
		case *types.Pointer:
			return visit(typ.Elem())
		case *types.Slice:
			return visit(typ.Elem())
		case *types.Array:
			return visit(typ.Elem())
		case *types.Map:
			return visit(typ.Key()) || visit(typ.Elem())
		case *types.Struct:
			for i := 0; i < typ.NumFields(); i++ {
				if visit(typ.Field(i).Type()) {
					return true
				}
			}
		}
		return false
	}
	return visit(typ)
}

func recipientBoundaryReducedKey(typ types.Type, evidence *types.Named) bool {
	if typ == nil {
		return false
	}
	if recipientBoundaryContains(typ, evidence) {
		return true
	}
	if named, ok := types.Unalias(typ).(*types.Named); ok {
		if named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == recipientBoundaryModule+"runtime/core/forkrecipient" && named.Obj().Name() == "Key" {
			return false
		}
		if named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == recipientBoundaryModule+"events" {
			return named.Obj().Name() == "DeliveryRecipient"
		}
		return recipientBoundaryReducedKey(named.Underlying(), evidence)
	}
	switch typ := types.Unalias(typ).(type) {
	case *types.Array:
		return recipientBoundaryReducedKey(typ.Elem(), evidence)
	case *types.Struct:
		for i := 0; i < typ.NumFields(); i++ {
			if recipientBoundaryReducedKey(typ.Field(i).Type(), evidence) {
				return true
			}
		}
	}
	return false
}

func recipientBoundaryAuthorityExpr(expr ast.Expr, info *types.Info, evidence *types.Named, tainted map[types.Object]bool) bool {
	if expr == nil {
		return false
	}
	if recipientBoundaryContains(info.TypeOf(expr), evidence) {
		return true
	}
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if subexpression, ok := node.(ast.Expr); ok && recipientBoundaryContains(info.TypeOf(subexpression), evidence) {
			found = true
		}
		if id, ok := node.(*ast.Ident); ok && tainted[info.ObjectOf(id)] {
			found = true
		}
		if selector, ok := node.(*ast.SelectorExpr); ok {
			selection := info.Selections[selector]
			if selection != nil && selection.Kind() == types.FieldVal &&
				(selection.Obj().Name() == "RecipientPlanning" || selection.Obj().Name() == "RouteTopology") {
				typ := types.Unalias(selection.Recv())
				if ptr, ok := typ.(*types.Pointer); ok {
					typ = types.Unalias(ptr.Elem())
				}
				if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil {
					path := named.Obj().Pkg().Path()
					found = found || (path == recipientBoundaryModule+"runtime/manager" && named.Obj().Name() == "SelectedContractRouteRecoveryRecord") ||
						(path == recipientBoundaryModule+"runtime/runfork" && named.Obj().Name() == "RunForkSelectedContractRouteRecovery")
				}
			}
		}
		return !found
	})
	return found
}

// Local assignment propagation covers bytes/reader/decoder/interface aliases.
// It deliberately does not claim to trace arbitrary helper implementations.
func recipientBoundaryTaint(declaration ast.Decl, info *types.Info, evidence *types.Named) map[types.Object]bool {
	tainted := map[types.Object]bool{}
	changed := true
	for changed {
		changed = false
		mark := func(left ast.Expr, right ast.Expr) {
			id, ok := left.(*ast.Ident)
			if !ok || id.Name == "_" {
				return
			}
			object := info.ObjectOf(id)
			if object != nil && !tainted[object] && recipientBoundaryAuthorityExpr(right, info, evidence, tainted) {
				tainted[object], changed = true, true
			}
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for index, left := range n.Lhs {
					if index < len(n.Rhs) {
						mark(left, n.Rhs[index])
					}
				}
			case *ast.ValueSpec:
				for index, name := range n.Names {
					if index < len(n.Values) {
						mark(name, n.Values[index])
					}
				}
			}
			return true
		})
	}
	return tainted
}

func recipientBoundaryJSONErase(call *ast.CallExpr, info *types.Info, evidence *types.Named, tainted map[types.Object]bool) bool {
	var (
		fn     *types.Func
		source ast.Expr
		target ast.Expr
	)
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		fn, _ = info.Uses[selector.Sel].(*types.Func)
		if fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "encoding/json" {
			switch fn.Name() {
			case "Unmarshal":
				if len(call.Args) == 2 {
					source, target = call.Args[0], call.Args[1]
				}
			case "Decode":
				if len(call.Args) == 1 {
					source, target = selector.X, call.Args[0]
				}
			}
		}
	}
	return source != nil && recipientBoundaryAuthorityExpr(source, info, evidence, tainted) &&
		!recipientBoundaryContains(info.TypeOf(target), evidence)
}

func recipientBoundaryProblems(findings []recipientBoundaryFinding, allowances map[string]recipientBoundaryAllowance, requireAll bool) []string {
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
		if allowance.Reason == "" || counts[key] > allowance.Count || (requireAll && counts[key] != allowance.Count) {
			problems = append(problems, fmt.Sprintf("%s: observed %d, audited %d (%s)", key, counts[key], allowance.Count, allowance.Reason))
		}
	}
	sort.Strings(problems)
	return problems
}

func recipientBoundaryHostile(t *testing.T, imports recipientBoundaryImports, diagnostic, evidence *types.Named) {
	t.Helper()
	// This source is compiler checked at an existing approved adapter's filename.
	// No fixture writes or executable runtime producers are introduced.
	source := `package runforkexecution
import (
    oddbus "github.com/division-sh/swarm/internal/runtime/bus"
    chosen "github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
    facts "github.com/division-sh/swarm/internal/events"
    model "github.com/division-sh/swarm/internal/runtime/runfork"
    codec "encoding/json"
    mirror "reflect"
    "bytes"
)
type Mask = oddbus.PublishDiagnosticRecipient
type Inherited struct { *Mask }
func unfamiliarName(zebra Mask) bool { return zebra.Path == "receiver" }
func embeddedName(octopus Inherited) string { return octopus.ID }
func boxedName(otter []Mask) any { return otter }
func envelopeRead(heron oddbus.PublishRecipientPlan) any { return heron.RoutedRecipients }
func privateDiagnostic(pine chosen.Evidence) string { return pine.RouteSourceCode() }
func reducedIdentity(birch chosen.Evidence) string { return birch.Recipient.ID() }
func selectedContractNodeDeliveryRoutes(badger chosen.Evidence) {
    _ = badger.Recipient
    _ = badger.Recipient
    _ = badger.Recipient
    _ = map[facts.DeliveryRecipient]bool{}
}
type TinyKey struct { member facts.DeliveryRecipient; location string }
var reduced = map[TinyKey]bool{}
var wrongDomain = map[chosen.Evidence]bool{}
type Copy chosen.Evidence
func fabricated() chosen.Evidence { return chosen.Evidence{Path: "x"} }
func compare(oak, elm chosen.Evidence) bool { return oak == elm }
func comparePlanning(ibis, crane model.RunForkSelectedContractRecipientPlanning) bool {
    return mirror.DeepEqual(ibis, crane)
}
func compareTopology(ibis, crane *model.RunForkSelectedContractRouteTopology) bool {
    return mirror.DeepEqual(ibis, crane)
}
func compareFrontier(ibis, crane []model.RunForkSelectedContractFrontierEvent) bool {
    return mirror.DeepEqual(ibis, crane)
}
func compareBoxed(ibis, crane model.RunForkSelectedContractRecipientPlanning) bool {
    return mirror.DeepEqual(any(ibis), any(crane))
}
func equalSelectedContractRecipientPlanning(ibis, crane model.RunForkSelectedContractRecipientPlanning) bool {
    _ = mirror.DeepEqual(ibis, crane)
    return mirror.DeepEqual(&ibis, &crane)
}
func erased(willow model.RunForkSelectedContractRecipientPlanning) error {
    encoded, err := codec.Marshal(willow)
    if err != nil { return err }
    copied := encoded
    var smaller map[string]any
    return codec.Unmarshal(copied, &smaller)
}
func rawRecovered(cedar model.RunForkSelectedContractRouteRecovery) error {
    opaque := cedar.RecipientPlanning
    var smaller struct { Owner string }
    return codec.Unmarshal(opaque, &smaller)
}
func decoderErasure(maple chosen.Evidence) error {
    encoded, _ := codec.Marshal(maple)
    reader := bytes.NewReader(encoded)
    parser := codec.NewDecoder(reader)
    var smaller any
    return parser.Decode(&smaller)
}
func allowedTypedDecode(cedar model.RunForkSelectedContractRouteRecovery) error {
    var complete model.RunForkSelectedContractRecipientPlanning
    return codec.Unmarshal(cedar.RecipientPlanning, &complete)
}
func allowedCanonicalKey(birch chosen.Evidence) (chosen.Key, error) { return birch.Key() }
func allowedTypedActuals(heron oddbus.PublishRecipientPlan) { _, _ = heron.RecipientActuals() }
type Unrelated struct { Path string }
func allowedLookalike(fox Unrelated) string { return fox.Path }
func allowedMetadata(fox, owl Unrelated) bool { return mirror.DeepEqual(fox, owl) }
type CustomComparer struct{}
func (CustomComparer) DeepEqual(a, b any) bool { return true }
func allowedUnrelatedComparer(fox, owl model.RunForkSelectedContractRecipientPlanning) bool {
    return (CustomComparer{}).DeepEqual(fox, owl)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "internal/runtime/runforkexecution/recipient_planning.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	pkg, err := (&types.Config{Importer: imports}).Check(recipientBoundaryModule+"runtime/runforkexecution", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("hostile guard fixture must compile: %v", err)
	}
	findings := recipientBoundaryCollect(pkg, info, fset, file, diagnostic, evidence)
	want := map[string]bool{
		"unfamiliarName/diagnostic_field": false, "embeddedName/diagnostic_field": false,
		"boxedName/diagnostic_value": false, "privateDiagnostic/diagnostic_method": false,
		"envelopeRead/diagnostic_projection":                           false,
		"reducedIdentity/evidence_field:Recipient":                     false,
		"selectedContractNodeDeliveryRoutes/reduced_recipient_map_key": false,
		"<package>/reduced_recipient_map_key":                          false, "<package>/replacement_evidence_type": false,
		"fabricated/raw_evidence_construction": false, "compare/raw_evidence_equality": false,
		"comparePlanning/raw_aggregate_equality": false, "compareTopology/raw_aggregate_equality": false,
		"compareFrontier/raw_aggregate_equality": false, "compareBoxed/raw_aggregate_equality": false,
		"equalSelectedContractRecipientPlanning/raw_aggregate_equality": false,
		"erased/raw_authority_json":                                     false, "rawRecovered/raw_authority_json": false, "decoderErasure/raw_authority_json": false,
	}
	for _, finding := range findings {
		key := strings.TrimPrefix(finding.Scope, "runtime/runforkexecution::") + "/" + finding.Kind
		if _, exists := want[key]; exists {
			want[key] = true
		}
		if strings.HasPrefix(key, "allowed") {
			t.Fatalf("canonical operation/lookalike falsely rejected: %s", key)
		}
	}
	for key, found := range want {
		if !found {
			t.Errorf("compiler guard missed hostile %s: %#v", key, findings)
		}
	}
	problems := recipientBoundaryProblems(findings, recipientBoundaryAllowances(), false)
	if !strings.Contains(strings.Join(problems, "\n"), "selectedContractNodeDeliveryRoutes/reduced_recipient_map_key") {
		t.Fatal("existing adapter function name exempted an unrelated forbidden operation")
	}
	if !strings.Contains(strings.Join(problems, "\n"), "selectedContractNodeDeliveryRoutes/evidence_field:Recipient: observed 3, audited 2") {
		t.Fatal("approved function field-read budget did not reject an additional operation")
	}
	recipientBoundaryHostileModelOwner(t, imports, diagnostic, evidence)
}

func recipientBoundaryHostileModelOwner(t *testing.T, imports recipientBoundaryImports, diagnostic, evidence *types.Named) {
	t.Helper()
	const source = `package runfork
import (
    mirror "reflect"
    chosen "github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
)
type Model struct { Recipients []chosen.Evidence }
func EqualSelectedContractRecipientPlanning(ibis, crane Model) bool {
    _ = mirror.DeepEqual(ibis, crane)
    return mirror.DeepEqual(&ibis, &crane)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "internal/runtime/runfork/recipient_model_equality.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	pkg, err := (&types.Config{Importer: imports}).Check(recipientBoundaryModule+"runtime/runfork", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("hostile canonical-owner fixture must compile: %v", err)
	}
	findings := recipientBoundaryCollect(pkg, info, fset, file, diagnostic, evidence)
	problems := recipientBoundaryProblems(findings, recipientBoundaryAllowances(), false)
	if !strings.Contains(strings.Join(problems, "\n"), "runtime/runfork::EqualSelectedContractRecipientPlanning/raw_aggregate_equality: observed 2, audited 1") {
		t.Fatalf("approved metadata comparator exempted additional raw authority comparison: %v", problems)
	}
}
