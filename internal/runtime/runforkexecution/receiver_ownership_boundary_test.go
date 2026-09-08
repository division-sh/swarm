package runforkexecution

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// F33 is an active-build compiler fence, not a general taint framework. It
// checks writes to the fork ownership carriers and intraprocedural propagation
// of raw event receiver projections into those carriers. It does not prove SQL
// semantics, reflection/unsafe, opaque cross-function string flow, or the
// correctness of an approved same-count write; the behavioral matrix does that.
func TestSelectedForkReceiverOwnershipBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	patterns := []string{
		"./internal/runtime/runforkreadiness",
		"./internal/runtime/runfork", "./internal/runtime/runforkexecution",
		"./internal/runtime/manager", "./internal/runtime/bus", "./internal/runtime/pipeline",
		"./internal/store/internal/backend/runforkpersistence", "./internal/store/internal/runtimepersistence",
	}
	loaded, err := packages.Load(&packages.Config{Dir: root, Tests: false,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(loaded) != 0 || len(loaded) != len(patterns) {
		t.Fatal("receiver ownership guard requires all audited packages to compiler-resolve")
	}
	imports := recipientBoundaryImports{}
	packages.Visit(loaded, func(pkg *packages.Package) bool {
		if pkg.Types != nil {
			imports[pkg.PkgPath] = pkg.Types
		}
		return true
	}, nil)
	var findings []recipientBoundaryFinding
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			findings = append(findings, receiverOwnershipCollect(pkg.Types, pkg.TypesInfo, pkg.Fset, file)...)
		}
	}
	t.Run("production", func(t *testing.T) {
		if problems := recipientBoundaryProblems(findings, receiverOwnershipAllowances(), true); len(problems) != 0 {
			t.Fatalf("fork receiver ownership escaped its semantic owners:\n%s", strings.Join(problems, "\n"))
		}
	})
	t.Run("hostile", func(t *testing.T) { receiverOwnershipHostile(t, imports) })
}

func receiverOwnershipAllowances() map[string]recipientBoundaryAllowance {
	const model = "write:runtime/runfork."
	const backend = "store/internal/backend/runforkpersistence::"
	return map[string]recipientBoundaryAllowance{
		backend + "runForkSourceStateAdmission.project/" + model + "RunForkEntityState.EntityID":                                      {1, "lookup fixed-revision source metadata through its canonical owner; not receiver assignment"},
		"runtime/runfork::ProjectEntityOwnership/" + model + "EntityIdentity.EntityID":                                                {2, "validate source coordinate, then remap only the canonical root"},
		"runtime/runfork::ProjectEntityOwnership/" + model + "EntityIdentity.FlowInstance":                                            {2, "validate source coordinate, then remap only the canonical root"},
		"runtime/runforkreadiness::selectedContractReadinessState/" + model + "RunForkSelectedContractWorkflowState.EntityID":         {1, "copy exact fixed-revision receiving entity"},
		"runtime/runforkreadiness::selectedContractReadinessState/" + model + "RunForkSelectedContractWorkflowState.EntityType":       {1, "copy validated metadata type, not authored fields"},
		"runtime/runforkreadiness::selectedContractReadinessState/" + model + "RunForkSelectedContractWorkflowState.FlowID":           {1, "selected declaration corroborated against receiving metadata"},
		"runtime/runforkreadiness::selectedContractReadinessState/" + model + "RunForkSelectedContractWorkflowState.AddressKind":      {2, "closed root versus exact receiving address"},
		"runtime/runforkreadiness::selectedContractReadinessState/" + model + "RunForkSelectedContractWorkflowState.Route":            {1, "exact route composed from fixed metadata and selected semantic scope"},
		backend + "loadRunForkEntityStates/" + model + "RunForkEntityState.EntityID":                                                  {1, "reconstruct entity identity from revisioned mutation group"},
		backend + "attachRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkEntityState.MaterializationMetadata":           {2, "clear caller-supplied metadata before attaching the unique fixed-revision owner"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.Owner":        {1, "canonical fixed-revision metadata owner"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.Source":       {1, "entity metadata is the only materialization source"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.FlowInstance": {1, "validated unique fixed-revision metadata flow"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.EntityType":   {1, "validated unique fixed-revision metadata type"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.Slug":         {1, "fixed-revision presentation metadata, not event payload"},
		backend + "loadRunForkMaterializedEntitySnapshotMetadata/" + model + "RunForkMaterializedEntitySnapshotMetadata.Name":         {1, "fixed-revision presentation metadata, not event payload"},
		backend + "loadRunForkEntityMetadata/write:store/internal/backend/runforkpersistence.runForkEntityMetadata.FlowInstance":      {1, "validated unique plan metadata into SQL materialization adapter"},
		backend + "loadRunForkEntityMetadata/write:store/internal/backend/runforkpersistence.runForkEntityMetadata.EntityType":        {1, "validated unique plan metadata into SQL materialization adapter"},
		backend + "loadRunForkEntityMetadata/write:store/internal/backend/runforkpersistence.runForkEntityMetadata.Slug":              {1, "copy presentation metadata into SQL adapter"},
		backend + "loadRunForkEntityMetadata/write:store/internal/backend/runforkpersistence.runForkEntityMetadata.Name":              {1, "copy presentation metadata into SQL adapter"},
		backend + "materializeRunForkEntityState/write:store/internal/backend/runforkpersistence.runForkEntityMetadata.FlowInstance":  {1, "consume canonical ProjectEntityOwnership result; no receiver-based rehoming"},
	}
}

func receiverOwnershipType(typ types.Type) string {
	if typ == nil {
		return ""
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	return strings.TrimPrefix(named.Obj().Pkg().Path(), recipientBoundaryModule) + "." + named.Obj().Name()
}

func receiverOwnershipField(typ types.Type, field string) bool {
	switch receiverOwnershipType(typ) {
	case "runtime/runfork.RunForkMaterializedEntitySnapshotMetadata", "store/internal/backend/runforkpersistence.runForkEntityMetadata":
		return true
	case "runtime/runfork.RunForkEntityState":
		return field == "EntityID" || field == "MaterializationMetadata"
	case "runtime/runfork.RunForkSelectedContractWorkflowState":
		return field == "EntityID" || field == "EntityType" || field == "FlowID" || field == "Route" || field == "AddressKind"
	case "runtime/runfork.EntityIdentity":
		return true
	}
	return false
}

func receiverOwnershipCarrier(typ types.Type) bool {
	return receiverOwnershipType(typ) == "runtime/runfork.RunForkSelectedContractSourceEvent"
}

func receiverOwnershipRecord(typ types.Type) bool {
	switch receiverOwnershipType(typ) {
	case "runtime/runfork.RunForkMaterializedEntitySnapshotMetadata", "store/internal/backend/runforkpersistence.runForkEntityMetadata",
		"runtime/runfork.RunForkEntityState", "runtime/runfork.RunForkSelectedContractWorkflowState", "runtime/runfork.EntityIdentity":
		return true
	}
	return false
}

func receiverOwnershipNestedField(expr ast.Expr, info *types.Info) bool {
	selected, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if selection := info.Selections[selected]; selection != nil && receiverOwnershipField(selection.Recv(), selection.Obj().Name()) {
		return true
	}
	return receiverOwnershipNestedField(selected.X, info)
}

func receiverOwnershipRawProjection(selection *types.Selection) bool {
	if selection == nil {
		return false
	}
	switch receiverOwnershipType(selection.Recv()) {
	case "events.Event", "events.EventEnvelope", "store/internal/backend/runforkpersistence.runForkRevisionEvent":
		switch selection.Obj().Name() {
		case "EntityID", "FlowInstance", "Target", "TargetRoute", "TargetSet":
			return true
		}
	}
	return false
}

func receiverOwnershipRawExpr(expr ast.Expr, info *types.Info, aliases map[types.Object]bool) bool {
	if expr == nil {
		return false
	}
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			found = found || receiverOwnershipRawProjection(info.Selections[n])
		case *ast.Ident:
			found = found || aliases[info.ObjectOf(n)]
		}
		return !found
	})
	return found
}

func receiverOwnershipCollect(pkg *types.Package, info *types.Info, fset *token.FileSet, file *ast.File) []recipientBoundaryFinding {
	var findings []recipientBoundaryFinding
	for _, declaration := range file.Decls {
		scope := recipientBoundaryScope(pkg, info, declaration)
		add := func(n ast.Node, kind string) {
			findings = append(findings, recipientBoundaryFinding{Scope: scope, Kind: kind, Site: fset.Position(n.Pos()).String()})
		}
		// A small, monotone, local dependency closure catches arbitrary aliases,
		// including aliases introduced before their dependency is inspected.
		aliases := map[types.Object]bool{}
		changed := true
		for changed {
			changed = false
			bind := func(left ast.Expr, right ast.Expr) {
				id, ok := left.(*ast.Ident)
				if !ok {
					return
				}
				object := info.ObjectOf(id)
				if object != nil && !aliases[object] && receiverOwnershipRawExpr(right, info, aliases) {
					aliases[object], changed = true, true
				}
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.AssignStmt:
					if len(n.Lhs) == len(n.Rhs) {
						for i := range n.Lhs {
							bind(n.Lhs[i], n.Rhs[i])
						}
					}
				case *ast.ValueSpec:
					if len(n.Names) == len(n.Values) {
						for i := range n.Names {
							bind(n.Names[i], n.Values[i])
						}
					}
				}
				return true
			})
		}
		write := func(node ast.Node, typ types.Type, field string, value ast.Expr) {
			if receiverOwnershipField(typ, field) {
				add(node, "write:"+receiverOwnershipType(typ)+"."+field)
				if receiverOwnershipRawExpr(value, info, aliases) {
					add(node, "event_receiver_projection_to_owner")
				}
			}
			if receiverOwnershipCarrier(typ) && receiverOwnershipRawExpr(value, info, aliases) {
				add(node, "event_receiver_projection_to_source_carrier")
			}
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CompositeLit:
				structure, ok := info.TypeOf(n).Underlying().(*types.Struct)
				if !ok {
					break
				}
				for i, element := range n.Elts {
					if pair, ok := element.(*ast.KeyValueExpr); ok {
						if field, ok := pair.Key.(*ast.Ident); ok {
							write(pair, info.TypeOf(n), field.Name, pair.Value)
						}
					} else if i < structure.NumFields() {
						write(element, info.TypeOf(n), structure.Field(i).Name(), element)
					}
				}
			case *ast.AssignStmt:
				for i, left := range n.Lhs {
					if selected, ok := left.(*ast.SelectorExpr); ok {
						if selection := info.Selections[selected]; selection != nil {
							var value ast.Expr
							if len(n.Lhs) == len(n.Rhs) {
								value = n.Rhs[i]
							}
							write(left, selection.Recv(), selection.Obj().Name(), value)
							if receiverOwnershipNestedField(selected.X, info) {
								add(left, "nested_ownership_field_write")
							}
						}
					}
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND && receiverOwnershipNestedField(n.X, info) {
					add(n, "ownership_field_address_escape")
				}
			case *ast.CallExpr:
				if info.Types[n.Fun].IsType() && len(n.Args) == 1 &&
					(receiverOwnershipRecord(info.TypeOf(n)) || receiverOwnershipRecord(info.TypeOf(n.Args[0]))) &&
					!types.Identical(info.TypeOf(n), info.TypeOf(n.Args[0])) {
					add(n, "ownership_representation_conversion")
				}
			case *ast.TypeSpec:
				if obj := info.Defs[n.Name]; obj != nil && receiverOwnershipCarrier(obj.Type()) {
					structure := obj.Type().Underlying().(*types.Struct)
					allowed := map[string]bool{"SourceEventID": true, "EventName": true, "ExecutionMode": true, "Scope": true, "RoutingSource": true, "Payload": true}
					for i := 0; i < structure.NumFields(); i++ {
						if !allowed[structure.Field(i).Name()] {
							add(n, "competing_source_carrier_field:"+structure.Field(i).Name())
						}
					}
				}
			}
			return true
		})
	}
	return findings
}

func receiverOwnershipHostile(t *testing.T, imports recipientBoundaryImports) {
	t.Helper()
	for _, fixture := range []struct {
		name, pkg, file, source string
		want                    []string
	}{
		{"arbitrary_names_and_aliases", "runtime/runforkexecution", "workflow_state_projection.go", `package runforkexecution
import (f "github.com/division-sh/swarm/internal/runtime/runfork"; e "github.com/division-sh/swarm/internal/events")
type alias = f.RunForkMaterializedEntitySnapshotMetadata
func obscure(a e.Event, b *alias, c *f.RunForkSelectedContractWorkflowState) {
 z := a.FlowInstance(); q := z; b.FlowInstance = q; c.EntityID = a.EntityID()
 _ = &c.EntityID
}
`, []string{"obscure/write:", "event_receiver_projection_to_owner", "ownership_field_address_escape"}},
		{"approved_function_not_universal_authority", "runtime/runforkexecution", "workflow_state_projection.go", `package runforkexecution
import (f "github.com/division-sh/swarm/internal/runtime/runfork"; e "github.com/division-sh/swarm/internal/events")
func selectedContractReadinessState(z e.Event) f.RunForkSelectedContractWorkflowState {
 return f.RunForkSelectedContractWorkflowState{EntityID: z.EntityID()}
}
`, []string{"selectedContractReadinessState/event_receiver_projection_to_owner"}},
		{"approved_file_does_not_authorize_reconstruction", "store/internal/backend/runforkpersistence", "run_fork_entity_snapshot_metadata.go", `package runforkpersistence
import (f "github.com/division-sh/swarm/internal/runtime/runfork"; e "github.com/division-sh/swarm/internal/events")
func surprise(a e.Event) f.RunForkMaterializedEntitySnapshotMetadata {
 return f.RunForkMaterializedEntitySnapshotMetadata{FlowInstance: a.FlowInstance(), EntityType: "invented"}
}
`, []string{"surprise/write:", "event_receiver_projection_to_owner"}},
		{"source_carrier_reconstruction", "runtime/runforkexecution", "recipient_planning.go", `package runforkexecution
import (f "github.com/division-sh/swarm/internal/runtime/runfork"; e "github.com/division-sh/swarm/internal/events")
func unexpected(a e.Event) f.RunForkSelectedContractSourceEvent {
 b, _ := e.NewRootRoutingSource(a.EntityID())
 return f.RunForkSelectedContractSourceEvent{RoutingSource: b, Scope: a.FlowInstance()}
}
`, []string{"event_receiver_projection_to_source_carrier"}},
		{"historical_receiver_projection_is_not_metadata", "store/internal/backend/runforkpersistence", "run_fork_entity_snapshot_metadata.go", `package runforkpersistence
import f "github.com/division-sh/swarm/internal/runtime/runfork"
type runForkRevisionEvent struct { EntityID, FlowInstance string }
func loadRunForkMaterializedEntitySnapshotMetadata(q runForkRevisionEvent) f.RunForkMaterializedEntitySnapshotMetadata {
 var renamed = q.FlowInstance
 return f.RunForkMaterializedEntitySnapshotMetadata{FlowInstance: renamed}
}
`, []string{"loadRunForkMaterializedEntitySnapshotMetadata/event_receiver_projection_to_owner"}},
		{"nested_route_and_defined_type", "runtime/runforkexecution", "workflow_state_projection.go", `package runforkexecution
import f "github.com/division-sh/swarm/internal/runtime/runfork"
type disguised f.RunForkSelectedContractWorkflowState
func unrelated(p *f.RunForkSelectedContractWorkflowState) {
 p.Route.InstancePath = "foreign/path"
 q := (*disguised)(p); q.EntityID = "foreign"
}
`, []string{"nested_ownership_field_write", "ownership_representation_conversion"}},
		{"unkeyed_identity_reconstruction", "runtime/runfork", "entity_ownership.go", `package runfork
type EntityIdentity struct { EntityID, FlowInstance string }
func ordinary(a, b string) EntityIdentity { return EntityIdentity{a, b} }
`, []string{"ordinary/write:runtime/runfork.EntityIdentity.EntityID", "ordinary/write:runtime/runfork.EntityIdentity.FlowInstance"}},
		{"carrier_cannot_grow_competing_fields", "runtime/runfork", "models.go", `package runfork
type RunForkSelectedContractSourceEvent struct { SourceEventID string; EntityID string; FlowInstance string; AlternateOwner string }
`, []string{"competing_source_carrier_field:EntityID", "competing_source_carrier_field:FlowInstance", "competing_source_carrier_field:AlternateOwner"}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, fixture.file, fixture.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
			pkg, err := (&types.Config{Importer: imports}).Check(recipientBoundaryModule+fixture.pkg, fset, []*ast.File{file}, info)
			if err != nil {
				t.Fatalf("hostile fixture must compiler-resolve: %v", err)
			}
			findings := receiverOwnershipCollect(pkg, info, fset, file)
			problems := strings.Join(recipientBoundaryProblems(findings, receiverOwnershipAllowances(), false), "\n")
			for _, want := range fixture.want {
				if !strings.Contains(problems, want) {
					t.Fatalf("missing %q rejection:\n%s", want, problems)
				}
			}
		})
	}
	t.Run("typed_source_and_unrelated_same_spelling_are_legal", func(t *testing.T) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "workflow_state_projection.go", `package runforkexecution
import (f "github.com/division-sh/swarm/internal/runtime/runfork"; e "github.com/division-sh/swarm/internal/events")
type ordinary struct { EntityID, FlowInstance string }
func copyTyped(source e.RoutingSource) f.RunForkSelectedContractSourceEvent {
 incidental := ordinary{EntityID: "business", FlowInstance: "payload"}
 incidental.EntityID = "other"
 return f.RunForkSelectedContractSourceEvent{RoutingSource: source}
}
`, 0)
		if err != nil {
			t.Fatal(err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		pkg, err := (&types.Config{Importer: imports}).Check(recipientBoundaryModule+"runtime/runforkexecution", fset, []*ast.File{file}, info)
		if err != nil {
			t.Fatal(err)
		}
		if findings := receiverOwnershipCollect(pkg, info, fset, file); len(findings) != 0 {
			t.Fatalf("guard confused unrelated fields or typed source authority with receiver reconstruction: %#v", findings)
		}
	})
}
