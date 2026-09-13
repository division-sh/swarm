package semanticview_test

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestGeneratedSchemaConsumersCannotRestoreGlobalLookup(t *testing.T) {
	if findings := generatedSchemaFindings(t, nil); len(findings) != 0 {
		t.Fatalf("competing generated schema ownership: %v", findings)
	}
}

func TestGeneratedSchemaGuardRejectsReceiverAliasesAndScopeSearch(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	overlay := map[string][]byte{
		filepath.Join(root, "internal/runtime/semanticview/hostile_generated_schema.go"): []byte(`package semanticview
import alternate "github.com/division-sh/swarm/internal/runtime/contracts"
func hostileGeneratedSchema(unrelated *alternate.WorkflowContractBundle) any {
    borrowed := unrelated.GeneratedActivityEventEntries
    return borrowed()
}
`),
	}
	path := filepath.Join(root, "internal/runtime/semanticview/event_schema.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := "func ResolveEventSchema(source Source, flowID, eventType string) EventSchemaResolution {"
	if strings.Count(string(raw), marker) != 1 {
		t.Fatal("exact schema owner not found")
	}
	overlay[path] = []byte(strings.Replace(string(raw), marker, marker+"\n _ = source.FlowScopes()\n if backing, ok := Bundle(source); ok { _ = backing.ResolveEffectiveCompiledFlowEventSchema }", 1))
	compilerPath := filepath.Join(root, "internal/runtime/contracts/compiled_event_schema.go")
	compilerRaw, err := os.ReadFile(compilerPath)
	if err != nil {
		t.Fatal(err)
	}
	overlay[compilerPath] = append(compilerRaw, []byte(`
func hostileSchemaRecompile(unrelated *WorkflowContractBundle) any {
    borrowed := unrelated.compileCurrentEventDeclaration
    schema, _, _ := borrowed(".", "flow", "events.yaml", "x", "x", EventCatalogEntry{}, TypeCatalogDocument{})
    return schema
}
`)...)
	connectPath := filepath.Join(root, "internal/runtime/contracts/event_schema_ownership.go")
	connectRaw, err := os.ReadFile(connectPath)
	if err != nil {
		t.Fatal(err)
	}
	readerMarker := "func (b *WorkflowContractBundle) flowInputEventPinForResolvedEvent(flowID, eventType string) (CompiledFlowInputPin, bool) {"
	if strings.Count(string(connectRaw), readerMarker) != 1 {
		t.Fatal("exact input binding reader not found")
	}
	connectRaw = []byte(strings.Replace(string(connectRaw), readerMarker, readerMarker+"\n _ = b.ResolveFlowEventReference", 1))
	overlay[connectPath] = append(connectRaw, []byte(`
func hostileConnectRecompile(unrelated *WorkflowContractBundle) any {
    borrowed := compileEventSchemaOwnershipRows
    return borrowed(unrelated)
}
`)...)
	subscriptionPath := filepath.Join(root, "internal/runtime/semanticview/subscription_admission.go")
	subscriptionRaw, err := os.ReadFile(subscriptionPath)
	if err != nil {
		t.Fatal(err)
	}
	subscriptionMarker := "func fillAuthoredSubscriptionScope(source Source, req *AuthoredSubscriptionRequest) {"
	if strings.Count(string(subscriptionRaw), subscriptionMarker) != 1 {
		t.Fatal("subscription scope owner not found")
	}
	overlay[subscriptionPath] = []byte(strings.Replace(string(subscriptionRaw), subscriptionMarker, subscriptionMarker+"\n _ = runtimecontracts.ActivitySitesForNode", 1))
	connectorPath := filepath.Join(root, "internal/providerconnectors/packs.go")
	connectorRaw, err := os.ReadFile(connectorPath)
	if err != nil {
		t.Fatal(err)
	}
	connectorMarker := "func SourceWithConnectorPackImports(source semanticview.Source, registry *PackRegistry) (semanticview.Source, error) {"
	if strings.Count(string(connectorRaw), connectorMarker) != 1 {
		t.Fatal("connector composition owner not found")
	}
	overlay[connectorPath] = []byte(strings.Replace(string(connectorRaw), connectorMarker, connectorMarker+"\n renamedGenerator := runtimecontracts.ActivityResultEventSchemasForSite\n _ = renamedGenerator", 1))
	overlay[filepath.Join(root, "internal/runtime/hostile_connector_schema.go")] = []byte(`package runtime
import renamed "github.com/division-sh/swarm/internal/runtime/contracts"
func hostileEffectiveSourceGeneration() any {
    alias := renamed.ActivityApprovalEventCatalogEntry
    return alias
}
`)
	findings := generatedSchemaFindings(t, overlay)
	alias, scope, recompile, subscription, connect, receiver, composition, connector, effective := false, false, false, false, false, false, false, false, false
	for _, finding := range findings {
		alias = alias || strings.Contains(finding, "hostileGeneratedSchema")
		scope = scope || (strings.Contains(finding, "ResolveEventSchema") && strings.HasSuffix(finding, ":FlowScopes"))
		recompile = recompile || strings.Contains(finding, "hostileSchemaRecompile")
		subscription = subscription || strings.Contains(finding, "fillAuthoredSubscriptionScope")
		connect = connect || strings.Contains(finding, "hostileConnectRecompile")
		receiver = receiver || strings.Contains(finding, "flowInputEventPinForResolvedEvent")
		composition = composition || (strings.Contains(finding, "ResolveEventSchema") && strings.Contains(finding, ":bypassed_composed_source"))
		connector = connector || strings.Contains(finding, "SourceWithConnectorPackImports")
		effective = effective || strings.Contains(finding, "hostileEffectiveSourceGeneration")
	}
	if len(findings) != 9 || !alias || !scope || !recompile || !subscription || !connect || !receiver || !composition || !connector || !effective {
		t.Fatalf("guard missed alias or in-owner scope search: %v", findings)
	}
}

func generatedSchemaFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{Dir: agentNameGuardRepoRoot(t), Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedCompiledGoFiles},
		"./internal/runtime/contracts", "./internal/runtime/semanticview", "./internal/runtime/bootverify", "./internal/runtime/engine", "./internal/runtime/pipeline", "./internal/runtime/manager", "./internal/runtime/tools", "./internal/runtime/bus", "./internal/runtime/core/pinrouting", "./internal/runtime/accprojection", "./internal/runtime/scenarioderivation", "./internal/cliapp", "./internal/store/internal/backend/decisionpersistence", "./internal/providerconnectors", "./internal/runtime", "./internal/serveapp")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) != 0 {
		t.Fatal("generated schema guard must typecheck all consumers")
	}
	const contracts = "github.com/division-sh/swarm/internal/runtime/contracts"
	var findings []string
	for _, pkg := range pkgs {
		allowed := map[types.Object]bool{}
		retainedReceiverReaders := map[types.Object]bool{}
		var bindingCompiler types.Object
		var activityCompiler types.Object
		var bundleBindingAdapter types.Object
		if pkg.PkgPath == "github.com/division-sh/swarm/internal/runtime/semanticview" {
			adapter := pkg.Types.Scope().Lookup("bundleSource").Type().(*types.Named)
			for i := 0; i < adapter.NumMethods(); i++ {
				if method := adapter.Method(i); method.Name() == "ResolveEffectiveCompiledFlowEventSchema" {
					bundleBindingAdapter = method
				}
			}
		}
		if pkg.PkgPath == contracts {
			for _, name := range []string{"connectedEventSchemaOwnershipRow", "eventSchemaReceiverOwnerKey", "validateCompiledConnectEventSchemaOwnership"} {
				retainedReceiverReaders[pkg.Types.Scope().Lookup(name)] = true
			}
			allowed[pkg.Types.Scope().Lookup("EventSchemaRegistryFromBundle")] = true
			bundle := pkg.Types.Scope().Lookup("WorkflowContractBundle").Type().(*types.Named)
			for i := 0; i < bundle.NumMethods(); i++ {
				method := bundle.Method(i)
				if method.Name() == "EventEntries" || method.Name() == "ResolvedEventCatalog" {
					allowed[method] = true
				}
				if method.Name() == "compileEventSchemaBindings" {
					bindingCompiler = method
				}
				if method.Name() == "generatedActivityDeclarationRecords" {
					activityCompiler = method
				}
				if method.Name() == "flowInputEventPinForResolvedEvent" {
					retainedReceiverReaders[method] = true
				}
			}
		}
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner := pkg.TypesInfo.Defs[fn.Name]
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					id, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					used, ok := pkg.TypesInfo.Uses[id].(*types.Func)
					if !ok || used.Pkg() == nil {
						return true
					}
					if bundleBindingAdapter != nil && used.Pkg().Path() == contracts && used.Name() == "ResolveEffectiveCompiledFlowEventSchema" && owner != bundleBindingAdapter {
						findings = append(findings, owner.String()+":bypassed_composed_source")
					}
					if used.Pkg().Path() == contracts && (used.Name() == "GeneratedActivityEventEntries" || used.Name() == "GeneratedActivityEventSchemas") && !allowed[owner] {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					if used.Pkg().Path() == contracts && (used.Name() == "compileCurrentEventDeclaration" || used.Name() == "generatedActivityDeclarationRecords") && owner != bindingCompiler {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					if used.Pkg().Path() == contracts && (used.Name() == "ActivityResultEventCatalogEntry" || used.Name() == "ActivityResultEventSchemasForSite" || used.Name() == "ActivityApprovalEventCatalogEntry" || used.Name() == "activityApprovalEventSchema") && owner != activityCompiler {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					if used.Pkg().Path() == contracts && used.Name() == "compileEventSchemaOwnershipRows" && owner != pkg.Types.Scope().Lookup("populateEventSchemaOwnershipIndex") {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					if used.Pkg().Path() == contracts && retainedReceiverReaders[owner] && (used.Name() == "ResolveFlowEventReference" || used.Name() == "FlowPath" || used.Name() == "FlowScopes") {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					if used.Pkg().Path() == contracts && (used.Name() == "ActivitySitesForNode" || used.Name() == "ActivityResultEventsForSite") && owner == pkg.Types.Scope().Lookup("fillAuthoredSubscriptionScope") {
						findings = append(findings, owner.String()+":"+used.FullName())
					}
					// These exact functions resolve one supplied flow. Enumeration
					// there would reintroduce the foreign-sibling rescue.
					if used.Name() == "FlowScopes" && (owner == pkg.Types.Scope().Lookup("ResolveEventSchema") || owner == pkg.Types.Scope().Lookup("bindCompiledEventSchema")) {
						findings = append(findings, owner.String()+":FlowScopes")
					}
					return true
				})
			}
		}
	}
	return findings
}
