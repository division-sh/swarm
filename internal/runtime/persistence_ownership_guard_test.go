package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

func TestRuntimeCorePersistenceRolesAreConstructorInputs(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	assertNoProductionImports(t, filepath.Join(root, "internal", "runtime"), map[string]bool{
		"github.com/division-sh/swarm/internal/store":           true,
		"github.com/division-sh/swarm/internal/store/runbundle": true,
	})
	runtimeSource := readOwnershipSource(t, root, "internal/runtime/runtime.go")
	assertOwnershipSourceContains(t, runtimeSource,
		"WorkflowPersistence            runtimepipeline.WorkflowPersistence",
		"LiveSessionAcquirer            llm.LiveSessionAcquirer",
		"SessionResetter                sessions.Resetter",
	)
	assertOwnershipSourceExcludes(t, runtimeSource, "type Stores struct", "Stores Stores", "Stores *Stores")
}

func TestWorkflowPersistenceIsOpaqueAndConcreteStoreDoesNotEscape(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	path := filepath.Join(root, "internal", "runtime", "pipeline", "workflow_instance_store.go")
	file := parseOwnershipFile(t, path)
	foundOpaque := false
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec := spec.(*ast.TypeSpec)
			if typeSpec.Name.Name == "WorkflowInstanceStore" {
				t.Fatal("concrete WorkflowInstanceStore must remain unexported")
			}
			if typeSpec.Name.Name != "WorkflowPersistence" {
				continue
			}
			foundOpaque = true
			value, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatal("WorkflowPersistence must be an opaque struct")
			}
			for _, field := range value.Fields.List {
				for _, name := range field.Names {
					if name.Name != "" && unicode.IsUpper(rune(name.Name[0])) {
						t.Fatalf("WorkflowPersistence exposes field %s", name.Name)
					}
				}
			}
		}
	}
	if !foundOpaque {
		t.Fatal("WorkflowPersistence declaration not found")
	}
	assertProductionTreeExcludes(t, filepath.Join(root, "internal", "runtime", "pipeline"),
		"RunPipelineMutation(",
		"NewWorkflowInstanceStore(",
		"NewSQLiteWorkflowInstanceStore",
		"ConfigureDeliveryLifecycleStore",
		"ConfigurePipelineObligationStore",
		"DeliveryLifecycleStore interface",
	)
}

func TestEventBusDurableDependenciesAreExplicit(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	source := readOwnershipSource(t, root, "internal/runtime/bus/eventbus.go")
	assertOwnershipSourceContains(t, source,
		"type DurableDependencies struct",
		"FlowRouteTopology     FlowInstanceRouteTopologyPersistence",
		"RunLifecycle          runtimerunlifecycle.OperationOwner",
		"DeliveryLifecycle     runtimedelivery.Store",
	)
	assertOwnershipSourceExcludes(t, source,
		"RuntimeMutations",
		"RuntimeMutationRunner",
		"RunRuntimeMutationContext",
		"store.(runtimerunlifecycle.OperationOwner)",
		"store.(runtimedelivery.Store)",
	)
}

func TestEventBusConstructorsDoNotClassifyStoreCapabilities(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	source := readOwnershipSource(t, root, "internal/runtime/bus/eventbus.go")
	assertOwnershipSourceContains(t, source, "func NewEphemeralEventBus(", "func NewEphemeralEventBusWithOptions(")
	assertOwnershipSourceExcludes(t, source, "pipelineObligationStoreProvider", "deliveryLifecycleStore")
}

func TestEventBusRoutePersistenceDependenciesAreExplicit(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	source := readOwnershipSource(t, root, "internal/runtime/bus/eventbus.go")
	assertOwnershipSourceContains(t, source,
		"FlowRoutes            FlowInstanceRoutePersistence",
		"FlowRouteRecords      FlowInstanceRouteRecordReader",
		"FlowRouteSets         FlowInstanceRouteSetPersistence",
		"FlowRouteTopology     FlowInstanceRouteTopologyPersistence",
		"FlowRouteRollback     FlowInstanceRouteRollbackPersistence",
		"ActiveAgents          ActiveAgentDescriptorLister",
		"ActiveFlows           ActiveFlowInstanceDescriptorLister",
	)
}

func TestWorkflowMutationExecutorDoesNotEscapeOpaquePersistence(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	coordinator := readOwnershipSource(t, root, "internal/runtime/pipeline/coordinator.go")
	eventBus := readOwnershipSource(t, root, "internal/runtime/bus/eventbus.go")
	assertOwnershipSourceExcludes(t, coordinator+eventBus,
		"RuntimeMutations",
		"RuntimeMutationRunner",
		"RunRuntimeMutationContext",
	)

	workflowPersistence := readOwnershipSource(t, root, "internal/runtime/pipeline/workflow_instance_store.go")
	assertOwnershipSourceContains(t, workflowPersistence,
		"type WorkflowPersistenceOwner interface",
		"func NewWorkflowPersistence(owner WorkflowPersistenceOwner)",
	)
	assertOwnershipSourceExcludes(t, workflowPersistence,
		"type runtimeMutationRunner interface",
		"runtimeMutation  runtimeMutationRunner",
		"type RuntimeMutationRunner interface",
		"RuntimeMutationRunner runtimeMutationRunner",
		"*sql.DB",
	)
}

func TestAgentManagerPersistenceRolesAreExplicit(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	typesSource := readOwnershipSource(t, root, "internal/runtime/manager/types.go")
	assertOwnershipSourceContains(t, typesSource,
		"type PersistenceRoles struct",
		"LifecycleCensus      AgentLifecycleCellCensus",
		"LifecycleState       AgentLifecycleStateReader",
		"LifecycleEffects     runtimeeffects.Store",
		"EffectsRecovery      runtimeeffects.RecoveryStore",
		"EventExistence       EventExistenceReader",
		"DirectiveOperations  runtimeagentcontrol.DirectiveOperationStore",
		"DirectiveTargets     AgentDirectiveRunTargetResolver",
	)
	assertProductionTreeExcludes(t, filepath.Join(root, "internal", "runtime", "manager"),
		"store.(runtimeeffects.Store)",
		"store.(runtimeeffects.RecoveryStore)",
		"store.(AgentLifecycleDiagnosticPersistence)",
		"store.(runtimeagentcontrol.DirectiveOperationStore)",
		"store.(AgentDirectiveRunTargetResolver)",
	)
}

func TestAgentLifecycleDependenciesAreExplicit(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	source := readOwnershipSource(t, root, "internal/runtime/manager/lifecycle_coordinator.go")
	assertOwnershipSourceExcludes(t, source, ".(runtimeeffects.Store)", ".(runtimeeffects.RecoveryStore)")
	typesSource := readOwnershipSource(t, root, "internal/runtime/manager/types.go")
	assertOwnershipSourceContains(t, typesSource, "LifecycleStore                 AgentLifecyclePersistence")
}

func TestAgentManagerDeliveryRolesAreExplicit(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	typesSource := readOwnershipSource(t, root, "internal/runtime/manager/types.go")
	assertOwnershipSourceContains(t, typesSource,
		"DeliveryRuntimeOwner interface",
		"DeliveryRuntime      DeliveryRuntimeOwner",
		"DeliveryStore                  runtimedelivery.Store",
	)
}

func TestSelectedContractExecutionUsesSemanticPorts(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	dir := filepath.Join(root, "internal", "runtime", "runforkexecution")
	assertNoProductionImports(t, dir, map[string]bool{"github.com/division-sh/swarm/internal/store": true})
	portsPath := filepath.Join(root, "internal", "runtime", "runforkexecution", "store_ports.go")
	portsFile := parseOwnershipFile(t, portsPath)
	ownerFound := false
	for _, decl := range portsFile.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec := spec.(*ast.TypeSpec)
			if typeSpec.Name.Name == "SelectedContractExecutionStore" {
				t.Fatal("selected-contract execution must not expose a broad store interface")
			}
			if typeSpec.Name.Name != "SelectedContractExecutionOwner" {
				continue
			}
			ownerFound = true
			owner, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatal("selected-contract execution owner must be an opaque struct")
			}
			for _, field := range owner.Fields.List {
				for _, name := range field.Names {
					if name.IsExported() {
						t.Fatalf("selected-contract execution owner exposes field %s", name.Name)
					}
				}
			}
		}
	}
	if !ownerFound {
		t.Fatal("opaque SelectedContractExecutionOwner declaration not found")
	}
	execution := readOwnershipSource(t, root, "internal/runtime/runforkexecution/execution.go")
	assertOwnershipSourceContains(t, execution,
		"Owner             SelectedContractExecutionOwner",
		"ports, err := req.Owner.require()",
	)
	assertOwnershipSourceExcludes(t, execution,
		"Store               SelectedContractExecutionStore",
		"Store SelectedContractExecutionStore",
		"req.Store",
	)
	portsSource := readOwnershipSource(t, root, "internal/runtime/runforkexecution/store_ports.go")
	assertOwnershipSourceExcludes(t, portsSource, "*store.PostgresStore", "*store.SQLiteRuntimeStore")
}

func TestSelectedContractSourceLoaderIsExact(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	source := readOwnershipSource(t, root, "internal/runtime/runforkexecution/admission.go")
	assertOwnershipSourceContains(t, source,
		"type SelectedContractSourceLoader interface",
		"LoadRunForkSelectedContractSourceForRequest(context.Context, SelectedContractSourceLoadRequest)",
		"loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, req)",
	)
	assertOwnershipSourceExcludes(t, source, "loader.(", "sourceLoader.(")
}

func TestServeCompositionProvidesExactRuntimePorts(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	mainSource := readOwnershipSource(t, root, "internal/serveapp/main.go")
	facadeSource := readOwnershipSource(t, root, "internal/serveapp/store_facade.go")
	assertOwnershipSourceContains(t, mainSource,
		"runtimepipeline.NewWorkflowPersistence(pg)",
		"runtimepipeline.NewWorkflowPersistence(sqliteStore)",
		"LiveSessionAcquirer:            pg",
		"LiveSessionAcquirer:            sqliteStore",
		"SessionResetter:                pg",
		"SessionResetter:                sqliteStore",
	)
	assertOwnershipSourceContains(t, facadeSource,
		"WorkflowPersistence:            s.WorkflowPersistence",
		"LiveSessionAcquirer:            s.LiveSessionAcquirer",
		"SessionResetter:                s.SessionResetter",
	)
	assertOwnershipSourceExcludes(t, mainSource+facadeSource, "runtime.Stores", ".runtimeStores(", "predecessor.Stores")
}

func TestRuntimeSemanticModelsHaveSingleConsumerOwner(t *testing.T) {
	root := persistenceOwnershipRepoRoot(t)
	assertNoAliasesToRuntimeModels(t, filepath.Join(root, "internal"))
	for _, rel := range []string{"internal/runtime/runfork", "internal/runtime/runbundle"} {
		assertNoProductionImportsWithPrefix(t, filepath.Join(root, rel), []string{
			"github.com/division-sh/swarm/internal/store",
			"github.com/division-sh/swarm/internal/serveapp",
			"github.com/division-sh/swarm/internal/apiv1",
			"github.com/division-sh/swarm/internal/runtime/manager",
			"github.com/division-sh/swarm/internal/runtime/bus",
			"github.com/division-sh/swarm/internal/runtime/runforkadmission",
			"github.com/division-sh/swarm/internal/runtime/runforkexecution",
		})
	}
}

func persistenceOwnershipRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("resolve ownership guard path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func readOwnershipSource(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

func parseOwnershipFile(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func assertOwnershipSourceContains(t *testing.T, source string, required ...string) {
	t.Helper()
	for _, value := range required {
		if !strings.Contains(source, value) {
			t.Fatalf("required ownership declaration %q is missing", value)
		}
	}
}

func assertOwnershipSourceExcludes(t *testing.T, source string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if strings.Contains(source, value) {
			t.Fatalf("retired ownership seam %q remains", value)
		}
	}
}

func assertProductionTreeExcludes(t *testing.T, root string, forbidden ...string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, value := range forbidden {
			if strings.Contains(string(data), value) {
				t.Errorf("%s retains retired ownership seam %q", path, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

func assertNoProductionImports(t *testing.T, root string, forbidden map[string]bool) {
	t.Helper()
	assertProductionImports(t, root, func(path, importPath string) {
		if forbidden[importPath] {
			t.Errorf("%s imports forbidden ownership package %s", path, importPath)
		}
	})
}

func assertNoProductionImportsWithPrefix(t *testing.T, root string, forbidden []string) {
	t.Helper()
	assertProductionImports(t, root, func(path, importPath string) {
		for _, prefix := range forbidden {
			if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
				t.Errorf("%s has reverse semantic-owner import %s", path, importPath)
			}
		}
	})
}

func assertProductionImports(t *testing.T, root string, inspect func(string, string)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			inspect(path, value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk imports under %s: %v", root, err)
	}
}

func assertNoAliasesToRuntimeModels(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		aliases := map[string]bool{}
		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if importPath != "github.com/division-sh/swarm/internal/runtime/runfork" && importPath != "github.com/division-sh/swarm/internal/runtime/runbundle" {
				continue
			}
			name := filepath.Base(importPath)
			if imported.Name != nil {
				name = imported.Name.Name
			}
			aliases[name] = true
		}
		if len(aliases) == 0 {
			return nil
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec := spec.(*ast.TypeSpec)
				if !typeSpec.Assign.IsValid() {
					continue
				}
				usesModel := false
				ast.Inspect(typeSpec.Type, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					ident, ok := selector.X.(*ast.Ident)
					if ok && aliases[ident.Name] {
						usesModel = true
					}
					return true
				})
				if usesModel {
					t.Errorf("%s declares compatibility alias %s to consumer-owned runtime model", path, typeSpec.Name.Name)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan runtime model aliases: %v", err)
	}
}
