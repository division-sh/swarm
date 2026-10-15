package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAgentTopologyMutationProductionConsumersStatic(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	usage := newAgentTopologySourceUsage()
	for _, dir := range []string{
		"internal/store/internal/runtimepersistence",
		"internal/store/internal/backend/agentpersistence",
		"internal/store/internal/backend/effectpersistence",
	} {
		inspectAgentTopologyProductionPackage(t, root, dir, usage)
	}
	assertAgentTopologyWriteOwnership(t, usage)
	assertAgentTopologyAuthorizationCalls(t, usage.calls)

	manager := newAgentTopologySourceUsage()
	inspectAgentTopologyProductionPackage(t, root, "internal/runtime/manager", manager)
	if got := manager.calls["UpsertAgent"]; got != 0 {
		t.Fatalf("production UpsertAgent calls in manager = %d, want zero raw topology writers", got)
	}
	proveAgentTopologyFixtureWriteAccounting(t)
}

func assertAgentTopologyAuthorizationCalls(t *testing.T, calls map[string]int) {
	t.Helper()
	for name, want := range map[string]int{
		"authorizeAgentTopologyMutation":                2,
		"AuthorizePostgresDynamicAgentTopologyMutation": 0,
		"AuthorizeSQLiteDynamicAgentTopologyMutation":   0,
		"AuthorizePostgresRawAgentTopologyMutation":     0,
		"AuthorizeSQLiteRawAgentTopologyMutation":       0,
	} {
		if got := calls[name]; got != want {
			t.Fatalf("production %s calls = %d, want %d", name, got, want)
		}
	}
}

type agentTopologySourceUsage struct {
	calls, productionWrites map[string]int
	fixtureWrites           map[operatorSnapshotFixtureDeclaration]int
}

func newAgentTopologySourceUsage() *agentTopologySourceUsage {
	return &agentTopologySourceUsage{
		calls:            map[string]int{},
		productionWrites: map[string]int{},
		fixtureWrites:    map[operatorSnapshotFixtureDeclaration]int{},
	}
}

func agentTopologyWriteOwnershipMatches(usage *agentTopologySourceUsage) bool {
	wantWrites := map[string]int{
		// The original transaction owns absent-row insertion and predecessor CAS.
		// Lifecycle UPSERTs and effect-fence no-op UPDATEs are retired in R5.2.
		"internal/store/internal/backend/agentpersistence/lifecycle_cas.go": 2,
	}
	return reflect.DeepEqual(usage.productionWrites, wantWrites) &&
		reflect.DeepEqual(usage.fixtureWrites, operatorSnapshotAgentWriteDeclarations)
}

func assertAgentTopologyWriteOwnership(t *testing.T, usage *agentTopologySourceUsage) {
	t.Helper()
	if !agentTopologyWriteOwnershipMatches(usage) {
		t.Fatalf("agents-table writers: production=%#v fixtures=%#v; want exactly two lifecycle CAS writes and the two exact single-write fixture declarations", usage.productionWrites, usage.fixtureWrites)
	}
}

func inspectAgentTopologyProductionPackage(t *testing.T, root, dir string, usage *agentTopologySourceUsage) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.ToSlash(filepath.Join(dir, name))
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		inspectAgentTopologySource(path, parsed, usage)
	}
}

func inspectAgentTopologySource(path string, parsed *ast.File, usage *agentTopologySourceUsage) {
	for _, declaration := range parsed.Decls {
		identity := operatorSnapshotFixtureDeclaration{path: path}
		fixture := false
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			identity = operatorSnapshotDeclaration(path, fn)
			_, expected := operatorSnapshotAgentWriteDeclarations[identity]
			fixture = expected && operatorSnapshotOwnsFunction(path, fn)
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				switch fn := call.Fun.(type) {
				case *ast.Ident:
					usage.calls[fn.Name]++
				case *ast.SelectorExpr:
					usage.calls[fn.Sel.Name]++
				}
			}
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			raw := strings.ToUpper(literal.Value)
			if strings.Contains(raw, "INSERT INTO AGENTS") || strings.Contains(raw, "UPDATE AGENTS") || strings.Contains(raw, "DELETE FROM AGENTS") {
				if fixture {
					usage.fixtureWrites[identity]++
				} else {
					usage.productionWrites[path]++
				}
			}
			return true
		})
	}
}

func agentTopologyFixtureControlSource(identity operatorSnapshotFixtureDeclaration, body string) string {
	receiver := ""
	if identity.receiver != "" {
		receiver = "(f " + identity.receiver + ") "
	}
	return "func " + receiver + identity.name + "() { " + body + " }"
}

func agentTopologyFixtureControlUsage(t *testing.T, replacement operatorSnapshotFixtureDeclaration, source string) *agentTopologySourceUsage {
	t.Helper()
	usage := newAgentTopologySourceUsage()
	usage.productionWrites["internal/store/internal/backend/agentpersistence/lifecycle_cas.go"] = 2
	for identity := range operatorSnapshotAgentWriteDeclarations {
		body := agentTopologyFixtureControlSource(identity, "use(`UPDATE agents SET status='active'`)")
		if identity == replacement {
			body = source
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), identity.path, "package probe\n"+body, 0)
		if err != nil {
			t.Fatal(err)
		}
		inspectAgentTopologySource(identity.path, parsed, usage)
	}
	return usage
}

func proveAgentTopologyFixtureWriteAccounting(t *testing.T) {
	t.Helper()
	for identity := range operatorSnapshotAgentWriteDeclarations {
		t.Run(identity.name, func(t *testing.T) {
			original := agentTopologyFixtureControlSource(identity, "use(`UPDATE agents SET status='active'`)")
			if !agentTopologyWriteOwnershipMatches(agentTopologyFixtureControlUsage(t, identity, original)) {
				t.Fatal("exact fixture declarations must pass")
			}
			foreign := identity
			foreign.receiver = "*ForeignSnapshot"
			for label, source := range map[string]string{
				"ordinary-function": original + "\nfunc serve(){ use(`UPDATE agents SET status='active'`) }",
				"initializer":       original + "\nvar escape=func(){ use(`DELETE FROM agents`) }()",
				"extra-write":       agentTopologyFixtureControlSource(identity, "use(`UPDATE agents SET status='active'`); use(`DELETE FROM agents`)"),
				"missing-write":     agentTopologyFixtureControlSource(identity, "use(`SELECT * FROM agents`)"),
				"foreign-receiver":  agentTopologyFixtureControlSource(foreign, "use(`UPDATE agents SET status='active'`)"),
				"foreign-name":      strings.Replace(original, identity.name+"()", identity.name+"Foreign()", 1),
			} {
				t.Run(label, func(t *testing.T) {
					if agentTopologyWriteOwnershipMatches(agentTopologyFixtureControlUsage(t, identity, source)) {
						t.Fatal("foreign, missing or additional fixture write passed")
					}
				})
			}
			proveAgentTopologyFixtureAuthorizationAndConsumers(t, identity)
		})
	}
}

func proveAgentTopologyFixtureAuthorizationAndConsumers(t *testing.T, identity operatorSnapshotFixtureDeclaration) {
	t.Helper()
	source := agentTopologyFixtureControlSource(identity, "authorizeAgentTopologyMutation(); use(`UPDATE agents SET status='active'`)")
	usage := agentTopologyFixtureControlUsage(t, identity, source)
	if usage.calls["authorizeAgentTopologyMutation"] != 1 {
		t.Fatal("fixture role hid an authorization call")
	}
	for _, source := range []string{
		"func serve(){ mutateOperatorSnapshotAgentTx() }",
		"var escape=func(){ mutateOperatorSnapshotAgentTx() }()",
		"func (f *ForeignSnapshot) " + identity.name + "(){ mutateOperatorSnapshotAgentTx() }",
	} {
		violations, err := operatorSnapshotConsumerViolations(identity.path, []byte("package probe\n"+source))
		if err != nil || len(violations) == 0 {
			t.Fatalf("fixture production/foreign-receiver escape passed: %s: %v", source, err)
		}
	}
}
