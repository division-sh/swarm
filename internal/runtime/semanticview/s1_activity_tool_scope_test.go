package semanticview_test

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ActivityToolCompilationIncludesInheritedDeclarations(t *testing.T) {
	bundle := loadScopedActivityBundle(t, packfixture.ScopedActivityMembers())
	source, err := semanticview.CompileActivityToolBindings(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("inherited tool did not reach activity compilation: %v", err)
	}
	for _, flow := range []string{".", "child", "sibling", "inherited"} {
		event := "send.succeeded"
		if flow != "." {
			event = flow + "/" + event
		}
		resolution := semanticview.ResolveEventSchema(source, flow, event)
		if !resolution.HasCompiled || resolution.Classification != runtimecontracts.CompiledEventSchemaGenerated {
			t.Fatalf("activity outcome lost its exact compiled owner: flow=%s %+v", flow, resolution)
		}
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flow, "sender")
		if err != nil {
			t.Fatal(err)
		}
		tool, present := source.ActivityToolEntry(node, "shared")
		if !present {
			t.Fatalf("scoped activity tool unavailable in %s", flow)
		}
		origin := flow
		if flow == "." || flow == "inherited" {
			origin = "root"
		}
		if actual, _ := tool.OutputSchema().Property("origin"); !actual.Equal(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum(origin))) {
			t.Fatalf("activity execution and result schema disagree in %s", flow)
		}
	}
}

func loadScopedActivityBundle(t *testing.T, members map[string][]byte) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "scoped-activity", artifact)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

type hiddenActivityToolReadbacks struct{ semanticview.Source }

func (s hiddenActivityToolReadbacks) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	return nil
}

func (s hiddenActivityToolReadbacks) FlowScopes() []semanticview.FlowScope {
	scopes := s.Source.FlowScopes()
	for index := range scopes {
		scopes[index].Tools = nil
	}
	return scopes
}

func TestS1ActivityToolBindingsRetainFrozenCompilerSnapshot(t *testing.T) {
	bundle := loadScopedActivityBundle(t, packfixture.ScopedActivityMembers())
	source, err := semanticview.CompileActivityToolBindings(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("child", "sender")
	if err != nil {
		t.Fatal(err)
	}
	before, present := source.ActivityToolEntry(node, "shared")
	if !present {
		t.Fatal("missing bound child tool")
	}
	expected, err := before.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	rootTool, _ := bundle.ToolEntryForFlow(".", "shared")
	child, present := bundle.FlowViewByID("child")
	if !present {
		t.Fatal("missing actual child declaration")
	}
	child.Tools["shared"] = rootTool
	bundle.Tools["shared"] = rootTool
	for _, reader := range []semanticview.Source{source, hiddenActivityToolReadbacks{Source: source}} {
		after, present := reader.ActivityToolEntry(node, "shared")
		actual, err := after.CanonicalHash()
		if !present || err != nil || actual != expected {
			t.Fatalf("readback mutation replaced frozen activity binding: %v", err)
		}
		resolution := semanticview.ResolveEventSchema(reader, "child", "child/send.succeeded")
		if !resolution.HasCompiled {
			t.Fatal("hiding readbacks lost compiler-owned result evidence")
		}
	}
}

func TestS1ActivityToolBindingsNeverFallBackFromMissingCompiledEntry(t *testing.T) {
	members := packfixture.ScopedActivityMembers()
	members["tools.yaml"] = append(members["tools.yaml"], []byte("root_only:\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: 'https://root.example.test/only'}\n")...)
	bundle := loadScopedActivityBundle(t, members)
	tools := map[string]map[string]runtimecontracts.ToolSchemaEntry{}
	for _, view := range bundle.FlowViews() {
		tools[view.Paths.FlowPath] = bundle.ToolEntriesForFlow(view.Paths.FlowPath)
		delete(tools[view.Paths.FlowPath], "root_only")
	}
	compiled, err := runtimecontracts.CompileActivityToolBindings(bundle, tools)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(compiled)
	if _, present := source.ToolEntries()["root_only"]; !present {
		t.Fatal("fixture lost the competing global/authored fallback")
	}
	for _, flow := range []string{".", "child", "inherited", "foreign"} {
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flow, "sender")
		if err != nil {
			t.Fatal(err)
		}
		if _, present := source.ActivityToolEntry(node, "root_only"); present {
			t.Fatalf("missing bound entry fell back to authored/global tool in %s", flow)
		}
		for _, name := range []string{" shared", "shared ", "unknown"} {
			if _, present := source.ActivityToolEntry(node, name); present {
				t.Fatalf("foreign tool spelling acquired a binding: %q", name)
			}
		}
	}
	if _, present := source.ActivityToolEntry(runtimeidentity.ExecutableNode{}, "shared"); present {
		t.Fatal("zero owner gained a tool binding")
	}
}

func TestS1ActivityToolCompilerRefusesReplacingFrozenAdmission(t *testing.T) {
	for _, change := range []string{"replace", "delete", "insert", "invalid", "foreign-flow", "missing-flow"} {
		t.Run(change, func(t *testing.T) {
			source, err := semanticview.CompileActivityToolBindings(semanticview.Wrap(loadScopedActivityBundle(t, packfixture.ScopedActivityMembers())))
			if err != nil {
				t.Fatal(err)
			}
			bundle, found := semanticview.Bundle(source)
			if !found {
				t.Fatal("missing frozen compiler")
			}
			tools := map[string]map[string]runtimecontracts.ToolSchemaEntry{}
			for _, scope := range source.FlowScopes() {
				tools[scope.ID] = scope.Tools
			}
			switch change {
			case "replace":
				tools["child"]["shared"] = tools["."]["shared"]
			case "delete":
				delete(tools["child"], "shared")
			case "insert":
				tools["child"]["injected"] = tools["."]["shared"]
			case "invalid":
				tools["child"]["shared"] = runtimecontracts.ToolSchemaEntry{}
			case "foreign-flow":
				tools["foreign"] = tools["."]
			case "missing-flow":
				delete(tools, "child")
			}
			compiled, err := runtimecontracts.CompileActivityToolBindings(bundle, tools)
			if err == nil || compiled != nil {
				t.Fatalf("direct compiler caller replaced frozen authority: compiled=%v err=%v", compiled != nil, err)
			}
		})
	}
}
