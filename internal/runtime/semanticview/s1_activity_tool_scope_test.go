package semanticview_test

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ActivityToolCompilationIncludesInheritedDeclarations(t *testing.T) {
	artifact, err := sourceartifact.AdmitMembers(packfixture.ScopedActivityMembers())
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "scoped-activity", artifact)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
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
	}
}
