package bootverify_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterImportedApprovalReferences(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyLifecycleEmitterImportedApprovalReferences(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	source := projection.Source()
	generated := source.ResolvedEventCatalog()
	for _, local := range []string{"telegram_send_message.succeeded", "telegram_send_message.failed", "telegram_send_message.revision_requested", "telegram_send_message.rejected"} {
		canonical := "telegram-chat/" + local
		if _, ok := generated[canonical]; !ok {
			t.Fatalf("actual connector has no generated owner for %s", canonical)
		}
		if _, ok := source.AuthoredResolvedEventCatalog()[canonical]; ok {
			t.Fatalf("approval result %s has a dummy authored schema", canonical)
		}
		entry, key, ok := source.ResolveFlowEventCatalogEntry("telegram-chat", local)
		if !ok {
			t.Errorf("local generated schema missing: %s resolves to %s; canonical lookup=%v", local, source.ResolveFlowEventReference("telegram-chat", local), semanticview.ResolveFlowEventProof(source, "telegram-chat", canonical).HasSchema)
		} else if key != canonical || !reflect.DeepEqual(entry, generated[canonical]) {
			t.Errorf("wrong generated schema owner for %s: key=%s", local, key)
		}
		for _, foreign := range []string{"telegram-ingress", "."} {
			if _, _, ok := source.ResolveFlowEventCatalogEntry(foreign, local); ok {
				t.Errorf("another flow acquired local generated reference %s", local)
			}
		}
	}
	for _, unowned := range []string{"not_an_activity.revision_requested", "not_an_activity.rejected", "telegram_send_message.unknown_outcome"} {
		if _, _, ok := source.ResolveFlowEventCatalogEntry("telegram-chat", unowned); ok {
			t.Errorf("generated-looking spelling acquired schema authority: %s", unowned)
		}
	}
	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	for _, finding := range report.Findings {
		if finding.CheckID == "transition_reference_validation" && strings.Contains(finding.Message, "telegram_send_message") {
			t.Fatalf("generated approval references rejected: %#v", report.Findings)
		}
	}
}
