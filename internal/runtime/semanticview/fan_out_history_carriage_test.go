package semanticview_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestOriginalFanOutHistoryCarriageKeepsExactArtifactAndRole(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	load := func(root string) semanticview.Source {
		t.Helper()
		bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
		if err != nil {
			t.Fatal(err)
		}
		return semanticview.Wrap(bundle)
	}
	history := load(canonicalrouting.CopyForkFanOutCarrier(t, true, false))
	current := load(filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"))
	plan := history.FanOutPlans()[0]
	primary, err := semanticview.CompileOriginalLoopCarriage(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := primary.ResolveFanOut(plan.Ref, plan.Site.Node, plan.Site.EventType); err == nil {
		t.Fatal("current artifact inferred an archived fan-out role")
	}
	combined, err := primary.WithFanOutHistory(history)
	if err != nil {
		t.Fatal(err)
	}
	role, found, err := combined.ResolveFanOut(plan.Ref, plan.Site.Node, plan.Site.EventType)
	if err != nil || !found || role.FlowID() != plan.Site.Node.FlowPath() || role.LoopID() == "" || role.RevisionField() == "" {
		t.Fatalf("archived declaration lost its exact loop role: %+v %t %v", role, found, err)
	}
	currentBundle, _ := semanticview.Bundle(current)
	if err := combined.RequireSource(currentBundle.SourceArtifact.BundleHash()); err != nil {
		t.Fatal(err)
	}
	if combined.RequireSource(plan.Ref.BundleHash) == nil {
		t.Fatal("archived evidence changed the primary source authority")
	}
	if _, _, err := primary.ResolveFanOut(plan.Ref, plan.Site.Node, plan.Site.EventType); err == nil {
		t.Fatal("history extension mutated the original carriage")
	}
	bad := plan.Ref
	bad.BundleHash = "foreign"
	if _, _, err := combined.ResolveFanOut(bad, plan.Site.Node, plan.Site.EventType); err == nil {
		t.Fatal("history carriage guessed an unretained artifact")
	}
	if _, _, err := combined.ResolveFanOut(plan.Ref, plan.Site.Node, "other.handler"); err == nil {
		t.Fatal("history carriage borrowed a foreign handler's meaning")
	}
	if _, err := primary.WithFanOutHistory(semanticview.Wrap(&contracts.WorkflowContractBundle{})); err == nil {
		t.Fatal("unadmitted archive minted historical meaning")
	}
	var forged semanticview.OriginalLoopCarriage
	if err := json.Unmarshal([]byte(`{"bundle_hash":"forged"}`), &forged); err != nil {
		t.Fatal(err)
	}
	if _, err := forged.WithFanOutHistory(history); err == nil {
		t.Fatal("decoded JSON minted a primary source carriage")
	}
}
