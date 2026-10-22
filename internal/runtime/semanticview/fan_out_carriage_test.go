package semanticview_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type fanOutCarriageTestSource struct {
	semanticview.Source
	plans []contracts.FanOutCompiledPlan
}

func (s fanOutCarriageTestSource) FanOutPlans() []contracts.FanOutCompiledPlan { return s.plans }

func TestFanOutPlanCarriageRequiresExactArtifactAndDetachedEvidence(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopyForkFanOutCarrier(t, false, false), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	plans := source.FanOutPlans()
	if len(plans) != 1 {
		t.Fatal("fixture must contain one exact compiled plan")
	}
	carriage, err := semanticview.CompileFanOutPlanCarriage(source)
	if err != nil {
		t.Fatal(err)
	}
	ref, semantics, err := carriage.Resolve(plans[0].Ref.ElementRef)
	if err != nil || ref != plans[0].Ref {
		t.Fatalf("exact plan evidence: %+v %v", ref, err)
	}
	semantics.Emit.Fields["value"] = contracts.CELExpression("'changed'")
	_, again, err := carriage.Resolve(ref.ElementRef)
	if err != nil || !reflect.DeepEqual(again.Emit.Fields["value"], plans[0].Emit.Fields["value"]) {
		t.Fatal("consumer mutation changed retained evidence")
	}
	if err := carriage.RequireSource(ref.BundleHash); err != nil {
		t.Fatal(err)
	}
	if err := carriage.RequireSource("foreign"); err == nil {
		t.Fatal("carriage borrowed another artifact's authority")
	}
	missing := ref.ElementRef
	missing.SemanticPath += "/missing"
	if _, _, err := carriage.Resolve(missing); err == nil {
		t.Fatal("carriage inferred an absent declaration")
	}
	for _, variant := range []string{"digest", "bundle", "duplicate"} {
		t.Run(variant, func(t *testing.T) {
			bad := append([]contracts.FanOutCompiledPlan(nil), plans...)
			switch variant {
			case "digest":
				bad[0].Ref.SemanticDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
			case "bundle":
				bad[0].Ref.BundleHash = "foreign"
			case "duplicate":
				bad = append(bad, bad[0])
			}
			if _, err := semanticview.CompileFanOutPlanCarriage(fanOutCarriageTestSource{Source: source, plans: bad}); err == nil {
				t.Fatal("contradictory compiled evidence minted a carriage")
			}
		})
	}
	var decoded semanticview.FanOutPlanCarriage
	if err := json.Unmarshal([]byte(`{"bundle_hash":"forged","plans":{}}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.IsZero() || decoded.RequireSource(ref.BundleHash) == nil {
		t.Fatal("caller-authored JSON minted semantic evidence")
	}
	if _, err := semanticview.CompileFanOutPlanCarriage(semanticview.Wrap(&contracts.WorkflowContractBundle{})); err == nil {
		t.Fatal("unadmitted bundle minted semantic evidence")
	}
}
