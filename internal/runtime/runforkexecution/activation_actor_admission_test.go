package runforkexecution

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedFutureAgentDeclarationRetainsConfiguredRuntimeWithEmptyCensus(t *testing.T) {
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo,
		SourceRoot:       canonicalrouting.CopySelectedNestedConstructedAgentInput(t, false),
		PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := loader.LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	if !semanticview.SourceDeclaresAgents(loaded.Source) {
		t.Fatal("fixture lost its future template agent declaration")
	}
	planning := runfork.RunForkSelectedContractRecipientPlanning{Owner: runfork.RunForkSelectedContractRecipientPlanningOwner}
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}}
	plan, err := prepareSelectedContractAgentRuntimeMaterialization(context.Background(), loaded, planning, nil, SelectedContractAgentRuntimeOptions{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Options.Config != cfg || len(plan.Blueprints) != 0 || len(plan.ConfiguredPlans) != 0 || len(plan.Records) != 0 || !plan.Proof.MaterializationSupported || plan.Proof.MaterializationRequired {
		t.Fatalf("future declaration invented an initial actor or dropped runtime configuration: %+v", plan)
	}
	if _, err := prepareSelectedContractAgentRuntimeMaterialization(context.Background(), loaded, planning, nil, SelectedContractAgentRuntimeOptions{}); err == nil {
		t.Fatal("future agent declaration silently retained a nil execution factory")
	}
}

func TestSelectedFutureActorFingerprintBindsExactConstructionAttempt(t *testing.T) {
	admission := agenttopology.FlowReadinessPlan{
		RunID: uuid.NewString(), InstancePath: "work/one", PlanFingerprint: "sha256:" + strings.Repeat("a", 64), AttemptID: "1",
	}
	fingerprint, err := selectedPreparationFingerprint(admission)
	if err != nil {
		t.Fatal(err)
	}
	coordinates := managedcapabilities.SelectedForkPreparationCoordinates{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner", ProcessBootID: uuid.NewString(),
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64), SourceFingerprint: strings.Repeat("c", 64),
		AdmittedPlanFingerprint: fingerprint, ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64),
	}
	if err := coordinates.Validate(); err != nil {
		t.Fatalf("exact construction fact cannot enter existing probe authority: %v", err)
	}
	coordinates.AdmittedPlanFingerprint = admission.PlanFingerprint
	if err := coordinates.Validate(); err == nil {
		t.Fatal("namespaced planning hash silently replaced a preparation fingerprint")
	}
	for _, change := range []func(*agenttopology.FlowReadinessPlan){
		func(p *agenttopology.FlowReadinessPlan) { p.RunID = uuid.NewString() },
		func(p *agenttopology.FlowReadinessPlan) { p.InstancePath = "work/two" },
		func(p *agenttopology.FlowReadinessPlan) { p.PlanFingerprint = "sha256:" + strings.Repeat("f", 64) },
		func(p *agenttopology.FlowReadinessPlan) { p.AttemptID = "2" },
		func(p *agenttopology.FlowReadinessPlan) { p.Preparation = true },
	} {
		changed := admission
		change(&changed)
		next, err := selectedPreparationFingerprint(changed)
		if err != nil || next == fingerprint {
			t.Fatalf("construction coordinates aliased another scope/attempt: %+v err=%v", changed, err)
		}
	}
}
