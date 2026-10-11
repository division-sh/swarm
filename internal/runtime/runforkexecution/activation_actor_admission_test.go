package runforkexecution

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
