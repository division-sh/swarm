package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestVerifyRetainedForkReportsKeepExactInvocationContexts(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	configPath := writeTestVerifyRuntimeConfig(t)
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
	if err != nil {
		t.Fatal(err)
	}
	_, bundle, _, err := loadCLIWorkflowModuleWithRuntimeConfig(root, CLISourcePlatformSpecPathOptions{SourceRoot: root}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := admitStructuralSource(cfg, bundle)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	opts, err := verifyDeploymentWorkflowOptions(ctx, cfg, bundle)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ValidateWorkflowContractSurface(ctx, source, opts)
	if err != nil {
		t.Fatal(err)
	}
	inspectVerifySourceAdmission(ctx, cfg, source, opts, &result)
	policy := bootverify.AdmissionFindingPolicy{FatalWarnings: opts.FatalBootWarnings, ExcludedFatalWarningChecks: opts.ExcludedFatalBootWarningChecks}
	if decision := result.BootReport.AdmissionDecision(policy); !decision.Complete {
		t.Fatalf("ordinary source control failed: %+v %+v", decision, result.BootReport.Findings)
	}
	ordinaryCount := len(result.BootReport.Observations)
	for range 2 {
		forkID := uuid.NewString()
		before := len(result.BootReport.Observations)
		fork := runforkexecution.InspectedSelectedForkRecovery{
			Entry:          runfork.SelectedForkRecoveryEntry{Binding: runfork.RunForkSelectedContractBinding{ForkRunID: forkID}},
			SelectedSource: runforkexecution.LoadedSelectedContractSource{Source: source},
		}
		if err := inspectVerifyRetainedForkDependencies(ctx, cfg, fork, &result); err != nil {
			t.Fatal(err)
		}
		for _, observation := range result.BootReport.Observations[before:] {
			if !strings.HasPrefix(observation.Subject, "fork:"+forkID+"/") {
				t.Fatalf("fork evidence lost its invocation coordinate: %+v", observation)
			}
		}
	}
	if len(result.BootReport.Observations) <= ordinaryCount || !result.BootReport.AdmissionDecision(policy).Complete {
		t.Fatalf("repeated exact sources collided in the composed ledger: %+v", result.BootReport.AdmissionDecision(policy))
	}
	if _, err := result.BootReport.DiscoveredToolAdmission(); err != nil {
		t.Fatalf("composing fork evidence replaced ordinary MCP admission: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
		t.Fatalf("fork dependency observation materialized a runtime: %v", err)
	}
}
