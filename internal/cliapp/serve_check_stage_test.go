package cliapp

import (
	"context"
	"io"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func serveChecksForTest(t testing.TB, ctx context.Context, repo, source, configPath string) (ServeCheckStageResult, error) {
	t.Helper()
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	paths, err := ResolveCLISourcePaths(repo, CLISourcePathOptions{SourceRoot: source, ConfigPath: configPath})
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	base, err := LoadConfiguredPlatformPackBase(repo, cfg)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	_, bundle, err := NewSwarmWorkflowModuleWithPackBase(repo, paths.SourceRoot, base)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	admitted, _, err := admitVerifySource(cfg, bundle)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	request, _, err := prepareVerifySelectedStoreInspection(ctx, repo, paths, cfg, cliSwarmDirOptions{}, admitted)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	opts := DefaultServeOptions()
	profile, err := cfg.Config.LLMBackendProfile()
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	opts.TestLLMRuntime = serveCheckAnswerRuntime{profile: profile}
	opts.SourceRoot, opts.ConfigPath = source, configPath
	opts.APIListenAddr, opts.MCPListenAddr, err = cfg.ResolveServeListeners(opts, false, false)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
	return InspectServeCheckStage(ctx, ServeCheckStageRequest{Repo: repo, Paths: paths, Config: cfg, Options: opts, Selection: request.Selection, PackBases: bases})
}

type serveCheckAnswerRuntime struct {
	llm.NoopRuntime
	profile llmselection.Profile
}

func (r serveCheckAnswerRuntime) ProviderContract() llm.ProviderContract {
	contract, _ := llm.ProviderContractForProfile(r.profile)
	return contract
}

func (serveCheckAnswerRuntime) ObserveStartupAnswer(ctx context.Context, _ llmselection.Profile, _ llmselection.ResolvedModel) ([]byte, error) {
	return []byte(`{"status":"completed","stop_reason":"end_turn","content":[{"type":"text","text":"ready"}],"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ready"}}],"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ready"}]}]}`), ctx.Err()
}

func renderServeChecksForTest(t testing.TB, ctx context.Context, repo, source, configPath, format string, out, diagnostic io.Writer) int {
	t.Helper()
	stage, err := serveChecksForTest(t, ctx, repo, source, configPath)
	if err != nil && stage.Bundle == nil {
		t.Fatalf("check stage prerequisites: %v", err)
	}
	decision := stage.Decision()
	view := verifyCommandOutput(decision.FailureClass == "" && !decision.Interrupted, stage.SourceRoot, stage.Validation, packInventoryReadbackFromInventory(stage.Bundle.PackInventory), stage.Bundle.SourceArtifact)
	view.AdmissionComplete = decision.Complete
	opts := defaultVerifyCommandOptions()
	opts.output.asJSON, opts.output.quiet = format == "json", format == "quiet"
	return renderVerifyCommandResult(opts, view, stage.Validation, decision, out, diagnostic)
}
