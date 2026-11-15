package cliapp

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func serveChecksForTest(t testing.TB, ctx context.Context, repo, source, configPath string, selectedOptions ...ServeOptions) (ServeCheckStageResult, error) {
	t.Helper()
	opts := DefaultServeOptions()
	if len(selectedOptions) != 0 {
		opts = selectedOptions[0]
	}
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
	request, _, err := prepareVerifySelectedStoreInspection(ctx, repo, paths, cfg, cliSwarmDirOptions{SwarmDir: opts.SwarmDir, SwarmDirFlagSet: opts.SwarmDirSet}, admitted)
	if err != nil {
		return ServeCheckStageResult{}, err
	}
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

func renderServeChecksForTest(t testing.TB, ctx context.Context, repo, source, configPath, format string, out, diagnostic io.Writer, selectedOptions ...ServeOptions) int {
	t.Helper()
	stage, err := serveChecksForTest(t, ctx, repo, source, configPath, selectedOptions...)
	if err != nil && stage.Bundle == nil {
		_, _ = io.WriteString(diagnostic, FormatCLIAPIError(err)+"\n")
		return CLIExitValidation
	}
	decision := stage.Decision()
	view := verifyCommandOutput(decision.FailureClass == "" && !decision.Interrupted, stage.SourceRoot, stage.Validation, packInventoryReadbackFromInventory(stage.Bundle.PackInventory), stage.Bundle.SourceArtifact)
	view.AdmissionComplete = decision.Complete
	opts := defaultVerifyCommandOptions()
	opts.output.asJSON, opts.output.quiet = format == "json", format == "quiet"
	return renderVerifyCommandResult(opts, view, stage.Validation, decision, out, diagnostic)
}

// Preserve the public serve parser's coordinate precedence while inspecting
// the stage through its existing report presenters, not a shipped JSON flag.
func executeServeChecksForTest(t testing.TB, ctx context.Context, repo string, args []string, out, diagnostic io.Writer) int {
	t.Helper()
	format := "text"
	command := make([]string, 0, len(args)+1)
	for _, arg := range args {
		switch arg {
		case "verify":
			command = append(command, "serve", "--dry-run")
		case "--json", "--quiet":
			format = strings.TrimPrefix(arg, "--")
		default:
			command = append(command, arg)
		}
	}
	options := defaultRootCommandOptions()
	options.runServe = func(ctx context.Context, root InvocationRoot, selected ServeOptions) int {
		return renderServeChecksForTest(t, ctx, root.Path(), selected.SourceRoot, selected.ConfigPath, format, out, diagnostic, selected)
	}
	return executeRootCommandWithOptions(ctx, repo, command, out, diagnostic, options)
}

func TestServeCheckStageGeneratedBoundaryModePolicy(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://stale.invalid/mcp")
	for _, mode := range []ServeOptions{{}, {Dev: true}, {LocalRun: true}} {
		result := runtime.WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: bootverify.ExecutionValidation, SourceArtifactHash: "source"}}
		options := runtime.WorkflowContractValidationOptions{FatalBootWarnings: true}
		inspectServeGeneratedBoundary(context.Background(), mode, &result, &options)
		decision := (ServeCheckStageResult{Validation: result, Options: options}).Decision()
		local := mode.Dev || mode.LocalRun
		if (decision.FailureClass == "") != local || len(result.BootReport.Warnings()) != boolCount(local) {
			t.Fatalf("mode=%+v decision=%+v report=%+v", mode, decision, result.BootReport)
		}
	}
}
