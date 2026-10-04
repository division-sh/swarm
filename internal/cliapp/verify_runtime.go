package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type verifyCommandResult struct {
	BundleHash      string                          `json:"bundle_hash"`
	SourceLabel     string                          `json:"source_label"`
	Members         []sourceartifact.MemberEvidence `json:"members"`
	Manifest        *sourceartifact.Manifest        `json:"manifest,omitempty"`
	OK              bool                            `json:"ok"`
	SourceRoot      string                          `json:"source_root"`
	ValidationScope string                          `json:"validation_scope"`
	LiveReadiness   string                          `json:"live_readiness"`
	Errors          []verifyFindingOutput           `json:"errors"`
	Warnings        []verifyFindingOutput           `json:"warnings"`
	LintEvidence    []verifyFindingOutput           `json:"lint_evidence"`
	PackInventory   packInventoryReadback           `json:"pack_inventory"`
}

type verifyFindingOutput struct {
	CheckID     string   `json:"check_id"`
	Severity    string   `json:"severity"`
	Location    string   `json:"location"`
	Message     string   `json:"message"`
	Remediation string   `json:"remediation,omitempty"`
	Evidence    []string `json:"evidence,omitempty"`
}

type verifyCommandOptions struct {
	sourceRoot       string
	platformSpecPath string
	configPath       string
	output           cliOutputOptions
	logging          cliLoggingOptions
}

func defaultVerifyCommandOptions() verifyCommandOptions {
	return verifyCommandOptions{
		logging: defaultCLILoggingOptions(),
	}
}

func runVerifyCommandWithOutput(ctx context.Context, repo string, opts verifyCommandOptions, out, errOut io.Writer) int {
	if err := opts.logging.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: %v\n", err)
		}
		return 2
	}
	if err := opts.output.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: %v\n", err)
		}
		return 2
	}
	resolvedPaths, err := ResolveCLISourcePlatformSpecPaths(repo, CLISourcePlatformSpecPathOptions{
		SourceRoot:       opts.sourceRoot,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: resolve path config: %v\n", err)
		}
		return cliAPIErrorExitCode(err, cliAPIErrorClassifier{})
	}
	resolvedContractsPath := resolvedPaths.SourceRoot
	resolvedPlatformSpecPath := resolvedPaths.PlatformSpecPath
	sourceRoot, err := NormalizeSourceRoot(resolvedContractsPath)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	}
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	}
	if _, bundle, err := NewSwarmWorkflowModuleWithRuntimeConfig(repo, sourceRoot, resolvedPlatformSpecPath, configResult); err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	} else {
		packReadback := packInventoryReadbackFromInventory(bundle.PackInventory)
		source, validationOpts, err := admitStructuralSource(repo, opts.configPath, bundle)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: admit effective source: %v\n", err)
			}
			return 1
		}
		result, err := verifyBundleResultWithOptions(ctx, source, validationOpts)
		if err != nil {
			if opts.output.asJSON && verifyValidationResultHasBlockingBootFindings(result, validationOpts) {
				output := verifyCommandOutput(false, sourceRoot, result, packReadback, bundle.SourceArtifact)
				if renderErr := renderCLIOutput(out, errOut, opts.output, output, nil, nil); renderErr != nil {
					return 2
				}
				return 1
			}
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: %v\n", err)
			}
			return 1
		}
		output := verifyCommandOutput(true, sourceRoot, result, packReadback, bundle.SourceArtifact)
		if err := renderCLIOutput(out, errOut, opts.output, output, func(_ io.Writer) {
			writeVerifyFindings(errOut, result.BootReport.Warnings(), false)
			writeVerifyFindings(errOut, result.BootReport.LintEvidence(), false)
			if out != nil {
				fmt.Fprintf(out, "verify ok: source=%s\n", output.SourceLabel)
				fmt.Fprintln(out, "validation: structural; live readiness: not evaluated")
				writePackInventory(out, packReadback)
			}
		}, func() ([]string, error) {
			return []string{"ok"}, nil
		}); err != nil {
			return 2
		}
	}
	return 0
}

func verifyCommandOutput(ok bool, sourceRoot string, result runtime.WorkflowContractValidationResult, packInventory packInventoryReadback, artifact *sourceartifact.AdmittedSourceArtifact) verifyCommandResult {
	var manifest *sourceartifact.Manifest
	if metadata, present := artifact.RootManifest(); present {
		manifest = &metadata
	}
	return verifyCommandResult{
		BundleHash:      artifact.BundleHash(),
		SourceLabel:     artifact.HumanLabel(),
		Members:         artifact.MemberTable(),
		Manifest:        manifest,
		OK:              ok,
		SourceRoot:      sourceRoot,
		ValidationScope: "structural",
		LiveReadiness:   "not_evaluated",
		Errors:          verifyFindingOutputs(result.BootReport.Errors()),
		Warnings:        verifyFindingOutputs(result.BootReport.Warnings()),
		LintEvidence:    verifyFindingOutputs(result.BootReport.LintEvidence()),
		PackInventory:   packInventory,
	}
}

func verifyValidationResultHasBlockingBootFindings(result runtime.WorkflowContractValidationResult, opts runtime.WorkflowContractValidationOptions) bool {
	if len(result.BootReport.Errors()) > 0 {
		return true
	}
	if !opts.FatalBootWarnings {
		return false
	}
	excluded := make(map[string]struct{}, len(opts.ExcludedFatalBootWarningChecks))
	for _, checkID := range opts.ExcludedFatalBootWarningChecks {
		if checkID = strings.TrimSpace(checkID); checkID != "" {
			excluded[checkID] = struct{}{}
		}
	}
	for _, finding := range result.BootReport.Warnings() {
		if _, skip := excluded[strings.TrimSpace(finding.CheckID)]; skip {
			continue
		}
		return true
	}
	return false
}

func verifyFindingOutputs(findings []runtimebootverify.Finding) []verifyFindingOutput {
	out := make([]verifyFindingOutput, 0, len(findings))
	for _, finding := range findings {
		out = append(out, verifyFindingOutput{
			CheckID:     strings.TrimSpace(finding.CheckID),
			Severity:    strings.TrimSpace(finding.Severity),
			Location:    strings.TrimSpace(finding.Location),
			Message:     strings.TrimSpace(finding.Message),
			Remediation: strings.TrimSpace(finding.Remediation),
			Evidence:    trimmedStringSlice(finding.Evidence),
		})
	}
	return out
}

func trimmedStringSlice(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func verifyBundleResultWithOptions(ctx context.Context, source semanticview.Source, opts runtime.WorkflowContractValidationOptions) (runtime.WorkflowContractValidationResult, error) {
	if source == nil {
		return runtime.WorkflowContractValidationResult{}, errors.New("semantic source is required")
	}
	return runtime.ValidateWorkflowContractSurface(ctx, source, opts)
}

func verifyWorkflowContractValidationOptions(repo, configPath string, source semanticview.Source) (runtime.WorkflowContractValidationOptions, error) {
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("load runtime config: %w", err)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("workflow validation source must be bundle-backed")
	}
	metadata, err := packadmission.FromBundle(bundle)
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("admit pack metadata: %w", err)
	}
	opts := runtime.StructuralWorkflowContractValidationOptions()
	opts.ModelAliases = configResult.Config.LLM.Models
	opts.ProviderTriggerCatalog = metadata.ProviderTriggers
	opts.ChannelPlans = metadata.ChannelPlans
	return opts, nil
}

func admitStructuralSource(repo, configPath string, bundle *runtimecontracts.WorkflowContractBundle) (semanticview.Source, runtime.WorkflowContractValidationOptions, error) {
	source := semanticview.Wrap(bundle)
	opts, err := verifyWorkflowContractValidationOptions(repo, configPath, source)
	if err != nil {
		return nil, opts, err
	}
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return nil, opts, err
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(hash)
	if err != nil {
		return nil, opts, err
	}
	projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{
		Source: source, SourceArtifactFact: fact,
		ProviderTriggerCatalog: opts.ProviderTriggerCatalog, ChannelPlans: opts.ChannelPlans,
	})
	if err != nil {
		return nil, opts, err
	}
	return projection.Source(), opts, nil
}

func writeVerifyFindings(out io.Writer, findings []runtimebootverify.Finding, blocking bool) {
	if out == nil || len(findings) == 0 {
		return
	}
	for _, finding := range findings {
		fmt.Fprintln(out, runtimebootverify.FormatSurfaceFinding(finding, blocking))
	}
}
