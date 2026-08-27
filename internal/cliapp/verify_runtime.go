package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type verifyCommandResult struct {
	OK                      bool                  `json:"ok"`
	Contracts               string                `json:"contracts"`
	WorkspaceBackend        string                `json:"workspace_backend"`
	HarnessInjectedInputs   int                   `json:"harness_injected_inputs"`
	HarnessObservedOutputs  int                   `json:"harness_observed_outputs"`
	HarnessInputProvenance  []string              `json:"harness_input_provenance,omitempty"`
	HarnessOutputProvenance []string              `json:"harness_output_provenance,omitempty"`
	ProductionValid         bool                  `json:"production_valid"`
	Errors                  []verifyFindingOutput `json:"errors"`
	Warnings                []verifyFindingOutput `json:"warnings"`
	LintEvidence            []verifyFindingOutput `json:"lint_evidence"`
	CapabilitySubjects      []packs.Subject       `json:"capability_subjects"`
	PackInventory           packInventoryReadback `json:"pack_inventory"`
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
	contractsPath    string
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
	resolvedPaths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: resolve path config: %v\n", err)
		}
		return cliAPIErrorExitCode(err, cliAPIErrorClassifier{})
	}
	resolvedContractsPath := resolvedPaths.ContractsPath
	resolvedPlatformSpecPath := resolvedPaths.PlatformSpecPath
	contractsRoot, err := NormalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	}
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	}
	if _, bundle, err := NewSwarmWorkflowModuleWithRuntimeConfig(repo, contractsRoot, resolvedPlatformSpecPath, configResult); err != nil {
		writeCLIAPIError(errOut, err)
		return CLIExitValidation
	} else {
		packReadback := packInventoryReadbackFromInventory(bundle.PackInventory)
		source := semanticview.Wrap(bundle)
		validationOpts, err := verifyWorkflowContractValidationOptions(repo, opts.configPath, source)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: configure validation: %v\n", err)
			}
			return 1
		}
		workspaceBackend, err := resolveWorkspaceBackendDiagnostic(repo, opts.configPath, source)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: resolve workspace backend: %v\n", err)
			}
			return 1
		}
		workspaceBackendDetail := workspaceBackendDecisionDetail(workspaceBackend)
		bundleHash, err := runtimecontracts.BundleHash(bundle)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: hash bundle: %v\n", err)
			}
			return 1
		}
		sourceFact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: source identity: %v\n", err)
			}
			return 1
		}
		projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{
			Source: source, BundleSourceFact: sourceFact,
			ProviderTriggerCatalog: validationOpts.ProviderTriggerCatalog,
			ChannelPlans:           validationOpts.ChannelPlans, ChannelOutboundBindings: validationOpts.ChannelOutboundBindings,
		})
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: admit effective source: %v\n", err)
			}
			return 1
		}
		result, err := verifyBundleResultWithOptions(ctx, projection.Source(), validationOpts)
		if err != nil {
			if opts.output.asJSON && verifyValidationResultHasBlockingBootFindings(result, validationOpts) {
				output := verifyCommandOutput(false, contractsRoot, workspaceBackendDetail, result, packReadback)
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
		output := verifyCommandOutput(true, contractsRoot, workspaceBackendDetail, result, packReadback)
		if err := renderCLIOutput(out, errOut, opts.output, output, func(_ io.Writer) {
			writeVerifyFindings(errOut, result.BootReport.Warnings(), false)
			writeVerifyFindings(errOut, result.BootReport.LintEvidence(), false)
			if out != nil {
				if result.HarnessInjectedInputCount > 0 || result.HarnessObservedOutputCount > 0 {
					fmt.Fprintf(out, "verify ok: contracts=%s -- %s; not production-valid\n", contractsRoot, harnessValidationSummary(result))
				} else {
					fmt.Fprintf(out, "verify ok: contracts=%s\n", contractsRoot)
				}
				fmt.Fprintf(out, "%s\n", workspaceBackendDetail)
				writePackInventory(out, packReadback)
				for _, subject := range result.CapabilitySubjects {
					fmt.Fprintln(out, packs.RenderSubject(subject, false))
				}
			}
		}, func() ([]string, error) {
			return []string{"ok"}, nil
		}); err != nil {
			return 2
		}
	}
	return 0
}

func harnessValidationSummary(result runtime.WorkflowContractValidationResult) string {
	parts := make([]string, 0, 2)
	if result.HarnessInjectedInputCount > 0 {
		parts = append(parts, fmt.Sprintf("%d harness-injected input%s at [%s]", result.HarnessInjectedInputCount, pluralSuffix(result.HarnessInjectedInputCount), strings.Join(result.HarnessInputDeclarations, ", ")))
	}
	if result.HarnessObservedOutputCount > 0 {
		parts = append(parts, fmt.Sprintf("%d harness-observed output%s at [%s]", result.HarnessObservedOutputCount, pluralSuffix(result.HarnessObservedOutputCount), strings.Join(result.HarnessOutputDeclarations, ", ")))
	}
	return strings.Join(parts, ", ")
}

func verifyCommandOutput(ok bool, contractsRoot string, workspaceBackend string, result runtime.WorkflowContractValidationResult, packInventory packInventoryReadback) verifyCommandResult {
	return verifyCommandResult{
		OK:                      ok,
		Contracts:               contractsRoot,
		WorkspaceBackend:        workspaceBackend,
		HarnessInjectedInputs:   result.HarnessInjectedInputCount,
		HarnessObservedOutputs:  result.HarnessObservedOutputCount,
		HarnessInputProvenance:  append([]string(nil), result.HarnessInputDeclarations...),
		HarnessOutputProvenance: append([]string(nil), result.HarnessOutputDeclarations...),
		ProductionValid:         result.ProductionValid,
		Errors:                  verifyFindingOutputs(result.BootReport.Errors()),
		Warnings:                verifyFindingOutputs(result.BootReport.Warnings()),
		LintEvidence:            verifyFindingOutputs(result.BootReport.LintEvidence()),
		CapabilitySubjects:      append([]packs.Subject(nil), result.CapabilitySubjects...),
		PackInventory:           packInventory,
	}
}

func pluralSuffix(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
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

func VerifyBundle(ctx context.Context, source semanticview.Source, posture executionposture.Posture) error {
	_, err := verifyBundleResult(ctx, source, posture)
	return err
}

func verifyBundleResult(ctx context.Context, source semanticview.Source, posture executionposture.Posture) (runtime.WorkflowContractValidationResult, error) {
	credentialStore, err := BuildCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationResult{}, fmt.Errorf("configure credentials: %w", err)
	}
	managedCredentialStore, err := BuildManagedCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationResult{}, fmt.Errorf("configure managed credentials: %w", err)
	}
	opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore, posture)
	opts.ManagedCredentials = managedCredentialStore
	return verifyBundleResultWithOptions(ctx, source, opts)
}

func verifyBundleResultWithOptions(ctx context.Context, source semanticview.Source, opts runtime.WorkflowContractValidationOptions) (runtime.WorkflowContractValidationResult, error) {
	if source == nil {
		return runtime.WorkflowContractValidationResult{}, errors.New("semantic source is required")
	}
	return runtime.ValidateWorkflowContractSurface(ctx, source, opts)
}

func verifyWorkflowContractValidationOptions(repo, configPath string, source semanticview.Source) (runtime.WorkflowContractValidationOptions, error) {
	credentialStore, err := BuildCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("configure credentials: %w", err)
	}
	managedCredentialStore, err := BuildManagedCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("configure managed credentials: %w", err)
	}
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("load runtime config: %w", err)
	}
	profile, err := configResult.Config.LLMBackendProfile()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("resolve llm backend profile: %w", err)
	}
	posture, err := configResult.Config.ProcessExecutionPosture()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore, posture)
	opts.ManagedCredentials = managedCredentialStore
	opts.AllowHarnessInputs = true
	opts.AllowHarnessOutputs = true
	opts.ValidateLLMModelResolution = true
	opts.LLMProfile = profile
	opts.ModelAliases = configResult.Config.LLM.Models
	providerCredentials, err := BuildProviderCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("configure provider credentials: %w", err)
	}
	opts.ProviderCredentials = providerCredentials
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("workflow validation source must be bundle-backed")
	}
	packRuntime, err := LoadBundlePackRuntime(context.Background(), configResult, bundle, providerCredentials, managedCredentialStore)
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("load bundle pack runtime: %w", err)
	}
	opts.ProviderTriggerCatalog = packRuntime.ProviderTriggers.Catalog
	opts.ChannelPlans = packRuntime.Channels.Plans
	opts.ChannelOutboundBindings = packRuntime.Channels.Bindings
	return opts, nil
}

func writeVerifyFindings(out io.Writer, findings []runtimebootverify.Finding, blocking bool) {
	if out == nil || len(findings) == 0 {
		return
	}
	for _, finding := range findings {
		fmt.Fprintln(out, runtimebootverify.FormatSurfaceFinding(finding, blocking))
	}
}
