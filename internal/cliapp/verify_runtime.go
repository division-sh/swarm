package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type verifyCommandResult struct {
	BundleHash           string                                           `json:"bundle_hash"`
	SourceLabel          string                                           `json:"source_label"`
	Members              []sourceartifact.MemberEvidence                  `json:"members"`
	Manifest             *sourceartifact.Manifest                         `json:"manifest,omitempty"`
	OK                   bool                                             `json:"ok"`
	SourceRoot           string                                           `json:"source_root"`
	ValidationScope      string                                           `json:"validation_scope"`
	LiveReadiness        string                                           `json:"live_readiness"`
	AdmissionComplete    bool                                             `json:"admission_complete"`
	Observations         []runtimebootverify.AdmissionObservation         `json:"observations"`
	ExecutionObligations []runtimebootverify.AdmissionExecutionObligation `json:"execution_obligations"`
	Errors               []verifyFindingOutput                            `json:"errors"`
	Warnings             []verifyFindingOutput                            `json:"warnings"`
	LintEvidence         []verifyFindingOutput                            `json:"lint_evidence"`
	PackInventory        packInventoryReadback                            `json:"pack_inventory"`
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
	swarmDir         cliSwarmDirOptions
	portable         bool
	output           cliOutputOptions
	logging          cliLoggingOptions
}

func defaultVerifyCommandOptions() verifyCommandOptions {
	return verifyCommandOptions{
		logging: defaultCLILoggingOptions(),
	}
}

func runVerifyCommandWithOutput(ctx context.Context, repo string, opts verifyCommandOptions, out, errOut io.Writer) int {
	ctx, cancel := context.WithTimeout(ctx, verifyObservationDeadline)
	defer cancel()
	if err := opts.output.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: %v\n", err)
		}
		return 2
	}
	purpose := runtimebootverify.StructuralValidation
	if opts.configPath != "" && !opts.portable {
		purpose = runtimebootverify.ExecutionValidation
	}
	refuse := func(checkID, owner, subject string, err error, artifact *sourceartifact.AdmittedSourceArtifact) int {
		return renderVerifyAdmissionRefusal(ctx, opts, purpose, checkID, owner, subject, err, artifact, out, errOut)
	}
	if err := opts.logging.validate(); err != nil {
		return refuse("verify_logging_options", "internal/cliapp.cliLoggingOptions.validate", "verify", err, nil)
	}
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if configResult.DeploymentConfigured() && !opts.portable {
		purpose = runtimebootverify.ExecutionValidation
	}
	if err != nil {
		subject := "configuration:" + repo
		if opts.configPath != "" {
			subject = "configuration:" + opts.configPath
		}
		return refuse("runtime_configuration", "internal/cliapp.LoadRuntimeConfigWithOptions", subject, err, nil)
	}
	resolvedPaths, err := resolveCLISourcePlatformSpecPathsFromConfig(repo, CLISourcePlatformSpecPathOptions{
		SourceRoot:       opts.sourceRoot,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	}, configResult.cli)
	if err != nil {
		return refuse("source_path_selection", "internal/cliapp.resolveCLISourcePlatformSpecPathsFromConfig", "project:"+repo, err, nil)
	}
	resolvedContractsPath := resolvedPaths.SourceRoot
	resolvedPlatformSpecPath := resolvedPaths.PlatformSpecPath
	sourceRoot, err := NormalizeSourceRoot(resolvedContractsPath)
	if err != nil {
		return refuse("source_directory", "internal/cliapp.NormalizeSourceRoot", "source:"+resolvedContractsPath, err, nil)
	}
	base, err := LoadConfiguredPlatformPackBase(repo, configResult)
	if err != nil {
		return refuse("source_loading", "internal/cliapp.LoadConfiguredPlatformPackBase", "source:"+sourceRoot, err, nil)
	}
	packBases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		return refuse("source_loading", "internal/packartifact.NewPlatformPackBaseGenerationOwner", "source:"+sourceRoot, err, nil)
	}
	if _, bundle, err := NewSwarmWorkflowModuleWithPackBase(repo, sourceRoot, resolvedPlatformSpecPath, base); err != nil {
		return refuse("source_loading", "internal/cliapp.NewSwarmWorkflowModuleWithPackBase", "source:"+sourceRoot, err, nil)
	} else {
		packReadback := packInventoryReadbackFromInventory(bundle.PackInventory)
		source, validationOpts, err := admitStructuralSource(configResult, bundle)
		if err != nil {
			return refuse("effective_source_admission", "internal/runtime.AdmitEffectiveSourceProjection", "source:"+bundle.SourceArtifact.BundleHash(), err, bundle.SourceArtifact)
		}
		if purpose == runtimebootverify.ExecutionValidation {
			validationOpts, err = verifyDeploymentWorkflowOptions(ctx, configResult, bundle)
			if err != nil {
				return refuse("deployment_configuration", "internal/cliapp.verifyDeploymentWorkflowOptions", "source:"+bundle.SourceArtifact.BundleHash(), err, bundle.SourceArtifact)
			}
		}
		result, err := verifyBundleResultWithOptions(ctx, source, validationOpts)
		if purpose == runtimebootverify.ExecutionValidation {
			inspectVerifyDeployment(ctx, repo, resolvedPaths, configResult, opts.swarmDir, source, validationOpts, packBases, &result)
		} else {
			reason := "no explicit, project or local operator deployment configuration was selected"
			if opts.portable {
				reason = "--portable explicitly requests structural validation without deployment observations"
			}
			result.BootReport.Observations = append(result.BootReport.Observations, runtimebootverify.AdmissionObservation{
				CheckID: "deployment_context", Owner: "internal/cliapp.RuntimeConfigLoadResult.DeploymentConfigured", Subject: "project:" + repo,
				Class: runtimebootverify.AdmissionDeploymentObservation, Status: runtimebootverify.AdmissionNotRun, Reason: reason,
			})
		}
		decision := result.BootReport.AdmissionDecision(runtimebootverify.AdmissionFindingPolicy{
			FatalWarnings: validationOpts.FatalBootWarnings, ExcludedFatalWarningChecks: validationOpts.ExcludedFatalBootWarningChecks,
		})
		output := verifyCommandOutput(err == nil && decision.FailureClass == "" && !decision.Interrupted, sourceRoot, result, packReadback, bundle.SourceArtifact)
		output.AdmissionComplete = decision.Complete
		return renderVerifyCommandResult(opts, output, result, decision, out, errOut)
	}
}

func renderVerifyAdmissionRefusal(ctx context.Context, opts verifyCommandOptions, purpose runtimebootverify.ValidationPurpose, id, owner, subject string, cause error, artifact *sourceartifact.AdmittedSourceArtifact, out, errOut io.Writer) int {
	result := runtime.BlockedWorkflowContractAdmission(purpose, subject, id, "prerequisite refused the selected source or configuration before validation")
	class := failures.ClassSchemaInvalid
	if failure, ok := failures.As(cause); ok {
		class = failure.Failure.Class
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		class = failures.ClassDependencyUnavailable
	}
	result.BootReport.Interrupted = errors.Is(ctx.Err(), context.Canceled) || errors.Is(cause, context.Canceled)
	message := FormatCLIAPIError(cause)
	observedAt := time.Now().UTC()
	result.BootReport.Observations = append(result.BootReport.Observations, runtimebootverify.AdmissionObservation{
		CheckID: id, Owner: owner, Subject: subject, Class: runtimebootverify.AdmissionSourceObservation,
		Status: runtimebootverify.AdmissionFailed, Reason: message, StartedAt: observedAt, FinishedAt: observedAt, FailureClass: class,
	})
	finding := runtimebootverify.NewHardInvalidityFinding(id, subject, message, "Fix the named admission prerequisite before verification or startup.")
	finding.FailureClass = class
	result.BootReport.Add(finding)
	result.BootReport.Sort()
	decision := result.BootReport.AdmissionDecision(runtimebootverify.AdmissionFindingPolicy{})
	output := verifyCommandOutput(false, opts.sourceRoot, result, packInventoryReadback{}, artifact)
	return renderVerifyCommandResult(opts, output, result, decision, out, errOut)
}

func renderVerifyCommandResult(opts verifyCommandOptions, output verifyCommandResult, result runtime.WorkflowContractValidationResult, decision runtimebootverify.AdmissionDecision, out, errOut io.Writer) int {
	if err := renderCLIOutput(out, errOut, opts.output, output, func(w io.Writer) {
		writeVerifyFindings(errOut, result.BootReport.Errors(), true)
		writeVerifyFindings(errOut, result.BootReport.Warnings(), false)
		writeVerifyFindings(errOut, result.BootReport.LintEvidence(), false)
		if w == nil {
			return
		}
		if !output.OK {
			fmt.Fprintf(w, "verify failed: source=%s\n", output.SourceLabel)
		} else {
			marker := ""
			if !output.AdmissionComplete && output.ValidationScope == "deployment" {
				marker = "*"
			}
			fmt.Fprintf(w, "verify ok%s: source=%s\n", marker, output.SourceLabel)
		}
		for _, line := range verifyAdmissionTextLines(output) {
			fmt.Fprintln(w, line)
		}
		writePackInventory(w, output.PackInventory)
	}, func() ([]string, error) { return verifyAdmissionTextLines(output), nil }); err != nil {
		return CLIExitValidation
	}
	return verifyAdmissionExitCode(decision)
}

func verifyCommandOutput(ok bool, sourceRoot string, result runtime.WorkflowContractValidationResult, packInventory packInventoryReadback, artifact *sourceartifact.AdmittedSourceArtifact) verifyCommandResult {
	var manifest *sourceartifact.Manifest
	if metadata, present := artifact.RootManifest(); present {
		manifest = &metadata
	}
	scope := "structural"
	if result.BootReport.Purpose == runtimebootverify.ExecutionValidation {
		scope = "deployment"
	}
	return verifyCommandResult{
		BundleHash:           artifact.BundleHash(),
		SourceLabel:          artifact.HumanLabel(),
		Members:              append([]sourceartifact.MemberEvidence{}, artifact.MemberTable()...),
		Manifest:             manifest,
		OK:                   ok,
		SourceRoot:           sourceRoot,
		ValidationScope:      scope,
		LiveReadiness:        "not_evaluated",
		Observations:         append([]runtimebootverify.AdmissionObservation{}, result.BootReport.Observations...),
		ExecutionObligations: append([]runtimebootverify.AdmissionExecutionObligation{}, result.BootReport.ExecutionObligations...),
		Errors:               verifyFindingOutputs(result.BootReport.Errors()),
		Warnings:             verifyFindingOutputs(result.BootReport.Warnings()),
		LintEvidence:         verifyFindingOutputs(result.BootReport.LintEvidence()),
		PackInventory:        packInventory,
	}
}

func verifyAdmissionExitCode(decision runtimebootverify.AdmissionDecision) int {
	if decision.Interrupted {
		return cliExitInterrupted
	}
	switch decision.FailureClass {
	case "":
		return cliExitOK
	case failures.ClassSchemaInvalid:
		return CLIExitValidation
	case failures.ClassAuthenticationNeeded, failures.ClassAuthorizationDenied:
		return cliExitAuth
	case failures.ClassLifecycleConflict, failures.ClassConflictingDuplicate:
		return cliExitConflict
	default:
		return CLIExitRuntime
	}
}

func verifyAdmissionTextLines(output verifyCommandResult) []string {
	message := "admission validation failed; startup execution not performed"
	if output.OK && output.ValidationScope == "structural" {
		message = "portable structural checks passed; deployment admission not evaluated"
	} else if output.OK {
		message = "admission checks passed; startup execution not performed"
	}
	if output.OK && !output.AdmissionComplete && output.ValidationScope == "deployment" {
		message += " * (admission incomplete; unperformed checks are listed below)"
	}
	lines := []string{message}
	for _, observation := range output.Observations {
		if observation.NotRunCause != nil && observation.NotRunCause.Kind == runtimebootverify.AdmissionAbsentSQLiteStore {
			lines = append(lines, fmt.Sprintf("store admission: not evaluated \u2014 no selected store at %s (created on first serve)", observation.NotRunCause.Path))
		}
		if observation.Status == runtimebootverify.AdmissionPassed || observation.Status == runtimebootverify.AdmissionNotApplicable {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s @ %s (%s)", observation.Status, observation.CheckID, verifyAdmissionTextSubject(output, observation.Subject), observation.Reason))
	}
	for _, obligation := range output.ExecutionObligations {
		lines = append(lines, fmt.Sprintf("startup not performed: %s @ %s (%s)", obligation.ID, verifyAdmissionTextSubject(output, obligation.Subject), obligation.Reason))
	}
	return lines
}

func verifyAdmissionTextSubject(output verifyCommandResult, subject string) string {
	if output.BundleHash != "" && output.SourceLabel != "" {
		exact := "source:" + output.BundleHash
		if subject == exact || strings.HasSuffix(subject, "/"+exact) {
			return strings.TrimSuffix(subject, exact) + "source:" + output.SourceLabel
		}
	}
	return subject
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

func verifyWorkflowContractValidationOptions(configResult RuntimeConfigLoadResult, source semanticview.Source) (runtime.WorkflowContractValidationOptions, error) {
	if configResult.Config == nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("admitted runtime configuration is required")
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

func admitStructuralSource(configResult RuntimeConfigLoadResult, bundle *runtimecontracts.WorkflowContractBundle) (semanticview.Source, runtime.WorkflowContractValidationOptions, error) {
	source := semanticview.Wrap(bundle)
	opts, err := verifyWorkflowContractValidationOptions(configResult, source)
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
