package cliapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"syscall"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/devscratch"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

type ServeCheckStageRequest struct {
	Repo      string
	Paths     CLISourcePaths
	Config    RuntimeConfigLoadResult
	Options   ServeOptions
	Selection storebackend.Selection
	PackBases packartifact.PlatformPackBaseResolver
}

type ServeCheckStageResult struct {
	inputHash      string
	APIAuth        apiv1.AuthTokenResolution
	SourceRoot     string
	Bundle         *contracts.WorkflowContractBundle
	Source         semanticview.Source
	Validation     runtime.WorkflowContractValidationResult
	ProviderChecks *runtime.ManagedProviderBootChecks
	channelChecks  []credentials.ValueEvidence
	storeInput     *storeselected.BootCheckInput
	reset          *destructivereset.Operation
	Options        runtime.WorkflowContractValidationOptions
}

// InspectServeCheckStage shares the deployed check owners without constructing
// a runtime or retaining any native role beyond its inspection callback.
func InspectServeCheckStage(ctx context.Context, in ServeCheckStageRequest) (out ServeCheckStageResult, retErr error) {
	defer func() {
		if retErr != nil {
			if out.Validation.BootReport.SourceArtifactHash == "" {
				out.Validation = runtime.BlockedWorkflowContractAdmission(bootverify.ExecutionValidation, in.Paths.SourceRoot, "deployment_configuration", "prerequisite refused the selected source or configuration before validation")
			}
			verifyDeploymentObservation(ctx, &out.Validation, "check_stage_completion", "internal/cliapp.InspectServeCheckStage", in.Paths.SourceRoot, failures.ClassSchemaInvalid, func(context.Context) error { return retErr })
			out.Validation.BootReport.Sort()
		}
	}()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if in.PackBases == nil {
		return out, fmt.Errorf("invocation-selected platform pack generation is required for retained inspection")
	}
	var err error
	out.SourceRoot, err = NormalizeSourceRoot(in.Paths.SourceRoot)
	if err != nil {
		return out, err
	}
	out.Bundle, out.reset, err = inspectServeCheckSource(ctx, in, out.SourceRoot)
	if err != nil {
		return out, err
	}
	if out.Bundle != nil {
		out.Source, _, err = admitVerifySource(in.Config, out.Bundle)
		if err != nil {
			return out, err
		}
	}
	var certificates [][][]byte
	out.APIAuth, certificates, err = serveCheckHostInputs(in)
	if err != nil {
		return out, err
	}
	out.inputHash, err = serveCheckInputHash(in.Config, in.Paths, out.Bundle, out.APIAuth, certificates)
	if err != nil {
		return out, err
	}
	out.Options, err = verifyDeploymentWorkflowOptions(ctx, in.Config, out.Bundle)
	if err != nil {
		return out, err
	}
	if out.Source != nil {
		out.Validation, retErr = verifyBundleResultWithOptions(ctx, out.Source, out.Options)
	} else {
		out.Validation.BootReport.Purpose = bootverify.ExecutionValidation
		out.Validation.BootReport.Observations = append(out.Validation.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "source_population", Owner: "internal/runtime/destructivereset.Operation.RecoverySourceSet", Subject: "reset:" + out.reset.Request.OperationID,
			Class: bootverify.AdmissionSourceObservation, Status: bootverify.AdmissionNotApplicable, Reason: "the admitted reset reconstructs an empty source population; there are no authored provider targets",
		})
	}
	inspectServeGeneratedBoundary(ctx, in.Options, &out.Validation, &out.Options)
	preference, err := ResolveWorkspaceBackend(in.Options.WorkspaceBackend, in.Options.WorkspaceBackendSet, in.Config.Config)
	if err != nil {
		return out, errors.Join(retErr, err)
	}
	backend, inspection, workspaceOK := inspectServeSourceAdmission(ctx, in, &out, preference)
	intent := in.Options
	intent.MCPListenAddr = WorkspaceMCPListenAddr(intent.MCPListenAddr, intent.MCPListenAddrSource, backend)
	_, enabled, err := ResolveServePublicIngressMode(intent, in.Config.Config.Serve.PublicWebhook)
	if err != nil {
		return out, errors.Join(retErr, err)
	}
	if !enabled {
		intent.PublicWebhookListen = ""
	}
	inspectVerifyListeners(ctx, in.Repo, in.Config, &out.Validation, intent)
	inspectVerifyWorkspaceDependencies(ctx, in.Config, backend, workspaceOK, &out.Validation)
	inspectVerifyPublicIngress(ctx, in.Config, &out.Validation, "source:"+out.Validation.BootReport.SourceArtifactHash, intent)
	var retained *verifyRetainedAdmissionInventory
	if in.Options.Dev && !in.Options.LocalRun {
		inspectServeScratch(ctx, in, &out.Validation)
	} else {
		retained = inspectVerifySelectedStore(ctx, in.Repo, in.Paths, in.Config,
			cliSwarmDirOptions{SwarmDir: in.Options.SwarmDir, SwarmDirFlagSet: in.Options.SwarmDirSet},
			out.Source, out.Options, in.PackBases, inspection, workspaceOK, &out.Validation,
			serveStoreInspection{selection: in.Selection, dryRun: in.Options.DryRun, input: &out.storeInput, reset: out.reset})
	}
	if out.Source == nil {
		out.Validation.BootReport.Sort()
		return out, retErr
	}
	if retErr != nil || !serveProviderPrerequisitesReady(out.Validation.BootReport, in.Options.DryRun) {
		out.Validation.BootReport.Sort()
		return out, retErr
	}
	if retained != nil {
		verifyDeploymentBoundedObservation(ctx, verifyRemoteDeadline, &out.Validation, "channel_identity", "internal/runtime/registration.HTTPExecutor.Read", "source:"+out.Validation.BootReport.SourceArtifactHash, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
			var err error
			out.channelChecks, err = inspectServeChannelIdentities(ctx, retained.channels, out.Options.ChannelPlans, out.Options.Credentials)
			return err
		})
		if !serveProviderPrerequisitesReady(out.Validation.BootReport, in.Options.DryRun) {
			return out, ctx.Err()
		}
	}
	blueprints, err := manager.ResolveAgentDeclarationPreflightBlueprints(manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, LLMBackend: out.Options.LLMProfile.ID,
		ModelAliases: out.Options.ModelAliases, RequireModelResolution: true,
	}, out.Source)
	if err != nil {
		return out, err
	}
	var actors manager.RetainedActorInspection
	if retained != nil && retained.actorsOK {
		actors = retained.actors
	}
	targets, err := runtime.CompileManagedProviderCheckTargets(actors.Configurations, blueprints)
	if err != nil {
		return out, err
	}
	discovered, err := out.Validation.BootReport.DiscoveredToolAdmission()
	if err != nil {
		return out, err
	}
	var docker *workspace.DockerManager
	if backend.Backend == workspace.BackendDocker {
		cfg, err := dockerWorkspaceConfigFromRuntimeConfig(in.Config.Config)
		if err != nil {
			return out, err
		}
		docker = workspace.NewDockerManager()
		docker.SetConfig(cfg)
		docker.SetSemanticSource(out.Source)
	}
	verifyDeploymentObservation(ctx, &out.Validation, "managed_provider_startup", "internal/runtime.ObserveManagedProviderBootChecks", "source:"+out.Validation.BootReport.SourceArtifactHash, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		var err error
		out.ProviderChecks, err = runtime.ObserveManagedProviderBootChecks(ctx, in.Config.Config, out.Source, out.Options, targets, discovered, inspection, docker, intent.MCPListenAddr, in.Options.TestLLMRuntime)
		return err
	})
	out.Validation.BootReport.Sort()
	return out, ctx.Err()
}

func inspectServeScratch(ctx context.Context, in ServeCheckStageRequest, result *runtime.WorkflowContractValidationResult) {
	subject := "store:" + in.Selection.SQLitePath
	verifyDeploymentObservation(ctx, result, "dev_scratch_predecessor", "internal/store/devscratch.InspectPredecessor", subject, failures.ClassLifecycleConflict, func(context.Context) error {
		coordinate, err := devscratch.Resolve(in.Paths.SourceRoot)
		if err != nil {
			return err
		}
		if in.Selection.Backend != storebackend.BackendSQLite || in.Selection.SQLitePathSource != storebackend.SourceProjectScratch || in.Selection.SQLitePath != coordinate.DatabasePath {
			return fmt.Errorf("dev scratch selection disagrees with its canonical project owner")
		}
		return devscratch.InspectPredecessor(coordinate)
	})
	checks := append(bootverify.SelectedStoreAbsentAdmissionChecks(), bootverify.SelectedStoreAdmissionCheck{ID: "selected_store_access", Owner: bootverify.SelectedStoreAccessOwner})
	for _, check := range checks {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: check.ID, Owner: check.Owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotApplicable, Reason: "the dev epoch replaces predecessor contents; they are not retained obligations of the next boot",
		})
	}
	result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
		ID: "dev_scratch_preparation", Owner: "internal/store/devscratch.EpochAuthority.PrepareFreshStore", Subject: subject,
		Reason: "boot must acquire the existing native epoch exclusion, revalidate predecessor artifacts and prepare a fresh store; checks never acquire or reset it",
	})
}

func (r ServeCheckStageResult) BootStoreInput() *storeselected.BootCheckInput { return r.storeInput }

func (r ServeCheckStageResult) Revalidate(ctx context.Context, in ServeCheckStageRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: in.Repo, ExplicitPath: in.Options.ConfigPath, BackendOverride: in.Options.Backend})
	if err != nil {
		return err
	}
	paths, err := resolveCLISourcePathsFromConfig(in.Repo, CLISourcePathOptions{SourceRoot: in.Options.SourceRoot, ConfigPath: in.Options.ConfigPath}, cfg.cli)
	if err != nil {
		return err
	}
	root, err := NormalizeSourceRoot(paths.SourceRoot)
	if err != nil {
		return err
	}
	in.Config, in.Paths = cfg, paths
	current, reset, err := inspectServeCheckSource(ctx, in, root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(reset, r.reset) {
		return destructivereset.ErrOperationChanged
	}
	auth, certificates, err := serveCheckHostInputs(in)
	if err != nil {
		return err
	}
	hash, err := serveCheckInputHash(cfg, paths, current, auth, certificates)
	if err != nil || hash != r.inputHash {
		return errors.Join(fmt.Errorf("managed provider startup inputs changed"), err)
	}
	if r.ProviderChecks != nil {
		if err := r.ProviderChecks.ValidateConfiguration(ctx, cfg.Config, llm.NewProviderCredentialResolver(r.Options.ProviderCredentials)); err != nil {
			return err
		}
	}
	for _, evidence := range r.channelChecks {
		current, err := credentials.CurrentValueMatchesSeal(ctx, r.Options.Credentials, evidence)
		if err != nil || !current {
			return errors.Join(fmt.Errorf("channel startup inputs changed"), err)
		}
	}
	return nil
}

func serveCheckInputHash(cfg RuntimeConfigLoadResult, paths CLISourcePaths, bundle *contracts.WorkflowContractBundle, auth apiv1.AuthTokenResolution, certificates [][][]byte) (string, error) {
	var reference sourceartifact.RootAdmissionReference
	var pack string
	if bundle != nil {
		rooted, err := bundle.RootedSource()
		if err != nil {
			return "", err
		}
		reference, pack = rooted.Admission().Reference(), bundle.PackInventory.Digest()
	}
	input := struct {
		Config       *config.Config
		CLI          cliCommandConfig
		Paths        CLISourcePaths
		Root         sourceartifact.RootAdmissionReference
		Pack         string
		Auth         apiv1.AuthTokenResolution
		Certificates [][][]byte
	}{cfg.Config, cfg.cli, paths, reference, pack, auth, certificates}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func serveCheckHostInputs(in ServeCheckStageRequest) (apiv1.AuthTokenResolution, [][][]byte, error) {
	root, err := NewInvocationRoot(in.Repo)
	if err != nil {
		return apiv1.AuthTokenResolution{}, nil, err
	}
	auth, err := in.Config.ResolveServeAPIAuth(root, in.Options)
	if err != nil {
		return auth, nil, err
	}
	if err := ValidateServeAPIAuthBinding(in.Options.APIListenAddr, auth); err != nil {
		return auth, nil, err
	}
	mode, _, err := ResolveServePublicIngressMode(in.Options, in.Config.Config.Serve.PublicWebhook)
	if err != nil || mode != publicingress.ModeNativeTLS {
		return auth, nil, err
	}
	swarmDir, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{SwarmDir: in.Options.SwarmDir, SwarmDirFlagSet: in.Options.SwarmDirSet}, in.Config.cli)
	if err != nil {
		return auth, nil, err
	}
	configuration, err := publicingress.NativeTLSConfig(in.Config.Config.Serve.PublicWebhook, swarmDir.Path, root.Path())
	if err != nil {
		return auth, nil, err
	}
	var certificates [][][]byte
	for _, certificate := range configuration.Certificates {
		certificates = append(certificates, certificate.Certificate)
	}
	return auth, certificates, nil
}

func inspectServeGeneratedBoundary(ctx context.Context, mode ServeOptions, result *runtime.WorkflowContractValidationResult, options *runtime.WorkflowContractValidationOptions) {
	const id = "generated_boundary_environment"
	subject := "source:" + result.BootReport.SourceArtifactHash
	if err := ValidateGeneratedBoundaryEnv(); err != nil && (mode.Dev || mode.LocalRun) {
		// Local modes have always warned about generated endpoint overrides.
		// This is not evidence that the configured endpoint was validated.
		result.BootReport.Add(bootverify.Finding{CheckID: id, Location: subject, Severity: bootverify.SeveritySemanticDriftWarn, Message: err.Error(), Remediation: "Remove generated gateway endpoint overrides."})
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: id, Owner: "internal/cliapp.ValidateGeneratedBoundaryEnv", Subject: subject,
			Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotApplicable,
			Reason: "local-mode endpoint override is a warning, not an admitted generated endpoint",
		})
		options.ExcludedFatalBootWarningChecks = append(options.ExcludedFatalBootWarningChecks, id)
		return
	}
	verifyDeploymentObservation(ctx, result, id, "internal/cliapp.ValidateGeneratedBoundaryEnv", subject, failures.ClassSchemaInvalid, func(context.Context) error {
		return ValidateGeneratedBoundaryEnv()
	})
}

func (r ServeCheckStageResult) Decision() bootverify.AdmissionDecision {
	return r.Validation.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{
		FatalWarnings: r.Options.FatalBootWarnings, ExcludedFatalWarningChecks: r.Options.ExcludedFatalBootWarningChecks,
	})
}

func (r ServeCheckStageResult) ExitCode() int { return verifyAdmissionExitCode(r.Decision()) }

func (r ServeCheckStageResult) Failure() (failure error) {
	defer func() {
		if failure != nil {
			for _, observation := range r.Validation.BootReport.Observations {
				failure = errors.Join(failure, observation.Cause)
			}
		}
	}()
	decision := r.Decision()
	if decision.FailureClass == "" && !decision.Interrupted {
		return nil
	}
	for _, finding := range r.Validation.BootReport.Errors() {
		return errors.New(bootverify.FormatSurfaceFinding(finding, true))
	}
	for _, observation := range r.Validation.BootReport.Observations {
		if observation.Status == bootverify.AdmissionFailed || observation.Status == bootverify.AdmissionUnavailable || observation.Status == bootverify.AdmissionNotRun && observation.Reason != "" {
			return errors.New(observation.Reason)
		}
	}
	return errors.New("admission validation failed; startup execution not performed")
}

func (r ServeCheckStageResult) Write(out, errOut io.Writer) {
	writeVerifyFindings(errOut, r.Validation.BootReport.Errors(), true)
	writeVerifyFindings(errOut, r.Validation.BootReport.Warnings(), false)
	if out == nil {
		return
	}
	decision := r.Decision()
	var artifact *sourceartifact.AdmittedSourceArtifact
	var inventory packInventoryReadback
	if r.Bundle != nil {
		artifact, inventory = r.Bundle.SourceArtifact, packInventoryReadbackFromInventory(r.Bundle.PackInventory)
	}
	view := verifyCommandOutput(decision.FailureClass == "" && !decision.Interrupted, r.SourceRoot, r.Validation, inventory, artifact)
	view.AdmissionComplete = decision.Complete
	for _, line := range verifyAdmissionTextLines(view) {
		_, _ = io.WriteString(out, line+"\n")
	}
}

func serveProviderPrerequisitesReady(report bootverify.Report, dryRun bool) bool {
	// This only schedules further checks. It cannot make the final report pass
	// or authorize boot: the original NOT CHECKED observations stay in it.
	if dryRun {
		copy := report
		copy.Observations = nil
		for _, observation := range report.Observations {
			if observation.CheckID == "listener_availability" && observation.Owner == "internal/cliapp.ListenServeHTTPListener" && observation.Status == bootverify.AdmissionNotRun {
				continue
			}
			copy.Observations = append(copy.Observations, observation)
		}
		report = copy
	}
	decision := report.AdmissionDecision(bootverify.AdmissionFindingPolicy{})
	return decision.FailureClass == "" && !decision.Interrupted
}

func probeServeDryRunListener(ctx context.Context, name, addr string, held *[]verifyListenerBinding, result *runtime.WorkflowContractValidationResult) {
	var occupied error
	verifyDeploymentObservation(ctx, result, "listener_availability", "internal/cliapp.ListenServeHTTPListener", name+":"+addr, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		bounded, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
		defer cancel()
		listener, err := ListenServeHTTPListenerInContext(bounded, name, addr)
		if err != nil {
			if !errors.Is(err, syscall.EADDRINUSE) || bounded.Err() != nil {
				return err
			}
			_, wanted, _ := net.SplitHostPort(addr)
			for _, prior := range *held {
				_, port, _ := net.SplitHostPort(prior.listener.Addr().String())
				if port == wanted && port != "0" {
					return err // Never excuse a collision inside our own probe set.
				}
			}
			occupied = err
			return nil
		}
		*held = append(*held, verifyListenerBinding{subject: name + ":" + addr, listener: listener})
		return bounded.Err()
	})
	if occupied != nil {
		observation := &result.BootReport.Observations[len(result.BootReport.Observations)-1]
		observation.Status, observation.Reason = bootverify.AdmissionNotRun, occupied.Error()
	}
}
