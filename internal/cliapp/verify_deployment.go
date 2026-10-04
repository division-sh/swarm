package cliapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

const (
	verifyObservationDeadline = 60 * time.Second
	verifyLocalDeadline       = 5 * time.Second
	verifyRemoteDeadline      = 15 * time.Second
	verifyCleanupDeadline     = 5 * time.Second
)

func verifyDeploymentWorkflowOptions(ctx context.Context, cfg RuntimeConfigLoadResult, bundle *contracts.WorkflowContractBundle) (runtime.WorkflowContractValidationOptions, error) {
	if err := ctx.Err(); err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	if cfg.Config == nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("admitted runtime configuration is required")
	}
	credentials, err := BuildCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	providerCredentials, err := BuildProviderCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	managedCredentials, err := BuildManagedCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	packs, err := LoadBundlePackRuntime(ctx, cfg, bundle, credentials, managedCredentials)
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	publication, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(packs.Channels.Bindings)
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	profile, err := cfg.Config.LLMBackendProfile()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, err
	}
	opts := runtime.DefaultWorkflowContractValidationOptions(credentials, executionposture.Live)
	opts.Purpose = bootverify.ExecutionValidation
	opts.ValidateLLMModelResolution = true
	opts.LLMProfile, opts.ModelAliases = profile, cfg.Config.LLM.Models
	opts.ProviderCredentials, opts.ManagedCredentials = providerCredentials, managedCredentials
	opts.ProviderTriggerCatalog, opts.ChannelPlans = packs.ProviderTriggers.Catalog, packs.Channels.Plans
	opts.ChannelActivationPublication = publication
	opts.MCPDiscoveryTimeout = verifyRemoteDeadline
	return opts, nil
}

// This is presentation accounting around calls to existing semantic owners,
// not a check registry or an alternative admission implementation.
func verifyDeploymentBoundedObservation(ctx context.Context, budget time.Duration, result *runtime.WorkflowContractValidationResult, id, owner, subject string, failureClass failures.Class, inspect func(context.Context) error) bool {
	return verifyDeploymentObservation(ctx, result, id, owner, subject, failureClass, func(ctx context.Context) error {
		bounded, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		err := inspect(bounded)
		if bounded.Err() != nil {
			return errors.Join(bounded.Err(), err)
		}
		return err
	})
}

func verifyDeploymentObservation(ctx context.Context, result *runtime.WorkflowContractValidationResult, id, owner, subject string, failureClass failures.Class, inspect func(context.Context) error) bool {
	observation := bootverify.AdmissionObservation{
		CheckID: id, Owner: owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
		StartedAt: time.Now().UTC(),
	}
	err := ctx.Err()
	if err == nil {
		err = inspect(ctx)
	}
	if ctx.Err() != nil {
		err = errors.Join(ctx.Err(), err)
	}
	observation.FinishedAt = time.Now().UTC()
	if err == nil {
		observation.Status = bootverify.AdmissionPassed
		result.BootReport.Observations = append(result.BootReport.Observations, observation)
		return true
	}
	if failure, ok := failures.As(err); ok {
		failureClass = failure.Failure.Class
	}
	observation.Status, observation.FailureClass, observation.Reason = bootverify.AdmissionFailed, failureClass, FormatCLIAPIError(err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		observation.Status, observation.FailureClass = bootverify.AdmissionUnavailable, failures.ClassDependencyUnavailable
		result.BootReport.Interrupted = result.BootReport.Interrupted || errors.Is(err, context.Canceled)
	}
	result.BootReport.Observations = append(result.BootReport.Observations, observation)
	finding := bootverify.NewHardInvalidityFinding(id, subject, observation.Reason, "Resolve the named deployment admission prerequisite before startup.")
	finding.FailureClass = observation.FailureClass
	result.BootReport.Add(finding)
	return false
}

func inspectVerifyDeployment(ctx context.Context, repo string, paths CLISourcePlatformSpecPaths, cfg RuntimeConfigLoadResult, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, packBases packartifact.PlatformPackBaseResolver, result *runtime.WorkflowContractValidationResult) {
	backend, workspaces, workspaceOK := inspectVerifySourceAdmission(ctx, cfg, source, opts, result)
	inspectVerifyListeners(ctx, repo, cfg, result)
	inspectVerifyWorkspaceDependencies(ctx, cfg, backend, workspaceOK, result)
	inspectVerifyPublicIngress(ctx, result, "source:"+result.BootReport.SourceArtifactHash)
	inspectVerifySelectedStore(ctx, repo, paths, cfg, source, opts, packBases, workspaces, workspaceOK, result)
	result.BootReport.Sort()
}

func inspectVerifySourceAdmission(ctx context.Context, cfg RuntimeConfigLoadResult, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, result *runtime.WorkflowContractValidationResult) (WorkspaceBackendSelection, workspace.Resolver, bool) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	verifyDeploymentObservation(ctx, result, "declared_model_admission", "internal/runtime.ValidateDeclaredAgentModelAdmission", subject, failures.ClassSchemaInvalid, func(context.Context) error {
		return runtime.ValidateDeclaredAgentModelAdmission(executionposture.Live, cfg.Config, source)
	})
	backend, workspaceInspection, workspaceOK := inspectVerifyWorkspaceAdmission(ctx, cfg, source, result)
	providers, providerErr := llm.NewAgentProviderContracts(opts.LLMProfile)
	verifyDeploymentObservation(ctx, result, "native_tool_admission", "internal/runtime/tools.ValidateNativeToolBootConfig", subject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
		if providerErr != nil {
			return providerErr
		}
		if !workspaceOK {
			return fmt.Errorf("workspace capability admission did not complete")
		}
		warnings, err := runtimetools.ValidateNativeToolBootConfig(ctx, executionposture.Live, opts.ModelAliases, source, opts.Credentials, providers, workspaceInspection)
		for _, warning := range warnings {
			result.BootReport.Add(bootverify.Finding{CheckID: "native_tool_admission", Location: subject, Severity: bootverify.SeveritySemanticDriftWarn, Message: warning.Error(), Remediation: "Review the canonical native-tool admission warning."})
		}
		return err
	})
	inspectVerifyDeclaredProviderAdmission(ctx, source, opts, result)
	inspectVerifyManagedCatalogAdmission(ctx, source, opts, providers, providerErr, workspaceInspection, workspaceOK, result)
	result.BootReport.Sort()
	return backend, workspaceInspection, workspaceOK
}

func inspectVerifyWorkspaceAdmission(ctx context.Context, cfg RuntimeConfigLoadResult, source semanticview.Source, result *runtime.WorkflowContractValidationResult) (WorkspaceBackendSelection, workspace.Resolver, bool) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	var workspaceInspection workspace.Resolver
	var backend WorkspaceBackendSelection
	workspaceOK := verifyDeploymentObservation(ctx, result, "workspace_capability_admission", "internal/cliapp.DecideWorkspaceBackend", subject, failures.ClassSchemaInvalid, func(context.Context) error {
		preference, err := ResolveWorkspaceBackend("", false, cfg.Config)
		if err != nil {
			return err
		}
		backend, err = DecideWorkspaceBackend(executionposture.Live, preference, cfg.Config, source)
		if err != nil {
			return err
		}
		switch backend.Backend {
		case WorkspaceBackendNone:
			return nil
		case workspace.BackendDocker:
			configured, configErr := dockerWorkspaceConfigFromRuntimeConfig(cfg.Config)
			if configErr != nil {
				return configErr
			}
			workspaceInspection, err = workspace.NewDockerCapabilityInspection(configured, source)
		case workspace.BackendHost:
			configured, configErr := hostWorkspaceConfigFromRuntimeConfig(cfg.Config)
			if configErr != nil {
				return configErr
			}
			workspaceInspection, err = workspace.NewHostCapabilityInspection(configured, source)
		default:
			return fmt.Errorf("unsupported admitted workspace backend %q", backend.Backend)
		}
		return err
	})
	verifyDeploymentObservation(ctx, result, "workspace_source_admission", "internal/runtime/workspace.ValidateSourceAdmission", subject, failures.ClassSchemaInvalid, func(context.Context) error {
		if !workspaceOK {
			return fmt.Errorf("workspace capability admission did not complete")
		}
		switch backend.Backend {
		case WorkspaceBackendNone:
			return nil
		case workspace.BackendDocker:
			configured, err := dockerWorkspaceConfigFromRuntimeConfig(cfg.Config)
			if err != nil {
				return err
			}
			return workspace.ValidateDockerSourceAdmission(configured, source)
		case workspace.BackendHost:
			configured, err := hostWorkspaceConfigFromRuntimeConfig(cfg.Config)
			if err != nil {
				return err
			}
			return workspace.ValidateHostSourceAdmission(configured, source)
		default:
			return fmt.Errorf("unsupported admitted workspace backend %q", backend.Backend)
		}
	})
	return backend, workspaceInspection, workspaceOK
}

func inspectVerifyDeclaredProviderAdmission(ctx context.Context, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, result *runtime.WorkflowContractValidationResult) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	verifyDeploymentObservation(ctx, result, "declared_provider_prompt_admission", "internal/runtime.ManagedProviderStartupPrompt", subject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
		blueprints, err := manager.ResolveStaticTopologyBlueprints(manager.AgentManagerOptions{
			ExecutionPosture: executionposture.Live, LLMBackend: opts.LLMProfile.ID,
			ModelAliases: opts.ModelAliases, RequireModelResolution: true,
		}, source)
		if err != nil {
			return err
		}
		for _, actor := range blueprints {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := runtime.ManagedProviderStartupPrompt(actor.Config); err != nil {
				return fmt.Errorf("agent %s provider prompt: %w", actor.Config.ID, err)
			}
			profile, err := llmselection.ResolveActiveBackend(actor.Config.ResolvedLLMBackend)
			if err != nil {
				return err
			}
			actorSubject := actor.Identity.Description()
			verifyDeploymentObservation(ctx, result, "provider_environment_admission", "internal/runtime/llm.ValidateProviderAdmissionEnvironment", actorSubject, failures.ClassSchemaInvalid, func(context.Context) error {
				return llm.ValidateProviderAdmissionEnvironment(profile)
			})
			verifyDeploymentBoundedObservation(ctx, verifyLocalDeadline, result, "provider_credential_admission", "internal/runtime/llm.ProviderCredentialResolver.Resolve", actorSubject, failures.ClassAuthenticationNeeded, func(ctx context.Context) error {
				_, err := llm.NewProviderCredentialResolver(opts.ProviderCredentials).Resolve(ctx, profile)
				return err
			})
			if profile.ID == llmselection.BackendClaudeCLI {
				result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
					ID: "provider_visibility_and_gateway_handshake", Owner: "internal/runtime.ValidateManagedProviderPreflight", Subject: actorSubject,
					Reason: "startup must settle provider-owned visibility and authorization under its own process capability and gateway binding",
				})
			}
		}
		return nil
	})
}

func inspectVerifyManagedCatalogAdmission(ctx context.Context, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, providers llm.AgentProviderContracts, providerErr error, workspaceInspection workspace.Resolver, workspaceOK bool, result *runtime.WorkflowContractValidationResult) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	verifyDeploymentObservation(ctx, result, "managed_provider_catalog_admission", "internal/runtime/llm.CompileManagedCapabilityAdmission", subject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
		if providerErr != nil {
			return providerErr
		}
		if !workspaceOK {
			return fmt.Errorf("workspace capability admission did not complete")
		}
		discovered, err := result.BootReport.DiscoveredToolAdmission()
		if err != nil {
			return failures.New(failures.ClassDependencyUnavailable, "mcp_catalog_observation_incomplete", "verify", "managed_provider_catalog_admission", map[string]any{"reason": err.Error()})
		}
		blueprints, err := manager.ResolveStaticTopologyBlueprints(manager.AgentManagerOptions{
			ExecutionPosture: executionposture.Live, LLMBackend: opts.LLMProfile.ID,
			ModelAliases: opts.ModelAliases, RequireModelResolution: true,
		}, source)
		if err != nil {
			return err
		}
		for _, actor := range blueprints {
			contract, err := providers.ResolveAgentProviderContract(actor.Config)
			if err != nil {
				return err
			}
			definitions, capabilities, err := runtimetools.PrepareActorToolAdmission(ctx, source, actor.Config, discovered, runtimetools.NativeToolAdmissionOptions{
				ProviderContract: contract, Credentials: opts.Credentials, Workspaces: workspaceInspection,
			})
			if err != nil {
				return fmt.Errorf("agent %s tool admission: %w", actor.Config.ID, err)
			}
			if _, err := llm.CompileManagedCapabilityAdmission(actor.Config, contract, definitions, capabilities); err != nil {
				return fmt.Errorf("agent %s managed capability input: %w", actor.Config.ID, err)
			}
		}
		return nil
	})
}

func inspectVerifyPublicIngress(ctx context.Context, result *runtime.WorkflowContractValidationResult, subject string) {
	intent := ServeOptions{}
	mode, enabled, err := ResolveServePublicIngressMode(intent)
	if err != nil {
		verifyDeploymentObservation(ctx, result, "public_ingress_admission", "internal/cliapp.ResolveServePublicIngressMode", subject, failures.ClassSchemaInvalid, func(context.Context) error { return err })
		return
	}
	if !enabled {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "public_ingress_admission", Owner: "internal/cliapp.ResolveServePublicIngressMode", Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotApplicable, Reason: "the selected non-dev serve intent does not request public exposure; different serve flags require separate admission",
		})
		return
	}
	verifyDeploymentObservation(ctx, result, "public_ingress_admission", "internal/runtime/publicingress.ValidateConfiguration", subject, failures.ClassSchemaInvalid, func(context.Context) error {
		return publicingress.ValidateConfiguration(mode, intent.PublicWebhookBaseURL, intent.PublicWebhookListen)
	})
}

func inspectVerifyListeners(ctx context.Context, repo string, cfg RuntimeConfigLoadResult, result *runtime.WorkflowContractValidationResult) {
	var held []struct {
		subject  string
		listener net.Listener
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), verifyCleanupDeadline)
		defer cancel()
		for _, binding := range held {
			verifyDeploymentObservation(cleanup, result, "listener_cleanup", "net.Listener.Close", binding.subject, failures.ClassDependencyUnavailable, func(context.Context) error {
				return binding.listener.Close()
			})
		}
	}()
	var api, mcp string
	valid := verifyDeploymentObservation(ctx, result, "serve_listener_configuration", "internal/cliapp.RuntimeConfigLoadResult.ResolveServeListeners", "project:"+repo, failures.ClassSchemaInvalid, func(context.Context) error {
		var err error
		api, mcp, err = cfg.ResolveServeListeners(ServeOptions{}, false, false)
		return err
	})
	if !valid {
		return
	}
	verifyDeploymentObservation(ctx, result, "serve_api_auth_admission", "internal/cliapp.ValidateServeAPIAuthBinding", "api:"+api, failures.ClassAuthenticationNeeded, func(context.Context) error {
		root, err := NewInvocationRoot(repo)
		if err != nil {
			return err
		}
		auth, err := cfg.ResolveServeAPIAuth(root, ServeOptions{})
		if err != nil {
			return err
		}
		return ValidateServeAPIAuthBinding(api, auth)
	})
	for _, binding := range []struct{ name, addr string }{{"api", api}, {"mcp", mcp}} {
		verifyDeploymentObservation(ctx, result, "listener_availability", "internal/cliapp.ListenServeHTTPListener", binding.name+":"+binding.addr, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
			local, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
			defer cancel()
			listener, err := ListenServeHTTPListenerInContext(local, binding.name, binding.addr)
			if err != nil {
				return err
			}
			// Boot binds this set concurrently; isolated probes miss conflicts.
			held = append(held, struct {
				subject  string
				listener net.Listener
			}{binding.name + ":" + binding.addr, listener})
			return local.Err()
		})
		result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
			ID: "listener_binding", Owner: "internal/cliapp.ListenServeHTTPListener", Subject: binding.name + ":" + binding.addr,
			Reason: "availability is an observation, not a reservation; startup must bind and publish its own listener",
		})
	}
}
