package cliapp

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

// External metadata is observed after the consistent read callback returns.
// Only its immutable inventory escapes; no revoked store role escapes with it.
func inspectVerifyRetainedDependencies(ctx context.Context, retained *verifyRetainedAdmissionInventory, schemaOK bool, cfg RuntimeConfigLoadResult, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, workspaces workspace.Resolver, workspaceOK bool, result *runtime.WorkflowContractValidationResult) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	verifyDeploymentObservation(ctx, result, "selected_fork_source_dependencies", "internal/runtime.ValidateWorkflowContractSurface", subject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
		if !schemaOK || !retained.forksOK {
			return failures.New(failures.ClassDependencyUnavailable, "selected_fork_inventory_incomplete", "verify", "selected_fork_source_dependencies", nil)
		}
		for _, fork := range retained.forks {
			forkSource := fork.SelectedSource.Source
			bundle, ok := semanticview.Bundle(forkSource)
			if !ok || bundle == nil {
				return fmt.Errorf("selected fork inspection lacks admitted source")
			}
			forkOptions, err := verifyDeploymentWorkflowOptions(ctx, cfg, bundle)
			if err != nil {
				return err
			}
			forkResult, err := runtime.ValidateWorkflowContractSurface(ctx, forkSource, forkOptions)
			if err == nil {
				backend, _, workspaceOK := inspectVerifySourceAdmission(ctx, cfg, forkSource, forkOptions, &forkResult)
				inspectVerifyWorkspaceDependencies(ctx, cfg, backend, workspaceOK, &forkResult)
			}
			result.BootReport.Findings = append(result.BootReport.Findings, forkResult.BootReport.Findings...)
			result.BootReport.Observations = append(result.BootReport.Observations, forkResult.BootReport.Observations...)
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, forkResult.BootReport.ExecutionObligations...)
			result.BootReport.Interrupted = result.BootReport.Interrupted || forkResult.BootReport.Interrupted
			result.ProductionValid = result.ProductionValid && forkResult.ProductionValid
			if err != nil {
				return err
			}
			decision := forkResult.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{
				FatalWarnings: forkOptions.FatalBootWarnings, ExcludedFatalWarningChecks: forkOptions.ExcludedFatalBootWarningChecks,
			})
			if !decision.Complete || decision.FailureClass != "" {
				class := decision.FailureClass
				if class == "" {
					class = failures.ClassDependencyUnavailable
				}
				return failures.New(class, "selected_fork_source_admission_failed", "verify", "selected_fork_source_dependencies", map[string]any{"fork_run_id": fork.Entry.Binding.ForkRunID})
			}
		}
		return nil
	})
	verifyDeploymentObservation(ctx, result, "retained_actor_provider_dependencies", "internal/runtime/llm.CompileManagedCapabilityAdmission", subject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
		if !schemaOK || !retained.actorsOK {
			return failures.New(failures.ClassDependencyUnavailable, "retained_actor_inventory_incomplete", "verify", "retained_actor_provider_dependencies", nil)
		}
		if len(retained.actors.Configurations) != 0 && !workspaceOK {
			return failures.New(failures.ClassDependencyUnavailable, "retained_actor_workspace_incomplete", "verify", "retained_actor_provider_dependencies", nil)
		}
		for _, actor := range retained.actors.Configurations {
			if err := inspectVerifyRetainedActorConfiguration(ctx, source, actor, opts, workspaces, result); err != nil {
				return err
			}
		}
		return nil
	})
}

func inspectVerifyRetainedActorConfiguration(ctx context.Context, source semanticview.Source, actor models.AgentConfig, opts runtime.WorkflowContractValidationOptions, workspaces workspace.Resolver, result *runtime.WorkflowContractValidationResult) error {
	if _, err := runtime.ManagedProviderStartupPrompt(actor); err != nil {
		return err
	}
	profile, err := llmselection.ResolveActiveBackend(actor.ResolvedLLMBackend)
	if err != nil {
		return err
	}
	if err := llm.ValidateProviderAdmissionEnvironment(profile); err != nil {
		return err
	}
	credentialContext, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
	_, err = llm.NewProviderCredentialResolver(opts.ProviderCredentials).Resolve(credentialContext, profile)
	err = errors.Join(err, credentialContext.Err())
	cancel()
	if err != nil {
		return err
	}
	providers, err := llm.NewAgentProviderContracts(opts.LLMProfile)
	if err != nil {
		return err
	}
	contract, err := providers.ResolveAgentProviderContract(actor)
	if err != nil {
		return err
	}
	discovered, err := result.BootReport.DiscoveredToolAdmission()
	if err != nil {
		return failures.New(failures.ClassDependencyUnavailable, "retained_actor_catalog_incomplete", "verify", "retained_actor_provider_dependencies", nil)
	}
	definitions, capabilities, err := runtimetools.PrepareActorToolAdmission(ctx, source, actor, discovered, runtimetools.NativeToolAdmissionOptions{
		ProviderContract: contract, Credentials: opts.Credentials, Workspaces: workspaces,
	})
	if err != nil {
		return err
	}
	_, err = llm.CompileManagedCapabilityAdmission(actor, contract, definitions, capabilities)
	return err
}
