package cliapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func inspectVerifyWorkspaceDependencies(ctx context.Context, cfg RuntimeConfigLoadResult, backend WorkspaceBackendSelection, admitted bool, result *runtime.WorkflowContractValidationResult) {
	subject := "source:" + result.BootReport.SourceArtifactHash
	if !admitted {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "workspace_dependency_observation", Owner: "internal/runtime/workspace", Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotRun, Reason: "workspace backend selection did not complete", Dependencies: []string{"workspace_capability_admission"},
		})
		return
	}
	if backend.Backend == WorkspaceBackendNone {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "workspace_dependency_observation", Owner: "internal/cliapp.DecideWorkspaceBackend", Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotApplicable, Reason: "the selected source declares no agents and selects no workspace",
		})
		return
	}
	result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
		ID: "workspace_and_source_preparation", Owner: "internal/runtime/workspace.Lifecycle", Subject: subject,
		Reason: "startup must materialize and validate its owned physical source/data projections and workspace; verification creates none",
	})
	if backend.Backend == workspace.BackendHost {
		verifyDeploymentObservation(ctx, result, "workspace_dependency_observation", "internal/runtime/workspace.InspectHostPrerequisites", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
			configured, err := hostWorkspaceConfigFromRuntimeConfig(cfg.Config)
			if err != nil {
				return err
			}
			local, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
			defer cancel()
			_, err = workspace.InspectHostPrerequisites(local, configured)
			return err
		})
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "workspace_cli_version_observation", Owner: "internal/cliapp.DecideWorkspaceBackend", Subject: subject,
			Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotApplicable,
			Reason: "canonical host workspace admission excludes Claude CLI execution before this observer is reachable",
		})
		return
	}
	configured, err := dockerWorkspaceConfigFromRuntimeConfig(cfg.Config)
	if err != nil {
		verifyDeploymentObservation(ctx, result, "workspace_dependency_observation", "internal/cliapp.dockerWorkspaceConfigFromRuntimeConfig", subject, failures.ClassSchemaInvalid, func(context.Context) error { return err })
		return
	}
	docker := workspace.NewDockerManager()
	docker.SetConfig(configured)
	verifyDeploymentObservation(ctx, result, "docker_daemon_observation", "internal/runtime/workspace.DockerManager.InspectDockerAvailable", "docker:"+configured.DockerBin, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		local, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
		defer cancel()
		return docker.InspectDockerAvailable(local)
	})
	var imageID string
	imageOK := verifyDeploymentObservation(ctx, result, "workspace_image_observation", "internal/runtime/workspace.DockerManager.InspectWorkspaceImage", "image:"+configured.WorkspaceImage, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		local, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
		defer cancel()
		var err error
		imageID, err = docker.InspectWorkspaceImage(local)
		return err
	})
	verifyDeploymentObservation(ctx, result, "workspace_network_observation", "internal/runtime/workspace.DockerManager.InspectWorkspaceNetwork", "network:"+configured.WorkspaceNetwork, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		local, cancel := context.WithTimeout(ctx, verifyLocalDeadline)
		defer cancel()
		present, err := docker.InspectWorkspaceNetwork(local)
		if err == nil && !present && configured.WorkspaceNetwork != "" {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "workspace_network_preparation", Owner: "internal/runtime/workspace.DockerManager.EnsurePrereqs", Subject: "network:" + configured.WorkspaceNetwork,
				Reason: "the configured network is absent; boot can create it, but verification never does",
			})
		}
		return err
	})
	if !workspaceBackendHasReason(backend.Reasons, WorkspaceReasonClaudeCLI) {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "workspace_cli_version_observation", Owner: "internal/cliapp.DecideWorkspaceBackend", Subject: subject,
			Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotApplicable,
			Reason: "the exact selected actor census does not require Claude CLI execution",
		})
		return
	}
	verifyDeploymentObservation(ctx, result, "workspace_cli_version_observation", "internal/runtime/workspace.DockerManager.ProbeWorkspaceCLICommand", "command:"+cfg.Config.LLM.ClaudeCLI.Command+"@"+imageID, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		if !imageOK {
			return fmt.Errorf("CLI version probe requires an observed immutable workspace image")
		}
		remote, cancel := context.WithTimeout(ctx, verifyRemoteDeadline)
		defer cancel()
		return docker.ProbeWorkspaceCLICommand(remote, imageID, cfg.Config.LLM.ClaudeCLI.Command)
	})
}
