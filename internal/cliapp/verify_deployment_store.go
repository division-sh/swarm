package cliapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

func inspectVerifySelectedStore(ctx context.Context, repo string, paths CLISourcePlatformSpecPaths, cfg RuntimeConfigLoadResult, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, packBases packartifact.PlatformPackBaseResolver, workspaces workspace.Resolver, workspaceOK bool, result *runtime.WorkflowContractValidationResult) {
	subject := "project:" + repo
	var selected *storeselected.AdmissionInspection
	var schemaRequest store.SchemaBootstrapRequest
	storeBudget := verifyLocalDeadline
	defer func() {
		if selected == nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), verifyCleanupDeadline)
		defer cancel()
		verifyDeploymentObservation(cleanup, result, "selected_store_cleanup", "internal/store/selected.AdmissionInspection.Close", subject, failures.ClassDependencyUnavailable, func(context.Context) error {
			return selected.Close()
		})
	}()
	opened := verifyDeploymentObservation(ctx, result, "selected_store_access", "internal/store/selected.OpenAdmissionInspection", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		request, schema, err := prepareVerifySelectedStoreInspection(ctx, repo, paths, cfg, source)
		if err != nil {
			return err
		}
		schemaRequest = schema
		if request.Selection.Backend == storebackend.BackendPostgres {
			storeBudget = verifyRemoteDeadline
		}
		bounded, cancel := context.WithTimeout(ctx, storeBudget)
		defer cancel()
		selected, err = storeselected.OpenAdmissionInspection(bounded, request)
		if bounded.Err() != nil {
			return bounded.Err()
		}
		return err
	})
	if !opened {
		blockVerifyStoreTail(result, subject, "selected store access did not complete")
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: "startup_process_possession", Owner: "internal/store/selected.AdmissionInspection.ProbePossession", Subject: subject,
			Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotRun,
			Reason: "selected store access did not complete", Dependencies: []string{"selected_store_access"},
		})
		return
	}
	if verifyDeploymentBoundedObservation(ctx, storeBudget, result, "startup_process_possession", "internal/store/selected.AdmissionInspection.ProbePossession", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		possession, err := selected.ProbePossession(ctx)
		if err != nil {
			var acquisition *startupownership.AcquisitionError
			if errors.As(err, &acquisition) {
				return failures.Wrap(failures.ClassLifecycleConflict, "verify_possession_refused", "verify", "possession", nil, err)
			}
			return err
		}
		if !possession.Available {
			return failures.New(failures.ClassLifecycleConflict, "verify_possession_refused", "verify", "possession", map[string]any{"reason": "selected store possession is currently held"})
		}
		return possession.Validate()
	}) {
		result.BootReport.Observations[len(result.BootReport.Observations)-1].Reason = "momentary possession was available and released; this is not authority, MVCC evidence, or a guarantee of later boot acquisition"
		result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
			ID: "startup_process_acquisition", Owner: "internal/runtime/startupownership.Store.AcquireProcessCapability", Subject: subject,
			Reason: "boot must independently acquire and retain possession and record its authority; verification leaves no capability or lock",
		})
	}
	tailAccounted := false
	var retained *verifyRetainedAdmissionInventory
	schemaOK := verifyDeploymentBoundedObservation(ctx, storeBudget, result, "selected_store_schema", "internal/store/selected.AdmissionInspection.Inspect", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		inspection, err := selected.Inspect(ctx, schemaRequest, func(snapshot *storeselected.AdmissionSnapshot) error {
			fresh, err := snapshot.RequiresSchemaPreparation()
			if err != nil {
				return err
			}
			if fresh {
				accountVerifyStoreTail(result, subject, "canonical schema inspection proves the fresh store has no retained platform state", true)
				tailAccounted = true
				result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
					ID: "store_schema_preparation", Owner: "internal/store.SchemaBootstrapper.BootstrapSchema", Subject: subject,
					Reason: "fresh store schema must be prepared by boot; verification never creates tables",
				})
				return nil
			}
			retained = inspectVerifyRecoverySnapshot(ctx, snapshot, repo, paths, cfg, source, opts, packBases, result)
			tailAccounted = true
			return nil
		})
		if err == nil && !inspection.Fresh && len(inspection.MissingStateTables) != 0 {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "store_schema_preparation", Owner: "internal/store.SchemaBootstrapper.BootstrapSchema", Subject: subject,
				Reason: "boot must prepare missing generated state tables: " + strings.Join(inspection.MissingStateTables, ", ") + "; verification never creates tables",
			})
		}
		return err
	})
	if !tailAccounted {
		blockVerifyStoreTail(result, subject, "selected store schema inspection did not complete")
	}
	if retained != nil {
		inspectVerifyRetainedDependencies(ctx, retained, schemaOK, cfg, source, opts, workspaces, workspaceOK, result)
	}
}

func prepareVerifySelectedStoreInspection(ctx context.Context, repo string, paths CLISourcePlatformSpecPaths, cfg RuntimeConfigLoadResult, source semanticview.Source) (storeselected.AuthorityRequest, store.SchemaBootstrapRequest, error) {
	root, err := NewInvocationRoot(repo)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	swarmDir, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{}, cfg.cli)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	local, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot: repo, ResolvedPaths: paths, SwarmDir: swarmDir, Config: cfg.Config, EnforceLegacySQLite: true,
	})
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	request, err := selectedAuthorityRequest(ctx, local.StoreSelection, cfg.Config)
	if err != nil {
		return request, store.SchemaBootstrapRequest{}, err
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return request, store.SchemaBootstrapRequest{}, fmt.Errorf("admitted selected source bundle is required")
	}
	plans, err := StateStoreSchemaPlans(bundle)
	if err != nil {
		return request, store.SchemaBootstrapRequest{}, err
	}
	schema, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
	return request, schema, err
}

type verifyRetainedAdmissionInventory struct {
	forks    []runforkexecution.InspectedSelectedForkRecovery
	actors   manager.RetainedActorInspection
	forksOK  bool
	actorsOK bool
}

func inspectVerifyRecoverySnapshot(ctx context.Context, snapshot *storeselected.AdmissionSnapshot, repo string, paths CLISourcePlatformSpecPaths, cfg RuntimeConfigLoadResult, source semanticview.Source, opts runtime.WorkflowContractValidationOptions, packBases packartifact.PlatformPackBaseResolver, result *runtime.WorkflowContractValidationResult) *verifyRetainedAdmissionInventory {
	retained := &verifyRetainedAdmissionInventory{}
	hash := result.BootReport.SourceArtifactHash
	subject := "source:" + hash
	verifyDeploymentObservation(ctx, result, "startup_authority_lineage", "internal/runtime/startupownership.AdmitAuthorityInspection", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		inspection, err := snapshot.InspectAuthority(ctx)
		if err != nil {
			return err
		}
		return startupownership.AdmitAuthorityInspection(inspection)
	})
	verifyDeploymentObservation(ctx, result, "pinned_source_admission", "internal/runtime/runbundle.AdmitPinnedSources", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		return runbundle.AdmitPinnedSources(ctx, snapshot, hash, []string{hash})
	})
	verifyDeploymentObservation(ctx, result, "retained_source_integrity", "internal/runtime/startuprecovery.Inspect", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		_, err := startuprecovery.Inspect(ctx, startuprecovery.Request{AvailabilityReader: snapshot, ArtifactReader: snapshot})
		return err
	})
	verifyDeploymentObservation(ctx, result, "startup_recovery_admission", "internal/runtime.InspectStartupRecoveryAdmission", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		fact, err := correlation.NewSourceArtifactFact(hash)
		if err != nil {
			return err
		}
		_, err = runtime.InspectStartupRecoveryAdmission(ctx, runtime.StartupRecoveryReadRequest{
			SourceArtifact: fact, RecoveryOnStartup: cfg.Config.Runtime.RecoveryOnStartup,
			ObservedAt: time.Now().UTC(), Delivery: snapshot, Timers: snapshot, StandingRestarts: snapshot,
			ReadManagerState: func(ctx context.Context) (manager.RecoverableStateSnapshot, error) {
				return manager.InspectRecoverableStateSnapshot(ctx, fact, snapshot)
			},
		})
		return err
	})
	verifyDeploymentObservation(ctx, result, "pending_reset_admission", "internal/runtime/destructivereset.InspectPendingOperations", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		pending, err := destructivereset.InspectPendingOperations(ctx, snapshot)
		if err != nil {
			return err
		}
		for _, operation := range pending {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "pending_reset_recovery", Owner: "internal/runtime/destructivereset.Coordinator.RecoverPending",
				Subject: "reset:" + operation.Request.OperationID,
				Reason:  "boot must settle the inspected " + string(operation.Phase) + " reset under current process authority before source ingestion",
			})
		}
		return nil
	})
	verifyDeploymentObservation(ctx, result, "retained_route_admission", "internal/runtime/manager.InspectSelectedContractRouteRecoveries", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		_, err := manager.InspectSelectedContractRouteRecoveries(ctx, snapshot)
		return err
	})
	inspectVerifyRetainedChannels(ctx, snapshot, subject, result)
	retained.forksOK = verifyDeploymentObservation(ctx, result, "selected_fork_recovery_admission", "internal/runtime/runforkexecution.InspectSelectedForkRecoveries", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		loader, err := verifyRetainedSourceLoader(repo, paths, packBases)
		if err != nil {
			return err
		}
		forks, err := runforkexecution.InspectSelectedForkRecoveries(ctx, snapshot, loader)
		if err != nil {
			return err
		}
		retained.forks = forks
		for _, fork := range forks {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "selected_fork_recovery_settlement", Owner: "internal/runtime/runforkexecution.SelectedContractExecutionOwner.RecoverSelectedForkContexts",
				Subject: "fork:" + fork.Entry.Binding.ForkRunID,
				Reason:  "boot must independently admit current process ownership and settle the inspected " + string(fork.Evidence.Plan.Disposition) + " plan; inspection never fences, resumes or hydrates",
			})
		}
		return nil
	})
	retained.actorsOK = verifyDeploymentObservation(ctx, result, "retained_actor_admission", "internal/runtime/manager.InspectRetainedActors", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		fact, err := correlation.NewSourceArtifactFact(hash)
		if err != nil {
			return err
		}
		inspection, err := manager.InspectRetainedActors(ctx, snapshot, source, fact, manager.AgentManagerOptions{
			ExecutionPosture: executionposture.Live, LLMBackend: opts.LLMProfile.ID,
			ModelAliases: opts.ModelAliases, RequireModelResolution: true,
		})
		if err != nil {
			return err
		}
		retained.actors = inspection
		if inspection.Observed != 0 {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "retained_actor_reconciliation_and_hydration", Owner: "internal/runtime/manager.AgentManager.PrepareStaticTopologyForStartup",
				Subject: subject, Reason: "boot must independently acquire lifecycle authority, reconcile projected declarations and hydrate only owned actors; no actor is created by verification",
			})
		}
		return nil
	})
	return retained
}

func verifyRetainedSourceLoader(repo string, paths CLISourcePlatformSpecPaths, packBases packartifact.PlatformPackBaseResolver) (runforkexecution.SourceArtifactSelectedContractSourceLoader, error) {
	if packBases == nil {
		return runforkexecution.SourceArtifactSelectedContractSourceLoader{}, fmt.Errorf("invocation-selected platform pack generation is required for retained inspection")
	}
	return runforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot: repo, PlatformSpecPath: paths.PlatformSpecPath, PlatformPackBases: packBases,
	}, nil
}

func inspectVerifyRetainedChannels(ctx context.Context, snapshot *storeselected.AdmissionSnapshot, subject string, result *runtime.WorkflowContractValidationResult) {
	verifyDeploymentObservation(ctx, result, "retained_channel_admission", "internal/channelonboarding.InspectRetainedActivations", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		inventory, err := channelonboarding.InspectRetainedActivations(ctx, snapshot)
		if err != nil {
			if errors.Is(err, channelonboarding.ErrConflict) {
				return fmt.Errorf("%s: %w", err.Error(), failures.Wrap(failures.ClassLifecycleConflict, "retained_channel_ownership_conflict", "verify", "channel-inspection", nil, err))
			}
			return err
		}
		teardowns, err := channelonboarding.InspectPendingTeardowns(ctx, snapshot)
		if err != nil {
			if errors.Is(err, channelonboarding.ErrConflict) {
				return fmt.Errorf("%s: %w", err.Error(), failures.Wrap(failures.ClassLifecycleConflict, "retained_channel_teardown_conflict", "verify", "channel-inspection", nil, err))
			}
			return err
		}
		if len(inventory.Operations) != 0 || len(inventory.Activations) != 0 || len(teardowns) != 0 {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "connected_channel_reconciliation", Owner: "internal/channelonboarding.Service.ReconcileLocal/Recover", Subject: subject,
				Reason: "boot must independently reconcile exact successor contexts, credentials, identities and provider effects; durable row inspection is not activation or readiness",
			})
		}
		for _, operation := range teardowns {
			result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
				ID: "connected_channel_teardown", Owner: "internal/channelonboarding.DestructiveService.Recover", Subject: "channel-teardown:" + operation.TeardownID,
				Reason: "boot must settle the inspected " + string(operation.Phase) + " teardown; verification never retires authority or removes credentials",
			})
		}
		return nil
	})
}

func blockVerifyStoreTail(result *runtime.WorkflowContractValidationResult, subject, reason string) {
	accountVerifyStoreTail(result, subject, reason, false)
}

func accountVerifyStoreTail(result *runtime.WorkflowContractValidationResult, subject, reason string, fresh bool) {
	for _, blocked := range []struct{ id, owner string }{
		{"pinned_source_admission", "internal/runtime/runbundle.AdmitPinnedSources"},
		{"retained_source_integrity", "internal/runtime/startuprecovery.Inspect"},
		{"startup_recovery_admission", "internal/runtime.InspectStartupRecoveryAdmission"},
		{"startup_authority_lineage", "internal/runtime/startupownership.AdmitAuthorityInspection"},
		{"pending_reset_admission", "internal/runtime/destructivereset.InspectPendingOperations"},
		{"retained_route_admission", "internal/runtime/manager.InspectSelectedContractRouteRecoveries"},
		{"retained_channel_admission", "internal/channelonboarding.InspectRetainedActivations"},
		{"selected_fork_recovery_admission", "internal/runtime/runforkexecution"},
		{"retained_actor_admission", "internal/runtime/manager"},
		{"selected_fork_source_dependencies", "internal/runtime.ValidateWorkflowContractSurface"},
		{"retained_actor_provider_dependencies", "internal/runtime/llm.CompileManagedCapabilityAdmission"},
	} {
		observation := bootverify.AdmissionObservation{
			CheckID: blocked.id, Owner: blocked.owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotRun, Reason: reason, Dependencies: []string{"selected_store_access"},
		}
		if fresh {
			observation.Dependencies = []string{"selected_store_schema"}
			observation.Status = bootverify.AdmissionNotApplicable
		}
		result.BootReport.Observations = append(result.BootReport.Observations, observation)
	}
}
