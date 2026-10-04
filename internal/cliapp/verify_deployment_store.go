package cliapp

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

func inspectVerifySelectedStore(ctx context.Context, repo string, paths CLISourcePlatformSpecPaths, cfg RuntimeConfigLoadResult, source semanticview.Source, result *runtime.WorkflowContractValidationResult) {
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
		root, err := NewInvocationRoot(repo)
		if err != nil {
			return err
		}
		swarmDir, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{}, cfg.cli)
		if err != nil {
			return err
		}
		local, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
			RepoRoot: repo, ResolvedPaths: paths, SwarmDir: swarmDir, Config: cfg.Config, EnforceLegacySQLite: true,
		})
		if err != nil {
			return err
		}
		request, err := selectedAuthorityRequest(ctx, local.StoreSelection, cfg.Config)
		if err != nil {
			return err
		}
		bundle, ok := semanticview.Bundle(source)
		if !ok || bundle == nil {
			return fmt.Errorf("admitted selected source bundle is required")
		}
		plans, err := StateStoreSchemaPlans(bundle)
		if err != nil {
			return err
		}
		schemaRequest, err = SchemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
		if err != nil {
			return err
		}
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
		return
	}
	tailAccounted := false
	verifyDeploymentBoundedObservation(ctx, storeBudget, result, "selected_store_schema", "internal/store/selected.AdmissionInspection.Inspect", subject, failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		_, err := selected.Inspect(ctx, schemaRequest, func(snapshot *storeselected.AdmissionSnapshot) error {
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
			inspectVerifyRecoverySnapshot(ctx, snapshot, cfg, result)
			tailAccounted = true
			return nil
		})
		return err
	})
	if !tailAccounted {
		blockVerifyStoreTail(result, subject, "selected store schema inspection did not complete")
	}
}

func inspectVerifyRecoverySnapshot(ctx context.Context, snapshot *storeselected.AdmissionSnapshot, cfg RuntimeConfigLoadResult, result *runtime.WorkflowContractValidationResult) {
	hash := result.BootReport.SourceArtifactHash
	subject := "source:" + hash
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
	for _, pending := range []struct{ id, owner string }{
		{"startup_process_possession", "internal/runtime/startupownership.InspectAuthority"},
		{"selected_fork_recovery_admission", "internal/runtime/runforkexecution"},
		{"retained_actor_admission", "internal/runtime/manager"},
	} {
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: pending.id, Owner: pending.owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotRun, Reason: "required retained-state observer has not completed",
		})
	}
}

func blockVerifyStoreTail(result *runtime.WorkflowContractValidationResult, subject, reason string) {
	accountVerifyStoreTail(result, subject, reason, false)
}

func accountVerifyStoreTail(result *runtime.WorkflowContractValidationResult, subject, reason string, fresh bool) {
	for _, blocked := range []struct{ id, owner string }{
		{"pinned_source_admission", "internal/runtime/runbundle.AdmitPinnedSources"},
		{"retained_source_integrity", "internal/runtime/startuprecovery.Inspect"},
		{"startup_recovery_admission", "internal/runtime.InspectStartupRecoveryAdmission"},
		{"startup_process_possession", "internal/runtime/startupownership.InspectAuthority"},
		{"pending_reset_admission", "internal/runtime/destructivereset.InspectPendingOperations"},
		{"retained_route_admission", "internal/runtime/manager.InspectSelectedContractRouteRecoveries"},
		{"selected_fork_recovery_admission", "internal/runtime/runforkexecution"},
		{"retained_actor_admission", "internal/runtime/manager"},
	} {
		observation := bootverify.AdmissionObservation{
			CheckID: blocked.id, Owner: blocked.owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotRun, Reason: reason, Dependencies: []string{"selected_store_access"},
		}
		if fresh {
			observation.Dependencies = []string{"selected_store_schema"}
			if blocked.id == "startup_process_possession" {
				observation.Reason = "fresh schema does not prove that process possession is available"
			} else {
				observation.Status = bootverify.AdmissionNotApplicable
			}
		}
		result.BootReport.Observations = append(result.BootReport.Observations, observation)
	}
}
