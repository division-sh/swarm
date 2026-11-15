package cliapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

func inspectServeResetSnapshot(ctx context.Context, snapshot *storeselected.AdmissionSnapshot, operation destructivereset.Operation, result *runtime.WorkflowContractValidationResult) (*verifyRetainedAdmissionInventory, error) {
	subject := "reset:" + operation.Request.OperationID
	if !verifyDeploymentObservation(ctx, result, "pending_reset_admission", "internal/runtime/destructivereset.InspectPendingOperations", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		return inspectServeResetDiscard(ctx, snapshot, operation)
	}) {
		return nil, fmt.Errorf("reset discard population was not established")
	}
	verifyDeploymentObservation(ctx, result, "startup_process_generation", "internal/runtime/startupownership.AdmitAuthorityInspection", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		inspection, err := snapshot.InspectAuthority(ctx)
		if err != nil {
			return err
		}
		return startupownership.AdmitAuthorityInspection(inspection)
	})
	retained := &verifyRetainedAdmissionInventory{}
	retained.channels = inspectVerifyRetainedChannels(ctx, snapshot, subject, result, resetSurvivingChannelReader{snapshot})
	// Positive cleanup coverage below, rather than the mere presence of a reset,
	// makes these pre-reset executable populations inapplicable. Kept source bytes
	// were independently compiled before reaching this snapshot.
	for _, check := range bootverify.SelectedStoreDependentAdmissionChecks() {
		if check.ID == "pending_reset_admission" || check.ID == "startup_process_generation" || check.ID == "retained_channel_admission" {
			continue
		}
		result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
			CheckID: check.ID, Owner: check.Owner, Subject: subject, Class: bootverify.AdmissionDeploymentObservation,
			Status: bootverify.AdmissionNotApplicable,
			Reason: fmt.Sprintf("reset revision %d discards the complete observed run population under the existing cleanup catalog; executable recovery of those rows is not performed", operation.Revision),
		})
	}
	result.BootReport.ExecutionObligations = append(result.BootReport.ExecutionObligations, bootverify.AdmissionExecutionObligation{
		ID: "pending_reset_recovery", Owner: "internal/runtime/destructivereset.Coordinator.RecoverPending", Subject: subject,
		Reason: "boot must revalidate this operation under process ownership and settle its existing cleanup, reconstruction and completion receipts",
	})
	return retained, nil
}

func inspectServeResetDiscard(ctx context.Context, snapshot *storeselected.AdmissionSnapshot, operation destructivereset.Operation) error {
	plan, err := (destructivereset.InventoryPlanner{Reader: snapshot}).BuildPlan(ctx, operation.Request)
	if err != nil {
		return err
	}
	current := plan
	if operation.Plan != nil {
		plan = operation.Plan.Plan
	}
	if !plan.CleanupRunSetKnown || !current.CleanupRunSetKnown {
		return fmt.Errorf("reset cleanup population is unknown")
	}
	discard := map[string]bool{}
	for _, run := range plan.CleanupRuns {
		discard[run.RunID] = true
	}
	for _, run := range current.CleanupRuns {
		if !discard[run.RunID] {
			return destructivereset.ErrOperationChanged
		}
	}
	// These are the exact populations whose ordinary execution checks are omitted.
	catalog := destructivereset.CleanupCatalogByTableForPolicy(destructivereset.CleanupPolicy{IncludeSourceArtifacts: operation.Request.IncludeSourceArtifacts})
	for table, action := range map[string]string{
		"runs": destructivereset.CleanupDeleteAll, "agents": destructivereset.CleanupDeleteByRunID,
		"entity_mutations":                            destructivereset.CleanupDeleteByRunID,
		"run_fork_selected_contract_bindings":         destructivereset.CleanupDeleteByRunLineage,
		"run_fork_selected_contract_route_recoveries": destructivereset.CleanupDeleteByRunLineage,
		"connected_channel_activations":               destructivereset.CleanupDeleteAll,
		"channel_onboarding_operations":               destructivereset.CleanupDeleteAll,
	} {
		if catalog[table].Classification != action {
			return fmt.Errorf("reset does not discard the inspected %s population", table)
		}
	}
	return nil
}

// Deployment registrations and teardown journals survive reset; learned runtime
// activations and onboarding rows are explicitly delete-all in the checked plan.
type resetSurvivingChannelReader struct {
	*storeselected.AdmissionSnapshot
}

func (r resetSurvivingChannelReader) ListChannelOnboardingOperations(ctx context.Context) ([]channelonboarding.Operation, error) {
	operations, err := r.AdmissionSnapshot.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	var surviving []channelonboarding.Operation
	for _, operation := range operations {
		if operation.DeploymentRegistration {
			surviving = append(surviving, operation)
		}
	}
	return surviving, nil
}

func (r resetSurvivingChannelReader) ListCurrentConnectedChannelActivations(ctx context.Context) ([]channelonboarding.ConnectedChannelActivation, error) {
	return nil, ctx.Err()
}
