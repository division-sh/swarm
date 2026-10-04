package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

const workflowAdmissionOwner = "internal/runtime.ValidateWorkflowContractSurface"

func (result *WorkflowContractValidationResult) settleWorkflowAdmission(ctx context.Context, err error, phase string, reportBlocks, registryExecuted bool, blockingPrerequisite string) error {
	result.bindAdmissionSourceSubject()
	if ctx.Err() != nil {
		result.BootReport.Interrupted = result.BootReport.Interrupted || errors.Is(ctx.Err(), context.Canceled)
		if err == nil {
			err = ctx.Err()
		}
	}
	if err != nil && !reportBlocks {
		result.failAdmissionClause(phase, err)
	}
	if err != nil {
		dependency := phase
		if reportBlocks {
			dependency = blockingPrerequisite
		}
		result.blockAdmissionTail(dependency, registryExecuted)
	}
	result.BootReport.Sort()
	return err
}

func (result *WorkflowContractValidationResult) bindAdmissionSourceSubject() {
	if result.BootReport.SourceArtifactHash == "" {
		return
	}
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		if observation.Owner == workflowAdmissionOwner {
			observation.Subject = "source:" + result.BootReport.SourceArtifactHash
		}
	}
}

func (result *WorkflowContractValidationResult) failAdmissionClause(phase string, err error) {
	finding := bootverify.NewHardInvalidityFinding("workflow_contract_validation", "global", err.Error(), "Fix the named admission input before validation or execution.")
	finding.FailureClass = failures.ClassSchemaInvalid
	if failure, ok := failures.As(err); ok {
		finding.FailureClass = failure.Failure.Class
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		finding.FailureClass = failures.ClassDependencyUnavailable
	}
	result.BootReport.Add(finding)
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		if observation.CheckID == phase && observation.Owner == workflowAdmissionOwner {
			observation.Status, observation.Reason = bootverify.AdmissionFailed, "canonical admission clause rejected the selected subject; see findings"
			observation.FinishedAt, observation.FailureClass = time.Now().UTC(), finding.FailureClass
		}
	}
}

func (result *WorkflowContractValidationResult) blockAdmissionTail(dependency string, registryExecuted bool) {
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		if observation.Owner == workflowAdmissionOwner && observation.Status == bootverify.AdmissionNotRun && dependency != "" {
			observation.Dependencies = []string{dependency}
		}
	}
	if !registryExecuted {
		result.BootReport.BlockRegisteredChecks("selected_source", dependency, "source admission prerequisite failed before registered checks could run")
	}
}

// BlockedWorkflowContractAdmission records the existing owners' unexecuted tail
// when configuration or loading refuses the source before validation can start.
func BlockedWorkflowContractAdmission(purpose bootverify.ValidationPurpose, subject, dependency, reason string) WorkflowContractValidationResult {
	result := WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: purpose, Observations: workflowAdmissionObservations(purpose)}}
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		observation.Subject, observation.Reason = subject, reason
		observation.Dependencies = []string{dependency}
	}
	result.BootReport.BlockRegisteredChecks(subject, dependency, reason)
	return result
}

// This is evidence for the existing sequential clauses, not another check
// registry: validation remains implemented by their canonical owners.
func workflowAdmissionObservations(purpose bootverify.ValidationPurpose) []bootverify.AdmissionObservation {
	observations := []bootverify.AdmissionObservation{
		{CheckID: "source_validation", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "tool_implementation_validation", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "emit_schema_coverage", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "generated_emit_schema_validation", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "durable_activity_surface", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "provider_connector_surface", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "standing_ingress_declaration", Class: bootverify.AdmissionSourceObservation},
		{CheckID: "provider_trigger_capability_projection", Class: bootverify.AdmissionDeploymentObservation},
		{CheckID: "channel_capability_projection", Class: bootverify.AdmissionDeploymentObservation},
		{CheckID: "channel_activation_publication", Class: bootverify.AdmissionDeploymentObservation},
		{CheckID: "capability_projection_normalization", Class: bootverify.AdmissionDeploymentObservation},
	}
	for i := range observations {
		observation := &observations[i]
		observation.Owner, observation.Subject = workflowAdmissionOwner, "selected_source"
		observation.Status, observation.Reason = bootverify.AdmissionNotRun, "an earlier validation clause must succeed before this clause"
		if purpose == bootverify.StructuralValidation && observation.Class == bootverify.AdmissionDeploymentObservation {
			observation.Reason = "portable validation does not observe deployment capability state"
		}
	}
	return observations
}

func (result *WorkflowContractValidationResult) startAdmissionClause(id string) {
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		if observation.CheckID == id && observation.Owner == workflowAdmissionOwner {
			observation.StartedAt = time.Now().UTC()
			return
		}
	}
	panic("unaccounted workflow admission clause: " + id)
}

func (result *WorkflowContractValidationResult) finishAdmissionClause(id string) {
	for i := range result.BootReport.Observations {
		observation := &result.BootReport.Observations[i]
		if observation.CheckID == id && observation.Owner == workflowAdmissionOwner {
			observation.FinishedAt = time.Now().UTC()
			observation.Status, observation.Reason = bootverify.AdmissionPassed, ""
			return
		}
	}
	panic("unaccounted workflow admission clause: " + id)
}
