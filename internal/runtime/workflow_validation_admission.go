package runtime

import (
	"time"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
)

const workflowAdmissionOwner = "internal/runtime.ValidateWorkflowContractSurface"

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
