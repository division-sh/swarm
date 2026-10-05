package bootverify

import (
	"path/filepath"
	"strings"
)

type AdmissionNotRunCauseKind string

const AdmissionAbsentSQLiteStore AdmissionNotRunCauseKind = "absent_sqlite_store"

const SelectedStoreAccessOwner = "internal/store/selected.OpenAdmissionInspection"

type AdmissionNotRunCause struct {
	Kind AdmissionNotRunCauseKind `json:"kind"`
	Path string                   `json:"path"`
}

type SelectedStoreAdmissionCheck struct {
	ID    string
	Owner string
}

// This is the existing finite retained-inspection tail, shared by its producer
// and the completeness reducer; dependencies alone never grant an exemption.
func SelectedStoreDependentAdmissionChecks() []SelectedStoreAdmissionCheck {
	return []SelectedStoreAdmissionCheck{
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
	}
}

func absentStoreRequiredAdmissionChecks() []SelectedStoreAdmissionCheck {
	return append(SelectedStoreDependentAdmissionChecks(),
		SelectedStoreAdmissionCheck{"selected_store_schema", "internal/store/selected.AdmissionInspection.Inspect"},
		SelectedStoreAdmissionCheck{"startup_process_possession", "internal/store/selected.AdmissionInspection.ProbePossession"})
}

func (o AdmissionObservation) validAbsentStore(purpose ValidationPurpose) bool {
	cause := o.NotRunCause
	return purpose == ExecutionValidation && cause != nil && cause.Kind == AdmissionAbsentSQLiteStore &&
		filepath.IsAbs(cause.Path) && filepath.Clean(cause.Path) == cause.Path &&
		o.CheckID == "selected_store_access" && o.Owner == SelectedStoreAccessOwner &&
		o.Subject == "store:"+cause.Path && o.Class == AdmissionDeploymentObservation &&
		o.Status == AdmissionNotRun && strings.TrimSpace(o.Reason) != "" && o.FailureClass == "" &&
		!o.StartedAt.IsZero() && !o.FinishedAt.Before(o.StartedAt) && len(o.Dependencies) == 0
}

func (o AdmissionObservation) validAbsentStoreDependent() bool {
	if o.NotRunCause != nil || o.Class != AdmissionDeploymentObservation || o.Status != AdmissionNotRun ||
		o.FailureClass != "" || strings.TrimSpace(o.Reason) == "" ||
		len(o.Dependencies) != 1 || o.Dependencies[0] != "selected_store_access" {
		return false
	}
	for _, check := range absentStoreRequiredAdmissionChecks() {
		if o.CheckID == check.ID {
			return o.Owner == check.Owner
		}
	}
	return false
}
