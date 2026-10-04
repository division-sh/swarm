package bootverify

import (
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

type AdmissionObservationClass string

const (
	AdmissionSourceObservation     AdmissionObservationClass = "source"
	AdmissionDeploymentObservation AdmissionObservationClass = "deployment"
)

type AdmissionObservationStatus string

const (
	AdmissionPassed        AdmissionObservationStatus = "passed"
	AdmissionFailed        AdmissionObservationStatus = "failed"
	AdmissionUnavailable   AdmissionObservationStatus = "unavailable"
	AdmissionNotRun        AdmissionObservationStatus = "not_run"
	AdmissionNotApplicable AdmissionObservationStatus = "not_applicable"
)

// Observations describe admission evidence, never a grant for later execution.
type AdmissionObservation struct {
	CheckID      string                     `json:"check_id"`
	Owner        string                     `json:"owner"`
	Subject      string                     `json:"subject"`
	Class        AdmissionObservationClass  `json:"class"`
	Status       AdmissionObservationStatus `json:"status"`
	Reason       string                     `json:"reason,omitempty"`
	StartedAt    time.Time                  `json:"started_at,omitzero"`
	FinishedAt   time.Time                  `json:"finished_at,omitzero"`
	Dependencies []string                   `json:"dependencies,omitempty"`
	FailureClass failures.Class             `json:"failure_class,omitempty"`
}

// Startup obligations require production authority and are not observations.
type AdmissionExecutionObligation struct {
	ID      string `json:"id"`
	Owner   string `json:"owner"`
	Subject string `json:"subject"`
	Reason  string `json:"reason"`
}

type AdmissionDecision struct {
	Complete     bool
	Interrupted  bool
	FailureClass failures.Class
}

type AdmissionFindingPolicy struct {
	FatalWarnings              bool
	ExcludedFatalWarningChecks []string
}

// AdmissionDecision preserves finding policy and reduces evidence independently
// of presentation. Unobserved deployment predicates are allowed only in portable
// validation, where completeness remains false.
func (r Report) AdmissionDecision(policy AdmissionFindingPolicy) AdmissionDecision {
	d := AdmissionDecision{Complete: r.Purpose == ExecutionValidation && len(r.Observations) > 0}
	priority := 0
	block := func(class failures.Class) {
		if class == "" {
			class = failures.ClassDependencyUnavailable
		}
		p := admissionFailurePriority(class)
		if p > priority {
			priority, d.FailureClass = p, class
		}
	}
	if !r.Purpose.Valid() || len(r.Observations) == 0 {
		d.Complete = false
		block(failures.ClassDependencyUnavailable)
	}
	seen := make(map[string]bool, len(r.Observations))
	for _, observation := range r.Observations {
		identity := observation.CheckID + "\x00" + observation.Subject + "\x00" + string(observation.Class)
		if !observation.validIdentity() || seen[identity] {
			d.Complete = false
			block(failures.ClassSchemaInvalid)
		}
		seen[identity] = true
		complete, class := observation.decision(r.Purpose)
		if !complete {
			d.Complete = false
		}
		if class != "" {
			block(class)
		}
	}
	obligations := make(map[string]bool, len(r.ExecutionObligations))
	for _, obligation := range r.ExecutionObligations {
		identity := obligation.ID + "\x00" + obligation.Subject
		if !obligation.validIdentity() || obligations[identity] {
			d.Complete = false
			block(failures.ClassSchemaInvalid)
		}
		obligations[identity] = true
	}
	for _, finding := range r.Errors() {
		class := finding.FailureClass
		if class == "" {
			class = failures.ClassSchemaInvalid
		}
		block(class)
	}
	if policy.FatalWarnings {
		for _, finding := range r.Warnings() {
			if admissionExcludedWarning(finding.CheckID, policy.ExcludedFatalWarningChecks) {
				continue
			}
			class := finding.FailureClass
			if class == "" {
				class = failures.ClassSchemaInvalid
			}
			block(class)
		}
	}
	if r.Interrupted {
		d.Complete, d.Interrupted = false, true
	}
	if d.FailureClass != "" {
		d.Complete = false
	}
	return d
}

func (o AdmissionObservation) validIdentity() bool {
	return strings.TrimSpace(o.CheckID) != "" && strings.TrimSpace(o.Owner) != "" && strings.TrimSpace(o.Subject) != "" &&
		(o.Class == AdmissionSourceObservation || o.Class == AdmissionDeploymentObservation)
}

func (o AdmissionObservation) decision(purpose ValidationPurpose) (bool, failures.Class) {
	switch o.Status {
	case AdmissionPassed:
		if o.FailureClass != "" || o.StartedAt.IsZero() || o.FinishedAt.Before(o.StartedAt) {
			return false, failures.ClassSchemaInvalid
		}
		return true, ""
	case AdmissionNotApplicable:
		if strings.TrimSpace(o.Reason) == "" || o.FailureClass != "" {
			return false, failures.ClassSchemaInvalid
		}
		return true, ""
	case AdmissionNotRun, AdmissionUnavailable:
		if purpose == StructuralValidation && o.Class == AdmissionDeploymentObservation && o.Status == AdmissionNotRun && o.Reason != "" {
			return false, ""
		}
	case AdmissionFailed:
	default:
		return false, failures.ClassSchemaInvalid
	}
	if o.FailureClass == "" {
		return false, failures.ClassDependencyUnavailable
	}
	return false, o.FailureClass
}

func (o AdmissionExecutionObligation) validIdentity() bool {
	return strings.TrimSpace(o.ID) != "" && strings.TrimSpace(o.Owner) != "" && strings.TrimSpace(o.Subject) != "" && strings.TrimSpace(o.Reason) != ""
}

func admissionExcludedWarning(id string, excluded []string) bool {
	for _, candidate := range excluded {
		if strings.TrimSpace(candidate) == id {
			return true
		}
	}
	return false
}

func admissionFailurePriority(class failures.Class) int {
	switch class {
	case failures.ClassSchemaInvalid:
		return 4
	case failures.ClassAuthenticationNeeded, failures.ClassAuthorizationDenied:
		return 3
	case failures.ClassLifecycleConflict, failures.ClassConflictingDuplicate:
		return 2
	default:
		return 1
	}
}
