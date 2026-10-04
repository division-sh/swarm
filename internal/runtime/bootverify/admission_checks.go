package bootverify

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const registeredAdmissionOwner = "internal/runtime/bootverify"

// BlockRegisteredChecks records the existing registry's unperformed clauses
// when an upstream admission prerequisite prevents registry execution.
func (r *Report) BlockRegisteredChecks(subject, dependency, reason string) {
	if r.SourceArtifactHash != "" {
		subject = "source:" + r.SourceArtifactHash
	}
	appendCheck := func(check Check) {
		observation := AdmissionObservation{
			CheckID: check.ID, Owner: registeredAdmissionOwner, Subject: subject,
			Class: AdmissionSourceObservation, Status: AdmissionNotRun,
			Reason: reason, Dependencies: []string{dependency},
		}
		if check.Mode == CheckDeployment {
			observation.Class = AdmissionDeploymentObservation
		}
		r.Observations = append(r.Observations, observation)
		if check.Mode == CheckMixed {
			observation.Class = AdmissionDeploymentObservation
			r.Observations = append(r.Observations, observation)
		}
	}
	for _, check := range bootCheckRegistry {
		appendCheck(check)
	}
	for _, check := range supplementalChecks {
		appendCheck(check)
	}
}

func (r *Report) runAdmissionCheck(c *checkerContext, check Check) {
	observation := AdmissionObservation{
		CheckID: check.ID, Owner: registeredAdmissionOwner, Subject: "selected_source",
		Class: AdmissionSourceObservation, Status: AdmissionPassed,
	}
	if r.SourceArtifactHash != "" {
		observation.Subject = "source:" + r.SourceArtifactHash
	}
	if check.Mode == CheckDeployment {
		observation.Class = AdmissionDeploymentObservation
	}
	if check.Mode < CheckSource || check.Mode > CheckMixed || check.Run == nil {
		observation.Status, observation.Reason = AdmissionFailed, "check has no admission classification or implementation"
		observation.FailureClass = failures.ClassSchemaInvalid
		r.Add(NewHardInvalidityFinding(check.ID, "global", observation.Reason, "Classify and implement the check in the existing registry."))
		r.Observations = append(r.Observations, observation)
		return
	}
	if err := c.ctx.Err(); err != nil {
		observation.Status, observation.Reason = AdmissionNotRun, "validation context ended before this check"
		observation.FailureClass = failures.ClassDependencyUnavailable
		r.Interrupted = r.Interrupted || errors.Is(err, context.Canceled)
		r.Observations = append(r.Observations, observation)
		if check.Mode == CheckMixed {
			observation.Class = AdmissionDeploymentObservation
			r.Observations = append(r.Observations, observation)
		}
		return
	}
	if check.Mode == CheckDeployment && c.opts.Purpose == StructuralValidation {
		observation.Status, observation.Reason = AdmissionNotRun, "portable validation does not observe deployment prerequisites"
		r.Observations = append(r.Observations, observation)
		return
	}
	observation.StartedAt = time.Now().UTC()
	findings := check.Run(c)
	observation.FinishedAt = time.Now().UTC()
	for _, finding := range findings {
		r.Add(finding)
		if normalizeFindingSeverity(finding.Severity) == SeverityHardInvalidity {
			observation.Status, observation.Reason = AdmissionFailed, "canonical check rejected the selected subject; see findings"
			observation.FailureClass = finding.FailureClass
			if observation.FailureClass == "" {
				observation.FailureClass = failures.ClassSchemaInvalid
			}
		}
	}
	if check.Mode == CheckDeployment {
		observation = c.deploymentObservation(check.ID, observation)
	}
	r.Observations = append(r.Observations, observation)
	if check.Mode == CheckMixed {
		deployment := observation
		deployment.Class = AdmissionDeploymentObservation
		deployment.Status, deployment.Reason, deployment.FailureClass = AdmissionPassed, "", ""
		if c.opts.Purpose == StructuralValidation {
			deployment.Status, deployment.Reason = AdmissionNotRun, "portable validation checks declarations, not concrete deployment fulfillment"
			deployment.StartedAt, deployment.FinishedAt = time.Time{}, time.Time{}
		} else {
			deployment = c.deploymentObservation(check.ID, deployment)
		}
		r.Observations = append(r.Observations, deployment)
	}
	if err := c.ctx.Err(); err != nil {
		r.Interrupted = r.Interrupted || errors.Is(err, context.Canceled)
	}
}

func (c *checkerContext) deploymentObservation(id string, observation AdmissionObservation) AdmissionObservation {
	switch id {
	case "invalid_field_detection":
		if len(semanticview.AgentDeclarations(c.source)) == 0 {
			observation.Status, observation.Reason = AdmissionNotApplicable, "the selected source declares no agent model to resolve"
		} else if !c.opts.ValidateModelResolution {
			observation.Status, observation.Reason = AdmissionNotRun, "concrete model resolution is disabled for this validation"
			observation.FailureClass = failures.ClassDependencyUnavailable
		}
	case "tool_resolution", "platform_tool_usage_hints", "required_mcp_tool_availability", "mcp_server_reachable":
		if c.mcpObservation != nil {
			dependency := *c.mcpObservation
			observation.StartedAt, observation.FinishedAt = dependency.StartedAt, dependency.FinishedAt
			if dependency.Status != AdmissionPassed {
				observation.Status, observation.Reason, observation.FailureClass = dependency.Status, dependency.Reason, dependency.FailureClass
			}
			if id != "mcp_server_reachable" {
				observation.Dependencies = []string{"mcp_server_reachable"}
			}
		}
	case "credential_key_exists":
		if c.credentialObservation != nil {
			dependency := *c.credentialObservation
			observation.StartedAt, observation.FinishedAt = dependency.StartedAt, dependency.FinishedAt
			if dependency.Status != AdmissionPassed {
				observation.Status, observation.Reason, observation.FailureClass = dependency.Status, dependency.Reason, dependency.FailureClass
			}
		}
	}
	return observation
}
