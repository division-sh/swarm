package cliapp

import (
	"fmt"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func validateCLICompletionEvidence(prefix string, evidence runtimeeffects.CompletionEvidence, resolved string) error {
	if err := evidence.Validate(); err != nil {
		return fmt.Errorf("malformed %s completion_evidence: %w", prefix, err)
	}
	if resolved != evidence.ResolvedModel() {
		return fmt.Errorf("malformed %s: resolved_model contradicts observations", prefix)
	}
	return nil
}

func cliCompletionEvidenceSummary(evidence runtimeeffects.CompletionEvidence) string {
	observations := make([]string, 0, len(evidence.Models))
	for _, model := range evidence.Models {
		observations = append(observations, cliSanitizeOneLineValue(model.Model)+" ["+model.Source+"]")
	}
	reported := strings.Join(observations, ", ")
	if reported == "" {
		reported = "unavailable"
	}
	return fmt.Sprintf("requested %s (%s); reported %s; cost %s", cliSanitizeOneLineValue(evidence.Request.RequestedModel), cliSanitizeOneLineValue(evidence.Request.ModelAlias), reported, evidence.CostProvenance)
}
