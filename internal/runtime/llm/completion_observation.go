package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type claudeCompletionResult struct {
	Type       string          `json:"type"`
	Model      json.RawMessage `json:"model"`
	ModelUsage json.RawMessage `json:"modelUsage"`
	Usage      json.RawMessage `json:"usage"`
	Cost       json.RawMessage `json:"total_cost_usd"`
	duplicates map[string]bool
}

// Each dimension is admitted separately: a malformed price cannot erase valid
// token totals, and missing tokens cannot erase an observed model or price.
func claudeCompletionUsageFromRaw(raw []byte) (runtimeeffects.CompletionUsage, error) {
	var terminal claudeCompletionResult
	found := false
	decode := func(raw []byte) bool {
		result, err := decodeClaudeCompletionResult(raw)
		if err != nil || strings.ToLower(strings.TrimSpace(result.Type)) != "result" {
			return false
		}
		terminal, found = result, true
		return true
	}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		decode(line)
	}
	if !found {
		decode(raw)
	}
	usage := unavailableCompletionUsage()
	if !found {
		return usage, fmt.Errorf("claude ResultMessage missing exact usage")
	}
	modelErr := observeClaudeTerminalModel(&usage.Evidence, terminal.Model)
	contributorErr := observeClaudeContributors(&usage.Evidence, terminal.ModelUsage)
	tokenErr := observeClaudeTokens(&usage, terminal.Usage)
	cost, costErr := completionObservedCost(terminal.Cost)
	if terminal.duplicates["model"] {
		usage.Evidence.Models = removeCompletionObservationSource(usage.Evidence.Models, runtimeeffects.CompletionModelCLITerminal)
		completionObservationProblem(&usage.Evidence, runtimeeffects.CompletionModelCLITerminal, "model", "invalid")
		modelErr = fmt.Errorf("duplicate claude terminal model")
	}
	if terminal.duplicates["modelUsage"] {
		usage.Evidence.Models = removeCompletionObservationSource(usage.Evidence.Models, runtimeeffects.CompletionModelCLIUsage)
		completionObservationProblem(&usage.Evidence, runtimeeffects.CompletionModelCLIUsage, "model", "invalid")
		contributorErr = fmt.Errorf("duplicate claude modelUsage")
	}
	if terminal.duplicates["usage"] {
		evidence := usage.Evidence
		usage = runtimeeffects.CompletionUsage{Evidence: evidence, Exactness: runtimeeffects.CompletionUsageUnavailable}
		completionObservationProblem(&usage.Evidence, "claude.result.usage", "tokens", "invalid")
		tokenErr = fmt.Errorf("duplicate claude terminal usage")
	}
	if terminal.duplicates["total_cost_usd"] {
		cost, costErr = nil, fmt.Errorf("duplicate claude terminal cost")
	}
	if costErr != nil {
		completionObservationProblem(&usage.Evidence, "claude.result.total_cost_usd", "cost", "invalid")
	}
	usage.ProviderReportedCostUSD = cost
	usage.Evidence = usage.Evidence.Canonical()
	usage.ResolvedModel = usage.Evidence.ResolvedModel()
	return usage.Clone(), errors.Join(modelErr, contributorErr, tokenErr, costErr)
}

func removeCompletionObservationSource(models []runtimeeffects.CompletionModelObservation, source string) []runtimeeffects.CompletionModelObservation {
	result := models[:0]
	for _, model := range models {
		if model.Source != source {
			result = append(result, model)
		}
	}
	return result
}

func decodeClaudeCompletionResult(raw []byte) (claudeCompletionResult, error) {
	var result claudeCompletionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return result, fmt.Errorf("claude result must be an object")
	}
	seen := map[string]bool{}
	result.duplicates = map[string]bool{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return result, err
		}
		name, ok := key.(string)
		if !ok {
			return result, fmt.Errorf("claude result field must be a string")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return result, err
		}
		if seen[name] {
			result.duplicates[name] = true
		}
		seen[name] = true
	}
	if result.duplicates["type"] {
		return result, fmt.Errorf("duplicate claude result type")
	}
	return result, nil
}

func completionObservationProblem(evidence *runtimeeffects.CompletionEvidence, source, dimension, code string) {
	evidence.Problems = append(evidence.Problems, runtimeeffects.CompletionEvidenceProblem{Source: source, Dimension: dimension, Code: code})
}

func completionObservationDecode(raw []byte, target any) error {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func observeClaudeTerminalModel(evidence *runtimeeffects.CompletionEvidence, raw json.RawMessage) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var model string
	if err := completionObservationDecode(raw, &model); err != nil {
		completionObservationProblem(evidence, runtimeeffects.CompletionModelCLITerminal, "model", "invalid")
		return fmt.Errorf("claude terminal model: %w", err)
	}
	if model == "" {
		return nil
	}
	if strings.TrimSpace(model) == "" {
		completionObservationProblem(evidence, runtimeeffects.CompletionModelCLITerminal, "model", "invalid")
		return fmt.Errorf("claude terminal model is blank")
	}
	evidence.Models = append(evidence.Models, runtimeeffects.CompletionModelObservation{Model: model, Source: runtimeeffects.CompletionModelCLITerminal})
	return nil
}

func observeClaudeContributors(evidence *runtimeeffects.CompletionEvidence, raw json.RawMessage) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var contributors map[string]json.RawMessage
	if err := completionObservationDecode(raw, &contributors); err != nil {
		completionObservationProblem(evidence, runtimeeffects.CompletionModelCLIUsage, "model", "invalid")
		return fmt.Errorf("claude modelUsage: %w", err)
	}
	var errs []error
	for model, raw := range contributors {
		if strings.TrimSpace(model) == "" {
			completionObservationProblem(evidence, runtimeeffects.CompletionModelCLIUsage, "model", "invalid")
			errs = append(errs, fmt.Errorf("claude modelUsage has a blank label"))
			continue
		}
		observation, err := observeClaudeContributor(model, raw, evidence)
		evidence.Models = append(evidence.Models, observation)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func observeClaudeContributor(model string, raw json.RawMessage, evidence *runtimeeffects.CompletionEvidence) (runtimeeffects.CompletionModelObservation, error) {
	observation := runtimeeffects.CompletionModelObservation{Model: model, Source: runtimeeffects.CompletionModelCLIUsage}
	var fields map[string]json.RawMessage
	if err := completionObservationDecode(raw, &fields); err != nil {
		completionObservationProblem(evidence, observation.Source, "tokens", "invalid")
		completionObservationProblem(evidence, observation.Source, "cost", "invalid")
		return observation, err
	}
	var errs []error
	for _, field := range []struct {
		name   string
		target **int64
	}{
		{"inputTokens", &observation.InputTokens}, {"outputTokens", &observation.OutputTokens},
		{"cacheReadInputTokens", &observation.CacheReadInputTokens}, {"cacheCreationInputTokens", &observation.CacheCreationInputTokens},
	} {
		value := fields[field.name]
		if len(value) == 0 {
			continue
		}
		var count *int64
		err := completionObservationDecode(value, &count)
		if err == nil && count != nil && *count < 0 {
			err = fmt.Errorf("negative %s", field.name)
		}
		if err != nil {
			completionObservationProblem(evidence, observation.Source, "tokens", "invalid")
			errs = append(errs, err)
			continue
		}
		*field.target = count
	}
	cost, err := completionObservedCost(fields["costUSD"])
	if err != nil {
		completionObservationProblem(evidence, observation.Source, "cost", "invalid")
	}
	observation.ProviderReportedCostUSD = cost
	return observation, errors.Join(append(errs, err)...)
}

func completionObservedCost(raw []byte) (*float64, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var cost *float64
	if err := completionObservationDecode(raw, &cost); err != nil {
		return nil, fmt.Errorf("invalid provider cost: %w", err)
	}
	if cost != nil && (*cost < 0 || math.IsNaN(*cost) || math.IsInf(*cost, 0)) {
		return nil, fmt.Errorf("provider cost must be finite and non-negative")
	}
	return cost, nil
}

func observeClaudeTokens(usage *runtimeeffects.CompletionUsage, raw []byte) error {
	var tokens struct {
		Input    *int64 `json:"input_tokens"`
		Output   *int64 `json:"output_tokens"`
		Read     *int64 `json:"cache_read_input_tokens"`
		Creation *int64 `json:"cache_creation_input_tokens"`
		Cache    struct {
			FiveMinutes *int64 `json:"ephemeral_5m_input_tokens"`
			OneHour     *int64 `json:"ephemeral_1h_input_tokens"`
		} `json:"cache_creation"`
	}
	err := completionObservationDecode(raw, &tokens)
	code := "invalid"
	if err == nil && (tokens.Input == nil || tokens.Output == nil) {
		err, code = fmt.Errorf("claude ResultMessage missing exact usage"), "absent"
	}
	if err == nil {
		candidate := runtimeeffects.CompletionUsage{Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: tokens.Input, OutputTokens: tokens.Output, CacheReadInputTokens: tokens.Read, CacheCreationInputTokens: tokens.Creation,
			CacheCreation5mInputTokens: tokens.Cache.FiveMinutes, CacheCreation1hInputTokens: tokens.Cache.OneHour}
		if err = candidate.Validate(); err == nil {
			candidate.Evidence = usage.Evidence
			*usage = candidate
			return nil
		}
	}
	completionObservationProblem(&usage.Evidence, "claude.result.usage", "tokens", code)
	return err
}
