package llm

import (
	"encoding/json"
	"math"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func contextCompactionUsage(backend, method string, raw []byte) runtimeeffects.CompletionUsage {
	inputField, outputField := "input_tokens", "output_tokens"
	if backend == llmselection.BackendOpenAICompatible {
		inputField, outputField = "prompt_tokens", "completion_tokens"
	}
	usage := apiCompletionUsageFromRaw(raw, inputField, outputField)
	if method != "anthropic_native" {
		return usage
	}
	// On-demand Anthropic compaction bills usage.iterations; the outer token
	// counters can be zero. Keep the provider's actual model observation.
	var response struct {
		Usage struct {
			Iterations []json.RawMessage `json:"iterations"`
		} `json:"usage"`
	}
	canonical, parseErr := canonicaljson.Canonicalize(raw)
	valid := parseErr == nil && json.Unmarshal(canonical, &response) == nil && len(response.Usage.Iterations) != 0
	input, output, read, created := int64(0), int64(0), int64(0), int64(0)
	for _, rawIteration := range response.Usage.Iterations {
		var part struct {
			Input   *int64 `json:"input_tokens"`
			Output  *int64 `json:"output_tokens"`
			Read    int64  `json:"cache_read_input_tokens"`
			Created int64  `json:"cache_creation_input_tokens"`
		}
		if json.Unmarshal(rawIteration, &part) != nil || part.Input == nil || part.Output == nil || *part.Input < 0 || *part.Output < 0 || part.Read < 0 || part.Created < 0 {
			valid = false
			break
		}
		for _, value := range []int64{*part.Input, part.Read, part.Created} {
			if value > math.MaxInt64-input {
				valid = false
				break
			}
			input += value
		}
		if *part.Output > math.MaxInt64-output || part.Read > math.MaxInt64-read || part.Created > math.MaxInt64-created {
			valid = false
			break
		}
		output += *part.Output
		read += part.Read
		created += part.Created
	}
	if !valid {
		usage.Exactness, usage.InputTokens, usage.OutputTokens = runtimeeffects.CompletionUsageUnavailable, nil, nil
		completionObservationProblem(&usage.Evidence, "response.usage", "tokens", "invalid")
		return usage
	}
	usage.Exactness, usage.InputTokens, usage.OutputTokens = runtimeeffects.CompletionUsageExact, &input, &output
	usage.CacheReadInputTokens, usage.CacheCreationInputTokens = &read, &created
	return usage
}
