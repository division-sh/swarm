package effects

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type completionRequestContextKey struct{}

func WithCompletionRequest(ctx context.Context, request CompletionRequest) context.Context {
	return context.WithValue(ctx, completionRequestContextKey{}, request)
}

// BindCompletionRequest freezes the authoritative frame or explicit fork request.
// It is shared by controller and selected-store admission, not a read fallback.
func BindCompletionRequest(authority Authority, req AuthorizeRequest) (Authority, error) {
	request := req.CompletionRequest
	if req.AgentFrame != nil {
		provider := req.AgentFrame.Session.Provider
		profile, err := llmselection.ResolveProviderContractProfile(provider.RuntimeMode, provider.Provider, provider.Transport)
		if err != nil {
			return authority, err
		}
		frameRequest := CompletionRequest{ModelAlias: provider.ModelAlias, RequestedModel: provider.Model,
			BackendProfile: profile.ID, Provider: provider.Provider, Transport: provider.Transport}
		if request != (CompletionRequest{}) && request != frameRequest {
			return authority, fmt.Errorf("completion request contradicts the admitted frame")
		}
		request = frameRequest
	}
	if err := request.Validate(); err != nil {
		return authority, err
	}
	authority.CompletionRequest = request
	return authority, nil
}

// CompletionRequest is request identity, not a provider execution observation.
type CompletionRequest struct {
	ModelAlias     string `json:"model_alias"`
	RequestedModel string `json:"requested_model"`
	BackendProfile string `json:"backend_profile"`
	Provider       string `json:"provider"`
	Transport      string `json:"transport"`
}

func (r CompletionRequest) Validate() error {
	for _, value := range []string{r.ModelAlias, r.RequestedModel, r.BackendProfile, r.Provider, r.Transport} {
		if strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
			return fmt.Errorf("completion request requires exact model and provider identity")
		}
	}
	profile, err := llmselection.ResolveActiveBackend(r.BackendProfile)
	if err != nil || profile.Provider != r.Provider || profile.Transport != r.Transport {
		return fmt.Errorf("completion request requires a canonical provider profile")
	}
	return nil
}

type CompletionModelObservation struct {
	Model                    string   `json:"model"`
	Source                   string   `json:"source"`
	InputTokens              *int64   `json:"input_tokens,omitempty"`
	OutputTokens             *int64   `json:"output_tokens,omitempty"`
	CacheReadInputTokens     *int64   `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens *int64   `json:"cache_creation_input_tokens,omitempty"`
	ProviderReportedCostUSD  *float64 `json:"provider_reported_cost_usd,omitempty"`
}

const (
	CompletionModelResponse    = "response.model"
	CompletionModelCLITerminal = "claude.result.model"
	CompletionModelCLIUsage    = "claude.result.modelUsage"
	CompletionCostReported     = "provider_reported"
	CompletionCostEstimated    = "locally_estimated"
	CompletionCostUnavailable  = "unavailable"
	CompletionCostReservation  = "reservation_exhaustion"
)

// CompletionEvidence is copied into the existing usage targets and spend row.
// Contributor ordering is stable; it never chooses a primary or served model.
type CompletionEvidence struct {
	Request                 CompletionRequest            `json:"request"`
	Models                  []CompletionModelObservation `json:"models"`
	CostProvenance          string                       `json:"cost_provenance"`
	ProviderReportedCostUSD *float64                     `json:"provider_reported_cost_usd,omitempty"`
	Problems                []CompletionEvidenceProblem  `json:"problems,omitempty"`
}

type CompletionEvidenceProblem struct {
	Source    string `json:"source"`
	Dimension string `json:"dimension"`
	Code      string `json:"code"`
}

// ResolvedModel projects an explicit terminal label or one unambiguous
// contributor. Multiple contributors never become a first-key selection.
func (e CompletionEvidence) ResolvedModel() string {
	var terminal, contributor string
	var terminalCount, contributorCount int
	for _, model := range e.Models {
		switch model.Source {
		case CompletionModelResponse, CompletionModelCLITerminal:
			terminal, terminalCount = model.Model, terminalCount+1
		case CompletionModelCLIUsage:
			contributor, contributorCount = model.Model, contributorCount+1
		}
	}
	if terminalCount == 1 {
		return terminal
	}
	if terminalCount == 0 && contributorCount == 1 {
		return contributor
	}
	return ""
}

func (e CompletionEvidence) Clone() CompletionEvidence {
	e.Models = slices.Clone(e.Models)
	e.Problems = slices.Clone(e.Problems)
	e.ProviderReportedCostUSD = cloneCompletionValue(e.ProviderReportedCostUSD)
	for i := range e.Models {
		m := &e.Models[i]
		m.InputTokens = cloneCompletionValue(m.InputTokens)
		m.OutputTokens = cloneCompletionValue(m.OutputTokens)
		m.CacheReadInputTokens = cloneCompletionValue(m.CacheReadInputTokens)
		m.CacheCreationInputTokens = cloneCompletionValue(m.CacheCreationInputTokens)
		m.ProviderReportedCostUSD = cloneCompletionValue(m.ProviderReportedCostUSD)
	}
	return e
}

func (e CompletionEvidence) Canonical() CompletionEvidence {
	e = e.Clone()
	if e.Models == nil {
		e.Models = []CompletionModelObservation{}
	}
	slices.SortFunc(e.Models, func(a, b CompletionModelObservation) int {
		if n := cmp.Compare(a.Source, b.Source); n != 0 {
			return n
		}
		return cmp.Compare(a.Model, b.Model)
	})
	slices.SortFunc(e.Problems, func(a, b CompletionEvidenceProblem) int {
		if n := cmp.Compare(a.Source, b.Source); n != 0 {
			return n
		}
		if n := cmp.Compare(a.Dimension, b.Dimension); n != 0 {
			return n
		}
		return cmp.Compare(a.Code, b.Code)
	})
	e.Problems = slices.Compact(e.Problems)
	return e
}

func (e CompletionEvidence) Validate() error {
	if err := e.Request.Validate(); err != nil {
		return err
	}
	if err := e.validatePrice(); err != nil {
		return err
	}
	if err := e.validateProblems(); err != nil {
		return err
	}
	return e.validateModels()
}

func (e CompletionEvidence) validatePrice() error {
	switch e.CostProvenance {
	case CompletionCostReported, CompletionCostEstimated, CompletionCostUnavailable, CompletionCostReservation:
	default:
		return fmt.Errorf("invalid completion cost provenance %q", e.CostProvenance)
	}
	if err := validateCompletionCost(e.ProviderReportedCostUSD); err != nil {
		return err
	}
	if e.CostProvenance == CompletionCostReported && e.ProviderReportedCostUSD == nil {
		return fmt.Errorf("reported cost provenance requires a reported total")
	}
	if e.ProviderReportedCostUSD != nil && e.CostProvenance != CompletionCostReported && e.CostProvenance != CompletionCostReservation {
		return fmt.Errorf("reported completion total contradicts price provenance")
	}
	if e.Request.BackendProfile == llmselection.BackendMock && (len(e.Models) != 0 || e.ProviderReportedCostUSD != nil || e.CostProvenance == CompletionCostReported) {
		return fmt.Errorf("mock completion cannot certify provider model or reported price")
	}
	return nil
}

func (e CompletionEvidence) validateProblems() error {
	for _, problem := range e.Problems {
		switch problem.Source {
		case CompletionModelResponse, CompletionModelCLITerminal, CompletionModelCLIUsage, "claude.result.usage", "claude.result.total_cost_usd":
		default:
			return fmt.Errorf("invalid completion evidence problem source")
		}
		if problem.Dimension != "model" && problem.Dimension != "tokens" && problem.Dimension != "cost" {
			return fmt.Errorf("invalid completion evidence problem dimension")
		}
		if problem.Code != "absent" && problem.Code != "invalid" {
			return fmt.Errorf("invalid completion evidence problem code")
		}
	}
	return nil
}

func (e CompletionEvidence) validateModels() error {
	seen := make(map[string]bool, len(e.Models))
	var terminalSource string
	for _, model := range e.Models {
		if strings.TrimSpace(model.Model) == "" || !utf8.ValidString(model.Model) {
			return fmt.Errorf("invalid provider-observed model label")
		}
		switch model.Source {
		case CompletionModelResponse, CompletionModelCLITerminal, CompletionModelCLIUsage:
		default:
			return fmt.Errorf("invalid provider model observation source %q", model.Source)
		}
		if model.Source != CompletionModelCLIUsage {
			if terminalSource != "" {
				return fmt.Errorf("multiple terminal provider model observations")
			}
			terminalSource = model.Source
		}
		key := model.Source + "\x00" + model.Model
		if (e.Request.BackendProfile == llmselection.BackendClaudeCLI) != (model.Source != CompletionModelResponse) {
			return fmt.Errorf("provider model source contradicts admitted adapter")
		}
		if seen[key] {
			return fmt.Errorf("duplicate provider model observation")
		}
		seen[key] = true
		for _, count := range []*int64{model.InputTokens, model.OutputTokens, model.CacheReadInputTokens, model.CacheCreationInputTokens} {
			if count != nil && *count < 0 {
				return fmt.Errorf("negative provider model usage")
			}
		}
		if err := validateCompletionCost(model.ProviderReportedCostUSD); err != nil {
			return err
		}
	}
	return nil
}

func DecodeCompletionEvidence(raw []byte) (CompletionEvidence, error) {
	var evidence CompletionEvidence
	if _, err := canonicaljson.Decode(raw); err != nil {
		return evidence, fmt.Errorf("decode completion evidence: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return evidence, fmt.Errorf("decode completion evidence: %w", err)
	}
	if evidence.Models == nil {
		return evidence, fmt.Errorf("completion model observations must be an array")
	}
	if err := evidence.Validate(); err != nil {
		return evidence, err
	}
	return evidence.Canonical(), nil
}

func (e CompletionEvidence) Encode() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e.Canonical())
}

func validateCompletionCost(value *float64) error {
	if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
		return fmt.Errorf("provider-reported completion cost must be finite and non-negative")
	}
	return nil
}

func cloneCompletionValue[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (u CompletionUsage) Clone() CompletionUsage {
	u.Evidence = u.Evidence.Clone()
	u.InputTokens = cloneCompletionValue(u.InputTokens)
	u.OutputTokens = cloneCompletionValue(u.OutputTokens)
	u.CacheReadInputTokens = cloneCompletionValue(u.CacheReadInputTokens)
	u.CacheCreationInputTokens = cloneCompletionValue(u.CacheCreationInputTokens)
	u.CacheCreation5mInputTokens = cloneCompletionValue(u.CacheCreation5mInputTokens)
	u.CacheCreation1hInputTokens = cloneCompletionValue(u.CacheCreation1hInputTokens)
	u.ProviderReportedCostUSD = cloneCompletionValue(u.ProviderReportedCostUSD)
	return u
}
