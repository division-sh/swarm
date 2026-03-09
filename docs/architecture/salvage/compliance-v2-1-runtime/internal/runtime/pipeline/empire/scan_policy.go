//go:build ignore

package empire

import (
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func (module) NormalizeMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return strings.ToLower(strings.TrimSpace(raw))
	case "local_underserved":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func (module) NormalizePriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "normal", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func (module) DefaultMode() string {
	return "saas_gap"
}

func (m module) ExpectedAgents(mode string) int {
	switch m.NormalizeMode(mode) {
	case "automation_micro", "saas_gap", "saas_trend", "corpus":
		return 1
	case "local_services":
		return 5
	default:
		return 1
	}
}

func (m module) PublishTargets(mode string) []string {
	switch m.NormalizeMode(mode) {
	case "saas_gap", "corpus", "automation_micro", "derived", "":
		return []string{"market_research.scan_assigned"}
	case "saas_trend":
		return []string{"trend_research.scan_assigned"}
	case "local_services":
		return []string{
			"scanner.google_maps.scan_assigned",
			"scanner.instagram.scan_assigned",
			"scanner.reviews.scan_assigned",
			"scanner.directories.scan_assigned",
			"scanner.yelp.scan_assigned",
		}
	default:
		return []string{"market_research.scan_assigned"}
	}
}

func (m module) ShardStage(mode string) string {
	switch m.NormalizeMode(mode) {
	case "saas_gap":
		return runtimepipeline.ShardStageMarketResearch
	case "saas_trend":
		return runtimepipeline.ShardStageTrendResearch
	default:
		return ""
	}
}

func (m module) BuildDiscoveryCandidates(scanMode string, payload map[string]any) []runtimepipeline.DiscoveryCandidateSpec {
	baseMode := m.NormalizeMode(firstNonEmptyString(asString(payload["mode"]), scanMode))
	if baseMode == "" {
		baseMode = m.DefaultMode()
	}
	basePayload := cloneMap(payload)
	basePayload["mode"] = baseMode
	candidates := []runtimepipeline.DiscoveryCandidateSpec{{
		Mode:    baseMode,
		Signal:  asFloat(basePayload["signal_strength"]),
		Payload: basePayload,
	}}

	autoRaw, _ := payload["automation_micro"].(map[string]any)
	if len(autoRaw) == 0 {
		return candidates
	}

	autoPayload := cloneMap(payload)
	delete(autoPayload, "vertical_name")
	delete(autoPayload, "name")
	delete(autoPayload, "title")
	autoPayload["mode"] = "automation_micro"
	autoPayload["automation_micro"] = autoRaw
	autoPayload["signal_strength"] = autoRaw["signal_strength"]
	if v := strings.TrimSpace(asString(autoRaw["opportunity_hypothesis"])); v != "" {
		autoPayload["opportunity_hypothesis"] = v
	}
	if v := strings.TrimSpace(asString(autoRaw["evidence"])); v != "" {
		autoPayload["evidence"] = v
	}
	candidates = append(candidates, runtimepipeline.DiscoveryCandidateSpec{
		Mode:    "automation_micro",
		Signal:  asFloat(autoRaw["signal_strength"]),
		Payload: autoPayload,
	})
	return candidates
}

func (m module) RemainingCampaignModes(initialMode string) []string {
	cycle := []string{"saas_gap", "saas_trend", "local_services"}
	initialMode = m.NormalizeMode(initialMode)
	if initialMode == "corpus" {
		return []string{}
	}
	if initialMode == "" {
		initialMode = m.DefaultMode()
	}
	idx := 0
	for i, mode := range cycle {
		if mode == initialMode {
			idx = i
			break
		}
	}
	out := make([]string, 0, len(cycle)-1)
	for i := idx + 1; i < len(cycle); i++ {
		out = append(out, cycle[i])
	}
	return out
}

func (m module) CampaignModesForDirective(initialMode string, explicit bool) []string {
	initialMode = m.NormalizeMode(initialMode)
	if initialMode == "" {
		initialMode = m.DefaultMode()
	}
	if explicit {
		return []string{initialMode}
	}
	modes := []string{initialMode}
	return append(modes, m.RemainingCampaignModes(initialMode)...)
}

func (m module) ParseDirectiveMode(text string) (mode string, explicit bool) {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return m.DefaultMode(), false
	}
	switch {
	case strings.Contains(t, "corpus_path"), strings.Contains(t, " mode corpus"), strings.HasPrefix(t, "corpus"), strings.Contains(t, ".jsonl"), strings.Contains(t, ", corpus"), strings.Contains(t, " corpus "):
		return "corpus", true
	case strings.Contains(t, "automation_micro"), (strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap", true
	case strings.Contains(t, "local_services"), strings.Contains(t, "local service"):
		return "local_services", true
	case strings.Contains(t, "saas_trend"), (strings.Contains(t, "saas") && strings.Contains(t, "trend")):
		return "saas_trend", true
	case strings.Contains(t, "saas_gap"), strings.Contains(t, "gap scan"):
		return "saas_gap", true
	default:
		return m.DefaultMode(), false
	}
}

func (module) IsComplexDirectiveText(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return false
	}
	complexHints := []string{"latam", "across", "countries", "country with", "internet penetration", "focus on", "exclude", "excluding", "greater than", "less than", ">", "<", "compared to"}
	for _, hint := range complexHints {
		if strings.Contains(t, hint) {
			return true
		}
	}
	return false
}

func (module) ComplexDirectiveRecipients() []string {
	return []string{"empire-coordinator"}
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}
