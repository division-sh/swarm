package pipeline

import (
	"strconv"
	"strings"
)

func RemainingCampaignModes(initialMode string) []string {
	modes := campaignModesForDirectiveCompat(initialMode, false)
	if len(modes) <= 1 {
		return []string{}
	}
	return append([]string(nil), modes[1:]...)
}

func CampaignModesForDirective(initialMode string, explicit bool) []string {
	modes := campaignModesForDirectiveCompat(initialMode, explicit)
	return append([]string(nil), modes...)
}

func ParseDirectiveMode(text string) (mode string, explicit bool) {
	return parseDirectiveModeCompat(text)
}

func normalizeCampaignScanMode(raw string) string {
	return resolvePipelineScanMode(nil, raw)
}

func campaignModesForDirectiveCompat(initialMode string, explicit bool) []string {
	initialMode = normalizeCampaignScanMode(initialMode)
	if initialMode == "" {
		initialMode = pipelineModeName("saas", "gap")
	}
	if explicit {
		return []string{initialMode}
	}
	if initialMode == "corpus" {
		return []string{}
	}
	cycle := []string{
		pipelineModeName("saas", "gap"),
		pipelineModeName("saas", "trend"),
		pipelineModeName("local", "services"),
	}
	idx := 0
	for i, mode := range cycle {
		if mode == initialMode {
			idx = i
			break
		}
	}
	out := []string{initialMode}
	for i := idx + 1; i < len(cycle); i++ {
		out = append(out, cycle[i])
	}
	return out
}

func parseDirectiveModeCompat(text string) (mode string, explicit bool) {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return pipelineModeName("saas", "gap"), false
	}
	switch {
	case strings.Contains(t, "corpus_path"),
		strings.Contains(t, " mode corpus"),
		strings.HasPrefix(t, "corpus"),
		strings.Contains(t, ".jsonl"),
		strings.Contains(t, ", corpus"),
		strings.Contains(t, " corpus "):
		return "corpus", true
	case strings.Contains(t, "automation_micro"),
		(strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return pipelineModeName("saas", "gap"), true
	case strings.Contains(t, pipelineModeName("local", "services")),
		strings.Contains(t, "local service"):
		return pipelineModeName("local", "services"), true
	case strings.Contains(t, pipelineModeName("saas", "trend")),
		(strings.Contains(t, "saas") && strings.Contains(t, "trend")):
		return pipelineModeName("saas", "trend"), true
	case strings.Contains(t, pipelineModeName("saas", "gap")),
		strings.Contains(t, "gap scan"):
		return pipelineModeName("saas", "gap"), true
	default:
		return pipelineModeName("saas", "gap"), false
	}
}

func asString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}
