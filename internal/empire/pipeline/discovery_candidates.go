package empire

import (
	"strconv"
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func BuildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []runtimepipeline.DiscoveryCandidate {
	baseMode := strings.TrimSpace(discoveryPayloadString(payload["mode"]))
	if baseMode == "" {
		baseMode = strings.TrimSpace(scanMode)
	}
	baseMode = strings.TrimSpace(runtimepipeline.NormalizeScanMode(baseMode))
	if baseMode == "" {
		baseMode = "saas_gap"
	}
	basePayload := clonePayload(payload)
	basePayload["mode"] = baseMode
	candidates := []runtimepipeline.DiscoveryCandidate{{
		Mode:    baseMode,
		Signal:  discoveryPayloadFloat(basePayload["signal_strength"]),
		Payload: basePayload,
	}}

	autoRaw, _ := payload["automation_micro"].(map[string]any)
	if len(autoRaw) == 0 {
		return candidates
	}

	autoPayload := clonePayload(payload)
	delete(autoPayload, "vertical_name")
	delete(autoPayload, "name")
	delete(autoPayload, "title")
	autoPayload["mode"] = "automation_micro"
	autoPayload["automation_micro"] = autoRaw
	autoPayload["signal_strength"] = autoRaw["signal_strength"]
	if v := strings.TrimSpace(discoveryPayloadString(autoRaw["opportunity_hypothesis"])); v != "" {
		autoPayload["opportunity_hypothesis"] = v
	}
	if v := strings.TrimSpace(discoveryPayloadString(autoRaw["evidence"])); v != "" {
		autoPayload["evidence"] = v
	}
	candidates = append(candidates, runtimepipeline.DiscoveryCandidate{
		Mode:    "automation_micro",
		Signal:  discoveryPayloadFloat(autoRaw["signal_strength"]),
		Payload: autoPayload,
	})
	return candidates
}

func clonePayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func discoveryPayloadString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return ""
	}
}

func discoveryPayloadFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return f
		}
	}
	return 0
}
