package pipeline

import empirepipeline "empireai/internal/runtime/pipeline/empire"

func evaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	return empirepipeline.EvaluateDiscoveryPreFilter(payload, rawSignal)
}

func buildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	return empirepipeline.BuildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode)
}
