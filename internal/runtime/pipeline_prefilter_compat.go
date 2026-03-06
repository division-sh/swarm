package runtime

import runtimepipeline "empireai/internal/runtime/pipeline"

func evaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	return runtimepipeline.EvaluateDiscoveryPreFilterForTest(payload, rawSignal)
}

func buildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	return runtimepipeline.BuildPrefilterSkipDetailForTest(payload, rawSignal, adjustedSignal, reason, mode)
}

func cloneMap(in map[string]any) map[string]any {
	return runtimepipeline.CloneMapForTest(in)
}
