package pipeline

import runtimesharedjson "empireai/internal/runtime/sharedjson"

const DefaultSystemNodeRetryLimit = 5

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
