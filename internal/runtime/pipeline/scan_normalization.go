package pipeline

import runtimescanmode "empireai/internal/runtime/scanmode"

func NormalizeScanMode(raw string) string {
	return runtimescanmode.NormalizeMode(raw)
}

func NormalizeScanPriority(raw string) string {
	return runtimescanmode.NormalizePriority(raw)
}
