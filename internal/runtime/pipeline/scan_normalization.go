package pipeline

import runtimeproductpolicy "empireai/internal/runtime/productpolicy"

func NormalizeScanMode(raw string) string {
	return runtimeproductpolicy.NormalizeScanMode(raw)
}

func NormalizeScanPriority(raw string) string {
	return runtimeproductpolicy.NormalizeScanPriority(raw)
}
