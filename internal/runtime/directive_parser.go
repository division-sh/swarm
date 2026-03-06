package runtime

import (
	"strconv"
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

type PriceRange = runtimepipeline.PriceRange

type ParsedDirective = runtimepipeline.ParsedDirective

type DirectiveParser = runtimepipeline.DirectiveParser

func parseDirectiveMode(text string) (string, bool) {
	return runtimepipeline.ParseDirectiveMode(text)
}

func remainingCampaignModes(initialMode string) []string {
	return runtimepipeline.RemainingCampaignModes(initialMode)
}

func campaignModesForDirective(initialMode string, explicit bool) []string {
	return runtimepipeline.CampaignModesForDirective(initialMode, explicit)
}

func extractCorpusPathFromStrategicContext(strategic map[string]any) string {
	return runtimepipeline.ExtractCorpusPathFromStrategicContext(strategic)
}

func isComplexDirectiveText(text string) bool {
	return runtimepipeline.IsComplexDirectiveText(text)
}

func parseDirectiveGeography(text string) (name, country, region string) {
	return runtimepipeline.ParseDirectiveGeography(text)
}

func sanitizeGeographyPhrase(v string) string {
	return runtimepipeline.SanitizeGeographyPhrase(v)
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		if n, err := strconv.Atoi(t); err == nil {
			return n
		}
	}
	return 0
}
