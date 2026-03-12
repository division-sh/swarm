package pipeline

import (
	"strings"

	"empireai/internal/runtime/semanticview"
)

const scanModePolicyFlowID = "discovery"

func resolvePipelineScanMode(source semanticview.Source, raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return ""
	}
	mode = strings.ReplaceAll(mode, "-", "_")
	mode = strings.Join(strings.Fields(mode), "_")
	switch mode {
	case pipelineModeName("automation", "micro"), pipelineModeName("local", "services"), pipelineModeName("saas", "gap"), pipelineModeName("saas", "trend"), "corpus", "derived":
		return mode
	case pipelineModeName("local", "underserved"), "local", pipelineModeName("local", "service"), "services":
		return pipelineModeName("local", "services")
	case "discovery", "scan", "default", "automation", "micro", "saas":
		return pipelineModeName("saas", "gap")
	case "trend", pipelineModeName("trend", "scan"), pipelineModeName("saas", "trend", "scan"), pipelineModeName("trend", "opportunity"), pipelineModeName("adjacent", "opportunity"):
		return pipelineModeName("saas", "trend")
	case pipelineModeName("corpus", "mode"), pipelineModeName("signal", "corpus"):
		return "corpus"
	}
	if source != nil {
		if value, ok := scanModePolicyValue(source, "default_scan_mode"); ok {
			if configured := strings.TrimSpace(asString(value)); configured != "" && mode == strings.ToLower(strings.TrimSpace(configured)) {
				return mode
			}
		}
	}
	return ""
}

func defaultPipelineScanMode(source semanticview.Source) string {
	if source != nil {
		if value, ok := scanModePolicyValue(source, "default_scan_mode"); ok {
			if mode := resolvePipelineScanMode(source, asString(value)); mode != "" {
				return mode
			}
		}
	}
	if module := defaultWorkflowModuleOrNil(); module != nil {
		source := module.SemanticSource()
		if value, ok := scanModePolicyValue(source, "default_scan_mode"); ok {
			if mode := resolvePipelineScanMode(source, asString(value)); mode != "" {
				return mode
			}
		}
	}
	return pipelineModeName("saas", "gap")
}

func scanModePolicyValue(source semanticview.Source, key string) (any, bool) {
	if pv, ok := semanticview.PolicyValueForFlow(source, scanModePolicyFlowID, key); ok {
		return pv.Value, true
	}
	if pv, ok := semanticview.PolicyValueForFlow(source, "", key); ok {
		return pv.Value, true
	}
	return nil, false
}

func pipelineModeName(parts ...string) string {
	return strings.Join(parts, "_")
}
