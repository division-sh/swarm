package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func resolvePipelineScanMode(bundle *runtimecontracts.WorkflowContractBundle, raw string) string {
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
	if bundle != nil {
		if pv, ok := bundle.MergedPolicy.Values["default_scan_mode"]; ok {
			if configured := strings.TrimSpace(asString(pv.Value)); configured != "" && mode == strings.ToLower(strings.TrimSpace(configured)) {
				return mode
			}
		}
		if pv, ok := bundle.Policy.Values["default_scan_mode"]; ok {
			if configured := strings.TrimSpace(asString(pv.Value)); configured != "" && mode == strings.ToLower(strings.TrimSpace(configured)) {
				return mode
			}
		}
	}
	return ""
}

func defaultPipelineScanMode(bundle *runtimecontracts.WorkflowContractBundle) string {
	if bundle != nil {
		if pv, ok := bundle.MergedPolicy.Values["default_scan_mode"]; ok {
			if mode := resolvePipelineScanMode(bundle, asString(pv.Value)); mode != "" {
				return mode
			}
		}
		if pv, ok := bundle.Policy.Values["default_scan_mode"]; ok {
			if mode := resolvePipelineScanMode(bundle, asString(pv.Value)); mode != "" {
				return mode
			}
		}
	}
	if module := defaultWorkflowModuleOrNil(); module != nil {
		if pv, ok := module.ContractBundle().MergedPolicy.Values["default_scan_mode"]; ok {
			if mode := resolvePipelineScanMode(module.ContractBundle(), asString(pv.Value)); mode != "" {
				return mode
			}
		}
	}
	return pipelineModeName("saas", "gap")
}

func pipelineModeName(parts ...string) string {
	return strings.Join(parts, "_")
}
