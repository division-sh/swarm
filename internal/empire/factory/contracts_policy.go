package factory

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

type factoryContractPolicy struct {
	recipientsByEvent map[string][]string
	scanModeCases     []factoryScanModeCase
	scanModeSet       map[string]struct{}
	defaultScanMode   string
	rubricByMode      map[string]string
	categoryByMode    map[string]bool
	trendByMode       map[string]bool
}

type factoryScanModeCase struct {
	Mode           string   `yaml:"mode"`
	RemainingModes []string `yaml:"remaining_modes"`
}

var (
	factoryPolicyOnce sync.Once
	factoryPolicyData factoryContractPolicy
)

func deliveryRecipientsForEvent(eventType string) []string {
	policy := loadFactoryContractPolicy()
	if len(policy.recipientsByEvent) == 0 {
		return nil
	}
	recipients := policy.recipientsByEvent[strings.TrimSpace(eventType)]
	return append([]string(nil), recipients...)
}

func loadFactoryContractPolicy() factoryContractPolicy {
	factoryPolicyOnce.Do(func() {
		policy, err := buildFactoryContractPolicy()
		if err == nil {
			factoryPolicyData = policy
		}
	})
	return factoryPolicyData
}

func buildFactoryContractPolicy() (factoryContractPolicy, error) {
	repoRoot, err := factoryRepoRoot()
	if err != nil {
		return factoryContractPolicy{}, err
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(repoRoot)
	if err != nil {
		return factoryContractPolicy{}, err
	}
	source := semanticview.Wrap(bundle)
	policy := factoryContractPolicy{
		recipientsByEvent: map[string][]string{},
		scanModeSet:       map[string]struct{}{},
		rubricByMode:      map[string]string{},
		categoryByMode:    map[string]bool{},
		trendByMode:       map[string]bool{},
	}
	for eventType, entry := range source.EventEntries() {
		recipients := contractAgentRecipients(entry.ConsumerType, entry.Consumer)
		if len(recipients) == 0 {
			continue
		}
		policy.recipientsByEvent[strings.TrimSpace(eventType)] = recipients
	}
	if err := loadFactoryScanModes(source, &policy); err != nil {
		return factoryContractPolicy{}, err
	}
	return policy, nil
}

func loadFactoryScanModes(source semanticview.Source, policy *factoryContractPolicy) error {
	if policy == nil {
		return nil
	}
	for _, mode := range []string{"local_services", "saas_gap", "saas_trend", "corpus"} {
		rubric, ok := factoryBundlePolicyString(source, "scan_modes."+mode+".rubric")
		if !ok {
			continue
		}
		policy.scanModeCases = append(policy.scanModeCases, factoryScanModeCase{Mode: mode})
		policy.scanModeSet[mode] = struct{}{}
		policy.rubricByMode[mode] = rubric
		if value, ok := factoryBundlePolicyBool(source, "scan_modes."+mode+".emits_category_signals"); ok {
			policy.categoryByMode[mode] = value
		}
		if value, ok := factoryBundlePolicyBool(source, "scan_modes."+mode+".emits_trend_signals"); ok {
			policy.trendByMode[mode] = value
		}
	}
	if _, ok := policy.scanModeSet["saas_gap"]; ok {
		policy.scanModeSet["automation_micro"] = struct{}{}
		policy.scanModeSet["derived"] = struct{}{}
		if rubric := strings.TrimSpace(policy.rubricByMode["saas_gap"]); rubric != "" {
			policy.rubricByMode["automation_micro"] = rubric
			policy.rubricByMode["derived"] = rubric
		}
		if value, ok := policy.categoryByMode["saas_gap"]; ok {
			policy.categoryByMode["automation_micro"] = value
			policy.categoryByMode["derived"] = value
		}
		if value, ok := policy.trendByMode["saas_gap"]; ok {
			policy.trendByMode["automation_micro"] = value
			policy.trendByMode["derived"] = value
		}
	}
	if value, ok := factoryBundlePolicyString(source, "default_scan_mode"); ok {
		policy.defaultScanMode = strings.TrimSpace(normalizeFactoryPolicyScanMode(value))
	}
	if policy.defaultScanMode == "" && len(policy.scanModeCases) > 0 {
		policy.defaultScanMode = strings.TrimSpace(policy.scanModeCases[0].Mode)
	}
	return nil
}

func contractAgentRecipients(consumerType any, consumer any) []string {
	if strings.TrimSpace(strings.ToLower(asContractString(consumerType))) != "agent" {
		return nil
	}
	switch typed := consumer.(type) {
	case string:
		if recipient := normalizeContractRecipient(typed); recipient != "" {
			return []string{recipient}
		}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if recipient := normalizeContractRecipient(asContractString(item)); recipient != "" {
				out = append(out, recipient)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

func normalizeContractRecipient(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, " (),+") {
		return ""
	}
	return value
}

func asContractString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

func factoryRepoRoot() (string, error) {
	if _, file, _, ok := runtime.Caller(0); ok {
		dir := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
		if _, err := os.Stat(filepath.Join(dir, "contracts")); err == nil {
			return dir, nil
		}
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "contracts")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
	}
	return "", os.ErrNotExist
}

func normalizeFactoryScanMode(mode string) string {
	policy := loadFactoryContractPolicy()
	mode = strings.TrimSpace(normalizeFactoryPolicyScanMode(mode))
	if _, ok := policy.scanModeSet[mode]; ok {
		return mode
	}
	if strings.TrimSpace(policy.defaultScanMode) != "" {
		return policy.defaultScanMode
	}
	return defaultFactoryPolicyScanMode()
}

func defaultFactoryScanMode() string {
	mode := normalizeFactoryScanMode("")
	if mode != "" {
		return mode
	}
	return defaultFactoryPolicyScanMode()
}

func factoryModeUsesSaaSRubric(mode string) bool {
	switch normalizeFactoryScanMode(mode) {
	case "saas_gap", "saas_trend", "automation_micro", "derived":
		return true
	default:
		return false
	}
}

func factoryRubricName(mode string) string {
	policy := loadFactoryContractPolicy()
	mode = normalizeFactoryScanMode(mode)
	if rubric := strings.TrimSpace(policy.rubricByMode[mode]); rubric != "" {
		return rubric
	}
	switch mode {
	case "local_services":
		return "local_services_rubric"
	case "saas_trend":
		return "saas_trend_rubric"
	case "corpus":
		return "corpus_rubric"
	default:
		return "saas_gap_rubric"
	}
}

func factoryEmitsCategorySignals(mode string) bool {
	policy := loadFactoryContractPolicy()
	mode = normalizeFactoryScanMode(mode)
	if value, ok := policy.categoryByMode[mode]; ok {
		return value
	}
	return mode != "saas_trend"
}

func factoryEmitsTrendSignals(mode string) bool {
	policy := loadFactoryContractPolicy()
	mode = normalizeFactoryScanMode(mode)
	if value, ok := policy.trendByMode[mode]; ok {
		return value
	}
	return mode == "saas_trend"
}

func factoryBundlePolicyString(source semanticview.Source, key string) (string, bool) {
	value, ok := factoryBundlePolicyValue(source, key)
	if !ok {
		return "", false
	}
	valueString := strings.TrimSpace(asContractString(value))
	return valueString, valueString != ""
}

func factoryBundlePolicyBool(source semanticview.Source, key string) (bool, bool) {
	value, ok := factoryBundlePolicyValue(source, key)
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "y":
			return true, true
		case "false", "0", "no", "n":
			return false, true
		}
	}
	return false, false
}

func factoryBundlePolicyValue(source semanticview.Source, key string) (any, bool) {
	if source == nil {
		return nil, false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	if value, ok := semanticview.PolicyValueForFlow(source, "discovery", key); ok {
		return value.Value, true
	}
	if value, ok := semanticview.PolicyValueForFlow(source, "", key); ok {
		return value.Value, true
	}
	return nil, false
}

func normalizeFactoryPolicyScanMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	mode = strings.ReplaceAll(mode, "-", "_")
	mode = strings.Join(strings.Fields(mode), "_")
	switch mode {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return mode
	case "local_underserved", "local", "local_service", "services":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas_trend_scan", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func defaultFactoryPolicyScanMode() string {
	return "saas_gap"
}
