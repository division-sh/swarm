package factory

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
)

type factoryContractPolicy struct {
	recipientsByEvent map[string][]string
	scanModeCases     []factoryScanModeCase
	scanModeSet       map[string]struct{}
	defaultScanMode   string
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
	policy := factoryContractPolicy{
		recipientsByEvent: map[string][]string{},
		scanModeSet:       map[string]struct{}{},
	}
	for eventType, entry := range bundle.Events {
		recipients := contractAgentRecipients(entry.ConsumerType, entry.Consumer)
		if len(recipients) == 0 {
			continue
		}
		policy.recipientsByEvent[strings.TrimSpace(eventType)] = recipients
	}
	if err := loadFactoryScanModes(bundle, &policy); err != nil {
		return factoryContractPolicy{}, err
	}
	return policy, nil
}

func loadFactoryScanModes(bundle *runtimecontracts.WorkflowContractBundle, policy *factoryContractPolicy) error {
	if policy == nil {
		return nil
	}
	reader := runtimeproductpolicy.NewBundlePolicy(bundle)
	for _, mode := range []string{"local_services", "saas_gap", "saas_trend", "corpus"} {
		if _, ok := reader.ReadPolicy("scan_modes." + mode + ".rubric"); !ok {
			continue
		}
		policy.scanModeCases = append(policy.scanModeCases, factoryScanModeCase{Mode: mode})
		policy.scanModeSet[mode] = struct{}{}
	}
	if _, ok := policy.scanModeSet["saas_gap"]; ok {
		policy.scanModeSet["automation_micro"] = struct{}{}
		policy.scanModeSet["derived"] = struct{}{}
	}
	if value, ok := reader.ReadPolicy("default_scan_mode"); ok {
		policy.defaultScanMode = strings.TrimSpace(runtimeproductpolicy.NormalizeScanMode(asContractString(value)))
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
	mode = strings.TrimSpace(runtimeproductpolicy.NormalizeScanMode(mode))
	if _, ok := policy.scanModeSet[mode]; ok {
		return mode
	}
	if strings.TrimSpace(policy.defaultScanMode) != "" {
		return policy.defaultScanMode
	}
	return strings.TrimSpace(runtimeproductpolicy.DefaultScanMode())
}

func defaultFactoryScanMode() string {
	mode := normalizeFactoryScanMode("")
	if mode != "" {
		return mode
	}
	return strings.TrimSpace(runtimeproductpolicy.DefaultScanMode())
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
	if rubric := strings.TrimSpace(runtimeproductpolicy.RubricNameForScanMode(mode)); rubric != "" {
		return rubric
	}
	return defaultFactoryScanMode()
}

func factoryEmitsCategorySignals(mode string) bool {
	return runtimeproductpolicy.EmitsCategorySignals(mode)
}

func factoryEmitsTrendSignals(mode string) bool {
	return runtimeproductpolicy.EmitsTrendSignals(mode)
}
