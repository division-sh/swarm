package factory

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
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
	if err := loadFactoryScanModes(repoRoot, &policy); err != nil {
		return factoryContractPolicy{}, err
	}
	return policy, nil
}

func loadFactoryScanModes(repoRoot string, policy *factoryContractPolicy) error {
	if policy == nil {
		return nil
	}
	path := filepath.Join(repoRoot, "contracts", "test-vectors", "campaign-cycling.yaml")
	var doc struct {
		ModeCases []factoryScanModeCase `yaml:"mode_cases"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for _, modeCase := range doc.ModeCases {
		mode := strings.TrimSpace(modeCase.Mode)
		if mode == "" {
			continue
		}
		policy.scanModeCases = append(policy.scanModeCases, modeCase)
		policy.scanModeSet[mode] = struct{}{}
		if policy.defaultScanMode == "" && mode == "local_services" {
			policy.defaultScanMode = mode
		}
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
		dir := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
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
	mode = strings.TrimSpace(strings.ToLower(mode))
	if _, ok := policy.scanModeSet[mode]; ok {
		return mode
	}
	switch mode {
	case "automation_micro":
		if _, ok := policy.scanModeSet["automation_micro"]; ok {
			return "automation_micro"
		}
		if _, ok := policy.scanModeSet["saas_gap"]; ok {
			return "saas_gap"
		}
	}
	if strings.TrimSpace(policy.defaultScanMode) != "" {
		return policy.defaultScanMode
	}
	return "local_services"
}

func defaultFactoryScanMode() string {
	return normalizeFactoryScanMode("")
}

func factoryModeUsesSaaSRubric(mode string) bool {
	switch normalizeFactoryScanMode(mode) {
	case "saas_gap", "saas_trend", "automation_micro", "corpus":
		return true
	default:
		return false
	}
}

func factoryRubricName(mode string) string {
	if factoryModeUsesSaaSRubric(mode) {
		return "saas"
	}
	return defaultFactoryScanMode()
}

func factoryEmitsCategorySignals(mode string) bool {
	return normalizeFactoryScanMode(mode) == "saas_gap"
}

func factoryEmitsTrendSignals(mode string) bool {
	return normalizeFactoryScanMode(mode) == "saas_trend"
}
