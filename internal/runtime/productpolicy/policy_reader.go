package productpolicy

import (
	"fmt"
	"strconv"
	"strings"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type PolicyReader interface {
	ReadPolicy(key string) (any, bool)
}

type PromptSchemaGuard struct {
	PromptFile       string
	EmitTool         string
	RequiredTopLevel []string
	ForbiddenTokens  []string
}

// Policy is a compatibility wrapper around MAS policy.yaml values.
// Generic runtime code should prefer package helpers or PolicyReader.
type Policy struct {
	reader PolicyReader
}

func NewPolicy(reader PolicyReader) Policy {
	return Policy{reader: reader}
}

func NewStaticPolicy(values map[string]any) Policy {
	return NewPolicy(staticPolicyReader(values))
}

func NewBundlePolicy(bundle *runtimecontracts.WorkflowContractBundle) Policy {
	return NewPolicy(bundlePolicyReader{bundle: bundle})
}

func (p Policy) ReadPolicy(key string) (any, bool) {
	key = strings.TrimSpace(key)
	if key == "" || p.reader == nil {
		return nil, false
	}
	return p.reader.ReadPolicy(key)
}

func (p Policy) ManagerFallbackAgentID(models.AgentConfig) string {
	for _, key := range []string{"manager_fallback_agent_id", "control_plane_agent_id"} {
		if value, ok := p.ReadPolicy(key); ok {
			if agentID := strings.TrimSpace(asString(value)); agentID != "" {
				return agentID
			}
		}
	}
	return ""
}

func (Policy) InterceptRuntimeHandledDirective(models.AgentConfig, events.Event) bool {
	return false
}

var defaultPolicyFactory func() Policy

func SetDefaultFactory(factory func() Policy) {
	defaultPolicyFactory = factory
}

func DefaultOrNil() *Policy {
	if defaultPolicyFactory == nil {
		return nil
	}
	policy := defaultPolicyFactory()
	return &policy
}

func ControlPlaneAgentID() string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.ManagerFallbackAgentID(models.AgentConfig{}))
}

func NormalizeScanMode(raw string) string {
	mode := normalizeScanMode(raw)
	if mode != "" {
		return mode
	}
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	if configured, ok := policy.ReadPolicy("default_scan_mode"); ok {
		if strings.EqualFold(strings.TrimSpace(raw), strings.TrimSpace(asString(configured))) {
			return normalizeScanMode(asString(configured))
		}
	}
	return ""
}

func NormalizeScanPriority(raw string) string {
	return normalizeScanPriority(raw)
}

func DefaultScanMode() string {
	policy := DefaultOrNil()
	if policy != nil {
		if value, ok := policy.ReadPolicy("default_scan_mode"); ok {
			if mode := normalizeScanMode(asString(value)); mode != "" {
				return mode
			}
		}
	}
	return "saas_gap"
}

func DiscoveryFallbackMode() string {
	policy := DefaultOrNil()
	if policy != nil {
		if value, ok := policy.ReadPolicy("discovery_fallback_mode"); ok {
			if mode := normalizeScanMode(asString(value)); mode != "" {
				return mode
			}
		}
	}
	return DefaultScanMode()
}

func RubricNameForScanMode(mode string) string {
	policy := DefaultOrNil()
	mode = normalizeScanMode(mode)
	if policy != nil && mode != "" {
		if value, ok := policy.ReadPolicy("scan_modes." + mode + ".rubric"); ok {
			if rubric := strings.TrimSpace(asString(value)); rubric != "" {
				return rubric
			}
		}
	}
	switch mode {
	case "local_services":
		return "local_services_rubric"
	case "saas_trend":
		return "saas_trend_rubric"
	case "corpus":
		return "corpus_rubric"
	case "saas_gap":
		fallthrough
	default:
		return "saas_gap_rubric"
	}
}

func EmitsCategorySignals(mode string) bool {
	if value, ok := readModePolicyBool(mode, "emits_category_signals"); ok {
		return value
	}
	return normalizeScanMode(mode) != "saas_trend"
}

func EmitsTrendSignals(mode string) bool {
	if value, ok := readModePolicyBool(mode, "emits_trend_signals"); ok {
		return value
	}
	return normalizeScanMode(mode) == "saas_trend"
}

func ExpectedScannerCount(mode string) int {
	if value, ok := readModePolicyInt(mode, "expected_scanner_count"); ok && value > 0 {
		return value
	}
	if normalizeScanMode(mode) == "corpus" {
		return 1
	}
	return 3
}

func ScanDispatchKind(mode string) string {
	if value, ok := readModePolicyString(mode, "dispatch_kind"); ok {
		return value
	}
	if normalizeScanMode(mode) == "corpus" {
		return "jsonl"
	}
	return "fanout"
}

func ScanShardStage(mode string) string {
	if value, ok := readModePolicyString(mode, "shard_stage"); ok {
		return value
	}
	if normalizeScanMode(mode) == "corpus" {
		return "corpus_scan"
	}
	return "discovery"
}

func IsCorpusScanMode(mode string) bool {
	return normalizeScanMode(mode) == "corpus"
}

func CampaignModesForDirective(initialMode string, explicit bool) []string {
	initialMode = normalizeScanMode(initialMode)
	if initialMode == "" {
		initialMode = DefaultScanMode()
	}
	if explicit {
		return []string{initialMode}
	}
	if initialMode == "corpus" {
		return nil
	}
	cycle := []string{"saas_gap", "saas_trend", "local_services"}
	start := 0
	for i, mode := range cycle {
		if mode == initialMode {
			start = i
			break
		}
	}
	out := []string{initialMode}
	for i := start + 1; i < len(cycle); i++ {
		out = append(out, cycle[i])
	}
	return out
}

func ParseDirectiveMode(text string) (string, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return DefaultScanMode(), false
	}
	switch {
	case strings.Contains(text, "corpus"):
		return "corpus", true
	case strings.Contains(text, "trend"):
		return "saas_trend", true
	case strings.Contains(text, "automation micro"), strings.Contains(text, "automation_micro"):
		return "saas_gap", true
	case strings.Contains(text, "local"), strings.Contains(text, "service"):
		return "local_services", true
	default:
		return DefaultScanMode(), false
	}
}

type staticPolicyReader map[string]any

func (r staticPolicyReader) ReadPolicy(key string) (any, bool) {
	return readNestedValue(map[string]any(r), key)
}

type bundlePolicyReader struct {
	bundle *runtimecontracts.WorkflowContractBundle
}

func (r bundlePolicyReader) ReadPolicy(key string) (any, bool) {
	key = strings.TrimSpace(key)
	if key == "" || r.bundle == nil {
		return nil, false
	}
	root, rest := splitPolicyKey(key)
	if value, ok := bundlePolicyValue(r.bundle.MergedPolicy, root); ok {
		return descendPolicyValue(value, rest)
	}
	if value, ok := bundlePolicyValue(r.bundle.Policy, root); ok {
		return descendPolicyValue(value, rest)
	}
	return nil, false
}

func bundlePolicyValue(doc runtimecontracts.PolicyDocument, key string) (any, bool) {
	if doc.Values == nil {
		return nil, false
	}
	value, ok := doc.Values[strings.TrimSpace(key)]
	if !ok {
		return nil, false
	}
	return value.Value, true
}

func readModePolicyString(mode, field string) (string, bool) {
	policy := DefaultOrNil()
	if policy == nil {
		return "", false
	}
	value, ok := policy.ReadPolicy("scan_modes." + normalizeScanMode(mode) + "." + strings.TrimSpace(field))
	if !ok {
		return "", false
	}
	valueString := strings.TrimSpace(asString(value))
	return valueString, valueString != ""
}

func readModePolicyBool(mode, field string) (bool, bool) {
	policy := DefaultOrNil()
	if policy == nil {
		return false, false
	}
	value, ok := policy.ReadPolicy("scan_modes." + normalizeScanMode(mode) + "." + strings.TrimSpace(field))
	if !ok {
		return false, false
	}
	return asBool(value)
}

func readModePolicyInt(mode, field string) (int, bool) {
	policy := DefaultOrNil()
	if policy == nil {
		return 0, false
	}
	value, ok := policy.ReadPolicy("scan_modes." + normalizeScanMode(mode) + "." + strings.TrimSpace(field))
	if !ok {
		return 0, false
	}
	return asInt(value)
}

func readNestedValue(root map[string]any, key string) (any, bool) {
	if len(root) == 0 {
		return nil, false
	}
	current := any(root)
	parts := splitPolicyPath(key)
	for _, part := range parts {
		nextMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := nextMap[part]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func descendPolicyValue(value any, remainder string) (any, bool) {
	if strings.TrimSpace(remainder) == "" {
		return value, true
	}
	nextMap, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	return readNestedValue(nextMap, remainder)
}

func splitPolicyKey(key string) (string, string) {
	key = strings.TrimSpace(key)
	if idx := strings.IndexByte(key, '.'); idx >= 0 {
		return strings.TrimSpace(key[:idx]), strings.TrimSpace(key[idx+1:])
	}
	return key, ""
}

func splitPolicyPath(key string) []string {
	raw := strings.Split(strings.TrimSpace(key), ".")
	out := make([]string, 0, len(raw))
	for _, part := range raw {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func normalizeScanMode(raw string) string {
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

func normalizeScanPriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "normal", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func asBool(value any) (bool, bool) {
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

func asInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case uint:
		return int(typed), true
	case uint8:
		return int(typed), true
	case uint16:
		return int(typed), true
	case uint32:
		return int(typed), true
	case uint64:
		return int(typed), true
	case float32:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		value, err := strconv.Atoi(strings.TrimSpace(typed))
		if err == nil {
			return value, true
		}
	}
	return 0, false
}
