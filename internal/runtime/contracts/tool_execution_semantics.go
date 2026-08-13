package contracts

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ToolCategory is the closed author-facing grouping admitted for tools.
type ToolCategory uint8

const (
	ToolCategoryUnspecified ToolCategory = iota
	ToolCategoryProviderConnector
	ToolCategoryProviderRegistration
	ToolCategoryChannelOperation
	ToolCategoryPlatform
	ToolCategoryHumanDecision
	ToolCategoryFlowData
	ToolCategoryEntityPersistence
)

func ParseToolCategory(raw string) (ToolCategory, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ToolCategoryUnspecified, nil
	case "provider_connector":
		return ToolCategoryProviderConnector, nil
	case "provider_registration":
		return ToolCategoryProviderRegistration, nil
	case "channel_operation":
		return ToolCategoryChannelOperation, nil
	case "platform":
		return ToolCategoryPlatform, nil
	case "human_decision":
		return ToolCategoryHumanDecision, nil
	case "flow_data":
		return ToolCategoryFlowData, nil
	case "entity_persistence":
		return ToolCategoryEntityPersistence, nil
	default:
		return ToolCategoryUnspecified, fmt.Errorf("unsupported tool category %q", raw)
	}
}

func (c ToolCategory) String() string {
	switch c {
	case ToolCategoryUnspecified:
		return ""
	case ToolCategoryProviderConnector:
		return "provider_connector"
	case ToolCategoryProviderRegistration:
		return "provider_registration"
	case ToolCategoryChannelOperation:
		return "channel_operation"
	case ToolCategoryPlatform:
		return "platform"
	case ToolCategoryHumanDecision:
		return "human_decision"
	case ToolCategoryFlowData:
		return "flow_data"
	case ToolCategoryEntityPersistence:
		return "entity_persistence"
	default:
		return ""
	}
}

// ToolPermission is an admitted permission identity. The empty value means no
// permission is required.
type ToolPermission struct {
	value string
}

func NewToolPermission(raw string) (ToolPermission, error) {
	raw = strings.TrimSpace(raw)
	if !utf8.ValidString(raw) {
		return ToolPermission{}, fmt.Errorf("tool permission is not valid UTF-8")
	}
	if strings.ContainsAny(raw, " \t\r\n") {
		return ToolPermission{}, fmt.Errorf("tool permission %q must not contain whitespace", raw)
	}
	return ToolPermission{value: raw}, nil
}

func MustToolPermission(raw string) ToolPermission {
	permission, err := NewToolPermission(raw)
	if err != nil {
		panic(err)
	}
	return permission
}

func (p ToolPermission) String() string { return p.value }
func (p ToolPermission) IsZero() bool   { return p.value == "" }

// ToolRatePolicy is the admitted external dispatch rate policy. Syntax is
// parsed only at construction; runtime consumers receive durations directly.
type ToolRatePolicy struct {
	enabled bool
	limit   int
	period  time.Duration
	maxWait time.Duration
}

func NewToolRatePolicy(rateLimit, maxWait string) (ToolRatePolicy, error) {
	rateLimit = strings.TrimSpace(rateLimit)
	maxWait = strings.TrimSpace(maxWait)
	if rateLimit == "" && maxWait == "" {
		return ToolRatePolicy{}, nil
	}
	if rateLimit == "" {
		return ToolRatePolicy{}, fmt.Errorf("rate_limit_max_wait requires rate_limit")
	}
	if maxWait == "" {
		return ToolRatePolicy{}, fmt.Errorf("rate_limit requires rate_limit_max_wait")
	}
	limit, period, err := parseToolRate(rateLimit)
	if err != nil {
		return ToolRatePolicy{}, fmt.Errorf("rate_limit: %w", err)
	}
	wait, err := parseToolRateDuration(maxWait, true)
	if err != nil {
		return ToolRatePolicy{}, fmt.Errorf("rate_limit_max_wait: %w", err)
	}
	return ToolRatePolicy{enabled: true, limit: limit, period: period, maxWait: wait}, nil
}

func MustToolRatePolicy(rateLimit, maxWait string) ToolRatePolicy {
	policy, err := NewToolRatePolicy(rateLimit, maxWait)
	if err != nil {
		panic(err)
	}
	return policy
}

func (p ToolRatePolicy) Enabled() bool          { return p.enabled }
func (p ToolRatePolicy) Limit() int             { return p.limit }
func (p ToolRatePolicy) Period() time.Duration  { return p.period }
func (p ToolRatePolicy) MaxWait() time.Duration { return p.maxWait }

func (p ToolRatePolicy) Syntax() (string, string) {
	if !p.enabled {
		return "", ""
	}
	return strconv.Itoa(p.limit) + "/" + formatToolRateDuration(p.period), formatToolRateDuration(p.maxWait)
}

func parseToolRate(raw string) (int, time.Duration, error) {
	if err := rejectToolRateWhitespace(raw); err != nil {
		return 0, 0, err
	}
	countRaw, periodRaw, ok := strings.Cut(raw, "/")
	if !ok || countRaw == "" || periodRaw == "" || strings.Contains(periodRaw, "/") {
		return 0, 0, fmt.Errorf("must be <positive integer>/<period>")
	}
	count, err := strconv.Atoi(countRaw)
	if err != nil || count <= 0 {
		return 0, 0, fmt.Errorf("count must be a positive integer")
	}
	period, err := parseToolRateDuration(periodRaw, false)
	if err != nil {
		return 0, 0, err
	}
	return count, period, nil
}

func parseToolRateDuration(raw string, allowZero bool) (time.Duration, error) {
	if err := rejectToolRateWhitespace(raw); err != nil {
		return 0, err
	}
	if raw == "" {
		return 0, fmt.Errorf("duration is required")
	}
	unit := ""
	numberRaw := raw
	for _, candidate := range []string{"ms", "s", "m", "h", "d"} {
		if strings.HasSuffix(raw, candidate) {
			unit = candidate
			numberRaw = strings.TrimSuffix(raw, candidate)
			break
		}
	}
	if unit == "" {
		return 0, fmt.Errorf("duration unit must be one of ms, s, m, h, d")
	}
	if numberRaw == "" {
		numberRaw = "1"
	}
	amount, err := strconv.Atoi(numberRaw)
	if err != nil {
		return 0, fmt.Errorf("duration amount must be an integer")
	}
	if amount < 0 || (!allowZero && amount == 0) {
		return 0, fmt.Errorf("duration must be positive")
	}
	unitDuration := time.Millisecond
	switch unit {
	case "s":
		unitDuration = time.Second
	case "m":
		unitDuration = time.Minute
	case "h":
		unitDuration = time.Hour
	case "d":
		unitDuration = 24 * time.Hour
	}
	return time.Duration(amount) * unitDuration, nil
}

func rejectToolRateWhitespace(raw string) error {
	if strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, " \t\r\n") {
		return fmt.Errorf("must not contain whitespace")
	}
	return nil
}

func formatToolRateDuration(value time.Duration) string {
	if value == 0 {
		return "0s"
	}
	for _, candidate := range []struct {
		suffix string
		unit   time.Duration
	}{
		{suffix: "d", unit: 24 * time.Hour},
		{suffix: "h", unit: time.Hour},
		{suffix: "m", unit: time.Minute},
		{suffix: "s", unit: time.Second},
		{suffix: "ms", unit: time.Millisecond},
	} {
		if value%candidate.unit == 0 {
			amount := value / candidate.unit
			if amount == 1 {
				return candidate.suffix
			}
			return strconv.FormatInt(int64(amount), 10) + candidate.suffix
		}
	}
	return strconv.FormatInt(int64(value/time.Millisecond), 10) + "ms"
}

// ToolMCPBinding is the admitted execution address of one discovered MCP tool.
type ToolMCPBinding struct {
	server string
	remote string
}

func NewToolMCPBinding(server, remote string) (ToolMCPBinding, error) {
	server = strings.TrimSpace(server)
	remote = strings.TrimSpace(remote)
	if server == "" || remote == "" {
		return ToolMCPBinding{}, fmt.Errorf("MCP server and remote tool names are required")
	}
	if !utf8.ValidString(server) || !utf8.ValidString(remote) {
		return ToolMCPBinding{}, fmt.Errorf("MCP binding is not valid UTF-8")
	}
	return ToolMCPBinding{server: server, remote: remote}, nil
}

func MustToolMCPBinding(server, remote string) ToolMCPBinding {
	binding, err := NewToolMCPBinding(server, remote)
	if err != nil {
		panic(err)
	}
	return binding
}

func (b ToolMCPBinding) Server() string { return b.server }
func (b ToolMCPBinding) Remote() string { return b.remote }
func (b ToolMCPBinding) IsZero() bool   { return b.server == "" && b.remote == "" }
