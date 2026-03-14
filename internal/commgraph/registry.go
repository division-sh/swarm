package commgraph

import (
	"path"
	"sort"
	"strings"
	"sync"

	"empireai/internal/runtime/semanticview"
)

const (
	RuntimeProducerID = "sys:runtime"
	HumanProducerID   = "sys:human-board"
	MailboxNodeID     = "sys:mailbox"
)

type MessageAuthority struct {
	SenderRole     string
	RecipientRoles []string
	Scope          string // global | entity | local | any
}

type MailboxRoundTrip struct {
	SenderRole     string
	MailboxType    string
	DecisionEvents []string
	ReturnToRole   string // "requesting-agent" uses sender as fallback in graph projections.
	Timeout        string
}

type authorityKey struct {
	sender string
	scope  string
}

var extraProducerEvents = map[string][]string{
	"dashboard":   {"human_task.assigned", "runtime.reset"},
	"actor-agent": {"entity.routing_updated"},
}

type contractProducerRegistry struct {
	runtimeEvents []string
	humanEvents   []string
	producerRoles []string
	agentEvents   map[string][]string
}

var (
	contractRegistryOnce  sync.Once
	contractRegistryData  contractProducerRegistry
	messageAuthorityOnce  sync.Once
	messageAuthorityData  []MessageAuthority
	humanTaskDecisionOnce sync.Once
	humanTaskDecisionData []string
	activeContractSource  semanticview.Source
)

var roleAliases = map[string]string{}

func RuntimeEvents() []string {
	reg := contractProducerData()
	return append([]string(nil), reg.runtimeEvents...)
}

func HumanEvents() []string {
	reg := contractProducerData()
	return append([]string(nil), reg.humanEvents...)
}

func MessageAuthorities() []MessageAuthority {
	messageAuthorityOnce.Do(func() {
		derived, err := loadMessageAuthorityRegistry()
		if err == nil {
			messageAuthorityData = derived
			return
		}
		messageAuthorityData = cloneAuthorities(baseMessageAuthorities())
	})
	out := cloneAuthorities(messageAuthorityData)
	return out
}

func MailboxRoundTrips() []MailboxRoundTrip {
	base := baseMailboxRoundTrips()
	out := make([]MailboxRoundTrip, len(base))
	copy(out, base)
	return out
}

func HumanTaskDecisionRoles() []string {
	humanTaskDecisionOnce.Do(func() {
		humanTaskDecisionData = cloneRoles(baseHumanTaskDecisionRoles())
	})
	return cloneRoles(humanTaskDecisionData)
}

func CanDecideHumanTasks(role string) bool {
	role = canonicalRole(role)
	if role == "" {
		return false
	}
	for _, candidate := range HumanTaskDecisionRoles() {
		if canonicalRole(candidate) == role {
			return true
		}
	}
	return false
}

func CanonicalRole(role string) string {
	return canonicalRole(role)
}

func baseMessageAuthorities() []MessageAuthority {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.MessageAuthorities()
}

func baseMailboxRoundTrips() []MailboxRoundTrip {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.MailboxRoundTrips()
}

func baseHumanTaskDecisionRoles() []string {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.HumanTaskDecisionRoles()
}

func ProducerEventsForRole(role string) []string {
	reg := contractProducerData()
	key := canonicalRole(role)
	if key == "" {
		return nil
	}
	events := reg.agentEvents[key]
	if len(events) == 0 {
		return nil
	}
	out := make([]string, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, evt := range events {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		if _, ok := seen[evt]; ok {
			continue
		}
		seen[evt] = struct{}{}
		out = append(out, evt)
	}
	sort.Strings(out)
	return out
}

func ProducerRoles() []string {
	reg := contractProducerData()
	return append([]string(nil), reg.producerRoles...)
}

func KnownProducedEvents() map[string]struct{} {
	reg := contractProducerData()
	out := make(map[string]struct{}, 256)
	for _, evt := range reg.runtimeEvents {
		if v := strings.TrimSpace(evt); v != "" {
			out[v] = struct{}{}
		}
	}
	for _, evt := range reg.humanEvents {
		if v := strings.TrimSpace(evt); v != "" {
			out[v] = struct{}{}
		}
	}
	for _, events := range reg.agentEvents {
		for _, evt := range events {
			if v := strings.TrimSpace(evt); v != "" {
				out[v] = struct{}{}
			}
		}
	}
	return out
}

func HasProducerForPattern(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	known := KnownProducedEvents()
	if _, ok := known[pattern]; ok {
		return true
	}
	for evt := range known {
		if routeMatches(pattern, evt) {
			return true
		}
	}
	return false
}

func canonicalRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	if alias, ok := roleAliases[role]; ok {
		return alias
	}
	return role
}

func SetContractSource(source semanticview.Source) {
	activeContractSource = source
	resetDerivedCaches()
}

func resetDerivedCaches() {
	contractRegistryOnce = sync.Once{}
	contractRegistryData = contractProducerRegistry{}
	messageAuthorityOnce = sync.Once{}
	messageAuthorityData = nil
	humanTaskDecisionOnce = sync.Once{}
	humanTaskDecisionData = nil
	routingAuthorityOnce = sync.Once{}
	routingAuthorityData = nil
	managementAuthorityOnce = sync.Once{}
	managementAuthorityData = nil
	mailboxSendRoleOnce = sync.Once{}
	mailboxSendRoleData = nil
}

func contractProducerData() contractProducerRegistry {
	contractRegistryOnce.Do(func() {
		if registry, ok := loadContractProducerRegistry(activeContractSource); ok {
			contractRegistryData = registry
			return
		}
		contractRegistryData = fallbackProducerRegistry()
	})
	return contractRegistryData
}

func loadContractProducerRegistry(source semanticview.Source) (contractProducerRegistry, bool) {
	if source == nil {
		return contractProducerRegistry{}, false
	}
	return buildContractProducerRegistry(source), true
}

func buildContractProducerRegistry(source semanticview.Source) contractProducerRegistry {
	reg := contractProducerRegistry{
		agentEvents: make(map[string][]string),
	}
	runtimeSet := map[string]struct{}{}
	humanSet := map[string]struct{}{}
	eventCatalog := source.ResolvedEventCatalog()
	for eventType := range eventCatalog {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		runtimeSet[eventType] = struct{}{}
		switch {
		case strings.HasPrefix(eventType, "board."):
			humanSet[eventType] = struct{}{}
		case strings.HasPrefix(eventType, "timer."):
			runtimeSet[eventType] = struct{}{}
		}
	}
	agents := source.AgentEntries()
	for role, entry := range agents {
		role = canonicalRole(firstNonEmpty(role, entry.Role))
		if role == "" {
			continue
		}
		for _, eventType := range entry.EmitEvents {
			reg.agentEvents[role] = appendUniqueSortedEvent(reg.agentEvents[role], eventType)
		}
	}
	nodes := source.NodeEntries()
	for nodeID, node := range nodes {
		role := canonicalRole(nodeID)
		if role == "" {
			continue
		}
		for _, eventType := range node.Produces {
			runtimeSet[strings.TrimSpace(eventType)] = struct{}{}
			reg.agentEvents[role] = appendUniqueSortedEvent(reg.agentEvents[role], eventType)
		}
	}
	for _, timer := range source.WorkflowTimers() {
		if eventType := strings.TrimSpace(timer.Event); eventType != "" {
			runtimeSet[eventType] = struct{}{}
		}
	}
	for role, events := range extraProducerEvents {
		role = canonicalRole(role)
		for _, eventType := range events {
			reg.agentEvents[role] = appendUniqueSortedEvent(reg.agentEvents[role], eventType)
		}
	}
	reg.runtimeEvents = sortedSet(runtimeSet)
	reg.humanEvents = sortedSet(humanSet)
	for role := range reg.agentEvents {
		reg.producerRoles = append(reg.producerRoles, role)
	}
	sort.Strings(reg.producerRoles)
	return reg
}

func fallbackProducerRegistry() contractProducerRegistry {
	reg := contractProducerRegistry{
		agentEvents: make(map[string][]string, len(extraProducerEvents)),
	}
	for role, events := range extraProducerEvents {
		reg.agentEvents[role] = append([]string(nil), events...)
		reg.producerRoles = append(reg.producerRoles, role)
	}
	sort.Strings(reg.producerRoles)
	return reg
}

func loadMessageAuthorityRegistry() ([]MessageAuthority, error) {
	return cloneAuthorities(baseMessageAuthorities()), nil
}

func cloneAuthorities(in []MessageAuthority) []MessageAuthority {
	out := make([]MessageAuthority, len(in))
	for i, rule := range in {
		out[i] = MessageAuthority{
			SenderRole:     rule.SenderRole,
			RecipientRoles: append([]string(nil), rule.RecipientRoles...),
			Scope:          rule.Scope,
		}
	}
	return out
}

func cloneRoles(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, role := range in {
		role = canonicalRole(role)
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

func appendUniqueSortedEvent(events []string, eventType string) []string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return events
	}
	for _, existing := range events {
		if strings.TrimSpace(existing) == eventType {
			return events
		}
	}
	events = append(events, eventType)
	sort.Strings(events)
	return events
}

func sortedSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func routeMatches(pattern, eventType string) bool {
	pattern = normalizeEventPattern(pattern)
	eventType = normalizeEventPattern(eventType)
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		if strings.Contains(pattern, "*") {
			if ok, err := path.Match(pattern, eventType); err == nil && ok {
				return true
			}
		}
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
		}
		return pattern == eventType
	}
}

func normalizeEventPattern(value string) string {
	value = strings.TrimSpace(value)
	if idx := strings.LastIndex(value, "/"); idx >= 0 && idx+1 < len(value) {
		value = value[idx+1:]
	}
	return value
}
