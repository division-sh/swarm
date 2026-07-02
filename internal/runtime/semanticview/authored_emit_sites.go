package semanticview

import (
	"sort"
	"strconv"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type AuthoredEmitSiteSourceKind string

const (
	AuthoredEmitSiteSourceProject AuthoredEmitSiteSourceKind = "project"
	AuthoredEmitSiteSourceFlow    AuthoredEmitSiteSourceKind = "flow"
)

type AuthoredEmitSite struct {
	ID             string
	SourceKind     AuthoredEmitSiteSourceKind
	SourceScopeKey string
	FlowID         string
	FlowPath       string
	FlowPackageKey string
	NodeKey        string
	NodeID         string
	HandlerEvent   string
	Site           string
	SiteKey        string
	RuleID         string
	Spec           runtimecontracts.EmitSpec
	Handler        runtimecontracts.SystemNodeEventHandler
}

func AuthoredEmitSites(source Source) []AuthoredEmitSite {
	if source == nil {
		return nil
	}
	builder := authoredEmitSiteBuilder{
		seen: map[string]struct{}{},
	}
	projectScopes := sortedAuthoredProjectScopes(source.ProjectScopes())
	for _, scope := range projectScopes {
		if authoredEmitSiteSkipsProjectScope(scope) {
			continue
		}
		flowID := strings.TrimSpace(scope.OwningFlowID)
		flowPath, flowPackageKey := authoredEmitSiteFlowIdentity(source, flowID)
		builder.appendScope(AuthoredEmitSiteSourceProject, strings.TrimSpace(scope.Key), flowID, flowPath, flowPackageKey, scope.Nodes)
	}
	flowScopes := sortedAuthoredFlowScopes(source.FlowScopes())
	preferredFlowScopeKeys := authoredPreferredFlowScopeKeys(projectScopes, flowScopes)
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		scopeKey := authoredEmitSiteFlowScopeKey(scope)
		if preferred := preferredFlowScopeKeys[flowID]; preferred != "" && scopeKey != preferred {
			continue
		}
		builder.appendScope(AuthoredEmitSiteSourceFlow, scopeKey, flowID, strings.TrimSpace(scope.Path), strings.TrimSpace(scope.PackageKey), scope.Nodes)
	}
	sort.SliceStable(builder.sites, func(i, j int) bool {
		return builder.sites[i].ID < builder.sites[j].ID
	})
	return builder.sites
}

func authoredEmitSiteSkipsProjectScope(scope ProjectScope) bool {
	// The loader can expose a flow-owned "." project projection for the root
	// package; the corresponding FlowScope is the canonical authored source.
	return strings.TrimSpace(scope.Key) == "." && strings.TrimSpace(scope.OwningFlowID) != ""
}

type authoredEmitSiteBuilder struct {
	seen  map[string]struct{}
	sites []AuthoredEmitSite
}

func (b *authoredEmitSiteBuilder) appendScope(kind AuthoredEmitSiteSourceKind, scopeKey, flowID, flowPath, flowPackageKey string, nodes map[string]runtimecontracts.SystemNodeContract) {
	scopeKey = strings.TrimSpace(scopeKey)
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	flowPackageKey = strings.TrimSpace(flowPackageKey)
	for _, nodeKey := range sortedAuthoredNodeKeys(nodes) {
		node := nodes[nodeKey]
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(nodeKey)
		}
		for _, handlerEvent := range sortedAuthoredHandlerEvents(node.EventHandlers) {
			handler := node.EventHandlers[handlerEvent]
			b.appendHandlerSites(kind, scopeKey, flowID, flowPath, flowPackageKey, strings.TrimSpace(nodeKey), nodeID, handlerEvent, handler)
		}
	}
}

func (b *authoredEmitSiteBuilder) appendHandlerSites(kind AuthoredEmitSiteSourceKind, scopeKey, flowID, flowPath, flowPackageKey, nodeKey, nodeID, handlerEvent string, handler runtimecontracts.SystemNodeEventHandler) {
	add := func(site, siteKey, ruleID string, spec runtimecontracts.EmitSpec) {
		if spec.Empty() {
			return
		}
		id := authoredEmitSiteIdentity(flowID, scopeKey, nodeKey, handlerEvent, siteKey)
		if _, ok := b.seen[id]; ok {
			return
		}
		b.seen[id] = struct{}{}
		b.sites = append(b.sites, AuthoredEmitSite{
			ID:             id,
			SourceKind:     kind,
			SourceScopeKey: scopeKey,
			FlowID:         flowID,
			FlowPath:       flowPath,
			FlowPackageKey: flowPackageKey,
			NodeKey:        nodeKey,
			NodeID:         nodeID,
			HandlerEvent:   strings.TrimSpace(handlerEvent),
			Site:           site,
			SiteKey:        siteKey,
			RuleID:         strings.TrimSpace(ruleID),
			Spec:           spec,
			Handler:        handler,
		})
	}
	templateSites := runtimecontracts.HandlerRuleEmitTemplateSites(handler)
	if len(templateSites) == 0 {
		add("handler.emit", "handler.emit", "", handler.Emit)
	} else {
		for _, site := range templateSites {
			add(site.Source, site.SiteKey, site.RuleID, site.Spec)
		}
	}
	add("handler.on_success.emit", "handler.on_success.emit", "", handler.OnSuccess.Emit)
	if handler.Guard != nil {
		if emitSpec := authoredGuardEscalationEmitSpec(handler.Guard); !emitSpec.Empty() {
			add("handler.guard.on_fail.escalate", "handler.guard.on_fail.escalate", handler.Guard.ID, emitSpec)
		}
	}
	if len(templateSites) == 0 {
		for idx, rule := range handler.Rules {
			add("handler.rules.emit", indexedAuthoredEmitSiteKey("handler.rules", idx, "emit"), rule.ID, rule.Emit)
			if rule.FanOut != nil {
				add("handler.rules.fan_out.emit", indexedAuthoredEmitSiteKey("handler.rules", idx, "fan_out.emit"), rule.ID, rule.FanOut.Emit)
			}
		}
	}
	for idx, rule := range handler.OnComplete {
		add("handler.on_complete.emit", indexedAuthoredEmitSiteKey("handler.on_complete", idx, "emit"), rule.ID, rule.Emit)
		if rule.FanOut != nil {
			add("handler.on_complete.fan_out.emit", indexedAuthoredEmitSiteKey("handler.on_complete", idx, "fan_out.emit"), rule.ID, rule.FanOut.Emit)
		}
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			add("handler.accumulate.on_complete.emit", indexedAuthoredEmitSiteKey("handler.accumulate.on_complete", idx, "emit"), rule.ID, rule.Emit)
			if rule.FanOut != nil {
				add("handler.accumulate.on_complete.fan_out.emit", indexedAuthoredEmitSiteKey("handler.accumulate.on_complete", idx, "fan_out.emit"), rule.ID, rule.FanOut.Emit)
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			add("handler.accumulate.on_timeout.emit", "handler.accumulate.on_timeout.emit", handler.Accumulate.OnTimeout.ID, handler.Accumulate.OnTimeout.Emit)
			if handler.Accumulate.OnTimeout.FanOut != nil {
				add("handler.accumulate.on_timeout.fan_out.emit", "handler.accumulate.on_timeout.fan_out.emit", handler.Accumulate.OnTimeout.ID, handler.Accumulate.OnTimeout.FanOut.Emit)
			}
		}
	}
	if handler.FanOut != nil {
		add("handler.fan_out.emit", "handler.fan_out.emit", "", handler.FanOut.Emit)
	}
}

func sortedAuthoredProjectScopes(scopes []ProjectScope) []ProjectScope {
	out := append([]ProjectScope{}, scopes...)
	sort.SliceStable(out, func(i, j int) bool {
		if strings.TrimSpace(out[i].Key) != strings.TrimSpace(out[j].Key) {
			return strings.TrimSpace(out[i].Key) < strings.TrimSpace(out[j].Key)
		}
		return strings.TrimSpace(out[i].OwningFlowID) < strings.TrimSpace(out[j].OwningFlowID)
	})
	return out
}

func sortedAuthoredFlowScopes(scopes []FlowScope) []FlowScope {
	out := append([]FlowScope{}, scopes...)
	sort.SliceStable(out, func(i, j int) bool {
		if strings.TrimSpace(out[i].ID) != strings.TrimSpace(out[j].ID) {
			return strings.TrimSpace(out[i].ID) < strings.TrimSpace(out[j].ID)
		}
		return strings.TrimSpace(out[i].Path) < strings.TrimSpace(out[j].Path)
	})
	return out
}

func sortedAuthoredNodeKeys(nodes map[string]runtimecontracts.SystemNodeContract) []string {
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedAuthoredHandlerEvents(handlers map[string]runtimecontracts.SystemNodeEventHandler) []string {
	keys := make([]string, 0, len(handlers))
	for key := range handlers {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func authoredEmitSiteFlowIdentity(source Source, flowID string) (string, string) {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return "", ""
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return "", ""
	}
	return strings.Trim(strings.TrimSpace(scope.Path), "/"), strings.TrimSpace(scope.PackageKey)
}

func authoredEmitSiteFlowScopeKey(scope FlowScope) string {
	if key := strings.TrimSpace(scope.PackageKey); key != "" {
		return key
	}
	path := strings.Trim(strings.TrimSpace(scope.Path), "/")
	if path != "" {
		return path
	}
	if id := strings.TrimSpace(scope.ID); id != "" {
		return "flows/" + id
	}
	return ""
}

func authoredPreferredFlowScopeKeys(projectScopes []ProjectScope, flowScopes []FlowScope) map[string]string {
	out := map[string]string{}
	flowRootProjectScopeKeys := authoredFlowRootProjectScopeKeys(flowScopes)
	for _, scope := range projectScopes {
		flowID := strings.TrimSpace(scope.OwningFlowID)
		if flowID == "" {
			continue
		}
		key := strings.TrimSpace(scope.Key)
		if key == "" {
			continue
		}
		if _, ok := flowRootProjectScopeKeys[flowID][key]; !ok {
			continue
		}
		current := out[flowID]
		if current == "" || authoredFlowScopeKeyRank(key) > authoredFlowScopeKeyRank(current) {
			out[flowID] = key
		}
	}
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		key := authoredEmitSiteFlowScopeKey(scope)
		if key == "" {
			continue
		}
		current := out[flowID]
		if current == "" || authoredFlowScopeKeyRank(key) > authoredFlowScopeKeyRank(current) {
			out[flowID] = key
		}
	}
	return out
}

func authoredFlowRootProjectScopeKeys(flowScopes []FlowScope) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		keys := out[flowID]
		if keys == nil {
			keys = map[string]struct{}{}
			out[flowID] = keys
		}
		if key := strings.TrimSpace(scope.PackageKey); key != "" && key != "." {
			keys[key] = struct{}{}
		}
		if path := strings.Trim(strings.TrimSpace(scope.Path), "/"); path != "" && path != "." {
			keys[path] = struct{}{}
		}
		keys["flows/"+flowID] = struct{}{}
	}
	return out
}

func authoredFlowScopeKeyRank(key string) int {
	key = strings.TrimSpace(key)
	switch {
	case key == "" || key == ".":
		return 0
	case strings.HasPrefix(key, "flows/"):
		return 2
	default:
		return 1
	}
}

func authoredEmitSiteIdentity(flowID, scopeKey, nodeKey, handlerEvent, siteKey string) string {
	return strings.Join([]string{
		strings.TrimSpace(flowID),
		strings.TrimSpace(scopeKey),
		strings.TrimSpace(nodeKey),
		strings.TrimSpace(handlerEvent),
		strings.TrimSpace(siteKey),
	}, "\x1f")
}

func indexedAuthoredEmitSiteKey(prefix string, index int, suffix string) string {
	return prefix + "[" + strconv.Itoa(index) + "]." + suffix
}

func authoredGuardEscalationEmitSpec(guard *runtimecontracts.GuardSpec) runtimecontracts.EmitSpec {
	if guard == nil {
		return runtimecontracts.EmitSpec{}
	}
	failureSpec, err := guard.FailureSpec()
	if err != nil || failureSpec.Action != runtimecontracts.GuardFailureActionEscalate {
		return runtimecontracts.EmitSpec{}
	}
	return failureSpec.EscalationEmitSpec()
}
