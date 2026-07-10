package semanticview

import (
	"sort"
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
		seen:              map[string]struct{}{},
		representedNodes:  map[string]struct{}{},
		scopeNodeCounts:   map[string]int{},
		countedScopeNodes: map[string]struct{}{},
		effectiveNodes:    source.NodeEntries(),
	}
	if bundle, ok := Bundle(source); ok {
		builder.bundle = bundle
	}
	projectScopes := sortedAuthoredProjectScopes(source.ProjectScopes())
	flowScopes := sortedAuthoredFlowScopes(source.FlowScopes())
	preferredFlowScopeKeys := authoredPreferredFlowScopeKeys(projectScopes, flowScopes)
	for _, scope := range projectScopes {
		if authoredEmitSiteSkipsProjectScope(scope) {
			continue
		}
		builder.countScopeNodes(strings.TrimSpace(scope.Key), strings.TrimSpace(scope.OwningFlowID), scope.Nodes)
	}
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		scopeKey := authoredEmitSiteFlowScopeKey(scope)
		if preferred := preferredFlowScopeKeys[flowID]; preferred != "" && scopeKey != preferred {
			continue
		}
		builder.countScopeNodes(scopeKey, flowID, scope.Nodes)
	}
	for _, scope := range projectScopes {
		if authoredEmitSiteSkipsProjectScope(scope) {
			continue
		}
		flowID := strings.TrimSpace(scope.OwningFlowID)
		flowPath, flowPackageKey := authoredEmitSiteFlowIdentity(source, flowID)
		builder.appendScope(AuthoredEmitSiteSourceProject, strings.TrimSpace(scope.Key), flowID, flowPath, flowPackageKey, scope.Nodes)
	}
	for _, scope := range flowScopes {
		flowID := strings.TrimSpace(scope.ID)
		scopeKey := authoredEmitSiteFlowScopeKey(scope)
		if preferred := preferredFlowScopeKeys[flowID]; preferred != "" && scopeKey != preferred {
			continue
		}
		builder.appendScope(AuthoredEmitSiteSourceFlow, scopeKey, flowID, strings.TrimSpace(scope.Path), strings.TrimSpace(scope.PackageKey), scope.Nodes)
	}
	// Programmatic contract bundles can populate the effective flat registry
	// without constructing package/flow views. Keep authored endpoint discovery
	// exhaustive while preserving scoped declarations as the preferred source.
	for _, nodeKey := range sortedAuthoredNodeKeys(source.NodeEntries()) {
		node := source.NodeEntries()[nodeKey]
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(nodeKey)
		}
		if _, ok := builder.representedNodes[nodeID]; ok {
			continue
		}
		contractSource, _ := source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(contractSource.FlowID)
		flowPath, flowPackageKey := authoredEmitSiteFlowIdentity(source, flowID)
		if flowPackageKey == "" {
			flowPackageKey = strings.TrimSpace(contractSource.PackageKey)
		}
		kind := AuthoredEmitSiteSourceProject
		if flowID != "" {
			kind = AuthoredEmitSiteSourceFlow
		}
		builder.appendScope(kind, flowPackageKey, flowID, flowPath, flowPackageKey, map[string]runtimecontracts.SystemNodeContract{nodeKey: node})
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
	bundle            *runtimecontracts.WorkflowContractBundle
	seen              map[string]struct{}
	representedNodes  map[string]struct{}
	scopeNodeCounts   map[string]int
	countedScopeNodes map[string]struct{}
	effectiveNodes    map[string]runtimecontracts.SystemNodeContract
	sites             []AuthoredEmitSite
}

func (b *authoredEmitSiteBuilder) countScopeNodes(scopeKey, flowID string, nodes map[string]runtimecontracts.SystemNodeContract) {
	for _, nodeKey := range sortedAuthoredNodeKeys(nodes) {
		nodeID := strings.TrimSpace(nodes[nodeKey].ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(nodeKey)
		}
		occurrence := strings.Join([]string{strings.TrimSpace(flowID), strings.TrimSpace(scopeKey), nodeID}, "\x00")
		if _, exists := b.countedScopeNodes[occurrence]; exists {
			continue
		}
		b.countedScopeNodes[occurrence] = struct{}{}
		b.scopeNodeCounts[nodeID]++
	}
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
		if b.scopeNodeCounts[nodeID] == 1 {
			if effective, ok := b.effectiveNodes[nodeID]; ok {
				node = effective
			}
		}
		b.representedNodes[nodeID] = struct{}{}
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
		if b.bundle != nil {
			if lowered, err := b.bundle.LowerEmitSpecFields(runtimecontracts.EmitFieldLoweringContext{
				NodeID:           nodeID,
				FlowID:           flowID,
				TriggerEventType: handlerEvent,
				Site:             siteKey,
			}, spec); err == nil {
				spec = lowered
			}
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
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler) {
		add(site.Source, site.SiteKey, site.RuleID, site.Spec)
	}
	if handler.Guard != nil {
		if emitSpec := authoredGuardEscalationEmitSpec(handler.Guard); !emitSpec.Empty() {
			add("handler.guard.on_fail.escalate", "handler.guard.on_fail.escalate", handler.Guard.ID, emitSpec)
		}
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
