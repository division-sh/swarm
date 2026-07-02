package semanticview

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

type ImportBoundaryWildcardPattern struct {
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	Source           string
	EventPattern     string
	MatchPattern     string
	LocalizedEvent   string
	RouteSource      string
}

type ImportBoundaryWildcardIssue struct {
	Kind             string
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	Source           string
	EventPattern     string
	Message          string
}

type ImportBoundaryWildcardSubscriptionResolution struct {
	Scoped   bool
	Patterns []ImportBoundaryWildcardPattern
}

type importBoundaryWildcardScope struct {
	Kind        string
	ID          string
	PackageKey  string
	Path        string
	LocalEvents map[string]struct{}
}

type importBoundaryWildcardGrantPattern struct {
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	Source           string
	EventPattern     string
	LocalizedEvent   string
}

func ResolveImportBoundaryWildcardSubscription(source Source, packageKey, flowID, basePath string, localEvents map[string]struct{}, raw string) ImportBoundaryWildcardSubscriptionResolution {
	raw = eventidentity.Normalize(raw)
	if source == nil || raw == "" || !strings.Contains(raw, "*") {
		return ImportBoundaryWildcardSubscriptionResolution{}
	}
	packageKey = importBoundaryPackageKeyForContext(source, packageKey, flowID)
	if !importBoundaryPackageImported(source, packageKey) {
		return ImportBoundaryWildcardSubscriptionResolution{}
	}
	resolution := ImportBoundaryWildcardSubscriptionResolution{Scoped: true}
	for _, pattern := range importBoundaryDefaultWildcardPatterns(source, packageKey, flowID, basePath, localEvents, raw) {
		resolution.Patterns = appendUniqueImportBoundaryWildcardPattern(resolution.Patterns, pattern)
	}
	for _, grant := range importBoundaryWildcardGrantPatterns(source, packageKey) {
		if !importBoundaryWildcardGrantIntersects(raw, grant.LocalizedEvent, grant.EventPattern) {
			continue
		}
		resolution.Patterns = appendUniqueImportBoundaryWildcardPattern(resolution.Patterns, ImportBoundaryWildcardPattern{
			ParentPackageKey: grant.ParentPackageKey,
			ChildPackageKey:  grant.ChildPackageKey,
			ImportLabel:      grant.ImportLabel,
			Source:           grant.Source,
			EventPattern:     grant.EventPattern,
			MatchPattern:     raw,
			LocalizedEvent:   grant.LocalizedEvent,
			RouteSource:      "import_boundary_wildcard_grant",
		})
	}
	sortImportBoundaryWildcardPatterns(resolution.Patterns)
	return resolution
}

func ResolveImportBoundaryWildcardSubscriptionForNode(source Source, nodeID, raw string) ImportBoundaryWildcardSubscriptionResolution {
	if source == nil {
		return ImportBoundaryWildcardSubscriptionResolution{}
	}
	contractSource, ok := source.NodeContractSource(strings.TrimSpace(nodeID))
	if !ok {
		return ImportBoundaryWildcardSubscriptionResolution{}
	}
	return ResolveImportBoundaryWildcardSubscription(
		source,
		contractSource.PackageKey,
		contractSource.FlowID,
		importBoundaryBasePathForFlow(source, contractSource.FlowID),
		importBoundaryNodeLocalEvents(source, nodeID, contractSource),
		raw,
	)
}

func ImportBoundaryWildcardSubscriptionMatches(source Source, packageKey, flowID, basePath string, localEvents map[string]struct{}, raw, eventType string) (bool, bool) {
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false, false
	}
	resolution := ResolveImportBoundaryWildcardSubscription(source, packageKey, flowID, basePath, localEvents, raw)
	if !resolution.Scoped {
		return false, false
	}
	for _, pattern := range resolution.Patterns {
		if eventidentity.MatchPattern(pattern.EventPattern, eventType) {
			return true, true
		}
	}
	return false, true
}

func ImportBoundaryWildcardSubscriptionMatchesNode(source Source, nodeID, raw, eventType string) (bool, bool) {
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false, false
	}
	resolution := ResolveImportBoundaryWildcardSubscriptionForNode(source, nodeID, raw)
	if !resolution.Scoped {
		return false, false
	}
	for _, pattern := range resolution.Patterns {
		if eventidentity.MatchPattern(pattern.EventPattern, eventType) {
			return true, true
		}
	}
	return false, true
}

func ImportBoundaryWildcardHandlerFallbackDenied(source Source, nodeID, eventType string) bool {
	if source == nil {
		return false
	}
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	resolved := bundle.ResolveNodeEventHandler(nodeID, eventType)
	authoredEventType := eventidentity.Normalize(resolved.AuthoredEventType)
	if !resolved.Matched || authoredEventType == "" || !strings.Contains(authoredEventType, "*") {
		return false
	}

	scoped := false
	for _, candidate := range importBoundaryHandlerResolutionEventCandidates(resolved, eventType) {
		matched, candidateScoped := ImportBoundaryWildcardSubscriptionMatchesNode(source, nodeID, authoredEventType, candidate)
		if !candidateScoped {
			continue
		}
		scoped = true
		if matched {
			return false
		}
	}
	return scoped
}

func importBoundaryHandlerResolutionEventCandidates(resolved runtimecontracts.NodeEventHandlerResolution, eventType string) []string {
	seen := map[string]struct{}{}
	var out []string
	appendCandidate := func(candidate string) {
		candidate = eventidentity.Normalize(candidate)
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	appendCandidate(eventType)
	appendCandidate(resolved.RawEventType)
	appendCandidate(resolved.LocalizedEventType)
	appendCandidate(resolved.CanonicalEventType)
	return out
}

func ImportBoundaryWildcardGrantIssues(source Source) []ImportBoundaryWildcardIssue {
	if source == nil {
		return nil
	}
	projectByKey, _ := importBoundaryScopeIndexes(source)
	var issues []ImportBoundaryWildcardIssue
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			if len(site.Bind.Observe) == 0 {
				continue
			}
			child, ok := projectByKey[site.PackageKey]
			if !ok {
				continue
			}
			child.Key = normalizeImportPackageKey(child.Key)
			for _, grant := range site.Bind.Observe {
				issues = append(issues, importBoundaryWildcardGrantIssues(source, parent, child, site, grant)...)
			}
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		return strings.Compare(importBoundaryWildcardIssueSortKey(issues[i]), importBoundaryWildcardIssueSortKey(issues[j])) < 0
	})
	return issues
}

func ImportBoundaryWildcardSubscriptionIssues(source Source) []ImportBoundaryWildcardIssue {
	if source == nil {
		return nil
	}
	var issues []ImportBoundaryWildcardIssue
	for _, scope := range source.ProjectScopes() {
		packageKey := normalizeImportPackageKey(scope.Key)
		if !importBoundaryPackageImported(source, packageKey) {
			continue
		}
		for _, pattern := range importBoundaryProjectWildcardSubscriptions(scope) {
			resolution := ResolveImportBoundaryWildcardSubscription(source, packageKey, "", "", importBoundaryProjectLocalEventSet(scope), pattern)
			if resolution.Scoped && len(resolution.Patterns) == 0 {
				issues = append(issues, ImportBoundaryWildcardIssue{
					Kind:            "ungranted_or_unknown_subscription",
					ChildPackageKey: packageKey,
					EventPattern:    eventidentity.Normalize(pattern),
					Message:         "imported-package wildcard subscription has no package-subtree candidate and no matching observe grant",
				})
			}
		}
	}
	for _, scope := range source.FlowScopes() {
		packageKey := normalizeImportPackageKey(scope.PackageKey)
		if !importBoundaryPackageImported(source, packageKey) {
			continue
		}
		localEvents := importBoundaryFlowLocalEventSet(source, scope)
		for _, pattern := range importBoundaryFlowWildcardSubscriptions(scope) {
			resolution := ResolveImportBoundaryWildcardSubscription(source, packageKey, scope.ID, importBoundaryBasePathForFlow(source, scope.ID), localEvents, pattern)
			if resolution.Scoped && len(resolution.Patterns) == 0 {
				issues = append(issues, ImportBoundaryWildcardIssue{
					Kind:            "ungranted_or_unknown_subscription",
					ChildPackageKey: packageKey,
					EventPattern:    eventidentity.Normalize(pattern),
					Message:         "imported-package wildcard subscription has no package-subtree candidate and no matching observe grant",
				})
			}
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		return strings.Compare(importBoundaryWildcardIssueSortKey(issues[i]), importBoundaryWildcardIssueSortKey(issues[j])) < 0
	})
	return issues
}

func RuntimeEventOwners(source Source, eventType string) []string {
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return nil
	}
	owners := importBoundaryRuntimeDirectOwners(bundle, eventType)
	for nodeID, handlers := range bundle.Semantics.NodeHandlers {
		for pattern := range handlers {
			pattern = eventidentity.Normalize(pattern)
			if pattern == "" || !strings.Contains(pattern, "*") {
				continue
			}
			matched, scoped := ImportBoundaryWildcardSubscriptionMatchesNode(source, nodeID, pattern, eventType)
			if scoped {
				if matched {
					owners = appendUniqueStringLocal(owners, strings.TrimSpace(nodeID))
				}
				continue
			}
			nodeSource, _ := source.NodeContractSource(nodeID)
			basePath := importBoundaryBasePathForFlow(source, nodeSource.FlowID)
			localEvents := importBoundaryNodeLocalEvents(source, nodeID, nodeSource)
			resolved := importBoundaryResolvePatternInScope(importBoundaryWildcardScope{
				Path:        basePath,
				LocalEvents: localEvents,
			}, pattern)
			if resolved == "" {
				resolved = pattern
			}
			if eventidentity.MatchPattern(resolved, eventType) {
				owners = appendUniqueStringLocal(owners, strings.TrimSpace(nodeID))
			}
		}
	}
	sort.Strings(owners)
	return owners
}

func importBoundaryProjectWildcardSubscriptions(scope ProjectScope) []string {
	seen := map[string]struct{}{}
	appendPattern := func(pattern string) {
		pattern = eventidentity.Normalize(pattern)
		if pattern == "" || !strings.Contains(pattern, "*") {
			return
		}
		seen[pattern] = struct{}{}
	}
	for _, entry := range scope.Agents {
		for _, pattern := range append(append([]string{}, entry.Subscriptions...), entry.SubscribesTo...) {
			appendPattern(pattern)
		}
	}
	for _, entry := range scope.Nodes {
		for _, pattern := range runtimecontracts.EffectiveSystemNodeSubscriptions(entry) {
			appendPattern(pattern)
		}
	}
	return sortedStringSet(seen)
}

func importBoundaryFlowWildcardSubscriptions(scope FlowScope) []string {
	seen := map[string]struct{}{}
	appendPattern := func(pattern string) {
		pattern = eventidentity.Normalize(pattern)
		if pattern == "" || !strings.Contains(pattern, "*") {
			return
		}
		seen[pattern] = struct{}{}
	}
	for _, pattern := range scope.InputEvents {
		appendPattern(pattern)
	}
	for _, entry := range scope.Agents {
		for _, pattern := range append(append([]string{}, entry.Subscriptions...), entry.SubscribesTo...) {
			appendPattern(pattern)
		}
	}
	for _, entry := range scope.Nodes {
		for _, pattern := range runtimecontracts.EffectiveSystemNodeSubscriptions(entry) {
			appendPattern(pattern)
		}
	}
	return sortedStringSet(seen)
}

func importBoundaryRuntimeDirectOwners(bundle *runtimecontracts.WorkflowContractBundle, eventType string) []string {
	if bundle == nil {
		return nil
	}
	if owners := bundle.Semantics.EventOwners[eventType]; len(owners) > 0 {
		return append([]string{}, owners...)
	}
	if strings.Contains(eventType, "/") {
		return nil
	}
	var matched []string
	for canonical, owners := range bundle.Semantics.EventOwners {
		canonical = eventidentity.Normalize(canonical)
		if eventidentity.LeafName(canonical) != eventType {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = append([]string{}, owners...)
	}
	return matched
}

func importBoundaryWildcardGrantIssues(source Source, parent, child ProjectScope, site importBoundarySite, grant runtimecontracts.FlowPackageObserveGrant) []ImportBoundaryWildcardIssue {
	sourceRef := strings.TrimSpace(grant.Source)
	base := ImportBoundaryWildcardIssue{
		ParentPackageKey: parent.Key,
		ChildPackageKey:  child.Key,
		ImportLabel:      site.Label,
		Source:           sourceRef,
	}
	if sourceRef == "" {
		base.Kind = "empty_source"
		base.Message = "observe grant source is required"
		return []ImportBoundaryWildcardIssue{base}
	}
	if normalizeImportPackageKey(sourceRef) == "." {
		base.Kind = "broad_source"
		base.Message = "observe grant source must name a narrow parent-visible flow or package, not the root package"
		return []ImportBoundaryWildcardIssue{base}
	}
	sourceScopes, kind, message := importBoundaryResolveWildcardGrantSource(source, parent, sourceRef)
	if kind != "" {
		base.Kind = kind
		base.Message = message
		return []ImportBoundaryWildcardIssue{base}
	}
	if len(grant.Events) == 0 {
		base.Kind = "empty_events"
		base.Message = "observe grant events list is required"
		return []ImportBoundaryWildcardIssue{base}
	}
	var issues []ImportBoundaryWildcardIssue
	for _, eventPattern := range grant.Events {
		eventPattern = eventidentity.Normalize(eventPattern)
		issue := base
		issue.EventPattern = eventPattern
		if eventPattern == "" {
			issue.Kind = "empty_event_pattern"
			issue.Message = "observe grant event pattern is required"
			issues = append(issues, issue)
			continue
		}
		if importBoundaryWildcardGrantPatternBroad(eventPattern) {
			issue.Kind = "broad_event_pattern"
			issue.Message = "observe grant event pattern must include a concrete event family, not an unbounded wildcard"
			issues = append(issues, issue)
			continue
		}
		if !importBoundaryGrantPatternMatchesKnownEvent(sourceScopes, eventPattern) {
			issue.Kind = "unknown_event_pattern"
			issue.Message = "observe grant event pattern does not match any event under the grant source"
			issues = append(issues, issue)
		}
	}
	return issues
}

func importBoundaryWildcardGrantPatterns(source Source, childPackageKey string) []importBoundaryWildcardGrantPattern {
	childPackageKey = normalizeImportPackageKey(childPackageKey)
	if source == nil || childPackageKey == "" {
		return nil
	}
	projectByKey, _ := importBoundaryScopeIndexes(source)
	var out []importBoundaryWildcardGrantPattern
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			if !importBoundaryPackageInSubtree(childPackageKey, site.PackageKey) || len(site.Bind.Observe) == 0 {
				continue
			}
			child, ok := projectByKey[site.PackageKey]
			if !ok {
				continue
			}
			child.Key = normalizeImportPackageKey(child.Key)
			for _, grant := range site.Bind.Observe {
				sourceScopes, kind, _ := importBoundaryResolveWildcardGrantSource(source, parent, grant.Source)
				if kind != "" {
					continue
				}
				for _, eventPattern := range grant.Events {
					eventPattern = eventidentity.Normalize(eventPattern)
					if eventPattern == "" || importBoundaryWildcardGrantPatternBroad(eventPattern) || !importBoundaryGrantPatternMatchesKnownEvent(sourceScopes, eventPattern) {
						continue
					}
					for _, resolved := range importBoundaryResolveGrantPatterns(sourceScopes, eventPattern) {
						out = append(out, importBoundaryWildcardGrantPattern{
							ParentPackageKey: parent.Key,
							ChildPackageKey:  child.Key,
							ImportLabel:      site.Label,
							Source:           strings.TrimSpace(grant.Source),
							EventPattern:     resolved,
							LocalizedEvent:   eventPattern,
						})
					}
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(importBoundaryWildcardGrantPatternSortKey(out[i]), importBoundaryWildcardGrantPatternSortKey(out[j])) < 0
	})
	return out
}

func importBoundaryDefaultWildcardPatterns(source Source, packageKey, flowID, basePath string, localEvents map[string]struct{}, raw string) []ImportBoundaryWildcardPattern {
	var out []ImportBoundaryWildcardPattern
	for _, scope := range importBoundaryDefaultWildcardScopes(source, packageKey, flowID, basePath, localEvents) {
		pattern := importBoundaryResolvePatternInScope(scope, raw)
		if pattern == "" || !importBoundaryWildcardPatternWithinScope(pattern, scope) {
			continue
		}
		out = appendUniqueImportBoundaryWildcardPattern(out, ImportBoundaryWildcardPattern{
			ChildPackageKey: packageKey,
			EventPattern:    pattern,
			MatchPattern:    raw,
			LocalizedEvent:  raw,
			RouteSource:     "import_boundary_wildcard_subtree",
		})
	}
	return out
}

func importBoundaryDefaultWildcardScopes(source Source, packageKey, flowID, basePath string, localEvents map[string]struct{}) []importBoundaryWildcardScope {
	packageKey = normalizeImportPackageKey(packageKey)
	flowID = strings.TrimSpace(flowID)
	basePath = eventidentity.Normalize(basePath)
	if source == nil || packageKey == "" {
		return nil
	}
	if flowID != "" {
		if basePath == "" {
			basePath = importBoundaryBasePathForFlow(source, flowID)
		}
		events := cloneImportBoundaryEventSet(localEvents)
		if len(events) == 0 {
			if scope, ok := source.FlowScopeByID(flowID); ok {
				events = importBoundaryFlowLocalEventSet(source, scope)
			}
		}
		return []importBoundaryWildcardScope{{
			Kind:        "flow",
			ID:          flowID,
			PackageKey:  packageKey,
			Path:        basePath,
			LocalEvents: events,
		}}
	}
	projectByKey, flowByPackage := importBoundaryScopeIndexes(source)
	project := projectByKey[packageKey]
	if owner := strings.TrimSpace(project.OwningFlowID); owner != "" {
		events := cloneImportBoundaryEventSet(localEvents)
		if len(events) == 0 {
			events = importBoundaryProjectLocalEventSet(project)
		}
		return []importBoundaryWildcardScope{{
			Kind:        "package",
			ID:          packageKey,
			PackageKey:  packageKey,
			Path:        importBoundaryBasePathForFlow(source, owner),
			LocalEvents: events,
		}}
	}
	var out []importBoundaryWildcardScope
	for key, flows := range flowByPackage {
		if !importBoundaryPackageInSubtree(key, packageKey) {
			continue
		}
		for _, flow := range flows {
			out = append(out, importBoundaryWildcardScope{
				Kind:        "flow",
				ID:          strings.TrimSpace(flow.ID),
				PackageKey:  normalizeImportPackageKey(flow.PackageKey),
				Path:        importBoundaryBasePathForFlow(source, flow.ID),
				LocalEvents: importBoundaryFlowLocalEventSet(source, flow),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].Path+"|"+out[i].ID, out[j].Path+"|"+out[j].ID) < 0
	})
	return out
}

func importBoundaryResolveWildcardGrantSource(source Source, parent ProjectScope, raw string) ([]importBoundaryWildcardScope, string, string) {
	raw = strings.TrimSpace(raw)
	if source == nil || raw == "" {
		return nil, "empty_source", "observe grant source is required"
	}
	parent.Key = normalizeImportPackageKey(parent.Key)
	var matches [][]importBoundaryWildcardScope
	seenMatches := map[string]struct{}{}
	appendMatch := func(key string, scopes []importBoundaryWildcardScope) {
		key = strings.TrimSpace(key)
		if key == "" || len(scopes) == 0 {
			return
		}
		if _, ok := seenMatches[key]; ok {
			return
		}
		seenMatches[key] = struct{}{}
		matches = append(matches, scopes)
	}
	for _, site := range importBoundarySites(parent) {
		if strings.TrimSpace(site.FlowID) != raw {
			continue
		}
		if flow, ok := source.FlowScopeByID(site.FlowID); ok {
			flowID := strings.TrimSpace(flow.ID)
			appendMatch("flow:"+flowID, []importBoundaryWildcardScope{{
				Kind:        "flow",
				ID:          flowID,
				PackageKey:  normalizeImportPackageKey(site.PackageKey),
				Path:        importBoundaryBasePathForFlow(source, flow.ID),
				LocalEvents: importBoundaryFlowLocalEventSet(source, flow),
			}})
		}
	}
	for _, flow := range source.FlowScopes() {
		if strings.TrimSpace(flow.ID) != raw {
			continue
		}
		if !importBoundaryPackageInSubtree(flow.PackageKey, parent.Key) {
			continue
		}
		flowID := strings.TrimSpace(flow.ID)
		appendMatch("flow:"+flowID, []importBoundaryWildcardScope{{
			Kind:        "flow",
			ID:          flowID,
			PackageKey:  normalizeImportPackageKey(flow.PackageKey),
			Path:        importBoundaryBasePathForFlow(source, flow.ID),
			LocalEvents: importBoundaryFlowLocalEventSet(source, flow),
		}})
	}
	projectByKey, flowByPackage := importBoundaryScopeIndexes(source)
	if project, ok := projectByKey[normalizeImportPackageKey(raw)]; ok && importBoundaryPackageInSubtree(project.Key, parent.Key) {
		scopes := importBoundaryPackageWildcardScopes(source, project, flowByPackage)
		if len(scopes) > 0 {
			appendMatch("package:"+normalizeImportPackageKey(project.Key), scopes)
		}
	}
	switch len(matches) {
	case 0:
		return nil, "unknown_source", "observe grant source does not resolve to one parent-visible flow or package"
	case 1:
		return matches[0], "", ""
	default:
		return nil, "ambiguous_source", "observe grant source resolves to multiple parent-visible typed sources"
	}
}

func importBoundaryPackageWildcardScopes(source Source, project ProjectScope, flowByPackage map[string][]FlowScope) []importBoundaryWildcardScope {
	project.Key = normalizeImportPackageKey(project.Key)
	if source == nil || project.Key == "" {
		return nil
	}
	if owner := strings.TrimSpace(project.OwningFlowID); owner != "" {
		return []importBoundaryWildcardScope{{
			Kind:        "package",
			ID:          project.Key,
			PackageKey:  project.Key,
			Path:        importBoundaryBasePathForFlow(source, owner),
			LocalEvents: importBoundaryProjectLocalEventSet(project),
		}}
	}
	var out []importBoundaryWildcardScope
	for key, flows := range flowByPackage {
		if !importBoundaryPackageInSubtree(key, project.Key) {
			continue
		}
		for _, flow := range flows {
			out = append(out, importBoundaryWildcardScope{
				Kind:        "flow",
				ID:          strings.TrimSpace(flow.ID),
				PackageKey:  normalizeImportPackageKey(flow.PackageKey),
				Path:        importBoundaryBasePathForFlow(source, flow.ID),
				LocalEvents: importBoundaryFlowLocalEventSet(source, flow),
			})
		}
	}
	return out
}

func importBoundaryGrantPatternMatchesKnownEvent(scopes []importBoundaryWildcardScope, raw string) bool {
	for _, scope := range scopes {
		pattern := importBoundaryResolvePatternInScope(scope, raw)
		if pattern == "" {
			continue
		}
		for eventType := range scope.LocalEvents {
			concrete := importBoundaryResolveEventInScope(scope, eventType)
			if concrete != "" && eventidentity.MatchPattern(pattern, concrete) {
				return true
			}
		}
	}
	return false
}

func importBoundaryResolveGrantPatterns(scopes []importBoundaryWildcardScope, raw string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, scope := range scopes {
		pattern := importBoundaryResolvePatternInScope(scope, raw)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}
	sort.Strings(out)
	return out
}

func importBoundaryResolvePatternInScope(scope importBoundaryWildcardScope, raw string) string {
	raw = eventidentity.Normalize(raw)
	if raw == "" {
		return ""
	}
	identityScope := rawWildcardScope(scope.Path, scope.LocalEvents)
	pattern := eventidentity.Normalize(identityScope.ResolveSubscriptionPattern(raw, nil))
	if pattern != "" && pattern != raw {
		return pattern
	}
	if strings.Contains(raw, "/") || scope.Path == "" {
		return pattern
	}
	for local := range scope.LocalEvents {
		if eventidentity.MatchPattern(raw, local) {
			return eventidentity.Normalize(scope.Path + "/" + raw)
		}
	}
	return pattern
}

func importBoundaryResolveEventInScope(scope importBoundaryWildcardScope, raw string) string {
	return eventidentity.Normalize(rawWildcardScope(scope.Path, scope.LocalEvents).ResolveEvent(raw, nil))
}

func rawWildcardScope(basePath string, localEvents map[string]struct{}) eventidentity.Scope {
	return eventidentity.Scope{
		Path:        eventidentity.Normalize(basePath),
		LocalEvents: sortedStringSet(localEvents),
	}
}

func importBoundaryWildcardGrantIntersects(raw, grantLocal, grantPattern string) bool {
	raw = eventidentity.Normalize(raw)
	grantLocal = eventidentity.Normalize(grantLocal)
	grantPattern = eventidentity.Normalize(grantPattern)
	if raw == "" {
		return false
	}
	if grantLocal != "" && eventidentity.MatchPattern(raw, grantLocal) {
		return true
	}
	if grantPattern != "" && eventidentity.MatchPattern(raw, grantPattern) {
		return true
	}
	if leaf := eventidentity.LeafName(grantPattern); leaf != "" && eventidentity.MatchPattern(raw, leaf) {
		return true
	}
	return false
}

func importBoundaryWildcardGrantPatternBroad(raw string) bool {
	raw = eventidentity.Normalize(raw)
	if raw == "" || raw == "*" || raw == "**" {
		return true
	}
	leaf := strings.TrimSpace(eventidentity.LeafName(raw))
	if leaf == "" || leaf == "*" || leaf == "**" {
		return true
	}
	return strings.Trim(leaf, "*") == ""
}

func importBoundaryWildcardPatternWithinScope(pattern string, scope importBoundaryWildcardScope) bool {
	pattern = eventidentity.Normalize(pattern)
	base := eventidentity.Normalize(scope.Path)
	if pattern == "" {
		return false
	}
	if base == "" {
		return !strings.Contains(pattern, "/")
	}
	return pattern == base || strings.HasPrefix(pattern, base+"/")
}

func importBoundaryPackageKeyForContext(source Source, packageKey, flowID string) string {
	packageKey = normalizeImportPackageKey(packageKey)
	if packageKey != "." || strings.TrimSpace(flowID) == "" || source == nil {
		return packageKey
	}
	for _, parent := range source.ProjectScopes() {
		for _, site := range importBoundarySites(parent) {
			if strings.TrimSpace(site.FlowID) == strings.TrimSpace(flowID) {
				return normalizeImportPackageKey(site.PackageKey)
			}
		}
	}
	if scope, ok := source.FlowScopeByID(flowID); ok {
		return normalizeImportPackageKey(scope.PackageKey)
	}
	return packageKey
}

func importBoundaryPackageImported(source Source, packageKey string) bool {
	packageKey = normalizeImportPackageKey(packageKey)
	if source == nil || packageKey == "" || packageKey == "." {
		return false
	}
	projectByKey, _ := importBoundaryScopeIndexes(source)
	project, ok := projectByKey[packageKey]
	return ok && project.Depth > 0
}

func importBoundaryPackageInSubtree(candidate, parent string) bool {
	candidate = normalizeImportPackageKey(candidate)
	parent = normalizeImportPackageKey(parent)
	if candidate == "" || parent == "" {
		return false
	}
	if parent == "." {
		return candidate != "."
	}
	return candidate == parent || strings.HasPrefix(candidate, parent+"/")
}

func importBoundaryBasePathForFlow(source Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return ""
	}
	return eventidentity.Normalize(source.FlowPath(flowID))
}

func importBoundaryNodeLocalEvents(source Source, nodeID string, itemSource runtimecontracts.ContractItemSource) map[string]struct{} {
	if source == nil {
		return nil
	}
	if flowID := strings.TrimSpace(itemSource.FlowID); flowID != "" {
		if flow, ok := source.FlowScopeByID(flowID); ok {
			return importBoundaryFlowLocalEventSet(source, flow)
		}
	}
	projectByKey, _ := importBoundaryScopeIndexes(source)
	if project, ok := projectByKey[normalizeImportPackageKey(itemSource.PackageKey)]; ok {
		return importBoundaryProjectLocalEventSet(project)
	}
	return nil
}

func importBoundaryFlowLocalEventSet(source Source, scope FlowScope) map[string]struct{} {
	out := map[string]struct{}{}
	for eventType := range scope.Events {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range scope.OutputEvents {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range scope.InputEvents {
		eventType = eventidentity.Normalize(eventType)
		if eventType == "" || importBoundaryFlowInputHasExternalProducer(source, scope.ID, eventType) {
			continue
		}
		out[eventType] = struct{}{}
	}
	if autoEmit := eventidentity.Normalize(scope.AutoEmitEvent); autoEmit != "" {
		out[autoEmit] = struct{}{}
	}
	return out
}

func importBoundaryFlowInputHasExternalProducer(source Source, flowID, eventType string) bool {
	if source == nil || strings.TrimSpace(flowID) == "" || eventidentity.Normalize(eventType) == "" {
		return false
	}
	return ImportBoundaryInputAliasRequired(source, flowID, eventType) || len(source.ResolveFlowInputAutoWire(flowID, eventType).Patterns) > 0
}

func importBoundaryProjectLocalEventSet(scope ProjectScope) map[string]struct{} {
	out := map[string]struct{}{}
	for eventType := range scope.Events {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
}

func cloneImportBoundaryEventSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for key := range in {
		if key = eventidentity.Normalize(key); key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}

func appendUniqueImportBoundaryWildcardPattern(in []ImportBoundaryWildcardPattern, value ImportBoundaryWildcardPattern) []ImportBoundaryWildcardPattern {
	value.EventPattern = eventidentity.Normalize(value.EventPattern)
	if value.EventPattern == "" {
		return in
	}
	for _, existing := range in {
		if existing.EventPattern == value.EventPattern && existing.RouteSource == value.RouteSource && existing.Source == value.Source {
			return in
		}
	}
	return append(in, value)
}

func appendUniqueStringLocal(in []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return in
	}
	for _, existing := range in {
		if strings.TrimSpace(existing) == value {
			return in
		}
	}
	return append(in, value)
}

func sortImportBoundaryWildcardPatterns(values []ImportBoundaryWildcardPattern) {
	sort.Slice(values, func(i, j int) bool {
		return strings.Compare(importBoundaryWildcardPatternSortKey(values[i]), importBoundaryWildcardPatternSortKey(values[j])) < 0
	})
}

func importBoundaryWildcardPatternSortKey(value ImportBoundaryWildcardPattern) string {
	return strings.Join([]string{
		value.RouteSource,
		value.ParentPackageKey,
		value.ChildPackageKey,
		value.Source,
		value.EventPattern,
	}, "|")
}

func importBoundaryWildcardGrantPatternSortKey(value importBoundaryWildcardGrantPattern) string {
	return strings.Join([]string{
		value.ParentPackageKey,
		value.ChildPackageKey,
		value.ImportLabel,
		value.Source,
		value.EventPattern,
	}, "|")
}

func importBoundaryWildcardIssueSortKey(issue ImportBoundaryWildcardIssue) string {
	return strings.Join([]string{
		issue.Kind,
		issue.ParentPackageKey,
		issue.ChildPackageKey,
		issue.ImportLabel,
		issue.Source,
		issue.EventPattern,
	}, "|")
}
