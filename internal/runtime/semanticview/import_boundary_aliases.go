package semanticview

import (
	"path"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

const (
	ImportBoundaryAliasInput  = "input"
	ImportBoundaryAliasOutput = "output"
)

type ImportBoundaryPinAlias struct {
	Direction        string
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	FlowID           string
	Pin              string
	ParentEvent      string
	EventPattern     string
}

type ImportBoundaryPinAliasIssue struct {
	Direction        string
	Kind             string
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	FlowID           string
	Pin              string
	ParentEvent      string
	Message          string
}

type importBoundarySite struct {
	PackageKey string
	Label      string
	FlowID     string
	Bind       runtimecontracts.FlowPackageBind
}

type ImportBoundaryFlowSite struct {
	PackageKey string
	FlowID     string
}

func ImportBoundaryFlowSites(scope ProjectScope) []ImportBoundaryFlowSite {
	sites := importBoundarySites(scope)
	out := make([]ImportBoundaryFlowSite, 0, len(sites))
	for _, site := range sites {
		if strings.TrimSpace(site.FlowID) == "" {
			continue
		}
		out = append(out, ImportBoundaryFlowSite{
			PackageKey: site.PackageKey,
			FlowID:     site.FlowID,
		})
	}
	return out
}

func ResolveFlowInputAutoWire(source Source, targetFlowID, eventType string) runtimecontracts.FlowInputAutoWireResolution {
	targetFlowID = strings.TrimSpace(targetFlowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || targetFlowID == "" || eventType == "" || !source.FlowHasInputEvent(targetFlowID, eventType) {
		return runtimecontracts.FlowInputAutoWireResolution{EventType: eventType}
	}
	if importRequiresInput(source, targetFlowID, eventType) {
		out := runtimecontracts.FlowInputAutoWireResolution{EventType: eventType}
		seen := map[string]struct{}{}
		for _, alias := range ImportBoundaryInputAliases(source, targetFlowID, eventType) {
			pattern := eventidentity.Normalize(alias.EventPattern)
			if pattern == "" {
				continue
			}
			if _, ok := seen[pattern]; ok {
				continue
			}
			seen[pattern] = struct{}{}
			out.Patterns = append(out.Patterns, pattern)
		}
		sort.Strings(out.Patterns)
		return out
	}
	return rawResolveFlowInputAutoWire(source, targetFlowID, eventType)
}

func FlowInputProducerPatterns(source Source, targetFlowID, eventType string) []string {
	return append([]string{}, ResolveFlowInputAutoWire(source, targetFlowID, eventType).Patterns...)
}

func ImportBoundaryInputAliasRequired(source Source, targetFlowID, eventType string) bool {
	targetFlowID = strings.TrimSpace(targetFlowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || targetFlowID == "" || eventType == "" {
		return false
	}
	return importRequiresInput(source, targetFlowID, eventType)
}

func ImportBoundaryInputAliases(source Source, targetFlowID, eventType string) []ImportBoundaryPinAlias {
	targetFlowID = strings.TrimSpace(targetFlowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || targetFlowID == "" || eventType == "" {
		return nil
	}
	var out []ImportBoundaryPinAlias
	for _, alias := range importBoundaryPinAliases(source) {
		if alias.Direction != ImportBoundaryAliasInput {
			continue
		}
		if strings.TrimSpace(alias.FlowID) != targetFlowID || eventidentity.Normalize(alias.Pin) != eventType {
			continue
		}
		out = append(out, alias)
	}
	sortImportBoundaryAliases(out)
	return out
}

func ImportBoundaryInputAliasesForParentEvent(source Source, targetFlowID, parentEvent string) []ImportBoundaryPinAlias {
	targetFlowID = strings.TrimSpace(targetFlowID)
	parentEvent = eventidentity.Normalize(parentEvent)
	if source == nil || targetFlowID == "" || parentEvent == "" {
		return nil
	}
	var out []ImportBoundaryPinAlias
	for _, alias := range importBoundaryPinAliases(source) {
		if alias.Direction != ImportBoundaryAliasInput {
			continue
		}
		if strings.TrimSpace(alias.FlowID) != targetFlowID {
			continue
		}
		if eventidentity.Normalize(alias.ParentEvent) != parentEvent && eventidentity.Normalize(alias.EventPattern) != parentEvent {
			continue
		}
		out = append(out, alias)
	}
	sortImportBoundaryAliases(out)
	return out
}

func ImportBoundaryOutputAliasesForParent(source Source, parentPackageKey, parentFlowID string) []ImportBoundaryPinAlias {
	parentPackageKey = normalizeImportPackageKey(parentPackageKey)
	parentFlowID = strings.TrimSpace(parentFlowID)
	if source == nil {
		return nil
	}
	var out []ImportBoundaryPinAlias
	for _, alias := range importBoundaryPinAliases(source) {
		if alias.Direction != ImportBoundaryAliasOutput {
			continue
		}
		if parentPackageKey != "" && normalizeImportPackageKey(alias.ParentPackageKey) != parentPackageKey {
			continue
		}
		if parentFlowID != "" && !importBoundaryParentFlowMayConsume(source, parentFlowID, alias.ParentPackageKey) {
			continue
		}
		out = append(out, alias)
	}
	sortImportBoundaryAliases(out)
	return out
}

func ImportBoundaryOutputAliasesForParentEvent(source Source, parentPackageKey, parentFlowID, eventType string) []ImportBoundaryPinAlias {
	parentPackageKey = normalizeImportPackageKey(parentPackageKey)
	parentFlowID = strings.TrimSpace(parentFlowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return nil
	}
	var out []ImportBoundaryPinAlias
	for _, alias := range importBoundaryPinAliases(source) {
		if alias.Direction != ImportBoundaryAliasOutput {
			continue
		}
		if parentPackageKey != "" && normalizeImportPackageKey(alias.ParentPackageKey) != parentPackageKey {
			continue
		}
		if parentFlowID != "" && !importBoundaryParentFlowMayConsume(source, parentFlowID, alias.ParentPackageKey) {
			continue
		}
		if eventidentity.Normalize(alias.ParentEvent) != eventType {
			continue
		}
		out = append(out, alias)
	}
	sortImportBoundaryAliases(out)
	return out
}

func ImportBoundaryOutputParentEventsForEvent(source Source, parentPackageKey, parentFlowID, eventType string) []string {
	parentPackageKey = normalizeImportPackageKey(parentPackageKey)
	parentFlowID = strings.TrimSpace(parentFlowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	for _, alias := range importBoundaryPinAliases(source) {
		if alias.Direction != ImportBoundaryAliasOutput {
			continue
		}
		if parentPackageKey != "" && normalizeImportPackageKey(alias.ParentPackageKey) != parentPackageKey {
			continue
		}
		if parentFlowID != "" && !importBoundaryParentFlowMayConsume(source, parentFlowID, alias.ParentPackageKey) {
			continue
		}
		if eventidentity.Normalize(alias.EventPattern) != eventType {
			continue
		}
		parentEvent := eventidentity.Normalize(alias.ParentEvent)
		if parentEvent == "" {
			continue
		}
		if _, ok := seen[parentEvent]; ok {
			continue
		}
		seen[parentEvent] = struct{}{}
		out = append(out, parentEvent)
	}
	sort.Strings(out)
	return out
}

func ImportBoundaryPinAliasIssues(source Source) []ImportBoundaryPinAliasIssue {
	if source == nil {
		return nil
	}
	projectByKey, flowByPackage := importBoundaryScopeIndexes(source)
	var issues []ImportBoundaryPinAliasIssue
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			child, ok := projectByKey[site.PackageKey]
			if !ok || importRequiresEmpty(child.Manifest.Requires) {
				continue
			}
			flows := importBoundarySiteFlows(source, site, flowByPackage)
			issues = append(issues, importBoundaryDirectionIssues(source, parent, child, site, flows, ImportBoundaryAliasInput, child.Manifest.Requires.Inputs, site.Bind.Inputs)...)
			issues = append(issues, importBoundaryDirectionIssues(source, parent, child, site, flows, ImportBoundaryAliasOutput, child.Manifest.Requires.Outputs, site.Bind.Outputs)...)
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		return strings.Compare(importBoundaryIssueSortKey(issues[i]), importBoundaryIssueSortKey(issues[j])) < 0
	})
	return issues
}

func importBoundaryDirectionIssues(source Source, parent, child ProjectScope, site importBoundarySite, flows []FlowScope, direction string, required []string, bindings map[string]string) []ImportBoundaryPinAliasIssue {
	requiredSet := normalizeImportStringSet(required)
	pinExists := importBoundaryPinSet(flows, direction)
	var issues []ImportBoundaryPinAliasIssue
	for _, pin := range sortedStringSet(requiredSet) {
		if _, ok := pinExists[pin]; !ok {
			issues = append(issues, ImportBoundaryPinAliasIssue{
				Direction:        direction,
				Kind:             "undeclared_package_pin",
				ParentPackageKey: parent.Key,
				ChildPackageKey:  child.Key,
				ImportLabel:      site.Label,
				Pin:              pin,
				Message:          "required package pin is not declared by any imported flow schema",
			})
		}
	}
	for pin, parentEvent := range bindings {
		pin = eventidentity.Normalize(pin)
		parentEvent = eventidentity.Normalize(parentEvent)
		if pin == "" {
			continue
		}
		if _, ok := requiredSet[pin]; !ok {
			issues = append(issues, ImportBoundaryPinAliasIssue{
				Direction:        direction,
				Kind:             "unknown_required_pin",
				ParentPackageKey: parent.Key,
				ChildPackageKey:  child.Key,
				ImportLabel:      site.Label,
				Pin:              pin,
				ParentEvent:      parentEvent,
				Message:          "bind key is not declared in the imported package requires list",
			})
			continue
		}
		matches := importBoundaryParentEventMatches(source, parent, flows, parentEvent)
		if len(matches) == 0 {
			issues = append(issues, ImportBoundaryPinAliasIssue{
				Direction:        direction,
				Kind:             "unknown_parent_event",
				ParentPackageKey: parent.Key,
				ChildPackageKey:  child.Key,
				ImportLabel:      site.Label,
				Pin:              pin,
				ParentEvent:      parentEvent,
				Message:          "bind value does not resolve to a parent-facing event",
			})
			continue
		}
		if len(matches) > 1 {
			issues = append(issues, ImportBoundaryPinAliasIssue{
				Direction:        direction,
				Kind:             "ambiguous_parent_event",
				ParentPackageKey: parent.Key,
				ChildPackageKey:  child.Key,
				ImportLabel:      site.Label,
				Pin:              pin,
				ParentEvent:      parentEvent,
				Message:          "bind value resolves to multiple parent-facing event producers",
			})
		}
	}
	return issues
}

func importBoundaryPinAliases(source Source) []ImportBoundaryPinAlias {
	if source == nil {
		return nil
	}
	projectByKey, flowByPackage := importBoundaryScopeIndexes(source)
	var out []ImportBoundaryPinAlias
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			child, ok := projectByKey[site.PackageKey]
			if !ok || importRequiresEmpty(child.Manifest.Requires) {
				continue
			}
			for _, flow := range importBoundarySiteFlows(source, site, flowByPackage) {
				out = append(out, importBoundaryDirectionAliases(source, parent, child, site, flow, ImportBoundaryAliasInput, child.Manifest.Requires.Inputs, site.Bind.Inputs)...)
				out = append(out, importBoundaryDirectionAliases(source, parent, child, site, flow, ImportBoundaryAliasOutput, child.Manifest.Requires.Outputs, site.Bind.Outputs)...)
			}
		}
	}
	sortImportBoundaryAliases(out)
	return out
}

func importBoundaryDirectionAliases(source Source, parent, child ProjectScope, site importBoundarySite, flow FlowScope, direction string, required []string, bindings map[string]string) []ImportBoundaryPinAlias {
	requiredSet := normalizeImportStringSet(required)
	pinSet := importBoundaryPinSet([]FlowScope{flow}, direction)
	var out []ImportBoundaryPinAlias
	for _, pin := range sortedStringSet(requiredSet) {
		if _, ok := pinSet[pin]; !ok {
			continue
		}
		parentEvent := eventidentity.Normalize(bindings[pin])
		if parentEvent == "" {
			continue
		}
		pattern := parentEvent
		if direction == ImportBoundaryAliasOutput {
			pattern = eventidentity.Normalize(source.ResolveFlowEventReference(flow.ID, pin))
		}
		out = append(out, ImportBoundaryPinAlias{
			Direction:        direction,
			ParentPackageKey: parent.Key,
			ChildPackageKey:  child.Key,
			ImportLabel:      site.Label,
			FlowID:           strings.TrimSpace(flow.ID),
			Pin:              pin,
			ParentEvent:      parentEvent,
			EventPattern:     pattern,
		})
	}
	return out
}

func importRequiresInput(source Source, flowID, eventType string) bool {
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return false
	}
	projectByKey, _ := importBoundaryScopeIndexes(source)
	project, ok := projectByKey[normalizeImportPackageKey(scope.PackageKey)]
	if ok {
		required := normalizeImportStringSet(project.Manifest.Requires.Inputs)
		if _, ok := required[eventidentity.Normalize(eventType)]; ok {
			return true
		}
	}
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			if strings.TrimSpace(site.FlowID) != flowID {
				continue
			}
			child, ok := projectByKey[site.PackageKey]
			if !ok {
				continue
			}
			required := normalizeImportStringSet(child.Manifest.Requires.Inputs)
			if _, ok := required[eventidentity.Normalize(eventType)]; ok {
				return true
			}
		}
	}
	return false
}

func rawResolveFlowInputAutoWire(source Source, targetFlowID, eventType string) runtimecontracts.FlowInputAutoWireResolution {
	if bundle, ok := Bundle(source); ok && bundle != nil {
		return bundle.ResolveFlowInputAutoWire(targetFlowID, eventType)
	}
	out := runtimecontracts.FlowInputAutoWireResolution{EventType: eventidentity.Normalize(eventType)}
	if source == nil || targetFlowID == "" || out.EventType == "" || !source.FlowHasInputEvent(targetFlowID, out.EventType) {
		return out
	}
	seenPatterns := map[string]struct{}{}
	appendPattern := func(value string) {
		value = eventidentity.Normalize(value)
		if value == "" {
			return
		}
		if _, ok := seenPatterns[value]; ok {
			return
		}
		seenPatterns[value] = struct{}{}
		out.Patterns = append(out.Patterns, value)
	}
	for _, scope := range source.ProjectScopes() {
		if _, ok := scope.Events[out.EventType]; ok {
			appendPattern(out.EventType)
		}
	}
	seenFlows := map[string]struct{}{}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" || flowID == targetFlowID || !source.FlowHasOutputEvent(flowID, out.EventType) {
			continue
		}
		if _, ok := seenFlows[flowID]; ok {
			continue
		}
		seenFlows[flowID] = struct{}{}
		out.ProducerFlows = append(out.ProducerFlows, flowID)
	}
	sort.Strings(out.ProducerFlows)
	if len(out.ProducerFlows) == 1 {
		appendPattern(importBoundaryFlowEventReference(source, out.ProducerFlows[0], out.EventType))
	}
	sort.Strings(out.Patterns)
	return out
}

func importBoundaryScopeIndexes(source Source) (map[string]ProjectScope, map[string][]FlowScope) {
	projectByKey := map[string]ProjectScope{}
	for _, scope := range source.ProjectScopes() {
		scope.Key = normalizeImportPackageKey(scope.Key)
		if scope.Key != "" {
			projectByKey[scope.Key] = scope
		}
	}
	flowByPackage := map[string][]FlowScope{}
	for _, scope := range source.FlowScopes() {
		key := normalizeImportPackageKey(scope.PackageKey)
		if key == "" {
			continue
		}
		flowByPackage[key] = append(flowByPackage[key], scope)
	}
	for key := range flowByPackage {
		sort.Slice(flowByPackage[key], func(i, j int) bool {
			return strings.Compare(flowByPackage[key][i].ID, flowByPackage[key][j].ID) < 0
		})
	}
	return projectByKey, flowByPackage
}

func importBoundarySites(scope ProjectScope) []importBoundarySite {
	scope.Key = normalizeImportPackageKey(scope.Key)
	var sites []importBoundarySite
	for _, flow := range scope.Manifest.Flows {
		flowDir := strings.TrimSpace(flow.Flow)
		if flowDir == "" {
			continue
		}
		sites = append(sites, importBoundarySite{
			PackageKey: joinImportPackageKey(scope.Key, "flows", flowDir),
			Label:      "flow " + strings.TrimSpace(flow.ID),
			FlowID:     strings.TrimSpace(flow.ID),
			Bind:       flow.Bind,
		})
	}
	for _, ref := range scope.Manifest.ChildPackages() {
		location := importBoundaryPackageLocation(ref)
		if location == "" {
			continue
		}
		sites = append(sites, importBoundarySite{
			PackageKey: joinImportPackageKey(scope.Key, location),
			Label:      "package " + location,
			Bind:       ref.Bind,
		})
	}
	sort.Slice(sites, func(i, j int) bool {
		return strings.Compare(sites[i].PackageKey+"|"+sites[i].Label, sites[j].PackageKey+"|"+sites[j].Label) < 0
	})
	return sites
}

func importBoundaryPackageLocation(ref runtimecontracts.ProjectPackageRef) string {
	location := strings.TrimSpace(ref.ResolveLocation())
	if location == "" {
		return ""
	}
	location = strings.Trim(path.Clean(strings.ReplaceAll(location, "\\", "/")), "/")
	if strings.HasSuffix(strings.ToLower(location), ".yaml") {
		location = path.Dir(location)
	}
	if location == "." {
		return ""
	}
	return location
}

func importBoundaryPinSet(flows []FlowScope, direction string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, flow := range flows {
		var pins []string
		if direction == ImportBoundaryAliasOutput {
			pins = flow.OutputEvents
		} else {
			pins = flow.InputEvents
		}
		for _, pin := range pins {
			pin = eventidentity.Normalize(pin)
			if pin != "" {
				out[pin] = struct{}{}
			}
		}
	}
	return out
}

func importBoundaryParentEventMatches(source Source, parent ProjectScope, childFlows []FlowScope, parentEvent string) []string {
	parentEvent = eventidentity.Normalize(parentEvent)
	if source == nil || parentEvent == "" {
		return nil
	}
	excludedFlowIDs := map[string]struct{}{}
	for _, flow := range childFlows {
		if flowID := strings.TrimSpace(flow.ID); flowID != "" {
			excludedFlowIDs[flowID] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	appendMatch := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		seen[value] = struct{}{}
	}
	if _, ok := parent.Events[parentEvent]; ok {
		appendMatch("project:" + normalizeImportPackageKey(parent.Key))
	}
	for _, flow := range source.FlowScopes() {
		if _, excluded := excludedFlowIDs[strings.TrimSpace(flow.ID)]; excluded {
			continue
		}
		for _, output := range flow.OutputEvents {
			output = eventidentity.Normalize(output)
			if output == "" {
				continue
			}
			if output == parentEvent || eventidentity.Normalize(importBoundaryFlowEventReference(source, flow.ID, output)) == parentEvent {
				appendMatch("flow:" + strings.TrimSpace(flow.ID))
			}
		}
	}
	return sortedStringSet(seen)
}

func importBoundarySiteFlows(source Source, site importBoundarySite, flowByPackage map[string][]FlowScope) []FlowScope {
	if flowID := strings.TrimSpace(site.FlowID); flowID != "" {
		if flow, ok := source.FlowScopeByID(flowID); ok {
			return []FlowScope{flow}
		}
		return nil
	}
	return flowByPackage[site.PackageKey]
}

func importBoundaryFlowEventReference(source Source, flowID, eventType string) string {
	if source == nil {
		return eventidentity.Normalize(eventType)
	}
	if ref := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType)); ref != "" {
		return ref
	}
	flowPath := eventidentity.Normalize(source.FlowPath(flowID))
	eventType = eventidentity.Normalize(eventType)
	if flowPath == "" {
		return eventType
	}
	if eventType == "" {
		return flowPath
	}
	return flowPath + "/" + eventType
}

func importBoundaryParentFlowMayConsume(source Source, parentFlowID, parentPackageKey string) bool {
	if parentFlowID == "" {
		return true
	}
	scope, ok := source.FlowScopeByID(parentFlowID)
	if !ok {
		return false
	}
	return normalizeImportPackageKey(scope.PackageKey) == normalizeImportPackageKey(parentPackageKey)
}

func normalizeImportStringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = eventidentity.Normalize(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func importRequiresEmpty(requires runtimecontracts.FlowPackageRequires) bool {
	return len(requires.Inputs) == 0 &&
		len(requires.Outputs) == 0 &&
		len(requires.Policy) == 0 &&
		len(requires.Credentials) == 0 &&
		strings.TrimSpace(requires.PlatformVersion) == ""
}

func joinImportPackageKey(base string, segments ...string) string {
	parts := make([]string, 0, 1+len(segments))
	if base = normalizeImportPackageKey(base); base != "" && base != "." {
		parts = append(parts, base)
	}
	for _, segment := range segments {
		segment = strings.Trim(path.Clean(strings.ReplaceAll(strings.TrimSpace(segment), "\\", "/")), "/")
		if segment == "" || segment == "." {
			continue
		}
		parts = append(parts, segment)
	}
	if len(parts) == 0 {
		return "."
	}
	return normalizeImportPackageKey(path.Join(parts...))
}

func normalizeImportPackageKey(key string) string {
	key = strings.Trim(path.Clean(strings.ReplaceAll(strings.TrimSpace(key), "\\", "/")), "/")
	if key == "" || key == "." {
		return "."
	}
	return key
}

func sortedStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func sortImportBoundaryAliases(values []ImportBoundaryPinAlias) {
	sort.Slice(values, func(i, j int) bool {
		return strings.Compare(importBoundaryAliasSortKey(values[i]), importBoundaryAliasSortKey(values[j])) < 0
	})
}

func importBoundaryAliasSortKey(alias ImportBoundaryPinAlias) string {
	return strings.Join([]string{
		alias.Direction,
		alias.ParentPackageKey,
		alias.ChildPackageKey,
		alias.FlowID,
		alias.Pin,
		alias.ParentEvent,
		alias.EventPattern,
	}, "|")
}

func importBoundaryIssueSortKey(issue ImportBoundaryPinAliasIssue) string {
	return strings.Join([]string{
		issue.Direction,
		issue.Kind,
		issue.ParentPackageKey,
		issue.ChildPackageKey,
		issue.FlowID,
		issue.Pin,
		issue.ParentEvent,
	}, "|")
}
