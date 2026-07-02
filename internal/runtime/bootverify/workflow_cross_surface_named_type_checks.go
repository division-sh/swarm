package bootverify

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	crossSurfaceNamedTypeUseCheckID = "cross_surface_named_type_use"

	crossSurfaceExactDuplicateMinFields     = 3
	crossSurfaceExactDuplicateMinCandidates = 3
	crossSurfaceNearDuplicateMinPairs       = 4
	crossSurfaceNearDuplicateMaxSizeDelta   = 2
	crossSurfaceNearDuplicateMinPercent     = 75
)

type crossSurfaceShapeCandidate struct {
	Label     string
	Location  string
	Fields    map[string]string
	Signature string
	Pairs     []string
}

type crossSurfaceShapeGroup struct {
	Signature  string
	Fields     map[string]string
	Pairs      []string
	Candidates []crossSurfaceShapeCandidate
}

func (c *checkerContext) crossSurfaceNamedTypeUse() []Finding {
	if c.crossSurfaceNamedTypeUseLoaded {
		return c.crossSurfaceNamedTypeUseFindings
	}
	c.crossSurfaceNamedTypeUseLoaded = true
	c.crossSurfaceNamedTypeUseFindings = crossSurfaceNamedTypeUseFindings(c.source)
	return c.crossSurfaceNamedTypeUseFindings
}

func crossSurfaceNamedTypeUseFindings(source semanticview.Source) []Finding {
	candidates := collectCrossSurfaceShapeCandidates(source)
	groups := groupCrossSurfaceShapeCandidates(candidates)

	findings := make([]Finding, 0)
	for _, group := range groups {
		if !crossSurfaceExactDuplicateMeetsThreshold(group) {
			continue
		}
		findings = append(findings, Finding{
			CheckID:  crossSurfaceNamedTypeUseCheckID,
			Severity: SeverityLintEvidence,
			Message: fmt.Sprintf(
				"authored object shape reuse candidate: identical shape %s appears in %s; consider extracting or reusing a named type if these represent the same concept",
				crossSurfaceShapeText(group.Pairs),
				crossSurfaceLabelList(group.Candidates),
			),
			Location: "global",
		})
	}

	for _, near := range nearDuplicateCrossSurfaceShapeFindings(groups) {
		findings = append(findings, near)
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Location == findings[j].Location {
			return findings[i].Message < findings[j].Message
		}
		return findings[i].Location < findings[j].Location
	})
	return findings
}

func crossSurfaceExactDuplicateMeetsThreshold(group crossSurfaceShapeGroup) bool {
	if len(group.Candidates) < 2 {
		return false
	}
	return len(group.Pairs) >= crossSurfaceExactDuplicateMinFields || len(group.Candidates) >= crossSurfaceExactDuplicateMinCandidates
}

func collectCrossSurfaceShapeCandidates(source semanticview.Source) []crossSurfaceShapeCandidate {
	if source == nil {
		return nil
	}
	bundle, _ := semanticview.Bundle(source)
	out := make([]crossSurfaceShapeCandidate, 0)
	seen := map[string]struct{}{}
	add := func(label, location string, fields map[string]string) {
		candidate, ok := newCrossSurfaceShapeCandidate(label, location, fields)
		if !ok {
			return
		}
		key := candidate.Label + "\x00" + candidate.Signature
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		out = append(out, candidate)
	}

	if bundle != nil {
		collectCrossSurfaceTypeCatalogShapes(add, "root", bundle.RootTypeCatalog())
		for _, flowID := range sortedFlowIDs(source) {
			if doc, ok := bundle.FlowTypeCatalogByID(flowID); ok {
				collectCrossSurfaceTypeCatalogShapes(add, "flow "+flowID, doc)
			}
		}
		for _, entity := range sortedEntityContracts(bundle.RootEntityContracts()) {
			add("entity root."+entity.EntityType, "entity root."+entity.EntityType, crossSurfaceEntityFields(entity.Contract))
		}
		for _, flowID := range sortedFlowIDs(source) {
			entities, ok := bundle.FlowEntityContractsByID(flowID)
			if !ok {
				continue
			}
			for _, entity := range sortedEntityContracts(entities) {
				add("entity "+flowID+"."+entity.EntityType, "entity "+flowID+"."+entity.EntityType, crossSurfaceEntityFields(entity.Contract))
			}
		}
	}

	for _, eventType := range sortedEventTypes(source.ResolvedEventCatalog()) {
		entry := source.ResolvedEventCatalog()[eventType]
		add("event "+eventType+" payload", "event "+eventType, crossSurfaceEventPayloadFields(entry))
	}

	collectRootPolicy := true
	for _, scope := range sortedProjectScopes(source.ProjectScopes()) {
		collectRootPolicy = false
		scopeLabel := strings.TrimSpace(scope.Key)
		if scopeLabel == "" {
			scopeLabel = "root"
		}
		collectCrossSurfacePolicyShapes(add, "policy project "+scopeLabel, scope.Policy)
	}
	if collectRootPolicy && bundle != nil {
		collectCrossSurfacePolicyShapes(add, "policy root", bundle.Policy)
	}
	for _, scope := range sortedFlowScopes(source.FlowScopes()) {
		if strings.TrimSpace(scope.ID) == "" {
			continue
		}
		collectCrossSurfacePolicyShapes(add, "policy flow "+strings.TrimSpace(scope.ID), scope.Policy)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Label < out[j].Label
	})
	return out
}

func collectCrossSurfaceTypeCatalogShapes(add func(string, string, map[string]string), scope string, doc runtimecontracts.TypeCatalogDocument) {
	typeNames := make([]string, 0, len(doc.Types))
	for typeName := range doc.Types {
		typeNames = append(typeNames, strings.TrimSpace(typeName))
	}
	sort.Strings(typeNames)
	for _, typeName := range typeNames {
		if typeName == "" {
			continue
		}
		add("type "+scope+"."+typeName, "type "+scope+"."+typeName, crossSurfaceNamedTypeFields(doc.Types[typeName]))
	}
}

func collectCrossSurfacePolicyShapes(add func(string, string, map[string]string), scope string, policy runtimecontracts.PolicyDocument) {
	keys := make([]string, 0, len(policy.Values))
	for key := range policy.Values {
		keys = append(keys, strings.TrimSpace(key))
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			continue
		}
		walkCrossSurfacePolicyValue(add, scope, key, policy.Values[key].Value)
	}
}

func walkCrossSurfacePolicyValue(add func(string, string, map[string]string), scope, path string, value any) {
	if fields, ok := crossSurfacePolicyObjectFields(value); ok {
		add(scope+" "+path, scope+" "+path, fields)
	}
	if items, ok := policyListValue(value); ok {
		for _, item := range items {
			if fields, ok := crossSurfacePolicyObjectFields(item); ok {
				add(scope+" "+path+"[]", scope+" "+path+"[]", fields)
			}
			walkCrossSurfacePolicyValue(add, scope, path+"[]", item)
		}
		return
	}
	children, ok := policyMapValue(value)
	if !ok {
		return
	}
	keys := make([]string, 0, len(children))
	for key := range children {
		keys = append(keys, strings.TrimSpace(key))
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			continue
		}
		walkCrossSurfacePolicyValue(add, scope, path+"."+key, children[key])
	}
}

func newCrossSurfaceShapeCandidate(label, location string, fields map[string]string) (crossSurfaceShapeCandidate, bool) {
	label = strings.TrimSpace(label)
	location = strings.TrimSpace(location)
	if label == "" || len(fields) < 2 {
		return crossSurfaceShapeCandidate{}, false
	}
	normalized := normalizeCrossSurfaceShapeFields(fields)
	if len(normalized) < 2 {
		return crossSurfaceShapeCandidate{}, false
	}
	pairs := crossSurfaceFieldPairs(normalized)
	return crossSurfaceShapeCandidate{
		Label:     label,
		Location:  location,
		Fields:    normalized,
		Signature: strings.Join(pairs, "|"),
		Pairs:     pairs,
	}, true
}

func crossSurfaceNamedTypeFields(decl runtimecontracts.NamedTypeDecl) map[string]string {
	fields := make(map[string]string, len(decl.Fields))
	for name, spec := range decl.Fields {
		fields[name] = spec.Type
	}
	return fields
}

func crossSurfaceEventPayloadFields(entry runtimecontracts.EventCatalogEntry) map[string]string {
	fields := make(map[string]string, len(entry.Payload.Properties))
	for name, spec := range entry.Payload.Properties {
		fields[name] = spec.Type
	}
	return fields
}

func crossSurfaceEntityFields(contract runtimecontracts.EntityContract) map[string]string {
	fields := make(map[string]string, len(contract.Fields))
	for name, spec := range contract.Fields {
		fields[name] = spec.Type
	}
	return fields
}

func normalizeCrossSurfaceShapeFields(fields map[string]string) map[string]string {
	out := make(map[string]string, len(fields))
	for name, typeRef := range fields {
		name = strings.TrimSpace(name)
		typeRef = normalizeCrossSurfaceTypeRef(typeRef)
		if name == "" || strings.HasPrefix(name, "_") || typeRef == "" {
			continue
		}
		out[name] = typeRef
	}
	return out
}

func normalizeCrossSurfaceTypeRef(typeRef string) string {
	typeRef = strings.Join(strings.Fields(strings.TrimSpace(typeRef)), " ")
	if strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") {
		return "[" + strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(typeRef, "["), "]")) + "]"
	}
	return typeRef
}

func crossSurfaceFieldPairs(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+":"+fields[key])
	}
	return pairs
}

func groupCrossSurfaceShapeCandidates(candidates []crossSurfaceShapeCandidate) []crossSurfaceShapeGroup {
	bySignature := map[string]*crossSurfaceShapeGroup{}
	for _, candidate := range candidates {
		group := bySignature[candidate.Signature]
		if group == nil {
			fields := make(map[string]string, len(candidate.Fields))
			for key, value := range candidate.Fields {
				fields[key] = value
			}
			group = &crossSurfaceShapeGroup{
				Signature: candidate.Signature,
				Fields:    fields,
				Pairs:     append([]string{}, candidate.Pairs...),
			}
			bySignature[candidate.Signature] = group
		}
		group.Candidates = append(group.Candidates, candidate)
	}
	groups := make([]crossSurfaceShapeGroup, 0, len(bySignature))
	for _, group := range bySignature {
		sort.Slice(group.Candidates, func(i, j int) bool {
			return group.Candidates[i].Label < group.Candidates[j].Label
		})
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Signature == groups[j].Signature {
			return crossSurfaceLabelList(groups[i].Candidates) < crossSurfaceLabelList(groups[j].Candidates)
		}
		return groups[i].Signature < groups[j].Signature
	})
	return groups
}

func nearDuplicateCrossSurfaceShapeFindings(groups []crossSurfaceShapeGroup) []Finding {
	findings := make([]Finding, 0)
	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			common, total, commonPairs, ok := conservativeNearDuplicateShape(groups[i], groups[j])
			if !ok {
				continue
			}
			findings = append(findings, Finding{
				CheckID:  crossSurfaceNamedTypeUseCheckID,
				Severity: SeverityLintEvidence,
				Message: fmt.Sprintf(
					"authored object shape reuse candidate: near-duplicate shapes share %d/%d field/type pairs (%s) between %s and %s; consider extracting or reusing a named type if these represent the same concept",
					common,
					total,
					strings.Join(commonPairs, ", "),
					crossSurfaceLabelList(groups[i].Candidates),
					crossSurfaceLabelList(groups[j].Candidates),
				),
				Location: "global",
			})
		}
	}
	return findings
}

func conservativeNearDuplicateShape(left, right crossSurfaceShapeGroup) (int, int, []string, bool) {
	if left.Signature == right.Signature || len(left.Pairs) < crossSurfaceNearDuplicateMinPairs || len(right.Pairs) < crossSurfaceNearDuplicateMinPairs {
		return 0, 0, nil, false
	}
	leftPairs := map[string]struct{}{}
	for _, pair := range left.Pairs {
		leftPairs[pair] = struct{}{}
	}
	commonPairs := make([]string, 0)
	for _, pair := range right.Pairs {
		if _, ok := leftPairs[pair]; ok {
			commonPairs = append(commonPairs, pair)
		}
	}
	common := len(commonPairs)
	total := maxInt(len(left.Pairs), len(right.Pairs))
	if common < crossSurfaceNearDuplicateMinPairs ||
		total-common > crossSurfaceNearDuplicateMaxSizeDelta ||
		common*100 < total*crossSurfaceNearDuplicateMinPercent {
		return 0, 0, nil, false
	}
	sort.Strings(commonPairs)
	return common, total, commonPairs, true
}

func crossSurfacePolicyObjectFields(value any) (map[string]string, bool) {
	children, ok := policyMapValue(value)
	if !ok || len(children) < 2 {
		return nil, false
	}
	fields := make(map[string]string, len(children))
	for key, child := range children {
		key = strings.TrimSpace(key)
		if key == "" || strings.HasPrefix(key, "_") {
			continue
		}
		typeRef := inferCrossSurfacePolicyType(child)
		if typeRef == "" || typeRef == "object" || typeRef == "[object]" {
			return nil, false
		}
		fields[key] = typeRef
	}
	return fields, len(fields) >= 2
}

func inferCrossSurfacePolicyType(value any) string {
	if _, ok := policyMapValue(value); ok {
		return "object"
	}
	if items, ok := policyListValue(value); ok {
		if len(items) == 0 {
			return "[]"
		}
		itemTypes := map[string]struct{}{}
		for _, item := range items {
			itemTypes[inferCrossSurfacePolicyType(item)] = struct{}{}
		}
		if len(itemTypes) == 1 {
			for itemType := range itemTypes {
				return "[" + itemType + "]"
			}
		}
		return "[mixed]"
	}
	switch value.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		return "numeric"
	case nil:
		return "null"
	default:
		return "text"
	}
}

func policyMapValue(value any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	if typed, ok := value.(map[string]any); ok {
		return typed, true
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, true
}

func policyListValue(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	if typed, ok := value.([]any); ok {
		return typed, true
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out = append(out, rv.Index(i).Interface())
	}
	return out, true
}

func sortedFlowIDs(source semanticview.Source) []string {
	seen := map[string]struct{}{}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		seen[flowID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for flowID := range seen {
		out = append(out, flowID)
	}
	sort.Strings(out)
	return out
}

func sortedEventTypes(events map[string]runtimecontracts.EventCatalogEntry) []string {
	out := make([]string, 0, len(events))
	for eventType := range events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

func sortedEntityContracts(entities runtimecontracts.EntityContractsDocument) []struct {
	EntityType string
	Contract   runtimecontracts.EntityContract
} {
	keys := make([]string, 0, len(entities))
	for entityType := range entities {
		entityType = strings.TrimSpace(entityType)
		if entityType != "" {
			keys = append(keys, entityType)
		}
	}
	sort.Strings(keys)
	out := make([]struct {
		EntityType string
		Contract   runtimecontracts.EntityContract
	}, 0, len(keys))
	for _, entityType := range keys {
		out = append(out, struct {
			EntityType string
			Contract   runtimecontracts.EntityContract
		}{EntityType: entityType, Contract: entities[entityType]})
	}
	return out
}

func sortedProjectScopes(scopes []semanticview.ProjectScope) []semanticview.ProjectScope {
	out := append([]semanticview.ProjectScope{}, scopes...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}

func sortedFlowScopes(scopes []semanticview.FlowScope) []semanticview.FlowScope {
	out := append([]semanticview.FlowScope{}, scopes...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func crossSurfaceLabelList(candidates []crossSurfaceShapeCandidate) string {
	labels := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		labels = append(labels, candidate.Label)
	}
	sort.Strings(labels)
	const maxLabels = 8
	if len(labels) <= maxLabels {
		return strings.Join(labels, "; ")
	}
	return strings.Join(labels[:maxLabels], "; ") + fmt.Sprintf("; and %d more", len(labels)-maxLabels)
}

func crossSurfaceShapeText(pairs []string) string {
	return "{" + strings.Join(pairs, ", ") + "}"
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
