package bootverify

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

type nodeStateTypedCounterpart struct {
	FieldName     string
	TypeRef       string
	NamedListItem string
	Surface       string
}

func (c *checkerContext) nodeStateSchemaTypedCounterpart() []Finding {
	if c.nodeStateSchemaLoaded {
		return c.nodeStateSchemaFindings
	}
	c.nodeStateSchemaLoaded = true

	nodes := c.source.NodeEntries()
	for _, nodeID := range sortedNodeIDs(c.source) {
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		flowID := ""
		if sourceRef, ok := c.source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		types := nodeStateResolvedTypes(c.source, flowID)
		counterparts := nodeStateTypedCounterparts(c.source, flowID)
		for _, field := range node.StateSchema.Fields {
			fieldName := strings.TrimSpace(field.Name)
			if fieldName == "" {
				continue
			}
			normalizedType, err := runtimecontracts.NormalizeNodeStateFieldType(field.Type)
			if err != nil {
				c.nodeStateSchemaFindings = append(c.nodeStateSchemaFindings, Finding{
					CheckID:  "node_state_schema_typed_counterpart",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("flow %s node %s state_schema field %s has unsupported type %q: %v", defaultFlowLabel(flowID), nodeID, fieldName, strings.TrimSpace(field.Type), err),
					Location: nodeID,
				})
				continue
			}
			if namedType, ok := runtimecontracts.NodeStateNamedTypeName(normalizedType); ok {
				if !nodeStateDeclaredTypeCatalogRef(types, namedType) {
					c.nodeStateSchemaFindings = append(c.nodeStateSchemaFindings, Finding{
						CheckID:  "node_state_schema_typed_counterpart",
						Severity: SeverityHardInvalidity,
						Message:  fmt.Sprintf("flow %s node %s state_schema field %s references undeclared type catalog name %s; node state_schema named references must be declared in types.yaml", defaultFlowLabel(flowID), nodeID, fieldName, namedType),
						Location: nodeID,
					})
				}
				continue
			}
			if !runtimecontracts.IsNodeStateJSONBType(normalizedType) {
				continue
			}
			if counterpart, ok := nodeStateJSONBTypedCounterpart(fieldName, counterparts); ok {
				c.nodeStateSchemaFindings = append(c.nodeStateSchemaFindings, Finding{
					CheckID:  "node_state_schema_typed_counterpart",
					Severity: SeverityHardInvalidity,
					Message:  fmt.Sprintf("flow %s node %s state_schema field %s is jsonb but has typed downstream counterpart %s %s:%s; switch the node state field to the declared named type instead of jsonb", defaultFlowLabel(flowID), nodeID, fieldName, counterpart.Surface, counterpart.FieldName, counterpart.TypeRef),
					Location: nodeID,
				})
				continue
			}
			c.nodeStateSchemaFindings = append(c.nodeStateSchemaFindings, Finding{
				CheckID:  "node_state_schema_typed_counterpart",
				Severity: SeverityLintEvidence,
				Message:  fmt.Sprintf("flow %s node %s state_schema field %s remains jsonb; confirm no typed downstream counterpart exists before accepting this shape-variant state", defaultFlowLabel(flowID), nodeID, fieldName),
				Location: nodeID,
			})
		}
	}
	return c.nodeStateSchemaFindings
}

func nodeStateDeclaredTypeCatalogRef(types runtimecontracts.TypeCatalogDocument, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	if _, ok := types.Types[name]; ok {
		return true
	}
	if _, ok := types.Enums[name]; ok {
		return true
	}
	if _, ok := types.Scalars[name]; ok {
		return true
	}
	return false
}

func nodeStateResolvedTypes(source semanticview.Source, flowID string) runtimecontracts.TypeCatalogDocument {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return runtimecontracts.TypeCatalogDocument{}
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return bundle.RootTypeCatalog()
	}
	return bundle.ResolvedTypeCatalogForFlow(flowID)
}

func nodeStateTypedCounterparts(source semanticview.Source, flowID string) []nodeStateTypedCounterpart {
	out := make([]nodeStateTypedCounterpart, 0)
	addEntity := func(view wave1EntityContractView) {
		if !view.Defined {
			return
		}
		fieldNames := make([]string, 0, len(view.Contract.Fields))
		for fieldName := range view.Contract.Fields {
			fieldNames = append(fieldNames, strings.TrimSpace(fieldName))
		}
		sort.Strings(fieldNames)
		for _, fieldName := range fieldNames {
			if fieldName == "" || wave1EntityEnvelopeField(fieldName) {
				continue
			}
			typeRef := strings.TrimSpace(view.Contract.Fields[fieldName].Type)
			if !nodeStateTypeIsTypedCounterpart(typeRef) {
				continue
			}
			out = append(out, nodeStateTypedCounterpart{
				FieldName:     fieldName,
				TypeRef:       typeRef,
				NamedListItem: nodeStateListNamedType(typeRef, view.Types),
				Surface:       fmt.Sprintf("entity_type %s", view.EntityType),
			})
		}
	}

	if flowID = strings.TrimSpace(flowID); flowID != "" {
		addEntity(wave1EntityContractForFlow(source, flowID))
	}
	addEntity(wave1EntityContractForFlow(source, ""))

	for eventType, entry := range source.ResolvedEventCatalog() {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		fieldNames := make([]string, 0, len(entry.Payload.Properties))
		for fieldName := range entry.Payload.Properties {
			fieldNames = append(fieldNames, strings.TrimSpace(fieldName))
		}
		sort.Strings(fieldNames)
		for _, fieldName := range fieldNames {
			if fieldName == "" {
				continue
			}
			typeRef := strings.TrimSpace(entry.Payload.Properties[fieldName].Type)
			if !nodeStateTypeIsTypedCounterpart(typeRef) {
				continue
			}
			out = append(out, nodeStateTypedCounterpart{
				FieldName: fieldName,
				TypeRef:   typeRef,
				Surface:   fmt.Sprintf("event %s payload", eventType),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Surface == out[j].Surface {
			return out[i].FieldName < out[j].FieldName
		}
		return out[i].Surface < out[j].Surface
	})
	return out
}

func nodeStateTypeIsTypedCounterpart(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return false
	}
	switch strings.ToLower(typeRef) {
	case "json", "jsonb", "object":
		return false
	default:
		return true
	}
}

func nodeStateJSONBTypedCounterpart(fieldName string, counterparts []nodeStateTypedCounterpart) (nodeStateTypedCounterpart, bool) {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return nodeStateTypedCounterpart{}, false
	}
	for _, counterpart := range counterparts {
		if strings.EqualFold(counterpart.FieldName, fieldName) {
			return counterpart, true
		}
	}
	candidates := nodeStateAccumulatorCandidateNames(fieldName)
	for _, counterpart := range counterparts {
		if counterpart.NamedListItem == "" {
			continue
		}
		if nodeStateNamedTypeMatchesAnyCandidate(counterpart.NamedListItem, candidates) {
			return counterpart, true
		}
	}
	return nodeStateTypedCounterpart{}, false
}

func nodeStateListNamedType(typeRef string, types runtimecontracts.TypeCatalogDocument) string {
	item := nodeStateListItemType(typeRef)
	if item == "" {
		return ""
	}
	if scalar, ok := types.Scalars[item]; ok {
		item = strings.TrimSpace(scalar.Base)
	}
	if _, ok := types.Types[item]; ok {
		return item
	}
	return ""
}

func nodeStateListItemType(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">"):
		return strings.TrimSpace(typeRef[len("list<") : len(typeRef)-1])
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		return strings.TrimSpace(typeRef[1 : len(typeRef)-1])
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2])
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:])
	default:
		return ""
	}
}

func nodeStateAccumulatorCandidateNames(fieldName string) []string {
	base := strings.ToLower(strings.TrimSpace(fieldName))
	if base == "" {
		return nil
	}
	suffixes := []string{
		"_received",
		"_completed",
		"_entries",
		"_items",
		"_records",
		"_state",
		"_evidence",
		"_logs",
		"_log",
	}
	candidates := []string{nodeStateNormalizeSemanticName(base)}
	for _, suffix := range suffixes {
		if strings.HasSuffix(base, suffix) {
			candidates = append(candidates, nodeStateNormalizeSemanticName(strings.TrimSuffix(base, suffix)))
		}
	}
	for _, part := range strings.Split(base, "_") {
		candidates = append(candidates, nodeStateNormalizeSemanticName(part))
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates)*2)
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		for _, value := range []string{candidate, nodeStateSingular(candidate)} {
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			out = append(out, value)
		}
	}
	return out
}

func nodeStateNamedTypeMatchesAnyCandidate(namedType string, candidates []string) bool {
	words := nodeStateCamelWords(namedType)
	if len(words) == 0 {
		return false
	}
	firstWord := nodeStateNormalizeSemanticName(words[0])
	fullName := nodeStateNormalizeSemanticName(strings.Join(words, ""))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if candidate == firstWord || strings.HasPrefix(fullName, candidate) {
			return true
		}
	}
	return false
}

func nodeStateCamelWords(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	words := make([]string, 0, 4)
	var current []rune
	for i, r := range value {
		if i > 0 && unicode.IsUpper(r) && len(current) > 0 {
			words = append(words, string(current))
			current = current[:0]
		}
		current = append(current, r)
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	return words
}

func nodeStateNormalizeSemanticName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func nodeStateSingular(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 3 && strings.HasSuffix(value, "ies") {
		return strings.TrimSuffix(value, "ies") + "y"
	}
	if len(value) > 1 && strings.HasSuffix(value, "s") {
		return strings.TrimSuffix(value, "s")
	}
	return value
}
