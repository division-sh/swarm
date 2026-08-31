package accprojection

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var ReservedAccumulatorMetadata = map[string]struct{}{
	"event_id":    {},
	"event_type":  {},
	"source":      {},
	"received_at": {},
}

type Binding struct {
	FlowID          string
	EntityType      string
	TargetField     string
	TargetDecl      runtimecontracts.EntityFieldDecl
	SourceNode      runtimeidentity.ExecutableNode
	SourceEventType string
	AccumulatorName string
	SourceField     runtimecontracts.NodeStateField
	SourceItemType  string
	SourceType      runtimecontracts.ResolvedCatalogType
	TargetItemType  string
	TargetType      runtimecontracts.ResolvedCatalogType
	Project         map[string]any
}

type Issue struct {
	Code            string
	Message         string
	Location        string
	FlowID          string
	SourceNode      runtimeidentity.ExecutableNode
	SourceEventType string
	AccumulatorName string
}

type Result struct {
	Bindings []Binding
	Issues   []Issue
}

type HandlerResult struct {
	Bindings                 []Binding
	Issues                   []Issue
	ExpectedBindingCount     int
	ActiveAccumulatorName    string
	ActiveAuthoredEventType  string
	ActiveCanonicalEventType string
}

func Resolve(source semanticview.Source) Result {
	if source == nil {
		return Result{}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return Result{Issues: []Issue{{Code: "bundle_unavailable", Message: "materialize_from resolver requires a workflow contract bundle"}}}
	}
	var result Result
	for _, target := range declaredMaterializedFields(source, bundle) {
		binding, issues := resolveTarget(source, bundle, target)
		result.Issues = append(result.Issues, issues...)
		if len(issues) == 0 {
			result.Bindings = append(result.Bindings, binding)
		}
	}
	sort.Slice(result.Bindings, func(i, j int) bool {
		if result.Bindings[i].FlowID == result.Bindings[j].FlowID {
			return result.Bindings[i].TargetField < result.Bindings[j].TargetField
		}
		return result.Bindings[i].FlowID < result.Bindings[j].FlowID
	})
	return result
}

func ForExecutableHandler(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) ([]Binding, []Issue) {
	result := ForExecutableHandlerWithAccumulator(source, node, eventType, "")
	return result.Bindings, result.Issues
}

func ForExecutableHandlerWithAccumulator(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType, activeAccumulatorName string) HandlerResult {
	resolved := Resolve(source)
	result := HandlerResult{}
	out := make([]Binding, 0)
	eventType = normalizeEventType(eventType)
	activeAccumulatorName = strings.TrimSpace(activeAccumulatorName)
	active := activeHandlerResolution(source, node, eventType)
	if activeAccumulatorName == "" {
		activeAccumulatorName = active.AccumulatorName
	}
	result.ActiveAccumulatorName = activeAccumulatorName
	result.ActiveAuthoredEventType = active.AuthoredEventType
	result.ActiveCanonicalEventType = active.CanonicalEventType
	for _, binding := range resolved.Bindings {
		if bindingMatchesActiveAccumulator(binding, node, activeAccumulatorName) {
			result.ExpectedBindingCount++
		}
		if binding.SourceNode.Equal(node) &&
			handlerEventMatches(source, node, binding.SourceEventType, eventType, active) {
			out = append(out, binding)
		}
	}
	issues := make([]Issue, 0)
	for _, issue := range resolved.Issues {
		if issueMatchesActiveAccumulator(issue, node, activeAccumulatorName) {
			result.ExpectedBindingCount++
		}
		if issueRelevantToHandler(source, issue, node, eventType, activeAccumulatorName, active) {
			issues = append(issues, issue)
		}
	}
	result.Bindings = out
	result.Issues = issues
	return result
}

type activeHandler struct {
	AccumulatorName    string
	AuthoredEventType  string
	CanonicalEventType string
}

func activeHandlerResolution(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) activeHandler {
	var active activeHandler
	if source == nil {
		return active
	}
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
	if resolved.Matched {
		active.AuthoredEventType = resolved.HandlerEventKey
		active.CanonicalEventType = canonicalNodeEvent(source, node, active.AuthoredEventType)
		if resolved.Handler.Accumulate != nil {
			active.AccumulatorName = strings.TrimSpace(resolved.Handler.Accumulate.Into)
		}
		return active
	}
	return active
}

func issueRelevantToHandler(source semanticview.Source, issue Issue, node runtimeidentity.ExecutableNode, eventType, accumulatorName string, active activeHandler) bool {
	if !issue.SourceNode.Equal(node) {
		return false
	}
	if issueFlowID := strings.TrimSpace(issue.FlowID); issueFlowID != "" && issueFlowID != node.FlowID() {
		return false
	}
	if issueEventType := strings.TrimSpace(issue.SourceEventType); issueEventType != "" {
		return handlerEventMatches(source, node, issueEventType, eventType, active)
	}
	if issueAccumulatorName := strings.TrimSpace(issue.AccumulatorName); issueAccumulatorName != "" {
		return issueAccumulatorName == strings.TrimSpace(accumulatorName)
	}
	return false
}

func bindingMatchesActiveAccumulator(binding Binding, node runtimeidentity.ExecutableNode, accumulatorName string) bool {
	if strings.TrimSpace(accumulatorName) == "" {
		return false
	}
	return binding.SourceNode.Equal(node) &&
		strings.TrimSpace(binding.AccumulatorName) == strings.TrimSpace(accumulatorName)
}

func issueMatchesActiveAccumulator(issue Issue, node runtimeidentity.ExecutableNode, accumulatorName string) bool {
	if strings.TrimSpace(accumulatorName) == "" {
		return false
	}
	if strings.TrimSpace(issue.FlowID) != "" && strings.TrimSpace(issue.FlowID) != node.FlowID() {
		return false
	}
	return issue.SourceNode.Equal(node) &&
		strings.TrimSpace(issue.AccumulatorName) == strings.TrimSpace(accumulatorName)
}

func handlerEventMatches(source semanticview.Source, node runtimeidentity.ExecutableNode, authoredEventType, runtimeEventType string, active activeHandler) bool {
	authoredEventType = normalizeEventType(authoredEventType)
	runtimeEventType = normalizeEventType(runtimeEventType)
	if authoredEventType == "" || runtimeEventType == "" {
		return false
	}
	if active.AuthoredEventType == "" {
		return false
	}
	if authoredEventType == active.AuthoredEventType {
		return true
	}
	authoredCanonical := canonicalNodeEvent(source, node, authoredEventType)
	runtimeCanonical := canonicalNodeEvent(source, node, runtimeEventType)
	if authoredCanonical != "" && runtimeCanonical != "" && authoredCanonical == runtimeCanonical {
		return true
	}
	return active.CanonicalEventType != "" && authoredCanonical == active.CanonicalEventType
}

func canonicalNodeEvent(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) string {
	eventType = normalizeEventType(eventType)
	if eventType == "" {
		return ""
	}
	if source == nil {
		return eventType
	}
	canonical := normalizeEventType(source.ResolveExecutableNodeEventReference(node, eventType))
	if canonical == "" {
		return eventType
	}
	return canonical
}

func normalizeEventType(eventType string) string {
	return strings.Trim(strings.TrimSpace(eventType), "/")
}

func scopedIssue(binding Binding, code, location, message string) Issue {
	return Issue{
		Code:            code,
		Message:         message,
		Location:        location,
		FlowID:          strings.TrimSpace(binding.FlowID),
		SourceNode:      binding.SourceNode,
		SourceEventType: strings.TrimSpace(binding.SourceEventType),
		AccumulatorName: strings.TrimSpace(binding.AccumulatorName),
	}
}

type materializedFieldTarget struct {
	FlowID      string
	PackageKey  string
	EntityType  string
	FieldName   string
	FieldDecl   runtimecontracts.EntityFieldDecl
	TypeCatalog runtimecontracts.TypeCatalogDocument
}

func declaredMaterializedFields(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle) []materializedFieldTarget {
	out := make([]materializedFieldTarget, 0)
	add := func(packageKey, flowID, entityType string, contract runtimecontracts.EntityContract, types runtimecontracts.TypeCatalogDocument) {
		fieldNames := make([]string, 0, len(contract.Fields))
		for fieldName := range contract.Fields {
			fieldNames = append(fieldNames, strings.TrimSpace(fieldName))
		}
		sort.Strings(fieldNames)
		for _, fieldName := range fieldNames {
			decl := contract.Fields[fieldName]
			if strings.TrimSpace(decl.MaterializeFrom) == "" {
				continue
			}
			out = append(out, materializedFieldTarget{
				FlowID:      strings.TrimSpace(flowID),
				PackageKey:  strings.TrimSpace(packageKey),
				EntityType:  strings.TrimSpace(entityType),
				FieldName:   fieldName,
				FieldDecl:   decl,
				TypeCatalog: types,
			})
		}
	}
	if root := bundle.RootEntityContracts(); len(root) > 0 {
		for entityType, contract := range root {
			add(runtimeidentity.RootPackageKey, "", entityType, contract, bundle.RootTypeCatalog())
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		entityType, contract, ok := bundle.FlowPrimaryEntityContract(flowID)
		if !ok {
			continue
		}
		add(scope.PackageKey, flowID, entityType, contract, bundle.ResolvedTypeCatalogForFlow(flowID))
	}
	return out
}

func resolveTarget(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle, target materializedFieldTarget) (Binding, []Issue) {
	binding := Binding{
		FlowID:      target.FlowID,
		EntityType:  target.EntityType,
		TargetField: target.FieldName,
		TargetDecl:  target.FieldDecl,
		Project:     cloneProject(target.FieldDecl.Project),
	}
	loc := locationFor(target.FlowID, target.EntityType, target.FieldName)
	issues := make([]Issue, 0)
	sourceNodeID, accName, ok := parseMaterializeFrom(target.FieldDecl.MaterializeFrom)
	if !ok {
		issues = append(issues, Issue{Code: "invalid_reference", Location: loc, Message: fmt.Sprintf("materialize_from %q must have shape <node_id>.<accumulator_name>", strings.TrimSpace(target.FieldDecl.MaterializeFrom))})
		return binding, issues
	}
	node, identityErr := runtimeidentity.AdmitExecutableNodeDeclaration(target.PackageKey, target.FlowID, sourceNodeID)
	if identityErr != nil {
		issues = append(issues, Issue{Code: "invalid_source_node", Location: loc, Message: identityErr.Error()})
		return binding, issues
	}
	binding.SourceNode = node
	binding.AccumulatorName = accName

	record, ok := source.ExecutableNode(node)
	if !ok {
		issues = append(issues, scopedIssue(binding, "unknown_source_node", loc, fmt.Sprintf("materialize_from %q references unknown node %q", strings.TrimSpace(target.FieldDecl.MaterializeFrom), sourceNodeID)))
		return binding, issues
	}
	sourceFlowID := node.FlowID()
	if sourceFlowID != strings.TrimSpace(target.FlowID) {
		issues = append(issues, scopedIssue(binding, "cross_flow_reference", loc, fmt.Sprintf("materialize_from %q references node in flow %s, but target entity %s belongs to flow %s", strings.TrimSpace(target.FieldDecl.MaterializeFrom), flowLabel(sourceFlowID), target.EntityType, flowLabel(target.FlowID))))
	}

	handlers := handlersForAccumulator(record.Entry, accName)
	switch len(handlers) {
	case 0:
		issues = append(issues, scopedIssue(binding, "missing_accumulate_into", loc, fmt.Sprintf("materialize_from %q does not resolve to an explicitly declared accumulator (accumulate.into:) on node %s", strings.TrimSpace(target.FieldDecl.MaterializeFrom), sourceNodeID)))
	case 1:
		binding.SourceEventType = handlers[0].EventType
	default:
		names := make([]string, 0, len(handlers))
		for _, handler := range handlers {
			names = append(names, handler.EventType)
		}
		sort.Strings(names)
		issues = append(issues, scopedIssue(binding, "duplicate_accumulate_into", loc, fmt.Sprintf("accumulate.into %q declared by multiple handlers on node %s: %v; v1 supports a single producing handler per projectable buffer", accName, sourceNodeID, names)))
	}
	if strings.TrimSpace(target.FieldDecl.UnusedReason) != "" {
		issues = append(issues, scopedIssue(binding, "unused_reason_with_materialize_from", loc, fmt.Sprintf("field %q declares both materialize_from and _unused_reason; remove _unused_reason", target.FieldName)))
	}

	field, ok := nodeStateField(record.Entry, accName)
	if !ok {
		issues = append(issues, scopedIssue(binding, "unknown_accumulator_state", loc, fmt.Sprintf("materialize_from %q references state_schema field %q missing on node %s", strings.TrimSpace(target.FieldDecl.MaterializeFrom), accName, sourceNodeID)))
	} else {
		binding.SourceField = field
		sourceItem, ok := namedListItemType(field.Type)
		if !ok {
			issues = append(issues, scopedIssue(binding, "unsupported_source_type", loc, fmt.Sprintf("materialize_from source %q is not a list of a named type; found %q", strings.TrimSpace(target.FieldDecl.MaterializeFrom), strings.TrimSpace(field.Type))))
		} else if _, declared := target.TypeCatalog.Types[sourceItem]; !declared {
			issues = append(issues, scopedIssue(binding, "undeclared_source_named_type", loc, fmt.Sprintf("materialize_from source %q references undeclared named type %s", strings.TrimSpace(target.FieldDecl.MaterializeFrom), sourceItem)))
		} else {
			resolved, err := (runtimecontracts.CatalogTypeReference{Type: sourceItem, Catalog: target.TypeCatalog}).Resolve()
			if err != nil {
				issues = append(issues, scopedIssue(binding, "invalid_source_named_type", loc, err.Error()))
			} else {
				binding.SourceItemType = sourceItem
				binding.SourceType = resolved
			}
		}
	}

	targetItem, ok := namedListItemType(target.FieldDecl.Type)
	if !ok {
		issues = append(issues, scopedIssue(binding, "unsupported_target_type", loc, fmt.Sprintf("materialize_from target %q must be declared list<NamedType>; found %q", target.FieldName, strings.TrimSpace(target.FieldDecl.Type))))
	} else if _, declared := target.TypeCatalog.Types[targetItem]; !declared {
		issues = append(issues, scopedIssue(binding, "undeclared_target_named_type", loc, fmt.Sprintf("materialize_from target %q references undeclared named type %s", target.FieldName, targetItem)))
	} else {
		resolved, err := (runtimecontracts.CatalogTypeReference{Type: targetItem, Catalog: target.TypeCatalog}).Resolve()
		if err != nil {
			issues = append(issues, scopedIssue(binding, "invalid_target_named_type", loc, err.Error()))
		} else {
			binding.TargetItemType = targetItem
			binding.TargetType = resolved
		}
	}

	if binding.SourceItemType != "" && binding.TargetItemType != "" {
		if binding.SourceItemType == binding.TargetItemType && len(binding.Project) > 0 {
			issues = append(issues, scopedIssue(binding, "project_forbidden", loc, fmt.Sprintf("materialize_from element types match (%s); project must be absent", binding.SourceItemType)))
		}
		if binding.SourceItemType != binding.TargetItemType && len(binding.Project) == 0 {
			issues = append(issues, scopedIssue(binding, "project_required", loc, "materialize_from element types differ; project must be present and name all target fields"))
		}
		if len(binding.Project) > 0 {
			issues = append(issues, validateProject(source, target, binding)...)
		}
		if binding.SourceEventType != "" && binding.SourceItemType != "" {
			issues = append(issues, validateEventTypedView(source, target.TypeCatalog, binding)...)
		}
	}
	return binding, issues
}

type handlerRef struct {
	EventType string
}

func handlersForAccumulator(node runtimecontracts.SystemNodeContract, accName string) []handlerRef {
	out := make([]handlerRef, 0)
	for eventType, handler := range node.EventHandlers {
		if handler.Accumulate == nil {
			continue
		}
		if strings.TrimSpace(handler.Accumulate.Into) == strings.TrimSpace(accName) {
			out = append(out, handlerRef{EventType: strings.TrimSpace(eventType)})
		}
	}
	return out
}

func validateProject(source semanticview.Source, target materializedFieldTarget, binding Binding) []Issue {
	loc := locationFor(target.FlowID, target.EntityType, target.FieldName)
	issues := make([]Issue, 0)
	targetFields := sortedTypeFields(binding.TargetType)
	for _, fieldName := range targetFields {
		field, _ := binding.TargetType.Field(fieldName)
		if _, ok := binding.Project[fieldName]; !ok && !field.IsOptional {
			issues = append(issues, scopedIssue(binding, "project_missing_target_field", loc, fmt.Sprintf("project must name every field of target item type %s; missing %s", binding.TargetItemType, fieldName)))
		}
	}
	for fieldName, rawExpr := range binding.Project {
		fieldName = strings.TrimSpace(fieldName)
		if _, ok := binding.TargetType.Field(fieldName); !ok {
			issues = append(issues, scopedIssue(binding, "project_unknown_target_field", loc, fmt.Sprintf("project names undeclared target field %q on item type %s", fieldName, binding.TargetItemType)))
		}
		expr, isString := rawExpr.(string)
		if !isString {
			continue
		}
		expr = strings.TrimSpace(expr)
		if sourceField, ok := strings.CutPrefix(expr, "source."); ok {
			sourceField = strings.TrimSpace(sourceField)
			if _, reserved := ReservedAccumulatorMetadata[sourceField]; reserved {
				issues = append(issues, scopedIssue(binding, "project_metadata_reference", loc, fmt.Sprintf("project.%s references %q; reserved accumulator metadata is not addressable through source.*", fieldName, expr)))
				continue
			}
			sourceSpec, ok := binding.SourceType.Field(sourceField)
			if !ok {
				issues = append(issues, scopedIssue(binding, "project_unknown_source_field", loc, fmt.Sprintf("project.%s references %q; %s is not a field of item type %s", fieldName, expr, sourceField, binding.SourceItemType)))
				continue
			}
			if targetSpec, ok := binding.TargetType.Field(fieldName); ok {
				if !typesAssignable(target.TypeCatalog, sourceSpec.TypeRef, targetSpec.TypeRef) {
					issues = append(issues, scopedIssue(binding, "project_type_mismatch", loc, fmt.Sprintf("project.%s references %q type %s, not assignable to target field type %s", fieldName, expr, strings.TrimSpace(sourceSpec.TypeRef), strings.TrimSpace(targetSpec.TypeRef))))
				} else if sourceSpec.IsOptional && !targetSpec.IsOptional {
					issues = append(issues, scopedIssue(binding, "project_presence_mismatch", loc, fmt.Sprintf("project.%s references optional source field %q but target field is required", fieldName, expr)))
				}
			}
			continue
		}
		if policyPath, ok := strings.CutPrefix(expr, "policy."); ok {
			policyPath = strings.TrimSpace(policyPath)
			if _, ok := semanticview.PolicyValueForFlow(source, target.FlowID, policyPath); !ok {
				issues = append(issues, scopedIssue(binding, "project_unknown_policy_field", loc, fmt.Sprintf("project.%s references %q; policy field %s is not declared for flow %s", fieldName, expr, policyPath, flowLabel(target.FlowID))))
			}
			continue
		}
		for _, forbidden := range []string{"entity.", "payload.", "event.", "fan_out.", "accumulated."} {
			if strings.HasPrefix(expr, forbidden) {
				issues = append(issues, scopedIssue(binding, "project_forbidden_binding", loc, fmt.Sprintf("project.%s uses forbidden binding %q; project supports source.*, policy.*, or literals", fieldName, expr)))
				break
			}
		}
	}
	return issues
}

func validateEventTypedView(source semanticview.Source, types runtimecontracts.TypeCatalogDocument, binding Binding) []Issue {
	entry, ok := source.EventEntry(binding.SourceEventType)
	if !ok {
		canonical := source.ResolveExecutableNodeEventReference(binding.SourceNode, binding.SourceEventType)
		if canonical != "" {
			entry, ok = source.EventEntry(canonical)
		}
	}
	if !ok {
		return []Issue{scopedIssue(binding, "unknown_source_event", binding.SourceNode.Key(), fmt.Sprintf("accumulate.into %q references event %q, but no event catalog entry exists", binding.AccumulatorName, binding.SourceEventType))}
	}
	issues := make([]Issue, 0)
	loc := fmt.Sprintf("node %s handler %s", binding.SourceNode.Key(), binding.SourceEventType)
	for _, fieldName := range sortedTypeFields(binding.SourceType) {
		sourceField, _ := binding.SourceType.Field(fieldName)
		expected := strings.TrimSpace(sourceField.TypeRef)
		payloadField, ok := entry.Payload.Properties[fieldName]
		if !ok {
			if !sourceField.IsOptional {
				issues = append(issues, scopedIssue(binding, "typed_view_missing_field", loc, fmt.Sprintf("event %q payload missing field %q required by accumulator element type %s", binding.SourceEventType, fieldName, binding.SourceItemType)))
			}
			continue
		}
		if !typesAssignable(types, payloadField.Type, expected) {
			issues = append(issues, scopedIssue(binding, "typed_view_type_mismatch", loc, fmt.Sprintf("event %q payload field %q has type %s not assignable to accumulator element type field %q type %s", binding.SourceEventType, fieldName, strings.TrimSpace(payloadField.Type), fieldName, expected)))
		} else if !sourceField.IsOptional && !containsString(entry.Payload.Required, fieldName) {
			issues = append(issues, scopedIssue(binding, "typed_view_presence_mismatch", loc, fmt.Sprintf("event %q payload field %q is optional but accumulator element type %s requires it", binding.SourceEventType, fieldName, binding.SourceItemType)))
		}
	}
	return issues
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == strings.TrimSpace(target) {
			return true
		}
	}
	return false
}

func nodeStateField(node runtimecontracts.SystemNodeContract, name string) (runtimecontracts.NodeStateField, bool) {
	name = strings.TrimSpace(name)
	for _, field := range node.StateSchema.Fields {
		if strings.TrimSpace(field.Name) == name {
			return field, true
		}
	}
	return runtimecontracts.NodeStateField{}, false
}

func parseMaterializeFrom(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	idx := strings.LastIndex(raw, ".")
	if idx <= 0 || idx >= len(raw)-1 {
		return "", "", false
	}
	nodeID := strings.TrimSpace(raw[:idx])
	accName := strings.TrimSpace(raw[idx+1:])
	return nodeID, accName, nodeID != "" && accName != ""
}

func namedListItemType(typeRef string) (string, bool) {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">"):
		item := strings.TrimSpace(typeRef[len("list<") : len(typeRef)-1])
		return item, item != "" && isNamedTypeName(item)
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		item := strings.TrimSpace(typeRef[1 : len(typeRef)-1])
		return item, item != "" && isNamedTypeName(item)
	case strings.HasSuffix(typeRef, "[]"):
		item := strings.TrimSpace(typeRef[:len(typeRef)-2])
		return item, item != "" && isNamedTypeName(item)
	case strings.HasPrefix(typeRef, "[]"):
		item := strings.TrimSpace(typeRef[2:])
		return item, item != "" && isNamedTypeName(item)
	default:
		return "", false
	}
}

func isNamedTypeName(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	first := raw[0]
	if first < 'A' || first > 'Z' {
		return false
	}
	for _, ch := range raw {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func sortedTypeFields(resolved runtimecontracts.ResolvedCatalogType) []string {
	out := make([]string, 0, len(resolved.Fields))
	for _, field := range resolved.Fields {
		out = append(out, field.Name)
	}
	return out
}

func typesAssignable(types runtimecontracts.TypeCatalogDocument, actual, expected string) bool {
	actual = normalizeType(types, actual)
	expected = normalizeType(types, expected)
	if actual == expected {
		return true
	}
	if actual == "integer" && expected == "numeric" {
		return true
	}
	return false
}

func normalizeType(types runtimecontracts.TypeCatalogDocument, raw string) string {
	raw = strings.TrimSpace(raw)
	if scalar, ok := types.Scalars[raw]; ok {
		return normalizeType(types, scalar.Base)
	}
	switch strings.ToLower(raw) {
	case "string":
		return "text"
	case "int", "bigint":
		return "integer"
	case "number", "float", "double", "real":
		return "numeric"
	case "bool":
		return "boolean"
	default:
		return raw
	}
}

func cloneProject(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[strings.TrimSpace(key)] = value
	}
	return out
}

func locationFor(flowID, entityType, fieldName string) string {
	return fmt.Sprintf("flow %s entity_type %s field %s", flowLabel(flowID), strings.TrimSpace(entityType), strings.TrimSpace(fieldName))
}

func flowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "<root>"
	}
	return flowID
}
