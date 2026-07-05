package contracts

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func lowerPolicySheetRuleNode(node *yaml.Node, rule *HandlerRuleEntry) error {
	if node == nil || node.Kind != yaml.MappingNode || rule == nil {
		return nil
	}
	rowNodes := map[string]*yaml.Node{}
	for _, key := range []string{"when", "case", "range", "else", "default"} {
		if value := yamlMappingValueNode(node, key); value != nil {
			rowNodes[key] = value
		}
	}
	if len(rowNodes) == 0 {
		rule.Condition = strings.TrimSpace(rule.Condition)
		return nil
	}
	if len(rowNodes) > 1 {
		return fmt.Errorf("POLICY-SHEET-ROW: rule %q declares multiple row types", strings.TrimSpace(rule.ID))
	}
	if strings.TrimSpace(rule.Condition) != "" {
		return fmt.Errorf("POLICY-SHEET-ROW: rule %q declares both condition and typed policy-sheet row syntax", strings.TrimSpace(rule.ID))
	}
	switch {
	case rowNodes["when"] != nil:
		condition, err := decodePolicySheetScalarString(rowNodes["when"], "when")
		if err != nil {
			return err
		}
		if condition == "" {
			return fmt.Errorf("POLICY-SHEET-ROW: when row %q requires a non-empty condition", strings.TrimSpace(rule.ID))
		}
		rule.Condition = condition
		rule.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindWhen}
	case rowNodes["case"] != nil:
		condition, metadata, err := decodePolicySheetCaseRow(rowNodes["case"])
		if err != nil {
			return err
		}
		rule.Condition = condition
		rule.PolicyRow = metadata
	case rowNodes["range"] != nil:
		condition, metadata, err := decodePolicySheetRangeRow(rowNodes["range"])
		if err != nil {
			return err
		}
		rule.Condition = condition
		rule.PolicyRow = metadata
	case rowNodes["else"] != nil:
		if err := requirePolicySheetBoolTrue(rowNodes["else"], "else"); err != nil {
			return err
		}
		rule.Condition = "else"
		rule.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}
	case rowNodes["default"] != nil:
		if err := requirePolicySheetBoolTrue(rowNodes["default"], "default"); err != nil {
			return err
		}
		rule.Condition = "else"
		rule.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}
	}
	return nil
}

func validatePolicySheetRows(rules []HandlerRuleEntry, context handlerRuleDecodeContext) error {
	hasPolicyRows := false
	for _, rule := range rules {
		if rule.PolicyRow.Kind != "" {
			hasPolicyRows = true
			break
		}
	}
	if !hasPolicyRows {
		return nil
	}
	if context != handlerRuleDecodeContextRules {
		return fmt.Errorf("POLICY-SHEET-ROW: typed policy-sheet rows are only supported under handler.rules")
	}
	seenIDs := map[string]int{}
	hasDefault := false
	caseKeys := map[string]int{}
	rangesByValue := map[string][]policySheetRangeForValidation{}
	for idx, rule := range rules {
		id := strings.TrimSpace(rule.ID)
		if id == "" {
			return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] requires stable id when policy-sheet rows are present", idx)
		}
		if prev, ok := seenIDs[id]; ok {
			return fmt.Errorf("POLICY-SHEET-ROW: duplicate stable row id %q at rules[%d] and rules[%d]", id, prev, idx)
		}
		seenIDs[id] = idx
		if strings.EqualFold(strings.TrimSpace(rule.Condition), "else") {
			hasDefault = true
		}
		switch rule.PolicyRow.Kind {
		case PolicySheetRowKindCase:
			key := strings.Join(rule.PolicyRow.Selectors, "\x00") + "\x01" + strings.Join(rule.PolicyRow.CaseValues, "\x00")
			if prev, ok := caseKeys[key]; ok {
				return fmt.Errorf("POLICY-SHEET-ROW: duplicate case key at rules[%d] and rules[%d]", prev, idx)
			}
			caseKeys[key] = idx
		case PolicySheetRowKindRange:
			metadata := rule.PolicyRow
			rangesByValue[metadata.RangeValue] = append(rangesByValue[metadata.RangeValue], policySheetRangeForValidation{
				Index:        idx,
				Lower:        metadata.RangeLower,
				Upper:        metadata.RangeUpper,
				Monotonicity: metadata.Monotonicity,
			})
		}
	}
	if !hasDefault {
		return fmt.Errorf("POLICY-SHEET-ROW: rules with typed policy-sheet rows require an else/default row")
	}
	for value, ranges := range rangesByValue {
		if err := validatePolicySheetRanges(value, ranges); err != nil {
			return err
		}
	}
	return nil
}

type policySheetRangeForValidation struct {
	Index        int
	Lower        PolicySheetRangeBound
	Upper        PolicySheetRangeBound
	Monotonicity []string
}

func decodePolicySheetCaseRow(node *yaml.Node) (string, PolicySheetRowMetadata, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case row must be a mapping")
	}
	if err := validatePolicySheetMappingKeys(node, "case", map[string]struct{}{"selector": {}, "selectors": {}, "equals": {}}); err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	selectors, err := decodePolicySheetSelectors(node)
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	equalsNode := yamlMappingValueNode(node, "equals")
	if equalsNode == nil {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case row requires equals")
	}
	values, literals, err := decodePolicySheetEquals(equalsNode)
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if len(values) != len(selectors) {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case row selector count %d does not match equals count %d", len(selectors), len(values))
	}
	parts := make([]string, 0, len(selectors))
	for i := range selectors {
		parts = append(parts, selectors[i]+" == "+literals[i])
	}
	return strings.Join(parts, " && "), PolicySheetRowMetadata{
		Kind:       PolicySheetRowKindCase,
		Selectors:  selectors,
		CaseValues: values,
	}, nil
}

func decodePolicySheetRangeRow(node *yaml.Node) (string, PolicySheetRowMetadata, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range row must be a mapping")
	}
	if err := validatePolicySheetMappingKeys(node, "range", map[string]struct{}{"value": {}, "gt": {}, "gte": {}, "lt": {}, "lte": {}, "monotonicity": {}}); err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	value, err := decodePolicySheetScalarString(yamlMappingValueNode(node, "value"), "range.value")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if err := validatePolicySheetSelector(value, "range.value"); err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	lower, err := decodePolicySheetRangeBound(node, "gt", ">")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	lowerEq, err := decodePolicySheetRangeBound(node, "gte", ">=")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	upper, err := decodePolicySheetRangeBound(node, "lt", "<")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	upperEq, err := decodePolicySheetRangeBound(node, "lte", "<=")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if lower.Value != "" && lowerEq.Value != "" {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range row declares both gt and gte")
	}
	if upper.Value != "" && upperEq.Value != "" {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range row declares both lt and lte")
	}
	if lower.Value == "" {
		lower = lowerEq
	}
	if upper.Value == "" {
		upper = upperEq
	}
	if lower.Value == "" && upper.Value == "" {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range row requires at least one bound")
	}
	monotonicity, err := decodePolicySheetStringList(yamlMappingValueNode(node, "monotonicity"), "range.monotonicity")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	parts := make([]string, 0, 2)
	if lower.Value != "" {
		parts = append(parts, value+" "+lower.Operator+" "+lower.Value)
	}
	if upper.Value != "" {
		parts = append(parts, value+" "+upper.Operator+" "+upper.Value)
	}
	return strings.Join(parts, " && "), PolicySheetRowMetadata{
		Kind:         PolicySheetRowKindRange,
		RangeValue:   value,
		RangeLower:   lower,
		RangeUpper:   upper,
		Monotonicity: monotonicity,
	}, nil
}

func decodePolicySheetSelectors(node *yaml.Node) ([]string, error) {
	selectorNode := yamlMappingValueNode(node, "selector")
	selectorsNode := yamlMappingValueNode(node, "selectors")
	if selectorNode != nil && selectorsNode != nil {
		return nil, fmt.Errorf("POLICY-SHEET-ROW: case row declares both selector and selectors")
	}
	var selectors []string
	var err error
	switch {
	case selectorNode != nil:
		var selector string
		selector, err = decodePolicySheetScalarString(selectorNode, "case.selector")
		selectors = []string{selector}
	case selectorsNode != nil:
		selectors, err = decodePolicySheetStringList(selectorsNode, "case.selectors")
	default:
		err = fmt.Errorf("POLICY-SHEET-ROW: case row requires selector or selectors")
	}
	if err != nil {
		return nil, err
	}
	if len(selectors) == 0 {
		return nil, fmt.Errorf("POLICY-SHEET-ROW: case row requires at least one selector")
	}
	for _, selector := range selectors {
		if err := validatePolicySheetSelector(selector, "case.selector"); err != nil {
			return nil, err
		}
	}
	return selectors, nil
}

func decodePolicySheetEquals(node *yaml.Node) ([]string, []string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil, fmt.Errorf("POLICY-SHEET-ROW: case.equals is required")
	}
	if node.Kind == yaml.SequenceNode {
		values := make([]string, 0, len(node.Content))
		literals := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			value, literal, err := decodePolicySheetLiteral(item, "case.equals")
			if err != nil {
				return nil, nil, err
			}
			values = append(values, value)
			literals = append(literals, literal)
		}
		return values, literals, nil
	}
	value, literal, err := decodePolicySheetLiteral(node, "case.equals")
	if err != nil {
		return nil, nil, err
	}
	return []string{value}, []string{literal}, nil
}

func decodePolicySheetLiteral(node *yaml.Node, label string) (string, string, error) {
	if node == nil || node.Kind != yaml.ScalarNode {
		return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s must be a scalar or scalar list", label)
	}
	raw := strings.TrimSpace(node.Value)
	switch strings.TrimSpace(node.Tag) {
	case "!!int", "!!float":
		if _, err := strconv.ParseFloat(raw, 64); err != nil {
			return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s numeric literal %q is invalid", label, raw)
		}
		return raw, raw, nil
	case "!!bool":
		if raw != "true" && raw != "false" {
			return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s bool literal %q is invalid", label, raw)
		}
		return raw, raw, nil
	case "!!str", "":
		return raw, strconv.Quote(raw), nil
	default:
		return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s uses unsupported literal tag %s", label, node.Tag)
	}
}

func decodePolicySheetRangeBound(node *yaml.Node, key, operator string) (PolicySheetRangeBound, error) {
	valueNode := yamlMappingValueNode(node, key)
	if valueNode == nil {
		return PolicySheetRangeBound{}, nil
	}
	if valueNode.Kind != yaml.ScalarNode {
		return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s must be a scalar bound", key)
	}
	raw := strings.TrimSpace(valueNode.Value)
	if raw == "" {
		return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s must be non-empty", key)
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return PolicySheetRangeBound{Operator: operator, Value: raw, Kind: "literal"}, nil
	}
	if isPolicySheetPolicyConstantExpression(raw) {
		return PolicySheetRangeBound{Operator: operator, Value: raw, Kind: "policy"}, nil
	}
	if policySheetRoot(raw) != "" {
		return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s dynamic bound %q is forbidden; normalize with compute/value rows first", key, raw)
	}
	return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s bound %q must be a numeric literal or policy constant", key, raw)
}

func validatePolicySheetRanges(value string, ranges []policySheetRangeForValidation) error {
	graph := newPolicySheetMonotonicityGraph()
	for _, row := range ranges {
		for _, constraint := range row.Monotonicity {
			if err := graph.addConstraint(constraint); err != nil {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range.monotonicity: %w", row.Index, err)
			}
		}
	}
	for _, row := range ranges {
		if row.Lower.Kind == "policy" || row.Upper.Kind == "policy" {
			if len(row.Monotonicity) == 0 {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range with policy-constant bounds requires monotonicity", row.Index)
			}
		}
		if row.Lower.Value != "" && row.Upper.Value != "" {
			if err := graph.proveOrdered(row.Lower.Value, row.Upper.Value); err != nil {
				return fmt.Errorf("POLICY-SHEET-ROW: rules[%d] range lower/upper bounds for %s: %w", row.Index, value, err)
			}
		}
	}
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			if err := validatePolicySheetRangePairDoesNotOverlap(value, graph, ranges[i], ranges[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validatePolicySheetRangePairDoesNotOverlap(value string, graph *policySheetMonotonicityGraph, a, b policySheetRangeForValidation) error {
	if aNumeric := policySheetRangeUsesOnlyLiteralBounds(a); aNumeric && policySheetRangeUsesOnlyLiteralBounds(b) {
		if policySheetNumericRangesOverlap(a, b) {
			return fmt.Errorf("POLICY-SHEET-ROW: overlapping literal ranges for %s at rules[%d] and rules[%d]", value, a.Index, b.Index)
		}
		return nil
	}
	if policySheetRangesAreStructurallyDisjoint(graph, a, b) {
		return nil
	}
	return fmt.Errorf("POLICY-SHEET-ROW: overlapping ranges for %s at rules[%d] and rules[%d]", value, a.Index, b.Index)
}

func policySheetRangeUsesOnlyLiteralBounds(row policySheetRangeForValidation) bool {
	if row.Lower.Value != "" && row.Lower.Kind != "literal" {
		return false
	}
	if row.Upper.Value != "" && row.Upper.Kind != "literal" {
		return false
	}
	return true
}

func policySheetRangesAreStructurallyDisjoint(graph *policySheetMonotonicityGraph, a, b policySheetRangeForValidation) bool {
	return policySheetUpperBeforeLower(graph, a.Upper, b.Lower) || policySheetUpperBeforeLower(graph, b.Upper, a.Lower)
}

func policySheetUpperBeforeLower(graph *policySheetMonotonicityGraph, upper, lower PolicySheetRangeBound) bool {
	if upper.Value == "" || lower.Value == "" {
		return false
	}
	if err := graph.proveOrdered(upper.Value, lower.Value); err != nil {
		return false
	}
	return upper.Operator == "<" || lower.Operator == ">"
}

func policySheetNumericRangesOverlap(a, b policySheetRangeForValidation) bool {
	aMin, aMax, aMinClosed, aMaxClosed, okA := policySheetNumericInterval(a)
	bMin, bMax, bMinClosed, bMaxClosed, okB := policySheetNumericInterval(b)
	if !okA || !okB {
		return false
	}
	if aMax < bMin || bMax < aMin {
		return false
	}
	if aMax == bMin {
		return aMaxClosed && bMinClosed
	}
	if bMax == aMin {
		return bMaxClosed && aMinClosed
	}
	return true
}

func policySheetNumericInterval(row policySheetRangeForValidation) (float64, float64, bool, bool, bool) {
	min := math.Inf(-1)
	max := math.Inf(1)
	minClosed := false
	maxClosed := false
	if row.Lower.Value != "" {
		if row.Lower.Kind != "literal" {
			return 0, 0, false, false, false
		}
		parsed, err := strconv.ParseFloat(row.Lower.Value, 64)
		if err != nil {
			return 0, 0, false, false, false
		}
		min = parsed
		minClosed = row.Lower.Operator == ">="
	}
	if row.Upper.Value != "" {
		if row.Upper.Kind != "literal" {
			return 0, 0, false, false, false
		}
		parsed, err := strconv.ParseFloat(row.Upper.Value, 64)
		if err != nil {
			return 0, 0, false, false, false
		}
		max = parsed
		maxClosed = row.Upper.Operator == "<="
	}
	return min, max, minClosed, maxClosed, true
}

type policySheetMonotonicityGraph struct {
	edges map[string]map[string]struct{}
}

func newPolicySheetMonotonicityGraph() *policySheetMonotonicityGraph {
	return &policySheetMonotonicityGraph{edges: map[string]map[string]struct{}{}}
}

func (g *policySheetMonotonicityGraph) addConstraint(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("constraint must be non-empty")
	}
	parts := strings.Split(raw, "<=")
	if len(parts) < 2 {
		return fmt.Errorf("constraint %q must use <= monotonicity", raw)
	}
	for i := range parts {
		parts[i] = canonicalPolicySheetTerm(parts[i])
		if err := validatePolicySheetMonotonicityTerm(parts[i]); err != nil {
			return err
		}
	}
	for i := 0; i+1 < len(parts); i++ {
		from, to := parts[i], parts[i+1]
		if g.edges[from] == nil {
			g.edges[from] = map[string]struct{}{}
		}
		g.edges[from][to] = struct{}{}
	}
	return nil
}

func (g *policySheetMonotonicityGraph) proveOrdered(lower, upper string) error {
	lower = canonicalPolicySheetTerm(lower)
	upper = canonicalPolicySheetTerm(upper)
	if lower == "" || upper == "" || lower == upper {
		return nil
	}
	if l, errL := strconv.ParseFloat(lower, 64); errL == nil {
		if u, errU := strconv.ParseFloat(upper, 64); errU == nil {
			if l <= u {
				return nil
			}
			return fmt.Errorf("%s is greater than %s", lower, upper)
		}
	}
	seen := map[string]struct{}{lower: {}}
	queue := []string{lower}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for next := range g.edges[current] {
			if next == upper {
				return nil
			}
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return fmt.Errorf("monotonicity does not prove %s <= %s", lower, upper)
}

func validatePolicySheetMonotonicityTerm(term string) error {
	term = canonicalPolicySheetTerm(term)
	if term == "" {
		return fmt.Errorf("monotonicity term must be non-empty")
	}
	if _, err := strconv.ParseFloat(term, 64); err == nil {
		return nil
	}
	if isPolicySheetPolicyConstantExpression(term) {
		return nil
	}
	return fmt.Errorf("POLICY-SHEET-ROW: range.monotonicity term %q must be a numeric literal or policy-constant expression", term)
}

func validatePolicySheetSelector(selector, label string) error {
	return validatePolicySheetPath(selector, label, []string{"payload", "entity", "policy", "event"})
}

func validatePolicySheetPath(expr, label string, allowedRoots []string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return fmt.Errorf("POLICY-SHEET-ROW: %s must be non-empty", label)
	}
	root := policySheetRoot(expr)
	if root == "" {
		return fmt.Errorf("POLICY-SHEET-ROW: %s %q must be a dotted path rooted in %s", label, expr, strings.Join(allowedRoots, ", "))
	}
	for _, allowed := range allowedRoots {
		if root == allowed {
			parts := strings.Split(expr, ".")
			for idx, part := range parts {
				if !isPolicySheetPathSegment(part) {
					return fmt.Errorf("POLICY-SHEET-ROW: %s %q must be a simple dotted path", label, expr)
				}
				if idx == 0 && part != allowed {
					return fmt.Errorf("POLICY-SHEET-ROW: %s %q uses unsupported root %q", label, expr, root)
				}
			}
			return nil
		}
	}
	return fmt.Errorf("POLICY-SHEET-ROW: %s %q uses unsupported root %q", label, expr, root)
}

func isPolicySheetPathSegment(segment string) bool {
	if segment == "" {
		return false
	}
	for idx, r := range segment {
		switch {
		case r == '_':
			continue
		case r >= 'a' && r <= 'z':
			continue
		case r >= 'A' && r <= 'Z':
			continue
		case idx > 0 && r >= '0' && r <= '9':
			continue
		default:
			return false
		}
	}
	return true
}

func policySheetRoot(expr string) string {
	expr = strings.TrimSpace(expr)
	if idx := strings.Index(expr, "."); idx > 0 {
		return expr[:idx]
	}
	return ""
}

func canonicalPolicySheetTerm(term string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(term)), "")
}

func isPolicySheetPolicyConstantExpression(expr string) bool {
	expr = canonicalPolicySheetTerm(expr)
	if expr == "" {
		return false
	}
	hasPolicy := false
	tokens := strings.FieldsFunc(expr, func(r rune) bool {
		switch r {
		case '+', '-', '*', '/', '(', ')':
			return true
		default:
			return false
		}
	})
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if _, err := strconv.ParseFloat(token, 64); err == nil {
			continue
		}
		if strings.HasPrefix(token, "policy.") {
			if err := validatePolicySheetPath(token, "policy-constant bound", []string{"policy"}); err != nil {
				return false
			}
			hasPolicy = true
			continue
		}
		return false
	}
	for _, r := range expr {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '+' || r == '-' || r == '*' || r == '/' || r == '(' || r == ')' {
			continue
		}
		return false
	}
	return hasPolicy
}

func requirePolicySheetBoolTrue(node *yaml.Node, label string) error {
	if node == nil || node.Kind == 0 {
		return fmt.Errorf("POLICY-SHEET-ROW: %s is required", label)
	}
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("POLICY-SHEET-ROW: %s must be true", label)
	}
	value := strings.TrimSpace(node.Value)
	if value != "true" {
		return fmt.Errorf("POLICY-SHEET-ROW: %s must be true", label)
	}
	return nil
}

func decodePolicySheetScalarString(node *yaml.Node, label string) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", fmt.Errorf("POLICY-SHEET-ROW: %s is required", label)
	}
	if node.Kind != yaml.ScalarNode {
		return "", fmt.Errorf("POLICY-SHEET-ROW: %s must be a scalar string", label)
	}
	return strings.TrimSpace(node.Value), nil
}

func decodePolicySheetStringList(node *yaml.Node, label string) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value == "" {
			return nil, nil
		}
		return []string{value}, nil
	case yaml.SequenceNode:
		out := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			value, err := decodePolicySheetScalarString(item, label)
			if err != nil {
				return nil, err
			}
			if value != "" {
				out = append(out, value)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("POLICY-SHEET-ROW: %s must be a scalar string or string list", label)
	}
}

func validatePolicySheetMappingKeys(node *yaml.Node, label string, allowed map[string]struct{}) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: %s field %q not in platform spec", label, key)
		}
	}
	return nil
}

func yamlMappingValueNode(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return node.Content[i+1]
		}
	}
	return nil
}
